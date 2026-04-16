"""Symptomizer batch dispatch: tagging, refinement, review preparation

Extracted from app.py. Uses _app() for cross-module access.
"""
from __future__ import annotations
import json, math, re, sys, time
from typing import Any, Dict, List, Optional, Sequence
import pandas as pd
import streamlit as st

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}
AI_DEL_HEADERS = [f"AI Symptom Delighter {i}" for i in range(1,11)]
AI_META_HEADERS = ["AI Safety","AI Reliability","AI # of Sessions"]
AI_DET_HEADERS = [f"AI Symptom Detractor {i}" for i in range(1,11)]

# ── Constants ──
RELIABILITY_ENUM = ["Not Mentioned","Negative","Neutral","Positive"]
SAFETY_ENUM = ["Not Mentioned","Concern","Positive"]
SESSIONS_ENUM = ["0","1","2–3","4–9","10+","Unknown"]

def _app():
    """Lazy access to app module namespace."""
    return sys.modules.get('__main__', sys.modules.get('app'))

def _esc(text):
    return html.escape(str(text or "")) if 'html' in dir() else str(text or "")

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _normalize_tag_list(tags):
    if not tags: return []
    seen = set()
    out = []
    for t in tags:
        s = str(t).strip()
        if not s or s.upper() in NON_VALUES: continue
        key = s.lower()
        if key not in seen: seen.add(key); out.append(s)
    return out


def _call_symptomizer_batch(*, client, items, allowed_delighters, allowed_detractors,
                             product_profile="", product_knowledge=None, max_ev_chars=120, aliases=None, include_universal_neutral=True):
    """Delegates to the canonical version in app.py.
    
    This module previously contained a full duplicate of the symptomizer batch
    function with its own prompt and parsing. That duplicate lacked v3 improvements
    (fuzzy evidence, confidence scoring, negation awareness, aspect dedup).
    Now delegates to ensure all improvements are used regardless of entry point.
    """
    app = _app()
    if app and hasattr(app, '_call_symptomizer_batch'):
        return app._call_symptomizer_batch(
            client=client, items=items, allowed_delighters=allowed_delighters,
            allowed_detractors=allowed_detractors, product_profile=product_profile,
            product_knowledge=product_knowledge, max_ev_chars=max_ev_chars,
            aliases=aliases, include_universal_neutral=include_universal_neutral,
        )
    return {}


def _ensure_ai_cols(df):
    for h in AI_DET_HEADERS + AI_DEL_HEADERS + AI_META_HEADERS:
        if h not in df.columns:
            df[h] = None
    return df



def _upsert_processed_symptom_record(processed_rows, row_id, dets, dels, *, row_meta=None, ev_det=None, ev_del=None):
    rid = str(row_id).strip()
    updated = []
    found = False
    for rec in processed_rows or []:
        if str(rec.get("idx", "")).strip() == rid:
            rec2 = dict(rec)
            rec2["wrote_dets"] = _normalize_tag_list(dets or [])[:10]
            rec2["wrote_dels"] = _normalize_tag_list(dels or [])[:10]
            rec2["ev_det"] = {k: list(v or [])[:2] for k, v in (ev_det or {}).items() if k in rec2["wrote_dets"]}
            rec2["ev_del"] = {k: list(v or [])[:2] for k, v in (ev_del or {}).items() if k in rec2["wrote_dels"]}
            if row_meta is not None:
                rec2["safety"] = row_meta.get("AI Safety", rec2.get("safety", "Not Mentioned"))
                rec2["reliability"] = row_meta.get("AI Reliability", rec2.get("reliability", "Not Mentioned"))
                rec2["sessions"] = row_meta.get("AI # of Sessions", rec2.get("sessions", "Unknown"))
            found = True
            updated.append(rec2)
        else:
            updated.append(rec)
    if not found:
        row_meta = row_meta or {}
        updated.append(dict(
            idx=int(rid),
            wrote_dets=_normalize_tag_list(dets or [])[:10],
            wrote_dels=_normalize_tag_list(dels or [])[:10],
            safety=row_meta.get("AI Safety", "Not Mentioned"),
            reliability=row_meta.get("AI Reliability", "Not Mentioned"),
            sessions=row_meta.get("AI # of Sessions", "Unknown"),
            ev_det={k: list(v or [])[:2] for k, v in (ev_det or {}).items() if k in _normalize_tag_list(dets or [])[:10]},
            ev_del={k: list(v or [])[:2] for k, v in (ev_del or {}).items() if k in _normalize_tag_list(dels or [])[:10]},
            unl_dels=[],
            unl_dets=[],
        ))
    return updated



def _detect_sym_cols(df):
    det_cols, del_cols = _app()._symptom_col_lists_from_columns(df.columns)
    return dict(
        manual_detractors=[c for c in det_cols if c.lower() in {f"symptom {i}" for i in range(1, 11)}],
        manual_delighters=[c for c in del_cols if c.lower() in {f"symptom {i}" for i in range(11, 21)}],
        ai_detractors=[c for c in det_cols if c.lower().startswith("ai symptom detractor")],
        ai_delighters=[c for c in del_cols if c.lower().startswith("ai symptom delighter")],
    )



def _detect_missing(df, colmap):
    out = df.copy()
    det_cols = colmap["manual_detractors"] + colmap["ai_detractors"]
    del_cols = colmap["manual_delighters"] + colmap["ai_delighters"]
    out["Has_Detractors"] = _app()._filled_mask(out, det_cols)
    out["Has_Delighters"] = _app()._filled_mask(out, del_cols)
    out["Needs_Detractors"] = ~out["Has_Detractors"]
    out["Needs_Delighters"] = ~out["Has_Delighters"]
    out["Needs_Symptomization"] = out["Needs_Detractors"] & out["Needs_Delighters"]
    return out



def _prioritize_for_symptomization(df):
    if df.empty:
        return df
    w = df.copy()
    text = w.get("title_and_text", w.get("review_text", pd.Series("", index=w.index))).fillna("").astype(str)
    rating = pd.to_numeric(w.get("rating", pd.Series(dtype=float)), errors="coerce").fillna(3)
    lengths = pd.to_numeric(w.get("review_length_words", text.str.split().str.len()), errors="coerce").fillna(text.str.split().str.len()).fillna(0)
    neg_kw = r"\b(broke|broken|fail|failed|defect|issue|problem|stopped|won't|doesn't|difficult|hard|confusing|loud|noise|leak|burned|smoke|stuck|cracked|itchy|rash|damaged|expensive|overpriced|slow)\b"
    pos_kw = r"\b(love|perfect|amazing|excellent|great|easy|simple|quiet|durable|worth|recommend|best|happy|works well|works great|comfortable|stylish|quick)\b"
    mixed_kw = r"\b(but|however|although|except|yet|while|wish)\b"
    w["_prio"] = (
        (rating <= 2).astype(int) * 4.0 +
        (rating >= 4.5).astype(int) * 2.5 +
        (rating.between(2.5, 3.5)).astype(int) * 1.2 +
        (lengths.clip(upper=500) / 120.0)
    )
    w["_prio"] += text.str.lower().str.count(neg_kw, flags=re.IGNORECASE).clip(upper=4) * 0.9
    w["_prio"] += text.str.lower().str.count(pos_kw, flags=re.IGNORECASE).clip(upper=4) * 0.45
    w["_prio"] += text.str.lower().str.count(mixed_kw, flags=re.IGNORECASE).clip(upper=2) * 0.9
    w["_prio"] += text.str.len().clip(upper=1200) / 1500.0
    return w.sort_values(["_prio"], ascending=False).drop(columns=["_prio"])



def _sample_reviews_for_symptomizer(df, sample_n):
    if df is None or getattr(df, "empty", True):
        return []
    n = max(0, int(sample_n or 0))
    if n <= 0:
        return []
    w = df.copy()
    text_series = w.get("title_and_text", w.get("review_text", pd.Series("", index=w.index))).fillna("").astype(str).str.strip()
    w = w.loc[text_series != ""].copy()
    if w.empty:
        return []
    text_series = text_series.loc[w.index]
    rating = pd.to_numeric(w.get("rating", pd.Series(np.nan, index=w.index)), errors="coerce").fillna(3).round().clip(lower=1, upper=5).astype(int)
    lengths = pd.to_numeric(w.get("review_length_words", text_series.str.split().str.len()), errors="coerce").fillna(text_series.str.split().str.len()).fillna(0)
    review_norm = text_series.str.lower()
    submitted = pd.to_datetime(w.get("submission_time", pd.Series(pd.NaT, index=w.index)), errors="coerce", utc=True)
    prioritized = _prioritize_for_symptomization(w.assign(_sample_rating_bucket=rating, _sample_length=lengths, _sample_submitted=submitted))

    selected_idx = []
    seen = set()

    def _extend(index_values, limit):
        added = 0
        for idx in list(index_values):
            if idx in seen:
                continue
            review_text = _safe_text(text_series.get(idx))
            if not review_text:
                continue
            selected_idx.append(idx)
            seen.add(idx)
            added += 1
            if len(selected_idx) >= n or added >= limit:
                break

    available_buckets = [bucket for bucket in [1, 2, 3, 4, 5] if (rating == bucket).any()]
    per_bucket = max(1, int(math.ceil((n * 0.34) / float(max(len(available_buckets), 1)))))
    for bucket in [1, 2, 3, 4, 5]:
        bucket_df = prioritized.loc[rating.loc[prioritized.index] == bucket]
        _extend(bucket_df.index.tolist(), per_bucket)
        if len(selected_idx) >= n:
            break

    long_threshold = float(lengths.quantile(0.75)) if len(lengths) >= 4 else float(lengths.max() or 0)
    signal_masks = [
        ("explicit defects", review_norm.str.contains(r"\b(leak|broken|crack|defect|defective|stopped working|won't|wouldn't|doesn't|does not|hard to|difficult|messy|loud|noisy|disconnect|pair|charge|battery|falls flat|frizz|flyaways|curl|hold|wears off|itch|rash|shrink|sag|suction)\b", regex=True, na=False), prioritized),
        ("mixed sentiment", review_norm.str.contains(r"\b(but|however|although|except|wish|if only|yet|while)\b", regex=True, na=False), prioritized),
        ("maintenance or workflow", review_norm.str.contains(r"\b(clean|cleanup|wash|descale|filter|residue|lint|maintenance|attachment|attachments|switch|swap|refill|empty|setup|assembly|pairing)\b", regex=True, na=False), prioritized),
        ("strong delight", review_norm.str.contains(r"\b(love|favorite|easy|quiet|comfortable|fast|quick|helpful|works great|worth it|smooth|volume|hydrat|clear sound|good flavor|supportive)\b", regex=True, na=False), prioritized),
        ("long reviews", lengths >= long_threshold, prioritized.sort_values(["_sample_length"], ascending=[False], na_position="last", kind="mergesort")),
    ]
    if submitted.notna().any():
        recent_sorted = prioritized.sort_values(["_sample_submitted", "_sample_length"], ascending=[False, False], na_position="last", kind="mergesort")
        signal_masks.insert(3, ("recent reviews", submitted.notna(), recent_sorted))

    signal_quota = max(1, int(math.ceil((n * 0.5) / float(max(len(signal_masks), 1)))))
    for _label, mask, base_df in signal_masks:
        scoped = base_df.loc[mask.loc[base_df.index]]
        _extend(scoped.index.tolist(), signal_quota)
        if len(selected_idx) >= n:
            break

    if len(selected_idx) < n:
        _extend(prioritized.index.tolist(), n - len(selected_idx))

    reviews = []
    for _, row in prioritized.loc[selected_idx].iterrows():
        review_text = _symptomizer_review_text(row)
        if review_text:
            reviews.append(review_text)
            if len(reviews) >= n:
                break
    return reviews



def _symptomizer_review_text(row):
    if row is None:
        return ""
    preferred_fields = [
        "title",
        "review_text",
        "pros",
        "cons",
        "headline",
        "body",
        "comments",
        "reviewer_comments",
        "usage_context",
        "routine",
        "variant",
        "size",
        "color",
        "flavor",
        "scent",
        "style",
        "fit",
        "material",
        "attachments",
        "accessories",
    ]
    excluded_markers = (
        "symptom",
        "ai ",
        "needs_",
        "has_",
        "rating",
        "review_id",
        "submission",
        "locale",
        "product_id",
        "sku",
        "token",
        "score",
    )

    values = []
    seen = set()
    title_and_text = _safe_text(row.get("title_and_text"))
    if title_and_text:
        values.append(title_and_text)
        seen.add(_app()._clean_text(title_and_text).lower())

    def _append_field(value, prefix=""):
        text = _safe_text(value)
        cleaned = _app()._clean_text(text)
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        values.append(f"{prefix}{cleaned}" if prefix else cleaned)

    for field in preferred_fields:
        if field in getattr(row, "index", []) or (hasattr(row, "get") and row.get(field) is not None):
            _append_field(row.get(field), prefix=f"{field.replace('_', ' ').title()}: ")

    for key in getattr(row, "index", []):
        key_text = str(key or "").strip()
        key_lower = key_text.lower()
        if not key_text or key_lower == "title_and_text":
            continue
        if any(marker in key_lower for marker in excluded_markers):
            continue
        if key_lower in preferred_fields:
            continue
        value = row.get(key)
        if value is None:
            continue
        text = _safe_text(value)
        if len(text.strip()) < 3:
            continue
        if len(text) > 400:
            text = text[:400]
        _append_field(text, prefix=f"{key_text.replace('_', ' ').title()}: ")

    return _app()._clean_text(" \n ".join(values))


