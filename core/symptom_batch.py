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
    out_by_idx = {}
    if not items:
        return out_by_idx
    det_list = "\n".join(f"  - {l}" for l in allowed_detractors) or "  (none defined)"
    del_list = "\n".join(f"  - {l}" for l in allowed_delighters) or "  (none defined)"
    category_info = _infer_taxonomy_category(product_profile, [it.get("review", "") for it in items[:12]])
    category = category_info.get("category", "general")
    product_knowledge_text = _product_knowledge_context_text(product_knowledge, limit_per_section=4)
    product_knowledge_block = f"Product knowledge:\n{product_knowledge_text}" if product_knowledge_text else ""
    taxonomy_context = _taxonomy_prompt_context(category)
    system_prompt = f"""You are an expert consumer product review analyst.
Your job is to tag customer reviews against a pre-defined symptom taxonomy for any product category.
Work clause by clause and capture every explicit or clearly described issue or strength.

{f"Product context: {product_profile[:700]}" if product_profile else ""}
{product_knowledge_block}
Likely category: {category}.
{taxonomy_context}

═══ ALLOWED DETRACTORS (problems / complaints) ═══
{det_list}

═══ ALLOWED DELIGHTERS (positives / strengths) ═══
{del_list}

═══ CLASSIFICATION ENUMS ═══
safety      → {SAFETY_ENUM}
reliability → {RELIABILITY_ENUM}
sessions    → {SESSIONS_ENUM}

═══ STRICT RULES ═══
1. HIGH RECALL: Find EVERY applicable symptom. Long reviews can legitimately map to many labels.
2. EXACT LABELS: Use label text EXACTLY as it appears in the catalog above. No paraphrasing for catalog labels.
3. EVIDENCE: Each evidence string must be verbatim text from the review (4-{max_ev_chars} chars). Max 2 per label.
3b. PRODUCT KNOWLEDGE IS CONTEXT ONLY: Use it to understand components, desired outcomes, workflow, and likely failure modes, never to invent unsupported tags.
4. NO INFERENCE: Only tag what is explicitly stated or clearly described. Never assume missing facts.
5. BROAD FALLBACKS: Use broad labels like Overall Satisfaction or Overall Dissatisfaction when the review is clearly broadly positive or negative and no sharper label fully covers that sentiment.
6. PREFER SPECIFIC OVER BROAD: Keep a broad universal label only when it adds signal beyond the more specific theme.
7. SYSTEMATIC LABELING: If an important theme is not in the catalog, add it to unlisted_detractors or unlisted_delighters using concise Title Case wording that does not duplicate an existing concept with alternate phrasing.
7b. FAVOR SHARP CROSS-FUNCTIONAL THEMES: Prefer labels that are useful to consumer insights, CX, product, and quality teams.
8. ZERO TAGS IS VALID: It is better to return no tag on one side than to force a weak match.
9. NO GENERIC FILLER: Generic praise (for example "works great" or "love it") cannot justify a specific positive symptom unless the review names that concept.
10. OPPOSITES: Do not assign opposite themes like Quiet and Loud unless the review explicitly describes both in different contexts with separate evidence.
11. STAR RATING IS CONTEXT ONLY: Rating can help with broad satisfaction or dissatisfaction, but cannot justify a specific non-universal symptom by itself.
12. SMALL CATALOGS: If the catalog is short, stay conservative and only tag when wording strongly matches.
13. ALL IDS: Return a result for EVERY review id in the input, even if no symptoms apply.

═══ OUTPUT SCHEMA (strict JSON) ═══
{{"items":[{{
  "id":"<review_id_string>",
  "detractors":[{{"label":"<exact catalog label>","evidence":["<verbatim text>"]}}],
  "delighters":[{{"label":"<exact catalog label>","evidence":["<verbatim text>"]}}],
  "unlisted_detractors":["<2-5 word theme>"],
  "unlisted_delighters":["<2-5 word theme>"],
  "safety":"<enum value>",
  "reliability":"<enum value>",
  "sessions":"<enum value>"
}}]}}"""
    payload = dict(items=[dict(id=str(it["idx"]), review=it["review"], rating=it.get("rating"), needs_delighters=it.get("needs_del", True), needs_detractors=it.get("needs_det", True)) for it in items])
    max_out = min(7000, max(1800, 230 * len(items) + 500))
    result_text = _chat_complete_with_fallback_models(
        client,
        model=_shared_model(),
        structured=True,
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": json.dumps(payload)}],
        temperature=0.0,
        response_format={"type": "json_object"},
        max_tokens=max_out,
        reasoning_effort=_shared_reasoning(),
    )
    data = _safe_json_load(result_text)
    items_out = data.get("items") or (data if isinstance(data, list) else [])
    by_id = {str(o.get("id")): o for o in items_out if isinstance(o, dict) and "id" in o}
    for it in items:
        idx = int(it["idx"])
        review_text = it.get("review", "")
        obj = by_id.get(str(idx)) or {}

        def _extract_side(objs, allowed):
            labels = []
            ev_map = {}
            for obj2 in (objs or []):
                if not isinstance(obj2, dict):
                    continue
                raw = str(obj2.get("label", "")).strip()
                lbl = _app()._match_label(raw, allowed, aliases=aliases)
                if not lbl:
                    continue
                raw_evs = [str(e) for e in (obj2.get("evidence") or []) if isinstance(e, str)]
                validated = _app()._validate_evidence(raw_evs, review_text, max_ev_chars)
                if not validated:
                    validated = [str(e).strip()[:max_ev_chars] for e in raw_evs if str(e).strip()][:1]
                if lbl not in labels:
                    labels.append(lbl)
                    ev_map[lbl] = validated[:2]
                if len(labels) >= 10:
                    break
            return labels, ev_map

        dels, ev_del = _extract_side(obj.get("delighters", []), allowed_delighters)
        dets, ev_det = _extract_side(obj.get("detractors", []), allowed_detractors)
        custom_universal_dels, custom_universal_dets = _custom_universal_catalog()
        refined = _refine_tag_assignment(
            review_text,
            dets,
            dels,
            allowed_detractors=allowed_detractors,
            allowed_delighters=allowed_delighters,
            evidence_det=ev_det,
            evidence_del=ev_del,
            aliases=aliases,
            max_per_side=10,
            include_universal_neutral=bool(include_universal_neutral),
            rating=it.get("rating"),
            extra_universal_detractors=custom_universal_dets,
            extra_universal_delighters=custom_universal_dels,
        )
        dets = list(refined.get("dets", []))[:10]
        dels = list(refined.get("dels", []))[:10]
        ev_det = dict(refined.get("ev_det", {}) or {})
        ev_del = dict(refined.get("ev_del", {}) or {})
        safety = str(obj.get("safety", "Not Mentioned")).strip()
        reliability = str(obj.get("reliability", "Not Mentioned")).strip()
        sessions = str(obj.get("sessions", "Unknown")).strip()
        safety = safety if safety in SAFETY_ENUM else "Not Mentioned"
        reliability = reliability if reliability in RELIABILITY_ENUM else "Not Mentioned"
        sessions = sessions if sessions in SESSIONS_ENUM else "Unknown"
        canon_unl_dels, _, _ = _app()._standardize_symptom_lists([str(x).strip() for x in (obj.get("unlisted_delighters") or []) if str(x).strip()][:10], [])
        _, canon_unl_dets, _ = _app()._standardize_symptom_lists([], [str(x).strip() for x in (obj.get("unlisted_detractors") or []) if str(x).strip()][:10])
        out_by_idx[idx] = dict(
            dels=dels,
            dets=dets,
            ev_del=ev_del,
            ev_det=ev_det,
            unl_dels=list(canon_unl_dels)[:10],
            unl_dets=list(canon_unl_dets)[:10],
            safety=safety,
            reliability=reliability,
            sessions=sessions,
        )
    return out_by_idx



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


