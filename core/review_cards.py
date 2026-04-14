"""Review card rendering, sorting, and evidence highlighting.

Extracted from app.py for cleaner organization.
"""
from __future__ import annotations
import html
import re
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import streamlit as st
import sys
def _app():
    return sys.modules.get('__main__', sys.modules.get('app'))

# Non-value sentinel set (imported from app namespace)
NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

def _esc(text):
    return html.escape(str(text or ""))

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _safe_bool(value, default=False):
    if value is None: return default
    if isinstance(value, bool): return value
    return str(value).strip().lower() in ("true", "1", "yes")

def _safe_int(value, default=0):
    try: return int(float(value))
    except: return default

def _is_missing(value):
    if value is None: return True
    s = str(value).strip().upper()
    return s in NON_VALUES


def _sort_reviews(df, sort_mode):
    w = df.copy()
    if sort_mode == "Newest":
        return w.sort_values(["submission_time", "review_id"], ascending=[False, False], na_position="last")
    if sort_mode == "Oldest":
        return w.sort_values(["submission_time", "review_id"], ascending=[True, True], na_position="last")
    if sort_mode == "Highest rating":
        return w.sort_values(["rating", "submission_time"], ascending=[False, False], na_position="last")
    if sort_mode == "Lowest rating":
        return w.sort_values(["rating", "submission_time"], ascending=[True, False], na_position="last")
    if sort_mode == "Longest":
        return w.sort_values(["review_length_words", "submission_time"], ascending=[False, False], na_position="last")
    if sort_mode in ("Most tagged", "Least tagged"):
        sym_cols = [c for c in w.columns if c.startswith("AI Symptom")]
        if sym_cols:
            w["_tag_n"] = sum((w[c].notna() & (w[c].astype(str).str.strip() != "") & (~w[c].astype(str).str.upper().isin({"","NA","N/A","NONE","NULL","NAN","<NA>","NOT MENTIONED"}))) for c in sym_cols)
            asc = sort_mode == "Least tagged"
            return w.sort_values(["_tag_n", "submission_time"], ascending=[asc, False], na_position="last").drop(columns=["_tag_n"])
    return w



def _highlight_keywords_in_text(text, keywords):
    """Highlight search keywords in review text with a subtle background."""
    if not keywords or not text:
        return str(text)
    result = str(text)
    for kw in keywords:
        kw = kw.strip()
        if len(kw) < 2: continue
        pattern = re.compile(re.escape(kw), re.IGNORECASE)
        result = pattern.sub(lambda m: f"<mark style='background:rgba(99,102,241,.15);padding:1px 3px;border-radius:3px;'>{_esc(m.group())}</mark>", result)
    return result



def _highlight_evidence(text, evidence_items):
    """Highlight evidence in review text with fuzzy matching.
    
    Tier 1: Exact regex match (case-insensitive)
    Tier 2: Fuzzy — find the best overlapping window in the review text
            where all significant words from the evidence appear nearby
    """
    text_str = str(text)
    if not evidence_items or not text_str.strip():
        return f"<div class='review-body'>{html.escape(text_str)}</div>"
    _STOP = {"the","a","an","and","to","for","of","in","on","with","is","it","very","really","so"}
    hits = []
    text_lower = text_str.lower()
    for ev_text, tag_label in evidence_items:
        ev_clean = ev_text.strip()
        if not ev_clean:
            continue
        # Tier 1: Exact match
        found = False
        for m in re.compile(re.escape(ev_clean), re.IGNORECASE).finditer(text_str):
            hits.append((m.start(), m.end(), tag_label, m.group()))
            found = True
        # Tier 2: Fuzzy match — find the tightest window containing all content words
        if not found:
            ev_words = [w for w in ev_clean.lower().split() if len(w) > 2 and w not in _STOP]
            if len(ev_words) >= 2:
                best_start, best_end, best_len = -1, -1, 999
                for w in ev_words:
                    pos = text_lower.find(w)
                    while pos >= 0:
                        window_start = max(0, pos - 10)
                        window_end = min(len(text_lower), pos + 120)
                        window = text_lower[window_start:window_end]
                        if all(ew in window for ew in ev_words):
                            positions = []
                            for ew in ev_words:
                                wp = window.find(ew)
                                if wp >= 0:
                                    positions.append((window_start + wp, window_start + wp + len(ew)))
                            if positions:
                                s = min(p[0] for p in positions)
                                e = max(p[1] for p in positions)
                                if (e - s) < best_len:
                                    best_start, best_end, best_len = s, e, e - s
                        pos = text_lower.find(w, pos + 1)
                if best_start >= 0 and best_len < 150:
                    hits.append((best_start, best_end, tag_label, text_str[best_start:best_end]))
    if not hits:
        return f"<div class='review-body'>{html.escape(text_str)}</div>"
    hits.sort(key=lambda h: h[0])
    deduped = []
    cursor = 0
    for h in hits:
        if h[0] >= cursor:
            deduped.append(h)
            cursor = h[1]
    parts = []
    cursor = 0
    for start, end, tag_label, matched in deduped:
        parts.append(html.escape(text_str[cursor:start]))
        tip = html.escape(f"AI tag: {tag_label}")
        parts.append(f'<span class="ev-highlight" data-tag="{tip}">{html.escape(matched)}</span>')
        cursor = end
    parts.append(html.escape(text_str[cursor:]))
    return f"<div class='review-body'>{''.join(parts)}</div>"



def _symptom_tags_html(det_tags, del_tags, *, ev_det=None, ev_del=None):
    if not det_tags and not del_tags:
        return ""
    ev_det = ev_det or {}
    ev_del = ev_del or {}
    def _chip(tag, color, ev_map):
        ev = ev_map.get(tag, [])
        tooltip = _esc(" | ".join(str(e)[:80] for e in ev)) if ev else "No evidence"
        return f"<span class='chip {color}' style='font-size:11px;padding:3px 8px;cursor:help;' title='{tooltip}'>{_esc(tag)}</span>"
    sym_html = "<div style='margin-top:9px;padding-top:9px;border-top:1px solid var(--border);display:flex;flex-direction:column;gap:6px;'>"
    if det_tags:
        det_chips = "".join(_chip(t, "red", ev_det) for t in det_tags)
        sym_html += f"<div style='display:flex;align-items:flex-start;gap:7px;flex-wrap:wrap;'><span style='font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--danger);font-weight:700;white-space:nowrap;padding-top:3px;'>Issues</span><div style='display:flex;gap:4px;flex-wrap:wrap;'>{det_chips}</div></div>"
    if del_tags:
        del_chips = "".join(_chip(t, "green", ev_del) for t in del_tags)
        sym_html += f"<div style='display:flex;align-items:flex-start;gap:7px;flex-wrap:wrap;'><span style='font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--success);font-weight:700;white-space:nowrap;padding-top:3px;'>Strengths</span><div style='display:flex;gap:4px;flex-wrap:wrap;'>{del_chips}</div></div>"
    sym_html += "</div>"
    return sym_html



def _render_review_card(row, evidence_items=None):
    rating_val = _safe_int(row.get("rating"), 0) if pd.notna(row.get("rating")) else 0
    stars = "★" * max(0, min(rating_val, 5)) + "☆" * max(0, 5 - rating_val)
    title = _safe_text(row.get("title"), "No title") or "No title"
    review_text = _safe_text(row.get("review_text"), "—") or "—"
    meta_bits = [b for b in [_safe_text(row.get("submission_date")), _safe_text(row.get("content_locale")), _safe_text(row.get("retailer")), _safe_text(row.get("product_or_sku"))] if b]
    is_organic = not _safe_bool(row.get("incentivized_review"), False)
    status_chips = f"<span class='chip {'gray' if is_organic else 'yellow'}'>{'Organic' if is_organic else 'Incentivized'}</span>"
    rec = row.get("is_recommended")
    if not _is_missing(rec):
        status_chips += f"<span class='chip {'gray' if _safe_bool(rec, False) else 'red'}'>{'Recommended' if _safe_bool(rec, False) else 'Not recommended'}</span>"
    det_cols, del_cols = _app()._symptom_col_lists_from_columns(row.index)
    det_tags = _app()._collect_row_symptom_tags(row, det_cols)
    del_tags = _app()._collect_row_symptom_tags(row, del_cols)
    with st.container(border=True):
        top_cols = st.columns([5, 1.5])
        with top_cols[0]:
            st.markdown(f"<span style='color:#f59e0b;letter-spacing:-.01em;'>{stars}</span>&nbsp;<span style='font-size:12px;color:var(--slate-500);font-weight:600;'>{rating_val}/5</span>", unsafe_allow_html=True)
            st.markdown(f"<div style='font-weight:700;font-size:14.5px;color:var(--navy);margin:3px 0 2px;'>{_esc(title)}</div>", unsafe_allow_html=True)
            if meta_bits:
                st.markdown(f"<div style='font-size:12px;color:var(--slate-400);margin-bottom:4px;'>{' · '.join(_esc(b) for b in meta_bits)}</div>", unsafe_allow_html=True)
        with top_cols[1]:
            st.markdown(f"<div class='chip-wrap' style='justify-content:flex-end;gap:4px;flex-wrap:wrap;padding-top:2px;'>{status_chips}</div>", unsafe_allow_html=True)
        if evidence_items:
            st.markdown(_highlight_evidence(review_text, evidence_items), unsafe_allow_html=True)
            st.caption("Yellow highlights = Symptomizer evidence · hover to see the AI tag")
        else:
            active_kw = str(st.session_state.get("rf_kw", "")).strip()
            if active_kw:
                highlighted = _highlight_keywords_in_text(html.escape(review_text), active_kw.split())
                st.markdown(f"<div class='review-body'>{highlighted}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='review-body'>{html.escape(review_text)}</div>", unsafe_allow_html=True)
        # Collect evidence from processed symptomizer records if available
        _ev_det_map, _ev_del_map = {}, {}
        try:
            _rid = _safe_text(row.get("review_id"))
            for pr in (st.session_state.get("sym_processed_rows") or []):
                if str(pr.get("rid")) == str(_rid) or str(pr.get("review_id")) == str(_rid):
                    _ev_det_map = pr.get("ev_det", {})
                    _ev_del_map = pr.get("ev_del", {})
                    break
        except Exception:
            pass
        tag_html = _symptom_tags_html(det_tags, del_tags, ev_det=_ev_det_map, ev_del=_ev_del_map)
        if tag_html:
            st.markdown(tag_html, unsafe_allow_html=True)
        footer_bits = []
        rid = _safe_text(row.get("review_id"))
        if rid:
            footer_bits.append(f"<span style='font-size:11.5px;color:var(--slate-400);'>ID {_esc(rid)}</span>")
        loc = _safe_text(row.get("user_location"))
        if loc:
            footer_bits.append(f"<span style='font-size:11.5px;color:var(--slate-400);'>{_esc(loc)}</span>")
        if footer_bits:
            st.markdown(f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:8px;'>{' · '.join(footer_bits)}</div>", unsafe_allow_html=True)


