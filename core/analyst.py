"""AI Analyst: context building, review selection, chat helpers.
"""
from __future__ import annotations
import json, re, sys, textwrap
from typing import Any, Dict, List, Optional
import pandas as pd
import streamlit as st

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

# ── Constants ──
AI_CONTEXT_TOKEN_BUDGET = 10_000

def _app():
    return sys.modules.get('__main__', sys.modules.get('app'))

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

def _trunc(text, max_len=120):
    s = _safe_text(text)
    return s[:max_len] + "…" if len(s) > max_len else s

def _norm_text(text):
    return re.sub(r"\s+", " ", str(text or "").lower()).strip()

def _tokenize(text):
    return set(re.findall(r"[a-z]{2,}", str(text or "").lower()))


def _select_relevant(df, question, max_reviews=22):
    if df.empty:
        return df.copy()
    w = df.copy()
    w["blob"] = w["title_and_text"].fillna("").astype(str).map(_norm_text)
    qt = _tokenize(question)

    def score(row):
        s = 0.0
        t = row["blob"]
        for tk in qt:
            if tk in t:
                s += 3 + t.count(tk)
        r = row.get("rating")
        if any(tk in {"defect", "broken", "issue", "problem", "bad", "fail", "broke"} for tk in qt):
            if pd.notna(r):
                s += max(0, 6 - float(r))
        if not _safe_bool(row.get("incentivized_review"), False):
            s += 0.5
        if pd.notna(row.get("review_length_words")):
            s += min(float(row.get("review_length_words", 0)) / 60, 2)
        return s

    w["_sc"] = w.apply(score, axis=1)
    ranked = w.sort_values(["_sc", "submission_time"], ascending=[False, False], na_position="last")
    combined = pd.concat([
        ranked.head(max_reviews),
        df[df["rating"].isin([1, 2])].head(max_reviews // 3 or 1),
        df[df["rating"].isin([4, 5])].head(max_reviews // 3 or 1),
    ], ignore_index=True).drop_duplicates(subset=["review_id"])
    return combined.head(max_reviews).drop(columns=["blob", "_sc"], errors="ignore")



def _snippet_rows(df, *, max_reviews=22):
    rows = []
    for _, row in df.head(max_reviews).iterrows():
        rows.append(dict(
            review_id=_safe_text(row.get("review_id")),
            rating=_safe_int(row.get("rating"), 0) if pd.notna(row.get("rating")) else None,
            incentivized_review=_safe_bool(row.get("incentivized_review"), False),
            content_locale=_safe_text(row.get("content_locale")),
            submission_date=_safe_text(row.get("submission_date")),
            title=_trunc(row.get("title", ""), 120),
            snippet=_trunc(row.get("review_text", ""), 600),
        ))
    return rows



def _build_ai_context(*, overall_df, filtered_df, summary, filter_description, question):
    om = _get_metrics(overall_df)
    fm = _get_metrics(filtered_df)
    try:
        rd = _app()._rating_dist(filtered_df).to_dict(orient="records")
    except Exception:
        rd = []
    try:
        md = _monthly_trend(filtered_df).tail(12).to_dict(orient="records")
    except Exception:
        md = []
    rel = _select_relevant(filtered_df, question, max_reviews=22)
    rec = filtered_df.sort_values(["submission_time", "review_id"], ascending=[False, False], na_position="last").head(10)
    low = filtered_df[filtered_df["rating"].isin([1, 2])].head(8)
    hi = filtered_df[filtered_df["rating"].isin([4, 5])].head(8)
    ev = pd.concat([rel, rec, low, hi], ignore_index=True).drop_duplicates(subset=["review_id"]).head(32)
    payload = dict(
        product=dict(product_id=summary.product_id, product_url=summary.product_url, product_name=_product_name(summary, overall_df)),
        analysis_scope=dict(filter_description=filter_description, overall_review_count=len(overall_df), filtered_review_count=len(filtered_df)),
        metric_snapshot=dict(overall=om, filtered=fm, rating_distribution_filtered=rd, monthly_trend_filtered=md),
        review_text_evidence=_snippet_rows(ev, max_reviews=32),
    )
    full_json = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    tok = _estimate_tokens(full_json)
    max_ev = 22
    while tok > AI_CONTEXT_TOKEN_BUDGET and max_ev >= 5:
        max_ev -= 4
        payload["review_text_evidence"] = _snippet_rows(ev, max_reviews=max_ev)
        full_json = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        tok = _estimate_tokens(full_json)
    return full_json



def _call_analyst(*, question, overall_df, filtered_df, summary, filter_description, chat_history, persona_name=None, target_words=1200, include_references=False):
    client = _get_client()
    if client is None:
        raise ReviewDownloaderError("No OpenAI API key configured.")
    target_words = _coerce_ai_target_words(target_words)
    floor_words = max(220, int(round(target_words * 0.8)))
    ceiling_words = min(2600, int(round(target_words * 1.15)))
    max_tok = _ai_target_token_budget(target_words)
    base_instructions = _persona_instructions(persona_name)
    length_note = (
        "RESPONSE LENGTH OVERRIDE: ignore any shorter length caps that may appear earlier in these instructions. "
        f"Aim for about {target_words:,} words, with a practical range of roughly {floor_words:,} to {ceiling_words:,} words. "
        "Use detailed evidence, concrete examples, and a full Next Steps section. Do not compress the answer into a short summary unless the user explicitly asks for that."
    )
    reference_note = (
        "REFERENCE MODE: include inline evidence references using the exact format (review_ids: 12345, 67890) for material claims."
        if include_references else
        "REFERENCE MODE: do NOT include inline citations, do NOT emit (review_ids: ...), and do NOT add reference callouts in the final answer."
    )
    instructions = base_instructions + "\n\n" + length_note + "\n\n" + reference_note
    ai_ctx = _build_ai_context(overall_df=overall_df, filtered_df=filtered_df, summary=summary, filter_description=filter_description, question=question)
    msgs = [{"role": m["role"], "content": m["content"]} for m in list(chat_history)[-8:]]
    msgs.append({"role": "user", "content": f"User request:\n{question}\n\nReview dataset context (JSON):\n{ai_ctx}"})
    result = _chat_complete_with_fallback_models(
        client,
        model=_shared_model(),
        structured=False,
        messages=[{"role": "system", "content": instructions}, *msgs],
        temperature=0.0,
        max_tokens=max_tok,
        reasoning_effort=_shared_reasoning(),
    )
    if not result:
        raise ReviewDownloaderError("OpenAI returned empty answer.")
    if not include_references:
        result = _strip_review_citations(result)
    return result

# ═══════════════════════════════════════════════════════════════════════════════
#  REVIEW PROMPT TAGGING
# ═══════════════════════════════════════════════════════════════════════════════



def _product_name(summary, df):
    if not df.empty and "original_product_name" in df.columns:
        names = [x for x in dict.fromkeys(df["original_product_name"].fillna("").astype(str).str.strip().tolist()) if x]
        if len(names) == 1:
            return names[0]
        if len(names) > 1:
            return f"Combined review workspace ({len(names)} products)"
    if not df.empty and "product_or_sku" in df.columns:
        prods = [x for x in dict.fromkeys(df["product_or_sku"].fillna("").astype(str).str.strip().tolist()) if x]
        if len(prods) > 1 and str(getattr(summary, "product_id", "")).startswith("MULTI_URL_WORKSPACE"):
            return f"Combined review workspace ({len(prods)} products)"
    return summary.product_id

# ═══════════════════════════════════════════════════════════════════════════════
#  AI ANALYST
# ═══════════════════════════════════════════════════════════════════════════════
GENERAL_INSTRUCTIONS = textwrap.dedent("""
    You are SharkNinja Review Analyst — an internal voice-of-customer AI assistant.
    ROLE: Synthesise consumer review data into sharp, actionable insights.
    Prioritise evidence from the supplied dataset over generic assumptions.
    ANSWER FORMAT
    • Use compact markdown. Prefer short bold section labels instead of large headings.
    • Lead with the most important insight — do not bury the lede.
    • Cite review IDs inline: (review_ids: 12345, 67890).
    • For every quantitative claim state the count or percentage from the data.
    • Mark inferences: [INFERRED].
    • Follow the caller-specified response-length target. If no target is given, prefer a detailed answer over a terse one.
    • End every response with a "**Next Steps**" section: 2–3 concrete actions.
    GUARDRAILS
    • Do not invent review IDs, quotes, counts, or trends not in the evidence.
    • If the data is insufficient, say so explicitly.
    • Never hallucinate product specs — only cite what reviews mention.
""").strip()



def _persona_instructions(name):
    if not name:
        return GENERAL_INSTRUCTIONS
    return PERSONAS[name]["instructions"]


GENERAL_INSTRUCTIONS = textwrap.dedent("""
    You are SharkNinja Review Analyst — an internal voice-of-customer AI assistant.
    ROLE: Synthesise consumer review data into sharp, actionable insights.
    Prioritise evidence from the supplied dataset over generic assumptions.
    ANSWER FORMAT
    • Use compact markdown. Prefer short bold section labels instead of large headings.
    • Lead with the most important insight — do not bury the lede.
    • Cite review IDs inline: (review_ids: 12345, 67890).
    • For every quantitative claim state the count or percentage from the data.
    • Mark inferences: [INFERRED].
    • Follow the caller-specified response-length target. If no target is given, prefer a detailed answer over a terse one.
    • End every response with a "**Next Steps**" section: 2–3 concrete actions.
    GUARDRAILS
    • Do not invent review IDs, quotes, counts, or trends not in the evidence.
    • If the data is insufficient, say so explicitly.
    • Never hallucinate product specs — only cite what reviews mention.
""").strip()

