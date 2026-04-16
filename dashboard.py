"""Catalog management: taxonomy normalization, product knowledge, aliases

Extracted from app.py. Uses _app() for cross-module access.
"""
from __future__ import annotations
import html, json, re, sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import streamlit as st

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

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


def _normalize_product_knowledge(knowledge):
    data = dict(knowledge or {}) if isinstance(knowledge, dict) else {}
    return {
        "product_archetype": _safe_text(data.get("product_archetype")),
        "product_areas": _coerce_product_knowledge_list(data.get("product_areas")),
        "use_cases": _coerce_product_knowledge_list(data.get("use_cases")),
        "desired_outcomes": _coerce_product_knowledge_list(data.get("desired_outcomes")),
        "comparison_set": _coerce_product_knowledge_list(data.get("comparison_set")),
        "workflow_steps": _coerce_product_knowledge_list(data.get("workflow_steps")),
        "user_contexts": _coerce_product_knowledge_list(data.get("user_contexts")),
        "csat_drivers": _coerce_product_knowledge_list(data.get("csat_drivers")),
        "likely_failure_modes": _coerce_product_knowledge_list(data.get("likely_failure_modes")),
        "likely_themes": _coerce_product_knowledge_list(data.get("likely_themes")),
        "likely_delighter_themes": _coerce_product_knowledge_list(data.get("likely_delighter_themes")),
        "likely_detractor_themes": _coerce_product_knowledge_list(data.get("likely_detractor_themes")),
        "watchouts": _coerce_product_knowledge_list(data.get("watchouts")),
        "confidence_note": _safe_text(data.get("confidence_note")),
    }



def _product_knowledge_context_text(knowledge, *, limit_per_section=5):
    info = _normalize_product_knowledge(knowledge)
    lines = []
    if info.get("product_archetype"):
        lines.append(f"Product archetype: {info['product_archetype']}")
    for heading, key in [
        ("Product areas", "product_areas"),
        ("Use cases", "use_cases"),
        ("Desired outcomes", "desired_outcomes"),
        ("Workflow steps", "workflow_steps"),
        ("Comparison set", "comparison_set"),
        ("User contexts", "user_contexts"),
        ("CSAT drivers", "csat_drivers"),
        ("Likely failure modes", "likely_failure_modes"),
        ("Likely themes", "likely_themes"),
        ("Likely delighter themes", "likely_delighter_themes"),
        ("Likely detractor themes", "likely_detractor_themes"),
        ("Watchouts", "watchouts"),
    ]:
        values = [str(v).strip() for v in list(info.get(key) or []) if str(v).strip()][:max(int(limit_per_section or 0), 1)]
        if values:
            lines.append(f"{heading}: " + ", ".join(values))
    note = _safe_text(info.get("confidence_note"))
    if note:
        lines.append(f"Knowledge note: {note}")
    return "\n".join(lines)



def _render_product_knowledge_panel(knowledge):
    knowledge = _normalize_product_knowledge(knowledge)
    visible_keys = [
        "product_archetype", "product_areas", "use_cases", "desired_outcomes", "comparison_set",
        "workflow_steps", "user_contexts", "csat_drivers", "likely_failure_modes",
        "likely_themes", "likely_delighter_themes", "likely_detractor_themes", "watchouts",
    ]
    if not any(knowledge.get(key) for key in visible_keys):
        return
    with st.container(border=True):
        st.markdown("**Generated product knowledge**")
        st.caption("Built from the review sample to make the first-pass symptom list broader, sharper, and more useful across CSAT, consumer insights, CX, product, and quality teams.")
        if knowledge.get("product_archetype"):
            st.markdown(_chip_html([(knowledge.get("product_archetype").replace("_", " ").title(), "blue")]), unsafe_allow_html=True)
        for heading, key, color in [
            ("Product areas", "product_areas", "blue"),
            ("Use cases", "use_cases", "indigo"),
            ("Desired outcomes", "desired_outcomes", "green"),
            ("Workflow steps", "workflow_steps", "gray"),
            ("Comparison set", "comparison_set", "indigo"),
            ("User contexts", "user_contexts", "blue"),
            ("CSAT drivers", "csat_drivers", "green"),
            ("Likely failure modes", "likely_failure_modes", "red"),
            ("Likely delighter themes", "likely_delighter_themes", "green"),
            ("Likely detractor themes", "likely_detractor_themes", "red"),
            ("Watchouts", "watchouts", "red"),
        ]:
            values = list(knowledge.get(key) or [])
            if values:
                st.markdown(f"**{heading}**")
                st.markdown(_chip_html([(item, color) for item in values]), unsafe_allow_html=True)
        if knowledge.get("likely_themes"):
            st.markdown("**Likely first-pass themes**")
            st.markdown(_chip_html([(item, "gray") for item in knowledge.get("likely_themes")[:12]]), unsafe_allow_html=True)
        note = _safe_text(knowledge.get("confidence_note"))
        if note:
            st.caption(note)


_GENERIC_ARCHETYPE_KEYWORDS = {
    "wireless_audio": ("earbuds", "headphones", "bluetooth", "pairing", "noise cancelling", "charging case", "anc"),
    "vacuum_floorcare": ("vacuum", "robot vacuum", "suction", "pet hair", "brushroll", "mop", "dock", "dust bin"),
    "coffee_espresso": ("coffee", "espresso", "frother", "steam wand", "portafilter", "brew", "carafe"),
    "air_fryer_oven": ("air fryer", "basket", "preheat", "crisp", "countertop oven", "tray"),
    "apparel": ("fabric", "fit", "runs small", "runs large", "shirt", "dress", "jacket", "leggings"),
    "footwear": ("shoe", "sneaker", "boot", "arch support", "insole", "toe box", "laces"),
    "mattress_bedding": ("mattress", "pillow", "cooling", "firm", "soft", "motion isolation", "off gassing", "support"),
    "drinkware": ("water bottle", "tumbler", "cup holder", "keeps cold", "keeps hot", "straw", "lid", "insulated"),
    "skincare_topical": ("serum", "cream", "moisturizer", "cleanser", "retinol", "breakout", "hydrating"),
    "oral_care": ("toothbrush", "water flosser", "reservoir", "brush head", "gums", "teeth"),
}


_GENERIC_DRIVER_LIBRARY = {
    "results": {
        "signals": ("result", "results", "performance", "coverage", "flavor", "taste", "suction", "support", "hydrat", "crisp", "volume"),
        "delighter": {"label": "Delivers Strong Results", "aliases": ["results are strong", "gets the job done"], "theme": "Performance", "family": "Results & Outcome"},
        "detractor": {"label": "Falls Short On Results", "aliases": ["results are weak", "doesn't deliver the outcome"], "theme": "Performance", "family": "Results & Outcome"},
    },
    "longevity": {
        "signals": ("hold", "last", "lasting", "staying power", "runtime", "wear time", "keeps cold", "keeps hot"),
        "delighter": {"label": "Results Last", "aliases": ["lasting results", "holds up over time"], "theme": "Reliability", "family": "Outcome Longevity"},
        "detractor": {"label": "Results Fade Quickly", "aliases": ["doesn't last", "wears off fast", "drops quickly"], "theme": "Reliability", "family": "Outcome Longevity"},
    },
    "learning_curve": {
        "signals": ("learn", "learning curve", "practice", "master", "figure out", "technique"),
        "delighter": {"label": "Easy To Learn", "aliases": ["quick to figure out", "easy learning curve"], "theme": "Ease Of Use", "family": "Learning Curve & Technique"},
        "detractor": {"label": "Hard To Learn", "aliases": ["steep learning curve", "takes practice"], "theme": "Ease Of Use", "family": "Learning Curve & Technique"},
    },
    "workflow": {
        "signals": ("workflow", "routine", "session", "attachment", "attachments", "switch", "swap", "step", "tips", "refill", "empty"),
        "delighter": {"label": "Workflow Feels Smooth", "aliases": ["easy routine", "flows well in use"], "theme": "Ease Of Use", "family": "Workflow & Attachments"},
        "detractor": {"label": "Workflow Feels Awkward", "aliases": ["routine feels clunky", "steps are awkward"], "theme": "Ease Of Use", "family": "Workflow & Attachments"},
    },
    "maintenance": {
        "signals": ("clean", "cleanup", "maintenance", "filter", "wash", "descale", "care"),
        "delighter": {"label": "Easy Routine Maintenance", "aliases": ["easy upkeep", "simple maintenance"], "theme": "Cleaning & Maintenance", "family": "Cleaning & Maintenance"},
        "detractor": {"label": "Maintenance Feels Confusing", "aliases": ["upkeep is confusing", "maintenance steps are unclear"], "theme": "Cleaning & Maintenance", "family": "Cleaning & Maintenance"},
    },
    "quality": {
        "signals": ("quality", "durable", "break", "broken", "leak", "crack", "wear", "flimsy"),
        "delighter": {"label": "Built To Last", "aliases": ["holds up well", "feels durable"], "theme": "Quality & Durability", "family": "Quality & Durability"},
        "detractor": {"label": "Breaks Too Easily", "aliases": ["doesn't hold up", "wears out too fast"], "theme": "Quality & Durability", "family": "Quality & Durability"},
    },
    "comfort_fit": {
        "signals": ("comfort", "comfortable", "support", "fit", "size", "runs small", "runs large", "heavy", "lightweight", "cup holder"),
        "delighter": {"label": "Comfortable To Use", "aliases": ["feels comfortable", "comfortable during use"], "theme": "Comfort", "family": "Comfort"},
        "detractor": {"label": "Uncomfortable To Use", "aliases": ["feels uncomfortable", "causes discomfort"], "theme": "Comfort", "family": "Comfort"},
    },
    "value": {
        "signals": ("value", "price", "worth", "expensive", "overpriced", "compare", "alternative", "premium"),
        "delighter": {"label": "Feels Worth The Price", "aliases": ["worth the price", "good value"], "theme": "Value", "family": "Value & Comparison"},
        "detractor": {"label": "Does Not Feel Worth The Price", "aliases": ["not worth the price", "overpriced for what it does"], "theme": "Value", "family": "Value & Comparison"},
    },
    "battery_connectivity": {
        "signals": ("battery", "charge", "charging", "power", "app", "wifi", "bluetooth", "pairing", "connect"),
        "delighter": {"label": "Battery And Connectivity Work Reliably", "aliases": ["battery lasts and connectivity is stable"], "theme": "Compatibility & Connectivity", "family": "Power & Connectivity"},
        "detractor": {"label": "Battery Or Connectivity Cause Friction", "aliases": ["battery drains or connection is unstable"], "theme": "Compatibility & Connectivity", "family": "Power & Connectivity"},
    },
}



def _get_symptom_whitelists(file_bytes):
    bio = io.BytesIO(file_bytes)
    df_sym = None
    try:
        df_sym = pd.read_excel(bio, sheet_name="Symptoms")
    except Exception:
        df_sym = None
    if df_sym is not None and not df_sym.empty:
        df_sym.columns = [str(c).strip() for c in df_sym.columns]
        lc = {c.lower(): c for c in df_sym.columns}
        alias_col = next((lc[c] for c in ["aliases", "alias"] if c in lc), None)
        label_col = next((lc[c] for c in ["symptom", "label", "name", "item"] if c in lc), None)
        type_col = next((lc[c] for c in ["type", "polarity", "category", "side"] if c in lc), None)
        pos_tags = {"delighter", "delighters", "positive", "pos", "pros"}
        neg_tags = {"detractor", "detractors", "negative", "neg", "cons"}

        def _clean(s):
            vals = s.dropna().astype(str).str.strip()
            out = []
            seen = set()
            for v in vals:
                if v and v not in seen:
                    seen.add(v)
                    out.append(v)
            return out

        delighters, detractors, alias_map = [], [], {}
        if label_col and type_col:
            df_sym[type_col] = df_sym[type_col].astype(str).str.lower().str.strip()
            delighters = _clean(df_sym.loc[df_sym[type_col].isin(pos_tags), label_col])
            detractors = _clean(df_sym.loc[df_sym[type_col].isin(neg_tags), label_col])
            if alias_col:
                for _, row in df_sym.iterrows():
                    lbl = str(row.get(label_col, "")).strip()
                    als = str(row.get(alias_col, "")).strip()
                    if lbl:
                        alias_map[lbl] = [p.strip() for p in als.replace(",", "|").split("|") if p.strip()] if als else []
        else:
            for lck, orig in lc.items():
                if "delight" in lck or "positive" in lck or lck == "pros":
                    delighters.extend(_clean(df_sym[orig]))
                if "detract" in lck or "negative" in lck or lck == "cons":
                    detractors.extend(_clean(df_sym[orig]))
            delighters = list(dict.fromkeys(delighters))
            detractors = list(dict.fromkeys(detractors))
        if delighters or detractors:
            delighters, detractors = _app()._canonical_symptom_catalog(delighters, detractors)
        return delighters, detractors, alias_map
    try:
        review_df, _sheet_name = _app()._read_best_uploaded_excel_sheet(file_bytes)
    except Exception:
        return [], [], {}
    if review_df is None or review_df.empty:
        return [], [], {}
    det_vals, del_vals, _, _ = _app()._symptom_filter_options(review_df)
    if del_vals or det_vals:
        del_vals, det_vals = _app()._canonical_symptom_catalog(list(del_vals), list(det_vals))
        return del_vals, det_vals, {}
    return [], [], {}



def _custom_universal_catalog():
    custom_dels, custom_dets, _ = _app()._standardize_symptom_lists(
        st.session_state.get("sym_custom_universal_delighters") or [],
        st.session_state.get("sym_custom_universal_detractors") or [],
    )
    built_in_dels = set(_UNIVERSAL_NEUTRAL_DELIGHTERS)
    built_in_dets = set(_UNIVERSAL_NEUTRAL_DETRACTORS)
    return [label for label in custom_dels if label not in built_in_dels], [label for label in custom_dets if label not in built_in_dets]



def _alias_map_for_catalog(delighters=None, detractors=None, *, extra_aliases=None, existing_aliases=None):
    labels = set(_normalize_tag_list(list(delighters or []) + list(detractors or [])))
    base = _build_taxonomy_alias_map(delighters or [], detractors or [], extra_aliases=extra_aliases or {})
    filtered_existing = {}
    for key, values in (existing_aliases or {}).items():
        label = re.sub(r"\s+", " ", str(key or "").strip()).title()
        if not label or label not in labels:
            continue
        filtered_existing[label] = [re.sub(r"\s+", " ", str(v or "").strip()).title() for v in (values or []) if str(v or "").strip()]
    merged = _merge_taxonomy_alias_maps(filtered_existing, base)
    return {key: vals for key, vals in merged.items() if key in labels and vals}



def _taxonomy_preview_items_with_side(ai_result):
    items = []
    result = ai_result or {}
    for side_key, bucket_key in (("delighter", "preview_delighters"), ("detractor", "preview_detractors")):
        for raw in (result.get(bucket_key) or []):
            row = dict(raw)
            row["side"] = side_key
            items.append(row)
    return items



def _render_structured_taxonomy_table(rows, *, key_prefix, empty_label="No taxonomy rows to show."):
    if not rows:
        st.info(empty_label)
        return
    df = pd.DataFrame(rows)
    visible_defaults = [col for col in ["L1 Theme", "L2 Symptom", "Side", "Bucket", "Review Hits", "Support %", "Aliases"] if col in df.columns]
    with st.expander("Taxonomy table tools", expanded=False):
        c1, c2, c3 = st.columns([2.0, 1.35, 1.35])
        search = c1.text_input("Search taxonomy", key=f"{key_prefix}_search", placeholder="Filter by symptom, theme, alias, or rationale")
        side_opts = ["All"] + sorted(df["Side"].dropna().astype(str).unique().tolist()) if "Side" in df.columns else ["All"]
        side_choice = c2.selectbox("Side", options=side_opts, key=f"{key_prefix}_side")
        bucket_opts = sorted(df["Bucket"].dropna().astype(str).unique().tolist()) if "Bucket" in df.columns else []
        bucket_choice = c3.multiselect("Buckets", options=bucket_opts, default=bucket_opts, key=f"{key_prefix}_bucket")
        c4, c5, c6 = st.columns([1.35, 1.0, 2.65])
        theme_opts = sorted(df["L1 Theme"].dropna().astype(str).unique().tolist()) if "L1 Theme" in df.columns else []
        theme_choice = c4.multiselect("L1 themes", options=theme_opts, default=theme_opts, key=f"{key_prefix}_theme")
        row_options = [25, 50, 100, 250, "All"]
        row_choice = c5.selectbox("Rows", options=row_options, index=1, key=f"{key_prefix}_rows")
        visible_cols = c6.multiselect("Visible columns", options=df.columns.tolist(), default=visible_defaults or df.columns.tolist(), key=f"{key_prefix}_visible")
    filtered = df.copy()
    if search:
        pattern = re.escape(str(search).strip())
        searchable_cols = [col for col in ["L2 Symptom", "L1 Theme", "Aliases", "Rationale", "Example"] if col in filtered.columns]
        if searchable_cols:
            mask = pd.Series(False, index=filtered.index)
            for col in searchable_cols:
                mask = mask | filtered[col].astype(str).str.contains(pattern, case=False, na=False)
            filtered = filtered[mask]
    if side_choice != "All" and "Side" in filtered.columns:
        filtered = filtered[filtered["Side"] == side_choice]
    if bucket_choice and "Bucket" in filtered.columns:
        filtered = filtered[filtered["Bucket"].isin(bucket_choice)]
    if theme_choice and "L1 Theme" in filtered.columns:
        filtered = filtered[filtered["L1 Theme"].isin(theme_choice)]
    sort_cols = []
    sort_ascending = []
    for col, asc in (("Side", True), ("L1 Theme", True), ("Review Hits", False), ("L2 Symptom", True)):
        if col in filtered.columns:
            sort_cols.append(col)
            sort_ascending.append(asc)
    if sort_cols:
        filtered = filtered.sort_values(sort_cols, ascending=sort_ascending, kind="mergesort")
    if row_choice != "All":
        filtered = filtered.head(int(row_choice))
    visible_cols = [col for col in (visible_cols or visible_defaults or df.columns.tolist()) if col in filtered.columns] or filtered.columns.tolist()
    st.markdown(_chip_html([
        (f"{filtered['L1 Theme'].nunique():,} themes" if "L1 Theme" in filtered.columns else f"{len(filtered):,} rows", "blue"),
        (f"{filtered['L2 Symptom'].nunique():,} L2 symptoms" if "L2 Symptom" in filtered.columns else f"{len(filtered):,} rows", "indigo"),
        (f"{len(filtered):,} rows showing", "gray"),
    ]), unsafe_allow_html=True)
    if filtered.empty:
        st.info("No taxonomy rows match the current filters.")
        return
    st.dataframe(filtered[visible_cols], use_container_width=True, hide_index=True, height=int(min(max(360, 36 * len(filtered) + 72), 760)))


