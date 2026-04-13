"""Symptom display: dashboard, scatter, taxonomy workbench, inline editing

Extracted from app.py. Uses _app() for cross-module access.
"""
from __future__ import annotations
import html, json, math, re, sys
from typing import Any, Dict, List, Optional, Sequence
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


def _render_symptom_dashboard(filtered_df, overall_df=None):
    od = overall_df if overall_df is not None else filtered_df
    sym_state = _app()._detect_symptom_state(od)
    st.markdown("<hr class='sw-divider'>", unsafe_allow_html=True)
    if sym_state == "none":
        st.markdown("""<div class="sym-state-banner">
          <div class="icon">💊</div><div class="title">No symptoms tagged yet</div>
          <div class="sub">Run the <strong>Symptomizer</strong> tab to AI-tag delighters and detractors,
          then return here for the full analytics.<br>
          If your file already contains <em>Symptom 1–20</em> or <em>AI Symptom</em> columns they'll appear automatically.</div>
        </div>""", unsafe_allow_html=True)
        return
    if sym_state == "partial":
        det_cols, del_cols = _app()._get_symptom_col_lists(od)
        missing = []
        if not _app()._filled_mask(od, det_cols).any():
            missing.append("detractors")
        if not _app()._filled_mask(od, del_cols).any():
            missing.append("delighters")
        if missing:
            st.info(f"Partial tagging — {' and '.join(missing)} not yet labelled.")
    st.markdown("<div class='section-title'>🩺 Detractors & Delighters</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>% Tagged Reviews uses only symptomized reviews. Net Hit stays signed and observed, Forecast Δ★ keeps shrinkage for steadier forecasting, and Impact Score now layers in confidence and symptom severity so the tables surface the most actionable themes first.</div>", unsafe_allow_html=True)
    with st.expander("How % Tagged Reviews, Net Hit, and Forecast Δ★ work", expanded=False):
        st.markdown("""
- **% Tagged Reviews** = review mentions / reviews in the current view that already have at least one tag on that side (detractor or delighter).
- **Avg Star** = average star rating of reviews that mention the symptom.
- **Avg Tags/Review** shows how many same-side symptoms those reviews usually carry. Higher values mean the review impact is split across more themes.
- **Net Hit** is the observed signed star impact in the filtered review set. Detractors are negative, delighters are positive, and each review's rating gap versus the product average is divided across the same-side symptoms tagged in that review.
- **Confidence %** blends sample size, weighted review share, and rating alignment. Themes mentioned in few reviews or with weak star separation get pushed down.
- **Forecast Δ★** is the shrunk, confidence-adjusted version of Net Hit. It is the steadier estimate to use when forecasting what each symptom is doing to rating.
- **Severity Wt** lightly boosts engineering-critical themes like breakage, defects, leaks, or irritation and lightly down-weights aesthetic-only themes.
- **Impact Score** = Forecast Δ★ × Severity Wt. It is the best single sort for what deserves action first.
- **Impact |Abs|** in Table tools is the magnitude view of Impact Score when you want the strongest movers regardless of sign.
        """)
    det_cols, del_cols = _app()._get_symptom_col_lists(od)
    avg_star = float(_app()._safe_mean(filtered_df["rating"]) or 0)
    total_reviews = len(filtered_df)
    det_base = _app().analyze_symptoms_fast(filtered_df, det_cols)
    del_base = _app().analyze_symptoms_fast(filtered_df, del_cols)
    det_tbl = _add_net_hit(det_base, avg_star, total_reviews=total_reviews, kind="detractors", detail_df=filtered_df, symptom_cols=det_cols)
    del_tbl = _add_net_hit(del_base, avg_star, total_reviews=total_reviews, kind="delighters", detail_df=filtered_df, symptom_cols=del_cols)
    st.caption("Full-width interactive tables below keep every metric visible. Click a header to sort, or open Table tools for saved filters, severity-aware impact ranking, and column visibility.")
    t1, t2 = st.tabs([f"🔴 Detractors ({len(det_tbl):,})", f"🟢 Delighters ({len(del_tbl):,})"])
    with t1:
        with st.container(border=True):
            _render_interactive_symptom_table(det_tbl, key_prefix="sw_det_table", empty_label="detractor")
    with t2:
        with st.container(border=True):
            _render_interactive_symptom_table(del_tbl, key_prefix="sw_del_table", empty_label="delighter")
    try:
        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            det_tbl.to_excel(writer, sheet_name="Detractors", index=False)
            del_tbl.to_excel(writer, sheet_name="Delighters", index=False)
        ds = st.session_state.get("analysis_dataset") or {}
        pid = (ds.get("summary") and ds["summary"].product_id) or "symptoms"
        st.download_button(
            "⬇️ Download Detractors + Delighters",
            data=out_xlsx.getvalue(),
            file_name=f"{pid}_symptoms.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="sw_sym_dl",
        )
    except Exception:
        pass
    st.markdown("<hr class='sw-divider'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>📊 Top Themes</div>", unsafe_allow_html=True)
    bar_ctrl = st.columns([1, 1, 1.2])
    top_n = int(bar_ctrl[0].slider("Top N", 5, 25, 12, 1, key="sw_top_n"))
    org_only = bar_ctrl[1].toggle("Organic only", value=False, key="sw_org_bar")
    show_pct = bar_ctrl[2].toggle("Show %", value=False, key="sw_pct_bar")
    bar_df = filtered_df[~filtered_df["incentivized_review"].fillna(False)] if org_only else filtered_df
    denom = max(1, len(bar_df))
    det_top = _app().analyze_symptoms_fast(bar_df, det_cols).head(top_n)
    del_top = _app().analyze_symptoms_fast(bar_df, del_cols).head(top_n)
    bc1, bc2 = st.columns(2)
    with bc1:
        with st.container(border=True):
            _app()._render_symptom_bar_chart(det_top, "Top Detractors", "#ef4444", denom, show_pct)
    with bc2:
        with st.container(border=True):
            _app()._render_symptom_bar_chart(del_top, "Top Delighters", "#22c55e", denom, show_pct)
    st.markdown("<hr class='sw-divider'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>🎯 Opportunity Matrix</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Mentions vs Avg ★ · Fix high-mention low-star detractors first · Amplify high-mention high-star delighters.</div>", unsafe_allow_html=True)
    opp_t1, opp_t2 = st.tabs(["🔴 Detractors", "🟢 Delighters"])
    with opp_t1:
        _opp_scatter(det_tbl, "detractors", avg_star, container_key="dash")
    with opp_t2:
        _opp_scatter(del_tbl, "delighters", avg_star, container_key="dash")

# ═══════════════════════════════════════════════════════════════════════════════
#  FILTERS
# ═══════════════════════════════════════════════════════════════════════════════
CORE_REVIEW_FILTER_SPECS = [
    {"key": "product_or_sku", "label": "SKU / Product", "kind": "column", "column": "product_or_sku"},
    {"key": "content_locale", "label": "Market / Locale", "kind": "column", "column": "content_locale"},
    {"key": "retailer", "label": "Retailer", "kind": "column", "column": "retailer"},
    {"key": "source_system", "label": "Source System", "kind": "column", "column": "source_system"},
    {"key": "loaded_from_host", "label": "Loaded Site", "kind": "column", "column": "loaded_from_host"},
    {"key": "source_file", "label": "Source File", "kind": "column", "column": "source_file"},
    {"key": "age_group", "label": "Age Group", "kind": "column", "column": "age_group"},
    {"key": "user_location", "label": "Reviewer Location", "kind": "column", "column": "user_location"},
    {"key": "review_type", "label": "Review Type", "kind": "derived"},
    {"key": "recommendation", "label": "Recommendation", "kind": "derived"},
    {"key": "syndication", "label": "Syndication", "kind": "derived"},
    {"key": "media", "label": "Media", "kind": "derived"},
]



def _render_interactive_symptom_table(tbl, *, key_prefix, empty_label):
    display = _app()._symptom_table_for_display(tbl)
    if display.empty:
        st.info(f"No {empty_label.lower()} data.")
        return

    st.caption("Click any column header to sort instantly. Confidence and Impact Score help separate stable high-signal themes from noisy one-offs.")
    with st.expander("Table tools", expanded=False):
        r1c1, r1c2, r1c3 = st.columns([2.2, 1.05, 1.05])
        search = r1c1.text_input("Search", key=f"{key_prefix}_search", placeholder="Filter symptom names")
        min_mentions = int(r1c2.number_input("Min mentions", min_value=0, value=0, step=1, key=f"{key_prefix}_min_mentions"))
        min_pct = float(r1c3.number_input("Min % tagged", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key=f"{key_prefix}_min_pct"))
        r2c1, r2c2, r2c3 = st.columns([1.35, 1.0, 2.65])
        sort_candidates = [c for c in (_SYMPTOM_TABLE_BASE_COLUMNS + _SYMPTOM_TABLE_AUX_COLUMNS) if c in display.columns]
        default_sort = "Impact |Abs|" if "Impact |Abs|" in sort_candidates else ("Impact Score" if "Impact Score" in sort_candidates else ("Mentions" if "Mentions" in sort_candidates else sort_candidates[0]))
        sort_by = r2c1.selectbox("Sort by", options=sort_candidates, index=sort_candidates.index(default_sort), key=f"{key_prefix}_sort_by")
        descending_default = sort_by != "Item"
        descending = bool(r2c2.toggle("Descending", value=descending_default, key=f"{key_prefix}_descending"))
        row_options = [25, 50, 100, 250, "All"]
        row_choice = r2c2.selectbox("Rows", options=row_options, index=1, key=f"{key_prefix}_row_limit")
        visible_options = [c for c in (_SYMPTOM_TABLE_BASE_COLUMNS + _SYMPTOM_TABLE_AUX_COLUMNS) if c in display.columns]
        visible_default = [c for c in _SYMPTOM_TABLE_BASE_COLUMNS if c in display.columns]
        visible_cols = r2c3.multiselect("Visible columns", options=visible_options, default=visible_default, key=f"{key_prefix}_visible_cols")

    filtered = display.copy()
    if search:
        mask = filtered["Item"].str.contains(re.escape(str(search).strip()), case=False, na=False)
        if "L1 Theme" in filtered.columns:
            mask = mask | filtered["L1 Theme"].astype(str).str.contains(re.escape(str(search).strip()), case=False, na=False)
        filtered = filtered[mask]
    if "Mentions" in filtered.columns:
        filtered = filtered[filtered["Mentions"].fillna(0) >= min_mentions]
    if "% Tagged Reviews" in filtered.columns:
        filtered = filtered[filtered["% Tagged Reviews"].fillna(0) >= min_pct]
    if sort_by in filtered.columns:
        filtered = filtered.sort_values(sort_by, ascending=not descending, na_position="last", kind="mergesort")
    if row_choice != "All":
        filtered = filtered.head(int(row_choice))
    visible_cols = [c for c in (visible_cols or visible_default) if c in filtered.columns] or [c for c in visible_default if c in filtered.columns]
    st.markdown(_chip_html([
        (f"{len(filtered):,} rows showing", "indigo"),
        (f"{len(display):,} total", "gray"),
        (f"Sorted by {sort_by}", "blue"),
    ]), unsafe_allow_html=True)
    if filtered.empty:
        st.info("No rows match the current symptom table filters.")
        return
    height_px = int(min(max(360, 36 * len(filtered) + 72), 760))
    st.dataframe(filtered[visible_cols], use_container_width=True, hide_index=True, height=height_px, column_config=_app()._symptom_table_column_config(filtered[visible_cols]))



def _opp_scatter(tbl, kind, baseline_avg, *, container_key=""):

    if tbl is None or tbl.empty:
        st.info("No data available.")
        return
    d = tbl.copy()
    d["Mentions"] = pd.to_numeric(d.get("Mentions"), errors="coerce").fillna(0)
    d["Avg Star"] = pd.to_numeric(d.get("Avg Star"), errors="coerce")
    d = d.dropna(subset=["Avg Star"])
    if d.empty:
        st.info("No data available.")
        return
    x = d["Mentions"].astype(float).to_numpy()
    y = d["Avg Star"].astype(float).to_numpy()
    names = d["Item"].astype(str).to_numpy()
    avg_tags = pd.to_numeric(d.get("Avg Tags/Review"), errors="coerce").fillna(np.nan) if "Avg Tags/Review" in d.columns else pd.Series(np.nan, index=d.index)
    confidence_col = pd.to_numeric(d.get("Confidence %"), errors="coerce") if "Confidence %" in d.columns else pd.Series(dtype=float)
    impact_col = pd.to_numeric(d.get("Impact Score"), errors="coerce") if "Impact Score" in d.columns else pd.Series(dtype=float)
    forecast_col = pd.to_numeric(d.get("Forecast Δ★"), errors="coerce") if "Forecast Δ★" in d.columns else pd.Series(dtype=float)
    if not impact_col.empty and impact_col.notna().any():
        score_signed = impact_col.fillna(0).to_numpy()
        score_strength = np.abs(score_signed)
        score_label = "Impact Score"
    elif not forecast_col.empty and forecast_col.notna().any():
        score_signed = forecast_col.fillna(0).to_numpy()
        score_strength = np.abs(score_signed)
        score_label = "Forecast Δ★"
    else:
        score_signed = (x * np.clip(float(baseline_avg) - y, 0, None) * (-1 if kind == "detractors" else 1))
        score_strength = np.abs(score_signed)
        score_label = "Impact |Abs|"
    show_labels = st.toggle("Show labels", value=False, key=f"opp_lbl_{kind}_{container_key}")
    labels_arr = np.array([""] * len(d), dtype=object)
    if show_labels:
        top_idx = np.argsort(-score_strength)[:10]
        labels_arr[top_idx] = names[top_idx]
    mx = max(float(np.nanmax(x)), 1e-9)
    size = (np.sqrt(x) / np.sqrt(mx)) * 24 + 8
    color = "#ef4444" if kind == "detractors" else "#22c55e"
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x,
        y=y,
        mode="markers+text" if show_labels else "markers",
        text=labels_arr,
        textposition="top center",
        textfont=dict(size=10, family="Inter"),
        customdata=np.column_stack([
            names,
            avg_tags.to_numpy(),
            forecast_col.reindex(d.index).fillna(0).to_numpy() if not forecast_col.empty else np.zeros(len(d)),
            impact_col.reindex(d.index).fillna(0).to_numpy() if not impact_col.empty else np.zeros(len(d)),
            confidence_col.reindex(d.index).fillna(0).to_numpy() if not confidence_col.empty else np.zeros(len(d)),
        ]),
        hovertemplate="%{customdata[0]}<br>Mentions=%{x:.0f}<br>Avg ★=%{y:.2f}<br>Avg tags/review=%{customdata[1]:.2f}<br>Forecast Δ★=%{customdata[2]:.3f}<br>Impact Score=%{customdata[3]:.3f}<br>Confidence=%{customdata[4]:.1f}%<extra></extra>",
        marker=dict(size=size, color=color, opacity=0.76, line=dict(width=1, color="rgba(148,163,184,0.38)")),
    ))
    fig.add_hline(y=float(baseline_avg), line_dash="dash", opacity=0.45, annotation_text=f"Avg ★ {baseline_avg:.2f}", annotation_position="right", annotation_font_size=11)
    fig.update_layout(height=420, xaxis_title="Mentions", yaxis_title="Avg ★")
    _app()._sw_style_fig(fig)
    _app()._show_plotly(fig)
    label = "Fix first — high mentions × below-baseline ★" if kind == "detractors" else "Amplify — high mentions × above-baseline ★"
    top15 = d.copy()
    top15[score_label] = score_strength
    top15 = top15.sort_values(score_label, ascending=False).head(15)
    with st.expander(f"📋 {label}", expanded=False):
        keep_cols = [c for c in ["Item", "Mentions", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", score_label] if c in top15.columns]
        ds = top15[keep_cols].copy()
        ds["Avg Star"] = ds["Avg Star"].map(lambda v: f"{float(v):.1f}" if pd.notna(v) else "—")
        if "Avg Tags/Review" in ds.columns:
            ds["Avg Tags/Review"] = ds["Avg Tags/Review"].map(lambda v: f"{float(v):.2f}" if pd.notna(v) else "—")
        if "Confidence %" in ds.columns:
            ds["Confidence %"] = ds["Confidence %"].map(lambda v: f"{float(v):.1f}%" if pd.notna(v) else "—")
        for col in ["Net Hit", "Forecast Δ★", "Impact Score", score_label]:
            if col in ds.columns:
                ds[col] = ds[col].map(lambda v: f"{float(v):.3f}" if pd.notna(v) else "—")
        st.dataframe(ds, use_container_width=True, hide_index=True)



def _add_net_hit(tbl, avg_rating, total_reviews=None, *, kind="detractors", shrink_k=3.0, detail_df=None, symptom_cols=None):
    if tbl is None or tbl.empty:
        return tbl
    d = tbl.copy()
    baseline = float(avg_rating or 0)
    sign = 1.0 if str(kind).lower().startswith("del") else -1.0
    total_reviews = int(total_reviews or _app()._infer_symptom_total_reviews(d) or (len(detail_df) if detail_df is not None else 0) or 0)
    if total_reviews <= 0:
        total_reviews = max(int(pd.to_numeric(d.get("Mentions"), errors="coerce").fillna(0).sum() or 0), 1)

    details = None
    symptomized_reviews = int(d.attrs.get("symptomized_review_count") or 0)
    if detail_df is not None and symptom_cols:
        details, symptomized_reviews = _compute_detailed_symptom_impact(detail_df, symptom_cols, baseline, kind=kind)
        if not details.empty:
            details = details.copy()
            details.index = details.index.to_series().astype(str).str.title()

    d["Item"] = d.get("Item", pd.Series(dtype="string")).astype("string").fillna("").str.strip().str.title()
    d["Mentions"] = pd.to_numeric(d.get("Mentions"), errors="coerce").fillna(0).astype(int)
    d["Avg Star"] = pd.to_numeric(d.get("Avg Star"), errors="coerce")

    if details is not None and not details.empty:
        mention_reviews = d["Item"].map(details["Mention Reviews"]).fillna(d["Mentions"]).astype(float)
        weighted_mentions = d["Item"].map(details["Weighted Mentions"]).fillna(mention_reviews).astype(float)
        d["Mentions"] = mention_reviews.astype(int)
        d["Avg Tags/Review"] = d["Item"].map(details["Avg Tags/Review"]).fillna(pd.to_numeric(d.get("Avg Tags/Review"), errors="coerce"))
        d["Avg Star"] = d["Item"].map(details["Avg Star"]).fillna(d["Avg Star"])
        raw_impact = d["Item"].map(details["Net Hit Raw"]).fillna(0.0).astype(float) / float(max(total_reviews, 1))
        review_conf = mention_reviews / (mention_reviews + float(max(shrink_k, 0.1)))
        weight_conf = weighted_mentions / (weighted_mentions + float(max(shrink_k / 2.0, 0.5)))
        align_conf = _app()._alignment_confidence(d["Avg Star"], baseline, kind=kind).astype(float)
        confidence = np.sqrt(review_conf * weight_conf) * align_conf
        d["Net Hit"] = (sign * raw_impact).round(3)
        d["Forecast Δ★"] = (d["Net Hit"].astype(float) * confidence).round(3)
        if symptomized_reviews <= 0:
            symptomized_reviews = int(details["Mention Reviews"].max() or 0)
    else:
        filled_stars = d["Avg Star"].fillna(baseline)
        if str(kind).lower().startswith("del"):
            rating_gap = (filled_stars - baseline).clip(lower=0)
        else:
            rating_gap = (baseline - filled_stars).clip(lower=0)
        share = d["Mentions"].astype(float) / float(max(total_reviews, 1))
        base_conf = d["Mentions"].astype(float) / (d["Mentions"].astype(float) + float(max(shrink_k, 0.1)))
        align_conf = _app()._alignment_confidence(filled_stars, baseline, kind=kind).astype(float)
        confidence = base_conf * align_conf
        d["Net Hit"] = (sign * share * rating_gap).round(3)
        d["Forecast Δ★"] = (d["Net Hit"].astype(float) * confidence).round(3)
        if "Avg Tags/Review" not in d.columns:
            d["Avg Tags/Review"] = np.nan

    if symptomized_reviews <= 0 and "__Symptomized Reviews" in d.columns:
        symptomized_reviews = int(pd.to_numeric(d["__Symptomized Reviews"], errors="coerce").fillna(0).max() or 0)
    if symptomized_reviews <= 0:
        symptomized_reviews = max(int(d["Mentions"].max() or 0), 1)

    pct_vals = (d["Mentions"].astype(float) / float(max(symptomized_reviews, 1)) * 100).round(1)
    d["% Tagged Reviews"] = pct_vals.astype(str) + "%"
    d["Avg Tags/Review"] = pd.to_numeric(d.get("Avg Tags/Review"), errors="coerce").round(2)
    d["Confidence %"] = (pd.to_numeric(confidence, errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0) * 100.0).round(1)
    d["Severity Wt"] = d["Item"].map(lambda value: round(float(_app()._label_severity_weight(value, kind=kind)), 2))
    d["Impact Score"] = (d["Forecast Δ★"].astype(float) * d["Severity Wt"].astype(float)).round(3)
    d.attrs["symptomized_review_count"] = symptomized_reviews
    d.attrs["all_review_count"] = total_reviews
    d["_impact_sort"] = np.maximum.reduce([
        d["Impact Score"].astype(float).abs(),
        d["Forecast Δ★"].astype(float).abs(),
        d["Net Hit"].astype(float).abs(),
    ])
    cols = [c for c in ["Item", "Mentions", "% Tagged Reviews", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", "Severity Wt"] if c in d.columns]
    return d.sort_values(["_impact_sort", "Mentions", "Item"], ascending=[False, False, True], ignore_index=True)[cols]



def _prepare_symptom_long(df_in, symptom_cols):
    if df_in is None or df_in.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0
    targets = {str(col).strip() for col in symptom_cols if str(col).strip()}
    if not targets:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0
    col_names = [str(col).strip() for col in df_in.columns]
    positions = [idx for idx, name in enumerate(col_names) if name in targets]
    if not positions:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0

    block = df_in.iloc[:, positions].copy()
    block.columns = [f"__sym_{idx}" for idx in range(block.shape[1])]
    block.insert(0, "__row", np.arange(len(block), dtype=int))
    long = block.melt(id_vars="__row", value_name="symptom", var_name="__col")
    s = long["symptom"].astype("string").fillna("").str.strip()
    mask = (s != "") & (~s.str.upper().isin(_app().SYMPTOM_NON_VALUES)) & (~s.str.startswith("<"))
    long = long.loc[mask, ["__row"]].copy()
    if long.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0

    long["symptom"] = s.loc[mask].str.title()
    long = long.drop_duplicates(subset=["__row", "symptom"])
    if long.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0

    symptomized_reviews = int(long["__row"].nunique())
    long = long.loc[~long["symptom"].isin(_app()._analytics_excluded_symptom_labels())].copy()
    if long.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), symptomized_reviews

    counts = long.groupby("__row", dropna=False)["symptom"].transform("nunique").astype(float)
    long["symptom_count"] = counts
    long["review_weight"] = (1.0 / counts.replace(0, np.nan)).fillna(0.0)
    if "rating" in df_in.columns:
        stars = pd.to_numeric(df_in.reset_index(drop=True)["rating"], errors="coerce").rename("star")
        long = long.join(stars, on="__row")
    else:
        long["star"] = np.nan
    return long, symptomized_reviews



def _build_theme_impact_table(df_in, symptom_cols, theme_map, *, avg_rating, kind="detractors", shrink_k=3.0):
    long, symptomized_reviews = _prepare_symptom_long(df_in, symptom_cols)
    if long.empty:
        return pd.DataFrame(columns=["Item", "Mentions", "% Tagged Reviews", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", "Severity Wt"])
    working = long.copy()
    working["symptom"] = working["symptom"].astype(str).str.title()
    working["Item"] = working["symptom"].map(lambda value: theme_map.get(str(value).title(), "Product Specific"))
    working = working[working["Item"].astype(str).str.strip() != ""]
    if working.empty:
        return pd.DataFrame(columns=["Item", "Mentions", "% Tagged Reviews", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", "Severity Wt"])
    working["severity_wt"] = working["symptom"].map(lambda value: float(_app()._label_severity_weight(value, kind=kind)))
    theme_review = working.groupby(["__row", "Item"], dropna=False).agg(
        star=("star", "first"),
        theme_l2_count=("symptom", "nunique"),
        theme_weight=("review_weight", "sum"),
        severity_wt=("severity_wt", "mean"),
    ).reset_index()
    baseline = float(avg_rating or 0)
    stars = pd.to_numeric(theme_review["star"], errors="coerce")
    sign = 1.0 if str(kind).lower().startswith("del") else -1.0
    if str(kind).lower().startswith("del"):
        gap = (stars - baseline).clip(lower=0)
    else:
        gap = (baseline - stars).clip(lower=0)
    theme_review["gap"] = gap.fillna(0.0)
    theme_review["attributed_gap"] = theme_review["theme_weight"].astype(float) * theme_review["gap"].astype(float)

    mention_reviews = theme_review.groupby("Item", dropna=False)["__row"].nunique().astype(int)
    avg_l2 = theme_review.groupby("Item", dropna=False)["theme_l2_count"].mean()
    avg_stars = theme_review.groupby("Item", dropna=False)["star"].mean()
    weighted_mentions = theme_review.groupby("Item", dropna=False)["theme_weight"].sum()
    severity = theme_review.groupby("Item", dropna=False)["severity_wt"].mean().fillna(1.0)
    raw_impact = theme_review.groupby("Item", dropna=False)["attributed_gap"].sum().astype(float) / float(max(len(df_in), 1))
    review_conf = mention_reviews.astype(float) / (mention_reviews.astype(float) + float(max(shrink_k, 0.1)))
    weight_conf = weighted_mentions.astype(float) / (weighted_mentions.astype(float) + float(max(shrink_k / 2.0, 0.5)))
    align_conf = _app()._alignment_confidence(avg_stars, baseline, kind=kind).astype(float)
    confidence = np.sqrt(review_conf * weight_conf) * align_conf

    out = pd.DataFrame({
        "Item": [str(item) for item in mention_reviews.index.tolist()],
        "Mentions": mention_reviews.values.astype(int),
        "% Tagged Reviews": (mention_reviews.values / float(max(symptomized_reviews, 1)) * 100.0).round(1).astype(str) + "%",
        "Avg Star": [round(float(avg_stars[item]), 1) if item in avg_stars and not pd.isna(avg_stars[item]) else None for item in mention_reviews.index],
        "Avg Tags/Review": np.round(avg_l2.values.astype(float), 2),
        "Confidence %": (confidence.reindex(mention_reviews.index).fillna(0.0).clip(lower=0.0, upper=1.0) * 100.0).round(1).values,
        "Net Hit": (sign * raw_impact.reindex(mention_reviews.index).fillna(0.0)).round(3).values,
        "Forecast Δ★": (sign * raw_impact.reindex(mention_reviews.index).fillna(0.0) * confidence.reindex(mention_reviews.index).fillna(0.0)).round(3).values,
        "Severity Wt": severity.reindex(mention_reviews.index).round(2).values,
    })
    out["Impact Score"] = (pd.to_numeric(out["Forecast Δ★"], errors="coerce") * pd.to_numeric(out["Severity Wt"], errors="coerce")).round(3)
    out.attrs["symptomized_review_count"] = symptomized_reviews
    out.attrs["all_review_count"] = int(len(df_in))
    return out.sort_values(["Impact Score", "Mentions", "Item"], ascending=[True if str(kind).lower().startswith("det") else False, False, True], kind="mergesort", ignore_index=True)



def _render_ai_taxonomy_preview_table(preview_items, *, key_prefix, side_label):
    rows = []
    for item in preview_items or []:
        rows.append({
            "L1 Theme": _safe_text(item.get("l1_theme") or item.get("theme") or item.get("family")) or "Product Specific",
            "L2 Symptom": _safe_text(item.get("label")) or "—",
            "Bucket": _safe_text(item.get("bucket")) or "Product Specific",
            "Family": _safe_text(item.get("family")) or "—",
            "Review Hits": int(item.get("review_hits", 0) or 0),
            "Support %": round(float(item.get("support_ratio", 0.0) or 0.0) * 100.0, 1),
            "Aliases": ", ".join(list(item.get("aliases") or [])[:4]) or "—",
            "Example": (_safe_text((item.get("examples") or [""])[0]) or "—"),
            "Rationale": _safe_text(item.get("rationale")) or "—",
        })
    df = pd.DataFrame(rows)
    if df.empty:
        st.info(f"No {side_label.lower()} were generated for the current sample.")
        return
    st.markdown(_chip_html([
        (f"{df['L1 Theme'].nunique():,} themes", "blue"),
        (f"{df['L2 Symptom'].nunique():,} L2 symptoms", "indigo"),
        (f"{int((df['Bucket'] == 'Category Driver').sum())} category drivers", "gray"),
    ]), unsafe_allow_html=True)
    st.caption("This preview is already structured as L1 Themes → L2 Symptoms so it is easier to consolidate before you accept the taxonomy.")
    with st.expander("Preview table tools", expanded=False):
        c1, c2, c3 = st.columns([2.0, 1.2, 2.4])
        search = c1.text_input("Search preview", key=f"{key_prefix}_search", placeholder="Filter symptoms, themes, or aliases")
        bucket_filter = c2.multiselect("Buckets", options=sorted(df["Bucket"].dropna().unique().tolist()), default=sorted(df["Bucket"].dropna().unique().tolist()), key=f"{key_prefix}_buckets")
        visible_cols = c3.multiselect("Visible columns", options=df.columns.tolist(), default=[c for c in ["L1 Theme", "L2 Symptom", "Bucket", "Review Hits", "Aliases", "Example"] if c in df.columns], key=f"{key_prefix}_cols")
    if search:
        pattern = re.escape(search)
        mask = (
            df["L2 Symptom"].str.contains(pattern, case=False, na=False)
            | df["L1 Theme"].str.contains(pattern, case=False, na=False)
            | df["Aliases"].str.contains(pattern, case=False, na=False)
            | df["Family"].str.contains(pattern, case=False, na=False)
        )
        df = df[mask]
    if bucket_filter:
        df = df[df["Bucket"].isin(bucket_filter)]
    visible_cols = [c for c in (visible_cols or df.columns.tolist()) if c in df.columns] or df.columns.tolist()
    st.dataframe(df[visible_cols], use_container_width=True, hide_index=True, height=int(min(max(320, 36 * len(df) + 72), 700)))



def _render_symptomizer_taxonomy_workbench(*, processed_df, delighters, detractors, aliases, category, preview_items):
    taxonomy_rows = _build_structured_taxonomy_rows(
        delighters,
        detractors,
        aliases=aliases,
        category=category,
        preview_items=preview_items,
    )
    if not taxonomy_rows:
        return
    merge_rows = _suggest_taxonomy_merges(taxonomy_rows)
    rows_df = pd.DataFrame(taxonomy_rows)
    l1_summary = pd.DataFrame()
    if not rows_df.empty:
        l1_summary = (
            rows_df.groupby(["Side", "L1 Theme"], dropna=False)
            .agg(
                L2_Symptoms=("L2 Symptom", "nunique"),
                Category_Drivers=("Bucket", lambda s: int((pd.Series(s).astype(str) == "Category Driver").sum())),
                Product_Specific=("Bucket", lambda s: int((pd.Series(s).astype(str) == "Product Specific").sum())),
                Universal_Neutral=("Bucket", lambda s: int((pd.Series(s).astype(str) == "Universal Neutral").sum())),
            )
            .reset_index()
            .rename(columns={"L1 Theme": "Item", "L2_Symptoms": "L2 Symptoms", "Category_Drivers": "Category Drivers", "Product_Specific": "Product Specific", "Universal_Neutral": "Universal Neutral"})
            .sort_values(["Side", "L2 Symptoms", "Item"], ascending=[True, False, True], ignore_index=True)
        )
    det_map = {row["L2 Symptom"]: row["L1 Theme"] for row in taxonomy_rows if row.get("side_key") == "detractor"}
    del_map = {row["L2 Symptom"]: row["L1 Theme"] for row in taxonomy_rows if row.get("side_key") == "delighter"}
    avg_rating = float(pd.to_numeric(processed_df.get("rating"), errors="coerce").mean() or 0.0) if processed_df is not None and not processed_df.empty else 0.0
    det_cols = [col for col in _app().AI_DET_HEADERS if col in processed_df.columns]
    del_cols = [col for col in _app().AI_DEL_HEADERS if col in processed_df.columns]
    det_l2 = _add_net_hit(_app().analyze_symptoms_fast(processed_df, det_cols), avg_rating, total_reviews=len(processed_df), kind="detractors", detail_df=processed_df, symptom_cols=det_cols) if det_cols else pd.DataFrame()
    del_l2 = _add_net_hit(_app().analyze_symptoms_fast(processed_df, del_cols), avg_rating, total_reviews=len(processed_df), kind="delighters", detail_df=processed_df, symptom_cols=del_cols) if del_cols else pd.DataFrame()
    if not det_l2.empty:
        det_l2["L1 Theme"] = det_l2["Item"].map(lambda value: det_map.get(str(value).title(), "Product Specific"))
    if not del_l2.empty:
        del_l2["L1 Theme"] = del_l2["Item"].map(lambda value: del_map.get(str(value).title(), "Product Specific"))
    det_l1 = _build_theme_impact_table(processed_df, det_cols, det_map, avg_rating=avg_rating, kind="detractors") if det_cols else pd.DataFrame()
    del_l1 = _build_theme_impact_table(processed_df, del_cols, del_map, avg_rating=avg_rating, kind="delighters") if del_cols else pd.DataFrame()

    st.markdown("### 4 · Structured taxonomy")
    st.markdown("<div class='section-sub'>The symptom catalog is now organized as <b>L1 Themes</b> and <b>L2 Symptoms</b>. Use this workbench to inspect consolidation opportunities and review both theme-level and symptom-level impact without leaving the Symptomizer tab.</div>", unsafe_allow_html=True)
    st.markdown(_chip_html([
        (f"{rows_df['L1 Theme'].nunique():,} L1 themes", "blue"),
        (f"{rows_df['L2 Symptom'].nunique():,} L2 symptoms", "indigo"),
        (f"{len(merge_rows):,} consolidation cues", "gray" if merge_rows else "green"),
        (f"{len(processed_df):,} processed reviews", "green"),
    ]), unsafe_allow_html=True)

    tabs = st.tabs(["🗂 Taxonomy map", "🔗 Consolidation cues", "🔴 Detractor impact", "🟢 Delighter impact"])
    with tabs[0]:
        st.caption("Systematic taxonomy view: each L2 symptom rolls up into one L1 theme, with bucket, aliases, and sample support kept in one place.")
        if not l1_summary.empty:
            st.dataframe(l1_summary, use_container_width=True, hide_index=True, height=int(min(max(240, 34 * len(l1_summary) + 72), 520)))
        _render_structured_taxonomy_table(taxonomy_rows, key_prefix="sym_struct_taxonomy")
    with tabs[1]:
        if merge_rows:
            st.caption("These are not auto-merged. They are consolidation cues to help keep the taxonomy clean when two labels look like the same concept.")
            st.dataframe(pd.DataFrame(merge_rows), use_container_width=True, hide_index=True, height=int(min(max(240, 34 * len(merge_rows) + 72), 520)))
        else:
            st.success("No obvious duplicate labels detected in the current taxonomy. Canonical naming and alias normalization are already keeping it tight.")
    with tabs[2]:
        st.caption("View detractors as L1 themes first, then drill down to L2 symptoms with impact metrics.")
        theme_tabs = st.tabs(["L1 theme rollup", "L2 symptom table"])
        with theme_tabs[0]:
            _render_interactive_symptom_table(det_l1, key_prefix="sym_det_l1", empty_label="Detractor themes")
        with theme_tabs[1]:
            _render_interactive_symptom_table(det_l2, key_prefix="sym_det_l2", empty_label="Detractors")
    with tabs[3]:
        st.caption("View delighters as L1 themes first, then drill down to L2 symptoms with impact metrics.")
        theme_tabs = st.tabs(["L1 theme rollup", "L2 symptom table"])
        with theme_tabs[0]:
            _render_interactive_symptom_table(del_l1, key_prefix="sym_del_l1", empty_label="Delighter themes")
        with theme_tabs[1]:
            _render_interactive_symptom_table(del_l2, key_prefix="sym_del_l2", empty_label="Delighters")

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════



def _candidate_rows_for_side(raw_candidates, *, side):
    side_key = "delighter" if str(side).lower().startswith("del") else "detractor"
    other_key = "detractor" if side_key == "delighter" else "delighter"
    category = str(st.session_state.get("sym_taxonomy_category") or (st.session_state.get("sym_ai_build_result") or {}).get("category") or "general")
    side_raw = {}
    for label, payload in (raw_candidates or {}).items():
        clean_label = _safe_text(label).strip()
        if not clean_label:
            continue
        if side_key == "delighter":
            canon_labels, _, _ = _app()._standardize_symptom_lists([clean_label], [])
            canonical_label = canon_labels[0] if canon_labels else clean_label.title()
        else:
            _, canon_labels, _ = _app()._standardize_symptom_lists([], [clean_label])
            canonical_label = canon_labels[0] if canon_labels else clean_label.title()
        if f"{side_key}_count" in payload or f"{other_key}_count" in payload:
            count = int(payload.get(f"{side_key}_count", 0) or 0)
            refs = list(payload.get(f"{side_key}_refs", []) or [])
            other_count = int(payload.get(f"{other_key}_count", 0) or 0)
        else:
            count = int(payload.get("count", 0) or 0)
            refs = list(payload.get("refs", []) or [])
            other_count = 0
        if count <= 0:
            continue
        bucket = side_raw.setdefault(canonical_label, {"count": 0, "refs": [], "other_count": 0, "raw_labels": []})
        bucket["count"] = int(bucket.get("count", 0)) + count
        bucket["other_count"] = int(bucket.get("other_count", 0)) + other_count
        bucket["raw_labels"] = _normalize_tag_list(list(bucket.get("raw_labels", [])) + [clean_label])
        existing_refs = list(bucket.get("refs", []))
        for ref in refs:
            if ref not in existing_refs and len(existing_refs) < 50:
                existing_refs.append(ref)
        bucket["refs"] = existing_refs
    deduped = _app()._dedup_candidates(side_raw) if side_raw else {}
    rows = []
    for label, payload in sorted(deduped.items(), key=lambda kv: (-int(kv[1].get("count", 0)), kv[0])):
        merged_from = payload.get("_merged_from", []) or []
        notes = []
        raw_labels = [lab for lab in payload.get("raw_labels", []) or [] if lab and lab != label]
        if int(payload.get("other_count", 0) or 0) > 0:
            notes.append(f"also seen as {other_key}: {int(payload.get('other_count', 0))}")
        if raw_labels:
            notes.append("normalized from: " + ", ".join(raw_labels[:3]))
        if merged_from:
            notes.append("merged from: " + ", ".join(merged_from[:3]))
        rows.append({
            "L1 Theme": _infer_taxonomy_l1_theme(label, side=side_key, category=category),
            "Label": label,
            "Bucket": _bucket_taxonomy_label(label, side=side_key, category=category),
            "Count": int(payload.get("count", 0) or 0),
            "Refs": ", ".join(str(r) for r in list(payload.get("refs", []) or [])[:5]),
            "Notes": " · ".join(notes),
        })
    return rows



def _save_inline_symptom_edit(row_id, row, final_dets, final_dels, *, updated_reviews, processed_rows, detractors, delighters, notice_prefix="Updated"):
    rid = str(row_id).strip()
    final_dels, final_dets, inline_aliases = _app()._standardize_symptom_lists(final_dels or [], final_dets or [])
    final_dets = list(final_dets)[:10]
    final_dels = list(final_dels)[:10]
    if not bool(st.session_state.get("sym_include_universal_neutral", True)):
        final_dets, final_dels = _strip_universal_neutral_tags(final_dets, final_dels)
        final_dets = final_dets[:10]
        final_dels = final_dels[:10]
    overlap = sorted(set(final_dets) & set(final_dels))
    if overlap:
        return False, f"The same symptom cannot be saved as both a detractor and a delighter in the same review: {', '.join(overlap)}"

    session_dels, session_dets = _app()._canonical_symptom_catalog(list(delighters or []) + final_dels, list(detractors or []) + final_dets)
    st.session_state["sym_detractors"] = session_dets
    st.session_state["sym_delighters"] = session_dels
    st.session_state["sym_aliases"] = _alias_map_for_catalog(session_dels, session_dets, extra_aliases=inline_aliases, existing_aliases=st.session_state.get("sym_aliases", {}))

    edited_reviews = updated_reviews.copy()
    edited_reviews = _app()._write_ai_symptom_row(
        edited_reviews,
        int(rid),
        dets=final_dets,
        dels=final_dels,
        safety=row.get("AI Safety"),
        reliability=row.get("AI Reliability"),
        sessions=row.get("AI # of Sessions"),
    )
    custom_universal_dels, custom_universal_dets = _custom_universal_catalog()
    support = _refine_tag_assignment(
        _symptomizer_review_text(row),
        final_dets,
        final_dels,
        allowed_detractors=final_dets,
        allowed_delighters=final_dels,
        evidence_det={},
        evidence_del={},
        aliases=st.session_state.get("sym_aliases", {}) or {},
        max_per_side=10,
        include_universal_neutral=bool(st.session_state.get("sym_include_universal_neutral", True)),
        rating=row.get("rating"),
        extra_universal_detractors=custom_universal_dets,
        extra_universal_delighters=custom_universal_dels,
    )
    support_det = support.get("support_det", {}) or {}
    support_del = support.get("support_del", {}) or {}
    ev_det = {label: list((support_det.get(label, {}) or {}).get("snippets") or [])[:2] for label in final_dets if list((support_det.get(label, {}) or {}).get("snippets") or [])[:2]}
    ev_del = {label: list((support_del.get(label, {}) or {}).get("snippets") or [])[:2] for label in final_dels if list((support_del.get(label, {}) or {}).get("snippets") or [])[:2]}

    dataset_edit = dict(st.session_state.get("analysis_dataset") or {})
    dataset_edit["reviews_df"] = edited_reviews
    st.session_state["analysis_dataset"] = dataset_edit
    st.session_state["sym_processed_rows"] = _upsert_processed_symptom_record(
        processed_rows,
        rid,
        final_dets,
        final_dels,
        row_meta=row,
        ev_det=ev_det,
        ev_del=ev_del,
    )
    export_suffix = " Export will regenerate when you click Prepare export."
    try:
        original_bytes = st.session_state.get("_uploaded_raw_bytes")
        summary_obj = dataset_edit.get("summary") or (st.session_state.get("analysis_dataset") or {}).get("summary")
        if original_bytes:
            st.session_state["sym_export_bytes"] = _gen_symptomized_workbook(original_bytes, edited_reviews)
            export_suffix = " Export refreshed."
        elif summary_obj is not None:
            st.session_state["sym_export_bytes"] = _build_master_excel(summary_obj, edited_reviews)
            export_suffix = " Export refreshed."
        else:
            st.session_state["sym_export_bytes"] = None
    except Exception:
        st.session_state["sym_export_bytes"] = None
    _app()._queue_inline_editor_defaults(rid, dets=final_dets, dels=final_dels, new_det="", new_del="")
    metrics_now = _app()._qa_accuracy_metrics(edited_reviews)
    st.session_state["sym_qa_notice"] = f"{notice_prefix} row {rid}. Accuracy is now {metrics_now.get('accuracy_pct', 100.0):.1f}%.{export_suffix}"
    return True, ""



def _build_inline_tag_suggestions(row, current_dets, current_dels, detractors, delighters):
    review_text = _symptomizer_review_text(row)
    if not review_text:
        return {"missing_detractors": [], "missing_delighters": []}
    aliases = st.session_state.get("sym_aliases", {}) or {}
    custom_universal_dels, custom_universal_dets = _custom_universal_catalog()
    refined = _refine_tag_assignment(
        review_text,
        current_dets,
        current_dels,
        allowed_detractors=_normalize_tag_list(detractors or []),
        allowed_delighters=_normalize_tag_list(delighters or []),
        evidence_det={},
        evidence_del={},
        aliases=aliases,
        max_per_side=10,
        include_universal_neutral=bool(st.session_state.get("sym_include_universal_neutral", True)),
        rating=row.get("rating"),
        extra_universal_detractors=custom_universal_dets,
        extra_universal_delighters=custom_universal_dels,
    )
    return {
        "missing_detractors": [label for label in refined.get("added_dets", []) if label not in current_dets],
        "missing_delighters": [label for label in refined.get("added_dels", []) if label not in current_dels],
    }


