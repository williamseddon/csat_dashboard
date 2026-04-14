"""Dashboard analytics: metrics, trends, rating distribution, chart helpers.
"""
from __future__ import annotations
import re, sys
from typing import Any, Dict, List, Optional
import pandas as pd
import streamlit as st

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

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


def _compute_metrics_cached(df_json):
    df = pd.read_json(io.StringIO(df_json), orient="split")
    return _compute_metrics_direct(df)



def _compute_metrics_direct(df):
    n = len(df)
    if n == 0:
        return dict(
            review_count=0,
            avg_rating=None,
            avg_rating_non_incentivized=None,
            pct_low_star=0.0,
            pct_one_star=0.0,
            pct_two_star=0.0,
            pct_five_star=0.0,
            pct_incentivized=0.0,
            pct_with_photos=0.0,
            pct_syndicated=0.0,
            recommend_rate=None,
            median_review_words=None,
            non_incentivized_count=0,
            low_star_count=0,
        )
    ni = df[~df["incentivized_review"].fillna(False)]
    rb = df[df["is_recommended"].notna()]
    rr = _app()._safe_pct(int(rb["is_recommended"].astype(bool).sum()), len(rb)) if not rb.empty else None
    mw = float(df["review_length_words"].median()) if "review_length_words" in df.columns and not df["review_length_words"].dropna().empty else None
    low = df["rating"].isin([1, 2])
    return dict(
        review_count=n,
        avg_rating=_app()._safe_mean(df["rating"]),
        avg_rating_non_incentivized=_app()._safe_mean(ni["rating"]),
        pct_low_star=_app()._safe_pct(int(low.sum()), n),
        pct_one_star=_app()._safe_pct(int((df["rating"] == 1).sum()), n),
        pct_two_star=_app()._safe_pct(int((df["rating"] == 2).sum()), n),
        pct_five_star=_app()._safe_pct(int((df["rating"] == 5).sum()), n),
        pct_incentivized=_app()._safe_pct(int(df["incentivized_review"].fillna(False).sum()), n),
        pct_with_photos=_app()._safe_pct(int(df["has_photos"].fillna(False).sum()), n),
        pct_syndicated=_app()._safe_pct(int(df["is_syndicated"].fillna(False).sum()), n),
        recommend_rate=rr,
        median_review_words=mw,
        non_incentivized_count=len(ni),
        low_star_count=int(low.sum()),
    )



def _get_metrics(df):
    try:
        return _compute_metrics_cached(_app()._df_cache_key(df))
    except Exception:
        return _compute_metrics_direct(df)


@st.cache_data(show_spinner=False, ttl=300)



def _rating_dist_cached(df_json):
    df = pd.read_json(io.StringIO(df_json), orient="split")
    base = pd.DataFrame({"rating": [1, 2, 3, 4, 5]})
    if df.empty:
        base["review_count"] = 0
        base["share"] = 0.0
        return base
    grouped = (
        df.dropna(subset=["rating"])
        .assign(rating=lambda x: x["rating"].astype(int))
        .groupby("rating", as_index=False)
        .size()
        .rename(columns={"size": "review_count"})
    )
    merged = base.merge(grouped, how="left", on="rating").fillna({"review_count": 0})
    merged["review_count"] = merged["review_count"].astype(int)
    merged["share"] = merged["review_count"] / max(len(df), 1)
    return merged



def _rating_dist(df):
    try:
        return _rating_dist_cached(_app()._df_cache_key(df))
    except Exception:
        return pd.DataFrame({"rating": [1, 2, 3, 4, 5], "review_count": [0] * 5, "share": [0.0] * 5})


@st.cache_data(show_spinner=False, ttl=300)



def _monthly_trend_cached(df_json):
    df = pd.read_json(io.StringIO(df_json), orient="split")
    if df.empty:
        return pd.DataFrame(columns=["submission_month", "review_count", "avg_rating", "month_start"])
    df["submission_time"] = pd.to_datetime(df.get("submission_time"), errors="coerce")
    return (
        df.dropna(subset=["submission_time"])
        .assign(month_start=lambda x: x["submission_time"].dt.to_period("M").dt.to_timestamp())
        .groupby("month_start", as_index=False)
        .agg(review_count=("review_id", "count"), avg_rating=("rating", "mean"))
        .assign(submission_month=lambda x: x["month_start"].dt.strftime("%Y-%m"))
        .sort_values("month_start")
    )



def _monthly_trend(df):
    try:
        return _monthly_trend_cached(_app()._df_cache_key(df))
    except Exception:
        return pd.DataFrame(columns=["submission_month", "review_count", "avg_rating", "month_start"])



def _cumulative_avg_region_trend(df, *, organic_only=False, top_n=None, smoothing_label="7-day"):
    if df.empty or "submission_time" not in df.columns or "rating" not in df.columns:
        return pd.DataFrame(), []

    w = df.copy()
    w["submission_time"] = pd.to_datetime(w["submission_time"], errors="coerce")
    w["rating"] = pd.to_numeric(w["rating"], errors="coerce")
    w = w.dropna(subset=["submission_time", "rating"]).copy()

    if organic_only and "incentivized_review" in w.columns:
        w = w[~w["incentivized_review"].fillna(False)].copy()

    if w.empty:
        return pd.DataFrame(), []

    w["day"] = w["submission_time"].dt.floor("D")
    w["region"] = w.get("content_locale", pd.Series(index=w.index, dtype="object")).map(_locale_to_region_label).fillna("Unknown")

    full_days = pd.date_range(w["day"].min(), w["day"].max(), freq="D")
    base = pd.DataFrame({"day": full_days})

    overall = w.groupby("day", as_index=False).agg(daily_volume=("review_id", "count"), rating_sum=("rating", "sum"))
    trend = base.merge(overall, on="day", how="left").fillna({"daily_volume": 0, "rating_sum": 0})
    trend["daily_volume"] = trend["daily_volume"].astype(int)
    overall_denom = trend["daily_volume"].cumsum()
    trend["overall_cum_avg"] = np.where(overall_denom > 0, trend["rating_sum"].cumsum() / overall_denom, np.nan)

    region_counts = (
        w[w["region"] != "Unknown"]
        .groupby("region")["review_id"]
        .count()
        .sort_values(ascending=False)
    )
    if top_n in (None, "All"):
        regions = region_counts.index.tolist()
    else:
        regions = region_counts.head(int(top_n)).index.tolist()
    if not regions and "Unknown" in set(w["region"]):
        regions = ["Unknown"]

    for region in regions:
        reg = w[w["region"] == region].groupby("day", as_index=False).agg(region_volume=("review_id", "count"), rating_sum=("rating", "sum"))
        reg = base.merge(reg, on="day", how="left").fillna({"region_volume": 0, "rating_sum": 0})
        reg_denom = reg["region_volume"].cumsum()
        trend[f"{region}_cum_avg"] = np.where(reg_denom > 0, reg["rating_sum"].cumsum() / reg_denom, np.nan)

    smoothing_window = _app()._parse_smoothing_window(smoothing_label)
    if smoothing_window > 1:
        for col in [c for c in trend.columns if c.endswith("_cum_avg")]:
            trend[col] = trend[col].rolling(smoothing_window, min_periods=1).mean()

    return trend.sort_values("day").reset_index(drop=True), regions



def _render_reviews_over_time_chart(df):
    with st.container(border=True):
        st.markdown("<div class='section-title'>📈 Cumulative Avg ★ Over Time by Region (Weighted)</div>", unsafe_allow_html=True)
        st.markdown("<div class='section-sub'>Cumulative average stays on the right axis. Volume bars are optional and use the left axis.</div>", unsafe_allow_html=True)

        r2c0, r2c1, r2c2, r2c3, r2c4, r2c5 = st.columns(6)
        organic_only = r2c0.toggle("Organic only", value=False, key="ot_organic_only")
        show_volume = r2c1.toggle("Show volume", value=bool(st.session_state.get("ot_show_volume", False)), key="ot_show_volume")
        show_overall = r2c2.toggle("Show overall", value=True, key="ot_show_overall")
        smoothing = r2c3.selectbox("Smoothing", ["Off", "7-day", "14-day", "30-day"], index=1, key="ot_smoothing")
        volume_mode = r2c4.selectbox("Volume bars", ["Reviews/day", "Reviews/week", "Reviews/month"], index=0, key="ot_volume_mode")
        y_view = r2c5.radio("Y-axis view", ["Zoomed-in", "Full scale"], horizontal=True, key="ot_y_view")

        top_n_sel = st.selectbox("Regions", ["All", 2, 4, 6, 8, 10], index=0, key="ot_top_n")
        top_n = None if top_n_sel == "All" else int(top_n_sel)

        trend, regions = _cumulative_avg_region_trend(df, organic_only=organic_only, top_n=top_n, smoothing_label=smoothing)
        if trend.empty:
            st.info("No dated reviews available for the over-time chart.")
            return

        volume_bars, volume_axis_title = _app()._build_volume_bar_series(trend, volume_mode)
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        if show_volume and not volume_bars.empty:
            fig.add_trace(
                go.Bar(
                    x=volume_bars["x"],
                    y=volume_bars["volume"],
                    width=volume_bars["width_ms"],
                    name=volume_axis_title,
                    marker_color="rgba(100,116,139,0.46)",
                    marker_line_color="rgba(71,85,105,0.58)",
                    opacity=0.88,
                    customdata=np.stack([volume_bars["label"]], axis=-1),
                    hovertemplate="%{customdata[0]}<br>Reviews: %{y:,}<extra></extra>",
                ),
                secondary_y=False,
            )

        region_colors = [
            "#f97316", "#10b981", "#3b82f6", "#ef4444",
            "#8b5cf6", "#eab308", "#06b6d4", "#ec4899",
            "#84cc16", "#f43f5e", "#14b8a6", "#a855f7",
            "#f59e0b", "#64748b", "#0ea5e9", "#e11d48",
            "#22c55e", "#d97706", "#7c3aed", "#be185d",
        ]
        for idx, region in enumerate(regions):
            col = f"{region}_cum_avg"
            if col not in trend.columns:
                continue
            fig.add_trace(
                go.Scatter(
                    x=trend["day"],
                    y=trend[col],
                    name=region,
                    mode="lines",
                    line=dict(color=region_colors[idx % len(region_colors)], width=2),
                    hovertemplate=f"{region}<br>%{{x|%Y-%m-%d}}<br>Cumulative Avg ★: %{{y:.3f}}<extra></extra>",
                ),
                secondary_y=True,
            )

        if show_overall and "overall_cum_avg" in trend.columns:
            fig.add_trace(
                go.Scatter(
                    x=trend["day"],
                    y=trend["overall_cum_avg"],
                    name="Overall",
                    mode="lines",
                    line=dict(color="#8b5cf6", width=3),
                    hovertemplate="Overall<br>%{x|%Y-%m-%d}<br>Cumulative Avg ★: %{y:.3f}<extra></extra>",
                ),
                secondary_y=True,
            )

        fig.update_layout(
            margin=dict(l=24, r=24, t=16, b=20),
            hovermode="x unified",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_family="Inter",
            legend=dict(orientation="h", y=-0.22, x=0, xanchor="left", yanchor="top", bgcolor="rgba(255,255,255,0.8)"),
            bargap=0.06,
            height=430,
        )
        fig.update_xaxes(title_text="", showgrid=False)

        visible_cols = [f"{region}_cum_avg" for region in regions if f"{region}_cum_avg" in trend.columns]
        if show_overall and "overall_cum_avg" in trend.columns:
            visible_cols.append("overall_cum_avg")
        vals = pd.concat([trend[c].dropna() for c in visible_cols], ignore_index=True) if visible_cols else pd.Series(dtype=float)

        if y_view == "Full scale" or vals.empty:
            y_range = [1.0, 5.0]
        else:
            ymin = max(1.0, float(vals.min()) - 0.06)
            ymax = min(5.0, float(vals.max()) + 0.06)
            if ymax - ymin < 0.18:
                mid = (ymin + ymax) / 2
                ymin = max(1.0, mid - 0.09)
                ymax = min(5.0, mid + 0.09)
            y_range = [ymin, ymax]

        fig.update_yaxes(title_text=volume_axis_title if show_volume else "", secondary_y=False, showgrid=False, rangemode="tozero", visible=show_volume)
        fig.update_yaxes(title_text="Cumulative Avg ★", range=y_range, secondary_y=True, showgrid=True, gridcolor="rgba(148,163,184,0.15)")

        if y_view == "Zoomed-in" and y_range[0] > 1.05:
            _app()._add_axis_break_indicator(fig, side="right")

        _app()._show_plotly(fig)

# ═══════════════════════════════════════════════════════════════════════════════
#  SYMPTOM ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════



def _render_dashboard_snapshot(chart_df, overall_df=None):
    scope_df = chart_df if chart_df is not None else pd.DataFrame()
    if scope_df.empty:
        return
    m = _get_metrics(scope_df)
    od = overall_df if overall_df is not None else scope_df
    det_cols, del_cols = _app()._get_symptom_col_lists(od)
    top_det, det_mentions = _top_theme_summary(scope_df, det_cols, kind="detractors")
    top_del, del_mentions = _top_theme_summary(scope_df, del_cols, kind="delighters")
    recommend_txt = _app()._fmt_pct(m.get("recommend_rate")) if m.get("recommend_rate") is not None else "n/a"
    recommend_sub = "Reviews with recommendation signal" if m.get("recommend_rate") is not None else "No recommendation field available"
    organic_share = max(0.0, 1.0 - float(m.get("pct_incentivized") or 0.0))
    st.markdown(
        f"""
<div class="soft-panel">
  <div style="font-weight:800;color:var(--navy);margin-bottom:8px;">Executive snapshot</div>
  <div class="summary-grid">
    <div class="summary-item">
      <div class="label">Current scope</div>
      <div class="value">{len(scope_df):,} reviews</div>
      <div class="sub">{int((~scope_df['incentivized_review'].fillna(False)).sum()):,} organic in the current view.</div>
    </div>
    <div class="summary-item">
      <div class="label">Satisfaction</div>
      <div class="value">{_app()._fmt_num(m.get('avg_rating'))} ★</div>
      <div class="sub">Recommend rate {recommend_txt}. {recommend_sub}.</div>
    </div>
    <div class="summary-item">
      <div class="label">Biggest risk</div>
      <div class="value">{_esc(top_det or 'Run Symptomizer')}</div>
      <div class="sub">{(str(det_mentions) + ' mentions in view') if top_det else 'Top risk themes will appear here once symptoms are tagged.'}</div>
    </div>
    <div class="summary-item">
      <div class="label">Quality of sample</div>
      <div class="value">{_app()._fmt_pct(organic_share)} organic</div>
      <div class="sub">Low-star share {_app()._fmt_pct(m.get('pct_low_star'))}. {(_esc(top_del) + ' leads positives.') if top_del else 'Top positive themes will appear here once symptoms are tagged.'}</div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )



def _top_theme_summary(df_in, cols, *, kind="detractors"):
    try:
        tbl = _app().analyze_symptoms_fast(df_in, cols)
    except Exception:
        tbl = pd.DataFrame()
    if tbl is None or tbl.empty:
        return None, 0
    if "rating" in df_in.columns:
        tbl = _add_net_hit(tbl, float(_app()._safe_mean(df_in["rating"]) or 0), total_reviews=len(df_in), kind=kind, detail_df=df_in, symptom_cols=cols)
    row = tbl.iloc[0]
    return _safe_text(row.get("Item")) or None, _safe_int(row.get("Mentions"), 0)



def _compute_detailed_symptom_impact(df_in, symptom_cols, baseline, *, kind):
    long, symptomized_reviews = _prepare_symptom_long(df_in, symptom_cols)
    if long.empty:
        return pd.DataFrame(columns=["Mention Reviews", "Avg Tags/Review", "Avg Star", "Weighted Mentions", "Net Hit Raw"]), 0

    stars = pd.to_numeric(long["star"], errors="coerce")
    if str(kind).lower().startswith("del"):
        gap = (stars - float(baseline)).clip(lower=0)
    else:
        gap = (float(baseline) - stars).clip(lower=0)
    long["gap"] = gap.fillna(0.0)
    long["attributed_gap"] = long["review_weight"].astype(float) * long["gap"].astype(float)

    grouped = long.groupby("symptom", dropna=False)
    out = grouped.agg(**{
        "Mention Reviews": ("__row", "nunique"),
        "Avg Tags/Review": ("symptom_count", "mean"),
        "Avg Star": ("star", "mean"),
        "Weighted Mentions": ("review_weight", "sum"),
        "Net Hit Raw": ("attributed_gap", "sum"),
    })
    return out, symptomized_reviews


_HIGH_SEVERITY_HINTS = {"broke", "broken", "break", "defect", "defective", "danger", "dangerous", "safety", "burn", "burned", "burnt", "smoke", "fire", "leak", "leaks", "leaking", "rash", "itch", "itchy", "irritat", "pain", "painful", "stopped working", "won't work", "doesn't work", "does not work", "shipping damage"}
_MEDIUM_SEVERITY_HINTS = {"poor performance", "unreliable", "hard to clean", "difficult", "connectivity", "battery", "charging", "loud", "wrong size", "instructions", "compatibility", "slow", "time consuming", "poor quality", "overpriced", "cheap", "flimsy", "hot", "overheat"}
_LOW_SEVERITY_HINTS = {"design", "appearance", "attractive", "stylish", "scent", "taste", "texture", "packaging", "price", "value"}
_HIGH_VALUE_DELIGHTS = {"reliable", "high quality", "performs well", "easy to use", "easy to clean", "saves time", "clear instructions", "compatible", "long battery life", "fast charging"}
_LOW_VALUE_DELIGHTS = {"attractive design", "pleasant scent", "great taste", "good texture", "good packaging"}


