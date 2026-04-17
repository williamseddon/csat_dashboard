from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


DEFAULT_REVIEW_ORIGIN = "Organic"
DEFAULT_CHANNEL = "Organic / Standard"
DEFAULT_MODERATION = "Approved"


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = _safe_text(value).lower()
    if text in {"true", "1", "yes", "y", "t"}:
        return True
    if text in {"false", "0", "no", "n", "f"}:
        return False
    return default


@dataclass(frozen=True)
class AlertThresholds:
    min_reviews: int = 50
    fix_delta_rating: float = -0.18
    protect_delta_rating: float = -0.10
    scale_delta_rating: float = 0.08
    watch_abs_delta_rating: float = 0.10
    watch_abs_delta_volume_pct: float = 0.25


def prepare_action_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame(
            columns=[
                "review_id",
                "submission_time",
                "rating",
                "incentivized_review",
                "review_origin_group",
                "review_acquisition_channel",
                "moderation_bucket",
                "country",
                "content_locale",
                "mapped_brand",
                "mapped_category",
                "mapped_subcategory",
                "base_model_number",
                "product_id",
                "original_product_name",
                "low_star",
                "organic_flag",
            ]
        )

    out = df.copy()
    required_defaults = {
        "review_id": pd.NA,
        "submission_time": pd.NaT,
        "rating": pd.NA,
        "incentivized_review": False,
        "review_origin_group": DEFAULT_REVIEW_ORIGIN,
        "review_acquisition_channel": DEFAULT_CHANNEL,
        "moderation_bucket": DEFAULT_MODERATION,
        "country": pd.NA,
        "content_locale": pd.NA,
        "mapped_brand": pd.NA,
        "mapped_category": pd.NA,
        "mapped_subcategory": pd.NA,
        "base_model_number": pd.NA,
        "product_id": pd.NA,
        "original_product_name": pd.NA,
    }
    for col, default in required_defaults.items():
        if col not in out.columns:
            out[col] = default

    out["submission_time"] = pd.to_datetime(out["submission_time"], errors="coerce")
    out["rating"] = pd.to_numeric(out["rating"], errors="coerce")
    out["incentivized_review"] = pd.Series(out["incentivized_review"]).astype("boolean").fillna(False).astype(bool)
    out["review_origin_group"] = out["review_origin_group"].astype("string").fillna("").str.strip()
    if out["review_origin_group"].eq("").all():
        out["review_origin_group"] = np.where(out["incentivized_review"], "Seeded / Incentivized", "Organic")
    else:
        fallback = np.where(out["incentivized_review"], "Seeded / Incentivized", "Organic")
        out["review_origin_group"] = out["review_origin_group"].where(out["review_origin_group"].ne(""), pd.Series(fallback, index=out.index))

    out["review_acquisition_channel"] = out["review_acquisition_channel"].astype("string").fillna("").str.strip().replace({"": DEFAULT_CHANNEL})
    out["moderation_bucket"] = out["moderation_bucket"].astype("string").fillna("").str.strip().replace({"": DEFAULT_MODERATION})
    out["country"] = out["country"].astype("string").fillna("").str.strip()
    if out["country"].eq("").all() and "content_locale" in out.columns:
        out["country"] = out["content_locale"].astype("string").fillna("").str.split("_").str[-1].str.strip()
    out["country"] = out["country"].replace({"": pd.NA})

    for col in ["mapped_brand", "mapped_category", "mapped_subcategory", "base_model_number", "product_id", "original_product_name", "content_locale"]:
        out[col] = out[col].astype("string").fillna("").str.strip().replace({"": pd.NA})

    out["low_star"] = out["rating"].le(2).fillna(False)
    out["organic_flag"] = ~out["incentivized_review"].fillna(False)
    return out


def apply_action_filters(
    df: pd.DataFrame,
    *,
    moderation_values: Optional[Sequence[str]] = None,
    organic_only: bool = False,
    brand_values: Optional[Sequence[str]] = None,
    country_values: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if isinstance(df, pd.DataFrame) else None)
    out = df.copy()
    if moderation_values:
        wanted = {_safe_text(v) for v in moderation_values if _safe_text(v)}
        if wanted and "moderation_bucket" in out.columns:
            out = out[out["moderation_bucket"].astype("string").isin(wanted)]
    if organic_only and "incentivized_review" in out.columns:
        out = out[~out["incentivized_review"].fillna(False)]
    if brand_values and "mapped_brand" in out.columns:
        wanted = {_safe_text(v) for v in brand_values if _safe_text(v)}
        if wanted:
            out = out[out["mapped_brand"].astype("string").isin(wanted)]
    if country_values and "country" in out.columns:
        wanted = {_safe_text(v) for v in country_values if _safe_text(v)}
        if wanted:
            out = out[out["country"].astype("string").isin(wanted)]
    return out.copy()


def build_trend_series(
    df: pd.DataFrame,
    *,
    group_cols: Sequence[str],
    freq: str = "M",
) -> pd.DataFrame:
    if df is None or df.empty:
        cols = list(group_cols) + ["period_start", "period_label", "review_count", "avg_rating", "low_star_share", "organic_share"]
        return pd.DataFrame(columns=cols)
    work = df.copy()
    work = work[work["submission_time"].notna()].copy()
    if work.empty:
        cols = list(group_cols) + ["period_start", "period_label", "review_count", "avg_rating", "low_star_share", "organic_share"]
        return pd.DataFrame(columns=cols)
    normalized_group_cols = [col for col in group_cols if col in work.columns]
    if not normalized_group_cols:
        work["__group"] = "All reviews"
        normalized_group_cols = ["__group"]
    work["period_start"] = work["submission_time"].dt.to_period(freq).dt.start_time
    grouped = (
        work.groupby(normalized_group_cols + ["period_start"], dropna=False)
        .agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            low_star_share=("low_star", "mean"),
            organic_share=("organic_flag", "mean"),
        )
        .reset_index()
        .sort_values(normalized_group_cols + ["period_start"])
    )
    fmt = "%Y-%m" if freq.upper().startswith("M") else "%Y-%m-%d"
    grouped["period_label"] = grouped["period_start"].dt.strftime(fmt)
    return grouped


def _windowed_delta(period_df: pd.DataFrame, *, group_cols: Sequence[str], recent_periods: int = 2, baseline_periods: int = 4) -> pd.DataFrame:
    if period_df is None or period_df.empty:
        cols = list(group_cols) + [
            "latest_period_start",
            "latest_reviews",
            "latest_avg_rating",
            "baseline_reviews",
            "baseline_avg_rating",
            "rating_delta",
            "volume_delta_pct",
        ]
        return pd.DataFrame(columns=cols)

    latest_period = pd.to_datetime(period_df["period_start"], errors="coerce").max()
    if pd.isna(latest_period):
        return pd.DataFrame(columns=list(group_cols))
    ordered_periods = sorted(period_df["period_start"].dropna().unique())
    latest_ix = ordered_periods.index(latest_period)
    recent_period_values = ordered_periods[max(0, latest_ix - recent_periods + 1): latest_ix + 1]
    baseline_start = max(0, latest_ix - recent_periods - baseline_periods + 1)
    baseline_end = max(0, latest_ix - recent_periods + 1)
    baseline_period_values = ordered_periods[baseline_start:baseline_end]

    recent = period_df[period_df["period_start"].isin(recent_period_values)].copy()
    baseline = period_df[period_df["period_start"].isin(baseline_period_values)].copy()
    if recent.empty:
        return pd.DataFrame(columns=list(group_cols))

    recent_agg = (
        recent.groupby(list(group_cols), dropna=False)
        .agg(
            latest_period_start=("period_start", "max"),
            latest_reviews=("review_count", "sum"),
            latest_avg_rating=("avg_rating", "mean"),
            latest_low_star_share=("low_star_share", "mean"),
            latest_organic_share=("organic_share", "mean"),
        )
        .reset_index()
    )
    if baseline.empty:
        recent_agg["baseline_reviews"] = np.nan
        recent_agg["baseline_avg_rating"] = np.nan
        recent_agg["baseline_low_star_share"] = np.nan
        recent_agg["rating_delta"] = np.nan
        recent_agg["volume_delta_pct"] = np.nan
        recent_agg["low_star_delta"] = np.nan
        return recent_agg

    baseline_agg = (
        baseline.groupby(list(group_cols), dropna=False)
        .agg(
            baseline_reviews=("review_count", "sum"),
            baseline_avg_rating=("avg_rating", "mean"),
            baseline_low_star_share=("low_star_share", "mean"),
            baseline_organic_share=("organic_share", "mean"),
        )
        .reset_index()
    )
    merged = recent_agg.merge(baseline_agg, on=list(group_cols), how="left")
    merged["rating_delta"] = merged["latest_avg_rating"] - merged["baseline_avg_rating"]
    baseline_reviews = pd.to_numeric(merged["baseline_reviews"], errors="coerce")
    merged["volume_delta_pct"] = np.where(
        baseline_reviews.gt(0),
        (pd.to_numeric(merged["latest_reviews"], errors="coerce") - baseline_reviews) / baseline_reviews,
        np.nan,
    )
    merged["low_star_delta"] = merged["latest_low_star_share"] - merged["baseline_low_star_share"]
    return merged


def summarize_dimension(
    df: pd.DataFrame,
    *,
    group_cols: Sequence[str],
    freq: str = "M",
    min_reviews: int = 50,
    recent_periods: int = 2,
    baseline_periods: int = 4,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    prepared = prepare_action_frame(df)
    valid_group_cols = [col for col in group_cols if col in prepared.columns]
    if not valid_group_cols:
        raise ValueError("At least one valid group column is required.")
    if prepared.empty:
        return pd.DataFrame(), pd.DataFrame()

    base = (
        prepared.groupby(valid_group_cols, dropna=False)
        .agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            low_star_share=("low_star", "mean"),
            organic_share=("organic_flag", "mean"),
            country_count=("country", lambda s: s.dropna().astype(str).nunique()),
            product_count=("product_id", lambda s: s.dropna().astype(str).nunique()),
        )
        .reset_index()
    )
    base = base[pd.to_numeric(base["review_count"], errors="coerce").fillna(0).ge(int(max(min_reviews, 1)))]
    if base.empty:
        return base, pd.DataFrame(columns=valid_group_cols + ["period_start", "period_label", "review_count", "avg_rating", "low_star_share", "organic_share"])

    period_df = build_trend_series(prepared, group_cols=valid_group_cols, freq=freq)
    summary_trend = _windowed_delta(period_df, group_cols=valid_group_cols, recent_periods=recent_periods, baseline_periods=baseline_periods)
    summary = base.merge(summary_trend, on=valid_group_cols, how="left")

    overall_avg = float(prepared["rating"].mean()) if prepared["rating"].notna().any() else np.nan
    overall_low = float(prepared["low_star"].mean()) if len(prepared) else np.nan
    summary["segment"] = assign_segments(summary, overall_avg=overall_avg, overall_low_star_share=overall_low)
    summary["trend_direction"] = np.select(
        [summary["rating_delta"].ge(0.08), summary["rating_delta"].le(-0.08)],
        ["Improving", "Softening"],
        default="Stable",
    )
    return summary.sort_values(["review_count", "avg_rating"], ascending=[False, False]).reset_index(drop=True), period_df


def assign_segments(summary: pd.DataFrame, *, overall_avg: float = np.nan, overall_low_star_share: float = np.nan, thresholds: AlertThresholds = AlertThresholds()) -> pd.Series:
    if summary is None or summary.empty:
        return pd.Series(dtype="string")
    review_count = pd.to_numeric(summary.get("review_count"), errors="coerce").fillna(0)
    avg_rating = pd.to_numeric(summary.get("avg_rating"), errors="coerce")
    delta = pd.to_numeric(summary.get("rating_delta"), errors="coerce")
    low_star_share = pd.to_numeric(summary.get("low_star_share"), errors="coerce")
    vol_delta = pd.to_numeric(summary.get("volume_delta_pct"), errors="coerce")
    high_volume_cut = float(max(np.nanpercentile(review_count, 65) if len(review_count) else thresholds.min_reviews, thresholds.min_reviews))

    labels: List[str] = []
    for cnt, rating, d_rating, low_star, d_volume in zip(review_count, avg_rating, delta, low_star_share, vol_delta):
        high_volume = cnt >= high_volume_cut
        weak_rating = (pd.notna(rating) and pd.notna(overall_avg) and rating <= overall_avg - 0.15) or (pd.notna(low_star) and pd.notna(overall_low_star_share) and low_star >= overall_low_star_share + 0.05)
        elite_rating = pd.notna(rating) and pd.notna(overall_avg) and rating >= overall_avg + 0.12
        if high_volume and (weak_rating or (pd.notna(d_rating) and d_rating <= thresholds.fix_delta_rating)):
            labels.append("Fix now")
        elif high_volume and elite_rating and (pd.isna(d_rating) or d_rating >= thresholds.scale_delta_rating):
            labels.append("Scale")
        elif high_volume and pd.notna(d_rating) and d_rating <= thresholds.protect_delta_rating:
            labels.append("Protect")
        elif (pd.notna(d_rating) and abs(d_rating) >= thresholds.watch_abs_delta_rating) or (pd.notna(d_volume) and abs(d_volume) >= thresholds.watch_abs_delta_volume_pct):
            labels.append("Watch")
        else:
            labels.append("Stable")
    return pd.Series(labels, index=summary.index, dtype="string")


def build_alert_feed(
    df: pd.DataFrame,
    *,
    min_reviews: int = 50,
    freq: str = "M",
) -> pd.DataFrame:
    prepared = prepare_action_frame(df)
    if prepared.empty:
        return pd.DataFrame(columns=[
            "entity_kind", "entity_label", "brand", "segment", "priority", "reason", "recommended_action",
            "review_count", "avg_rating", "rating_delta", "low_star_share", "volume_delta_pct",
        ])

    frames: List[pd.DataFrame] = []
    specs = [
        ("Category", ["mapped_brand", "mapped_category"]),
        ("Country", ["mapped_brand", "country"]),
        ("Base model", ["mapped_brand", "base_model_number"]),
    ]
    for kind, group_cols in specs:
        valid = [col for col in group_cols if col in prepared.columns]
        if len(valid) < 2:
            continue
        summary, _ = summarize_dimension(prepared, group_cols=valid, freq=freq, min_reviews=min_reviews)
        if summary.empty:
            continue
        label_col = valid[-1]
        out = summary.copy()
        out["entity_kind"] = kind
        out["entity_label"] = out[label_col].astype("string").fillna("Unknown")
        out["brand"] = out[valid[0]].astype("string").fillna("Unknown")
        out["priority"] = _alert_priority(out)
        out["reason"] = out.apply(_alert_reason, axis=1)
        out["recommended_action"] = out.apply(_alert_action, axis=1)
        frames.append(out[[
            "entity_kind", "entity_label", "brand", "segment", "priority", "reason", "recommended_action",
            "review_count", "avg_rating", "rating_delta", "low_star_share", "volume_delta_pct",
        ]])
    if not frames:
        return pd.DataFrame(columns=[
            "entity_kind", "entity_label", "brand", "segment", "priority", "reason", "recommended_action",
            "review_count", "avg_rating", "rating_delta", "low_star_share", "volume_delta_pct",
        ])
    alert_df = pd.concat(frames, ignore_index=True)
    priority_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    alert_df["_priority_rank"] = alert_df["priority"].map(priority_order).fillna(9)
    alert_df = alert_df.sort_values(["_priority_rank", "review_count", "avg_rating"], ascending=[True, False, True]).drop(columns=["_priority_rank"])
    return alert_df.reset_index(drop=True)


def _alert_priority(summary: pd.DataFrame) -> pd.Series:
    out: List[str] = []
    for seg, cnt, delta, low_share in zip(
        summary.get("segment", pd.Series(dtype="string")),
        pd.to_numeric(summary.get("review_count"), errors="coerce").fillna(0),
        pd.to_numeric(summary.get("rating_delta"), errors="coerce"),
        pd.to_numeric(summary.get("low_star_share"), errors="coerce"),
    ):
        if seg == "Fix now" and (cnt >= 200 or (pd.notna(low_share) and low_share >= 0.30)):
            out.append("Critical")
        elif seg in {"Fix now", "Protect"}:
            out.append("High")
        elif seg in {"Scale", "Watch"}:
            out.append("Medium")
        else:
            out.append("Low")
    return pd.Series(out, index=summary.index, dtype="string")


def _fmt_pct(value: Any) -> str:
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return "—"


def _alert_reason(row: pd.Series) -> str:
    seg = _safe_text(row.get("segment"))
    avg_rating = row.get("avg_rating")
    delta = row.get("rating_delta")
    low_share = row.get("low_star_share")
    vol = row.get("volume_delta_pct")
    if seg == "Fix now":
        return f"High-volume area with {float(avg_rating):.2f}★ and { _fmt_pct(low_share) } low-star share." if pd.notna(avg_rating) else "High-volume area with elevated downside risk."
    if seg == "Protect":
        return f"Ratings are softening ({float(delta):+.2f} vs baseline) in a high-visibility area." if pd.notna(delta) else "Ratings are softening in a high-visibility area."
    if seg == "Scale":
        return f"Strong performance at {float(avg_rating):.2f}★ with momentum holding or improving." if pd.notna(avg_rating) else "Strong performance worth amplifying."
    if seg == "Watch":
        if pd.notna(delta) and abs(float(delta)) >= 0.08:
            return f"Meaningful rating move ({float(delta):+.2f} vs baseline) needs monitoring."
        if pd.notna(vol):
            return f"Review volume moved {float(vol) * 100:+.0f}% vs baseline; check for launch or issue signals."
        return "Emerging change detected; monitor closely."
    return "Performance is broadly stable."


def _alert_action(row: pd.Series) -> str:
    seg = _safe_text(row.get("segment"))
    kind = _safe_text(row.get("entity_kind"))
    label = _safe_text(row.get("entity_label")) or "this area"
    if seg == "Fix now":
        return f"Pull the newest low-star reviews for {label}, identify the top detractor themes, and route them to Product / Quality this week."
    if seg == "Protect":
        return f"Audit what recently changed in {label} and set a watchlist for early warning in the next review cycle."
    if seg == "Scale":
        return f"Use {label} as a best-practice benchmark and reuse its winning claims in PDP / CRM / retailer content."
    if seg == "Watch":
        return f"Keep {label} on a monitored list and review the trend again after the next refresh."
    return f"No urgent action required for {label}."


def summarize_base_models(
    df: pd.DataFrame,
    *,
    min_reviews: int = 25,
    freq: str = "M",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    prepared = prepare_action_frame(df)
    if prepared.empty or "base_model_number" not in prepared.columns:
        return pd.DataFrame(), pd.DataFrame()
    work = prepared[prepared["base_model_number"].notna()].copy()
    if work.empty:
        return pd.DataFrame(), pd.DataFrame()
    summary, trend = summarize_dimension(work, group_cols=["mapped_brand", "base_model_number"], freq=freq, min_reviews=min_reviews)
    if summary.empty:
        return summary, trend
    extra = (
        work.groupby(["mapped_brand", "base_model_number"], dropna=False)
        .agg(
            mapped_category=("mapped_category", lambda s: s.dropna().astype(str).value_counts().index[0] if s.dropna().astype(str).size else pd.NA),
            mapped_subcategory=("mapped_subcategory", lambda s: s.dropna().astype(str).value_counts().index[0] if s.dropna().astype(str).size else pd.NA),
            product_count=("product_id", lambda s: s.dropna().astype(str).nunique()),
            country_count=("country", lambda s: s.dropna().astype(str).nunique()),
            exemplar_product_name=("original_product_name", lambda s: s.dropna().astype(str).value_counts().index[0] if s.dropna().astype(str).size else pd.NA),
        )
        .reset_index()
    )
    summary = summary.merge(extra, on=["mapped_brand", "base_model_number"], how="left", suffixes=("", "_extra"))
    return summary.sort_values(["segment", "review_count", "avg_rating"], ascending=[True, False, False]).reset_index(drop=True), trend


def detect_trend_movers(
    df: pd.DataFrame,
    *,
    group_col: Optional[str] = None,
    metric: str = "avg_rating",
    freq: str = "W",
    recent_periods: int = 1,
    baseline_periods: int = 4,
    min_group_reviews: int = 20,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    prepared = prepare_action_frame(df)
    if prepared.empty:
        return pd.DataFrame(), pd.DataFrame()
    group_cols = [group_col] if group_col and group_col in prepared.columns else []
    trend = build_trend_series(prepared, group_cols=group_cols, freq=freq)
    if trend.empty:
        return trend, pd.DataFrame()
    valid_metric = metric if metric in {"avg_rating", "review_count", "low_star_share", "organic_share"} else "avg_rating"
    if group_cols:
        totals = prepared.groupby(group_cols, dropna=False).agg(review_count_total=("review_id", "count")).reset_index()
        trend = trend.merge(totals, on=group_cols, how="left")
        trend = trend[pd.to_numeric(trend["review_count_total"], errors="coerce").fillna(0).ge(int(max(min_group_reviews, 1)))]
    else:
        trend["review_count_total"] = len(prepared)
    if trend.empty:
        return trend, pd.DataFrame()

    deltas = _windowed_delta(trend, group_cols=group_cols or ["__group"], recent_periods=recent_periods, baseline_periods=baseline_periods)
    if not group_cols:
        deltas = deltas.assign(**{"__group": "All reviews"})
    movers = deltas.copy()
    value_col = {
        "avg_rating": ("latest_avg_rating", "baseline_avg_rating", "rating_delta"),
        "review_count": ("latest_reviews", "baseline_reviews", "volume_delta_pct"),
        "low_star_share": ("latest_low_star_share", "baseline_low_star_share", "low_star_delta"),
        "organic_share": ("latest_organic_share", "baseline_organic_share", None),
    }[valid_metric]
    movers["latest_value"] = movers[value_col[0]]
    movers["baseline_value"] = movers[value_col[1]]
    if value_col[2] and value_col[2] in movers.columns:
        movers["delta"] = movers[value_col[2]]
    else:
        movers["delta"] = movers["latest_value"] - movers["baseline_value"]
    label_col = group_cols[0] if group_cols else "__group"
    movers["entity_label"] = movers[label_col].astype("string").fillna("All reviews")
    movers = movers.sort_values(["delta", "latest_reviews"], ascending=[False, False]).reset_index(drop=True)
    return trend.reset_index(drop=True), movers[["entity_label", "latest_value", "baseline_value", "delta", "latest_reviews", "latest_period_start"]]
