from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NON_VALUES = {
    "", "na", "n/a", "none", "null", "nan", "<na>", "not mentioned",
    "product specific", "not applicable", "unknown", "n/a - not mentioned",
}

SYMPTOM_NON_VALUES = NON_VALUES | {"product specific", "general feedback"}

_SYMPTOM_COL_RE = re.compile(
    r"^(?:AI\s+)?Symptom\s+(?:Detractor|Delighter)\s*\d*$",
    flags=re.IGNORECASE,
)

_EXCLUDED_ANALYTICS_LABELS = {"Overall Dissatisfaction", "Overall Satisfaction"}

_CANONICAL_LABEL_PATTERNS: Tuple[Tuple[str, str], ...] = (
    (r"\bexcess noise\b|\btoo noisy\b|\bnoise issue(?:s)?\b|\bvery noisy\b|\bloud during use\b", "Loud"),
    (r"\btoo loud\b", "Loud"),
    (r"\bquiet operation\b|\bruns quietly\b|\blow noise\b|\bnot noisy\b|\bnot loud\b", "Quiet"),
    (r"\bhard to use\b|\bconfusing to use\b|\bawkward to use\b|\bcomplicated to use\b|\bnot intuitive\b", "Difficult To Use"),
    (r"\beasy to use\b|\bsimple to use\b|\bstraightforward to use\b|\buser friendly\b|\bintuitive\b", "Easy To Use"),
    (r"\bdifficult to clean\b|\bcleanup is hard\b|\bmessy cleanup\b|\bhard cleanup\b|\bcleanup takes forever\b", "Hard To Clean"),
    (r"\beasy cleanup\b|\beasy clean\b|\bquick cleanup\b|\bsimple to clean\b", "Easy To Clean"),
    (r"\bfast charge\b|\bquick charge\b|\bcharges quickly\b|\bfast to recharge\b", "Fast Charging"),
    (r"\bslow charge\b|\bcharges slowly\b|\bslow to recharge\b|\btakes too long to charge\b", "Slow Charging"),
    (r"\bbad smell\b|\bchemical smell\b|\bweird scent\b|\bsmells awful\b|\bunpleasant scent\b", "Unpleasant Scent"),
    (r"\bbad flavor\b|\bawful taste\b|\boff taste\b|\btastes bad\b", "Bad Taste"),
    (r"\bruns small\b|\bruns large\b|\bdoes not fit\b|\bdoesn't fit\b|\bsize issue\b|\bbulky\b", "Wrong Size"),
    (r"\bbad directions\b|\bconfusing instructions\b|\bpoor instructions\b|\binstructions confusing\b|\bunclear instructions\b", "Instructions Unclear"),
    (r"\bclear instructions\b|\beasy to follow instructions\b|\bclear directions\b", "Clear Instructions"),
    (r"\bhard install\b|\bdifficult install\b|\bhard installation\b|\bassembly is hard\b|\bdifficult setup\b|\bsetup issues\b", "Difficult Setup"),
    (r"\beasy setup\b|\beasy install\b|\beasy installation\b|\beasy assembly\b|\bassembled easily\b", "Easy Setup"),
)


# ---------------------------------------------------------------------------
# SymptomRow — typed output row
# ---------------------------------------------------------------------------

@dataclass
class SymptomRow:
    item: str
    side: str
    count: int = 0
    pct: float = 0.0
    avg_rating: float = 0.0
    severity_weight: float = 1.0
    raw_impact: float = 0.0
    net_hit: float = 0.0
    forecast_delta: float = 0.0
    examples: List[str] = field(default_factory=list)

    @property
    def impact_score(self) -> float:
        return round(self.net_hit, 3)

    def as_display_dict(self) -> Dict[str, Any]:
        return {
            "Item": self.item,
            "Mentions": int(self.count),
            "% Tagged Reviews": f"{float(self.pct):.1f}%",
            "Avg Star": round(float(self.avg_rating), 1) if self.avg_rating else 0.0,
            "Avg Tags/Review": np.nan,
            "Net Hit": round(float(self.net_hit), 3),
            "Forecast Δ★": round(float(self.forecast_delta), 3),
            "Impact Score": round(float(self.impact_score), 3),
            "Severity Wt": round(float(self.severity_weight), 2),
        }


# ---------------------------------------------------------------------------
# Severity / value weight lookup
# ---------------------------------------------------------------------------

_HIGH_SEVERITY_RE = re.compile(
    r"\b(?:broke|broken|break|defect(?:ive)?|danger(?:ous)?|safety"
    r"|burn(?:s|ed|t)?|smok(?:e|ing)|fire|leak(?:s|ing)?"
    r"|rash|itch(?:y)?|irritat(?:e|es|ing|ion)?"
    r"|pain(?:ful)?|stopped working|won'?t work|doesn'?t work|does not work|shipping damage"
    r"|heat damage|hair damage|damage(?:s|d)?\s+hair|fried\s+my\s+hair"
    r"|hair\s+(?:fell|broke)\s+out|scalp\s+burn|too hot|scorching|dangerously hot"
    r"|dead\s+on\s+arrival|short\s+lifespan|premature\s+failure"
    r"|missing\s+parts?|arrived\s+broken|broken\s+on\s+arrival)\b",
    flags=re.IGNORECASE,
)

_MEDIUM_SEVERITY_RE = re.compile(
    r"\b(?:poor performance|unreliable|hard to clean|difficult"
    r"|connectivity|battery|charging|loud|wrong size|instructions"
    r"|compatibility|slow|time consuming|poor quality|overpriced|cheap|flimsy|hot|overheat(?:s)?"
    r"|attachment issues?|frizz(?:y)?|slow dry(?:ing)?|heavy|doesn't reduce frizz|no frizz control"
    r"|weak suction|clogs?\s+easily|tangled\s+brush|poor\s+navigation"
    r"|hard\s+to\s+empty|filter\s+issues?|limited\s+coverage|short\s+filter\s+life"
    r"|poor\s+build\s+quality)\b",
    flags=re.IGNORECASE,
)

_HIGH_VALUE_DELIGHT_RE = re.compile(
    r"\b(?:reliable|high quality|performs well|easy to use|easy to clean"
    r"|saves time|clear instructions|compatible|long battery life|fast charging"
    r"|effective|great results|gentle on skin"
    r"|salon.quality results|salon results|reduces frizz|frizz.free"
    r"|no frizz|gentle on hair|no heat damage|fast dry(?:ing)?|dries quickly|quick dry"
    r"|strong suction|picks up pet hair|effective filtration|long.lasting"
    r"|removes allergens|powerful suction|great pickup|incredible suction|amazing suction)\b",
    flags=re.IGNORECASE,
)

_MEDIUM_VALUE_DELIGHT_RE = re.compile(
    r"\b(?:quiet|easy setup|right size|comfortable|good value|worth it"
    r"|lightweight|compact|durable|sturdy|fast"
    r"|easy attachment swap|multiple heat settings|long cord|shiny hair|smooth finish|salon.like"
    r"|easy to maintain|low maintenance|easy to empty|easy filter|good maneuverability|self.emptying)\b",
    flags=re.IGNORECASE,
)

_SEVERITY_OVERRIDES: Dict[str, float] = {
    "heat damage": 1.35,
    "dead on arrival": 1.35,
    "short lifespan": 1.35,
    "missing parts": 1.35,
    "shipping damage": 1.35,
    "safety concern": 1.35,
    "weak suction": 1.18,
    "clogs easily": 1.18,
    "poor navigation": 1.18,
    "tangled brush roll": 1.18,
    "short battery life": 1.18,
    "loud": 1.18,
    "hard to clean": 1.18,
    "salon-quality results": 1.20,
    "gentle on hair": 1.20,
    "strong suction": 1.20,
    "picks up pet hair": 1.20,
    "long-lasting": 1.20,
    "effective filtration": 1.20,
    "easy to empty": 1.08,
    "good maneuverability": 1.08,
    "easy filter change": 1.08,
    "easy to maintain": 1.08,
    "quiet": 1.08,
}


def _canonicalize_tag_label(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip()).title()
    if not value:
        return ""
    for pattern, replacement in _CANONICAL_LABEL_PATTERNS:
        if re.search(pattern, value, flags=re.IGNORECASE):
            return replacement
    return value


def _label_severity_weight(label: str, *, kind: str = "detractors") -> float:
    label_norm = str(label or "").lower().strip()
    if kind == "detractors":
        if _HIGH_SEVERITY_RE.search(label_norm):
            return 1.35
        if _MEDIUM_SEVERITY_RE.search(label_norm):
            return 1.18
        return 1.05
    if _HIGH_VALUE_DELIGHT_RE.search(label_norm):
        return 1.20
    if _MEDIUM_VALUE_DELIGHT_RE.search(label_norm):
        return 1.08
    return 1.00


def get_severity_weight(label: str, *, kind: str = "detractors") -> float:
    override = _SEVERITY_OVERRIDES.get(str(label or "").lower().strip())
    if override is not None:
        return override
    return _label_severity_weight(label, kind=kind)


# ---------------------------------------------------------------------------
# Column detection and state helpers
# ---------------------------------------------------------------------------

def get_symptom_col_lists(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    det_cols: List[str] = []
    del_cols: List[str] = []
    for col in df.columns:
        cs = str(col).strip()
        if not _SYMPTOM_COL_RE.match(cs):
            continue
        if re.search(r"detract", cs, re.IGNORECASE):
            det_cols.append(col)
        elif re.search(r"delight", cs, re.IGNORECASE):
            del_cols.append(col)
    return det_cols, del_cols


def detect_symptom_state(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "empty"
    sv = str(value).strip()
    if sv == "":
        return "empty"
    if sv.lower() in SYMPTOM_NON_VALUES:
        return "non_value"
    return "valid"


# ---------------------------------------------------------------------------
# Core long-table prep
# ---------------------------------------------------------------------------

def _empty_symptom_table() -> pd.DataFrame:
    return pd.DataFrame(columns=["Item", "Mentions", "% Tagged Reviews", "Avg Star", "Avg Tags/Review"])


def _prepare_symptom_long(df_in: pd.DataFrame, symptom_cols: Sequence[str]) -> Tuple[pd.DataFrame, int]:
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
    mask = (s != "") & (~s.str.lower().isin(SYMPTOM_NON_VALUES)) & (~s.str.startswith("<"))
    long = long.loc[mask, ["__row"]].copy()
    if long.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0

    long["symptom"] = s.loc[mask].map(_canonicalize_tag_label)
    long = long.loc[long["symptom"].astype(str).str.strip().ne("")].copy()
    long = long.drop_duplicates(subset=["__row", "symptom"])
    if long.empty:
        return pd.DataFrame(columns=["__row", "symptom", "symptom_count", "review_weight", "star"]), 0

    symptomized_reviews = int(long["__row"].nunique())
    long = long.loc[~long["symptom"].isin(_EXCLUDED_ANALYTICS_LABELS)].copy()
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


# ---------------------------------------------------------------------------
# Public aggregation API
# ---------------------------------------------------------------------------

def analyze_symptoms_fast(
    df: pd.DataFrame,
    symptom_cols: Sequence[str],
    *,
    kind: Optional[str] = None,
    total_reviews: Optional[int] = None,
):
    """Aggregate symptom tags into either a display DataFrame or legacy rows.

    Backward compatibility:
    - kind is None  -> returns a display DataFrame used by current app/tests
    - kind provided -> returns a List[SymptomRow] used by older tests/callers
    """
    long, symptomized_reviews = _prepare_symptom_long(df, symptom_cols)
    if long.empty:
        return _empty_symptom_table() if kind is None else []

    grouped = long.groupby("symptom", dropna=False)
    mention_reviews = grouped["__row"].nunique().astype(int)
    avg_tags = grouped["symptom_count"].mean().astype(float)
    avg_stars = grouped["star"].mean() if "star" in long.columns else pd.Series(index=mention_reviews.index, dtype=float)
    weighted_mentions = grouped["review_weight"].sum().astype(float)

    items = [str(item).title() for item in mention_reviews.index.tolist()]

    table = pd.DataFrame({
        "Item": items,
        "Mentions": mention_reviews.values.astype(int),
        "% Tagged Reviews": (mention_reviews.values / max(symptomized_reviews, 1) * 100).round(1).astype(str) + "%",
        "Avg Star": [round(float(avg_stars[item]), 1) if item in avg_stars and not pd.isna(avg_stars[item]) else np.nan for item in mention_reviews.index],
        "Avg Tags/Review": np.round(avg_tags.values.astype(float), 2),
        "__Weighted Mentions": weighted_mentions.values.astype(float),
        "__Mention Reviews": mention_reviews.values.astype(int),
        "__Symptomized Reviews": symptomized_reviews,
        "__All Reviews": int(total_reviews or len(df)),
    }).sort_values(["Mentions", "__Weighted Mentions", "Item"], ascending=[False, False, True], ignore_index=True)
    table.attrs["symptomized_review_count"] = symptomized_reviews
    table.attrs["all_review_count"] = int(total_reviews or len(df))

    if kind is None:
        return table

    resolved_kind = str(kind or "detractors")
    rows: List[SymptomRow] = []
    for _, row in table.iterrows():
        label = str(row.get("Item", ""))
        mentions = int(row.get("Mentions", 0) or 0)
        pct_num = float(str(row.get("% Tagged Reviews", "0")).replace("%", "") or 0)
        avg_star = pd.to_numeric(pd.Series([row.get("Avg Star")]), errors="coerce").iloc[0]
        if pd.isna(avg_star):
            avg_star = 0.0
        weight = get_severity_weight(label, kind=resolved_kind)
        direction = -1.0 if resolved_kind.startswith("det") else 1.0
        rows.append(SymptomRow(
            item=label,
            side=resolved_kind,
            count=mentions,
            pct=pct_num,
            avg_rating=float(avg_star),
            severity_weight=weight,
            raw_impact=round(mentions * weight * direction, 2),
        ))
    return rows


def _infer_symptom_total_reviews(tbl: pd.DataFrame) -> int:
    if tbl is None or tbl.empty:
        return 0
    if "__All Reviews" in tbl.columns:
        total = int(pd.to_numeric(tbl["__All Reviews"], errors="coerce").fillna(0).max() or 0)
        if total > 0:
            return total
    pct_col = "% Tagged Reviews" if "% Tagged Reviews" in tbl.columns else ("% Total" if "% Total" in tbl.columns else None)
    if pct_col is None:
        return max(int(pd.to_numeric(tbl.get("Mentions"), errors="coerce").fillna(0).max() or 0), 0)
    pct = pd.to_numeric(tbl[pct_col].astype(str).str.replace("%", "", regex=False), errors="coerce")
    mentions = pd.to_numeric(tbl.get("Mentions"), errors="coerce").fillna(0)
    ratios = mentions / (pct / 100.0)
    ratios = ratios[(pct > 0) & ratios.notna() & (ratios > 0)]
    if ratios.empty:
        return max(int(mentions.max() or 0), 0)
    return max(int(round(float(ratios.median()))), 1)


def _compute_detailed_symptom_impact(df_in: pd.DataFrame, symptom_cols: Sequence[str], baseline: float, *, kind: str):
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


def _alignment_confidence(avg_star: pd.Series, baseline: float, *, kind: str) -> pd.Series:
    stars = pd.to_numeric(avg_star, errors="coerce")
    if str(kind).lower().startswith("del"):
        gap = (stars - float(baseline)).clip(lower=0)
    else:
        gap = (float(baseline) - stars).clip(lower=0)
    gap = gap.fillna(0.0)
    scaled = (gap / 1.25).clip(lower=0.0, upper=1.0)
    return 0.45 + 0.55 * scaled


def add_net_hit(
    rows: Sequence[Any],
    avg_rating: float,
    *,
    total_reviews: int = 0,
    kind: str = "detractors",
    shrink_k: float = 3.0,
    detail_df: Optional[pd.DataFrame] = None,
    symptom_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Compute net-hit / impact columns for a symptom table.

    Accepts either the display DataFrame from analyze_symptoms_fast(kind=None)
    or a legacy list of SymptomRow objects.
    """
    if isinstance(rows, pd.DataFrame):
        d = rows.copy()
    else:
        normalized_rows: List[Dict[str, Any]] = []
        for row in rows or []:
            if isinstance(row, SymptomRow):
                normalized_rows.append(row.as_display_dict())
            elif isinstance(row, dict):
                label = str(row.get("Item", row.get("item", ""))).strip()
                pct = row.get("% Tagged Reviews", row.get("pct", 0.0))
                if isinstance(pct, str):
                    pct = pct.replace("%", "")
                normalized_rows.append({
                    "Item": label,
                    "Mentions": int(row.get("Mentions", row.get("Count", row.get("count", 0))) or 0),
                    "% Tagged Reviews": f"{float(pct or 0):.1f}%",
                    "Avg Star": float(row.get("Avg Star", row.get("Avg Rating", row.get("avg_rating", 0.0))) or 0.0),
                    "Avg Tags/Review": row.get("Avg Tags/Review", np.nan),
                    "Net Hit": float(row.get("Net Hit", row.get("net_hit", 0.0)) or 0.0),
                    "Forecast Δ★": float(row.get("Forecast Δ★", row.get("forecast_delta", 0.0)) or 0.0),
                    "Impact Score": float(row.get("Impact Score", row.get("impact_score", 0.0)) or 0.0),
                    "Severity Wt": float(row.get("Severity Wt", row.get("severity_weight", get_severity_weight(label, kind=kind))) or get_severity_weight(label, kind=kind)),
                })
        d = pd.DataFrame(normalized_rows)

    if d is None or d.empty:
        return pd.DataFrame(columns=["Item", "Mentions", "% Tagged Reviews", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", "Severity Wt"])

    baseline = float(avg_rating or 0)
    sign = 1.0 if str(kind).lower().startswith("del") else -1.0
    total_reviews = int(total_reviews or _infer_symptom_total_reviews(d) or (len(detail_df) if detail_df is not None else 0) or 0)
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
        align_conf = _alignment_confidence(d["Avg Star"], baseline, kind=kind).astype(float)
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
        align_conf = _alignment_confidence(filled_stars, baseline, kind=kind).astype(float)
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
    d["Severity Wt"] = d["Item"].map(lambda value: round(float(get_severity_weight(value, kind=kind)), 2))
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
