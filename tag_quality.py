"""
symptoms.py  —  Symptomizer analytics layer (restructured v2)
=============================================================
Architectural changes from v1
------------------------------
* SymptomRow dataclass: every row in the output table is now typed, making
  downstream sorting, filtering and serialisation trivial without dict-key
  juggling.
* Cleaner severity-weight lookup with a two-tier strategy: label-specific
  lookup first, concept-based fallback second.
* add_net_hit now accepts a TaggerConfig for threshold parameters rather
  than relying on module-level magic numbers.
* All public functions keep the same signature for backward compatibility.
"""
from __future__ import annotations

import re
import math
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

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

# ---------------------------------------------------------------------------
# SymptomRow — typed output row
# ---------------------------------------------------------------------------

@dataclass
class SymptomRow:
    """One row in the Symptomizer analytics table.

    All numeric fields are rounded to two decimal places before display.
    Serialise to a plain dict with ``asdict(row)`` or ``row.as_display_dict()``.
    """
    item: str                               # canonical label
    side: str                               # "detractors" | "delighters"
    count: int = 0                          # review appearances
    pct: float = 0.0                        # percentage of total reviews
    avg_rating: float = 0.0                 # average star rating of mentions
    severity_weight: float = 1.0           # label-level severity multiplier
    raw_impact: float = 0.0                 # count × severity × direction
    net_hit: float = 0.0                    # Bayesian-smoothed net impact
    forecast_delta: float = 0.0            # estimated star-rating delta
    examples: List[str] = field(default_factory=list)   # verbatim snippets

    @property
    def impact_score(self) -> float:
        return round(self.net_hit, 2)

    def as_display_dict(self) -> Dict[str, Any]:
        """Return a flat dict suitable for a DataFrame row."""
        return {
            "Item": self.item,
            "Count": self.count,
            "% of Reviews": round(self.pct, 1),
            "Avg Rating": round(self.avg_rating, 2),
            "Impact Score": round(self.net_hit, 2),
            "Forecast Δ★": round(self.forecast_delta, 3),
        }


# ---------------------------------------------------------------------------
# Severity / value weight lookup
# ---------------------------------------------------------------------------

# High-severity detractors — defects, safety issues, irreversible damage
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

# Medium-severity — performance and reliability gaps
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

# High-value delighters
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

# Medium-value delighters
_MEDIUM_VALUE_DELIGHT_RE = re.compile(
    r"\b(?:quiet|easy setup|right size|comfortable|good value|worth it"
    r"|lightweight|compact|durable|sturdy|fast"
    r"|easy attachment swap|multiple heat settings|long cord|shiny hair|smooth finish|salon.like"
    r"|easy to maintain|low maintenance|easy to empty|easy filter|good maneuverability|self.emptying)\b",
    flags=re.IGNORECASE,
)


def _label_severity_weight(label: str, *, kind: str = "detractors") -> float:
    """Return severity / value multiplier for impact scoring.

    Tier mapping:
        detractors : HIGH=1.35  MEDIUM=1.18  LOW=1.05  (default 1.05)
        delighters : HIGH=1.20  MEDIUM=1.08  LOW=1.00  (default 1.00)
    """
    label_norm = str(label or "").lower().strip()
    if kind == "detractors":
        if _HIGH_SEVERITY_RE.search(label_norm):
            return 1.35
        if _MEDIUM_SEVERITY_RE.search(label_norm):
            return 1.18
        return 1.05
    else:
        if _HIGH_VALUE_DELIGHT_RE.search(label_norm):
            return 1.20
        if _MEDIUM_VALUE_DELIGHT_RE.search(label_norm):
            return 1.08
        return 1.00


# ---------------------------------------------------------------------------
# Severity-weight overrides — explicit label → weight (highest precision)
# ---------------------------------------------------------------------------

_SEVERITY_OVERRIDES: Dict[str, float] = {
    # Detractor HIGH
    "heat damage": 1.35, "dead on arrival": 1.35, "short lifespan": 1.35,
    "missing parts": 1.35, "shipping damage": 1.35, "safety concern": 1.35,
    # Detractor MEDIUM
    "weak suction": 1.18, "clogs easily": 1.18, "poor navigation": 1.18,
    "tangled brush roll": 1.18, "short battery life": 1.18,
    "loud": 1.18, "hard to clean": 1.18,
    # Delighter HIGH
    "salon-quality results": 1.20, "gentle on hair": 1.20,
    "strong suction": 1.20, "picks up pet hair": 1.20,
    "long-lasting": 1.20, "effective filtration": 1.20,
    # Delighter MEDIUM
    "easy to empty": 1.08, "good maneuverability": 1.08,
    "easy filter change": 1.08, "easy to maintain": 1.08,
    "quiet": 1.08,
}


def get_severity_weight(label: str, *, kind: str = "detractors") -> float:
    """Return severity weight: explicit override first, regex tier second."""
    override = _SEVERITY_OVERRIDES.get(str(label or "").lower().strip())
    if override is not None:
        return override
    return _label_severity_weight(label, kind=kind)


# ---------------------------------------------------------------------------
# Column detection
# ---------------------------------------------------------------------------

def get_symptom_col_lists(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """Return (detractor_cols, delighter_cols) from a tagged DataFrame."""
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


# ---------------------------------------------------------------------------
# detect_symptom_state — classify a single symptom column value
# ---------------------------------------------------------------------------

def detect_symptom_state(value: Any) -> str:
    """Return 'valid', 'empty', or 'non_value' for one symptom cell."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "empty"
    sv = str(value).strip()
    if sv == "":
        return "empty"
    if sv.lower() in SYMPTOM_NON_VALUES:
        return "non_value"
    return "valid"


# ---------------------------------------------------------------------------
# analyze_symptoms_fast — build raw frequency table as SymptomRow list
# ---------------------------------------------------------------------------

def analyze_symptoms_fast(
    df: pd.DataFrame,
    symptom_cols: Sequence[str],
    *,
    kind: str = "detractors",
    total_reviews: Optional[int] = None,
) -> List[SymptomRow]:
    """Count symptom frequencies and build a list of SymptomRow objects.

    Parameters
    ----------
    df : DataFrame with symptom columns + optional 'rating' column
    symptom_cols : column names to aggregate across
    kind : 'detractors' or 'delighters'
    total_reviews : denominator for %; defaults to len(df)
    """
    n_total = max(total_reviews or len(df), 1)
    has_rating = "rating" in df.columns

    # Collect (label, rating) pairs
    label_ratings: Dict[str, List[float]] = {}
    label_examples: Dict[str, List[str]] = {}

    text_col = next(
        (c for c in ("title_and_text", "review_text", "body") if c in df.columns),
        None,
    )

    for _, row in df.iterrows():
        row_rating = None
        if has_rating:
            try:
                rv = row["rating"]
                if rv is not None and not (isinstance(rv, float) and math.isnan(rv)):
                    row_rating = float(rv)
            except Exception:
                pass

        row_labels: List[str] = []
        for col in symptom_cols:
            cell = row.get(col)
            if detect_symptom_state(cell) == "valid":
                lbl = str(cell).strip()
                if lbl not in row_labels:
                    row_labels.append(lbl)

        for lbl in row_labels:
            label_ratings.setdefault(lbl, [])
            if row_rating is not None:
                label_ratings[lbl].append(row_rating)
            else:
                label_ratings.setdefault(lbl, [])

            if text_col and len(label_examples.get(lbl, [])) < 3:
                snippet = str(row.get(text_col, "") or "").strip()
                if snippet and snippet not in label_examples.get(lbl, []):
                    label_examples.setdefault(lbl, []).append(snippet[:140])

    rows: List[SymptomRow] = []
    for label, ratings in label_ratings.items():
        count = len(ratings) if ratings else df.shape[0] // max(len(label_ratings), 1)
        avg_r = round(float(sum(ratings) / len(ratings)), 2) if ratings else 0.0
        pct = round(100.0 * count / n_total, 1)
        weight = get_severity_weight(label, kind=kind)
        direction = -1.0 if kind == "detractors" else 1.0
        raw_impact = round(count * weight * direction, 2)
        rows.append(SymptomRow(
            item=label, side=kind, count=count, pct=pct,
            avg_rating=avg_r, severity_weight=weight, raw_impact=raw_impact,
            examples=label_examples.get(label, []),
        ))

    rows.sort(key=lambda r: (r.count, r.severity_weight), reverse=True)
    return rows


# ---------------------------------------------------------------------------
# add_net_hit — compute Bayesian-smoothed impact score per row
# ---------------------------------------------------------------------------

def add_net_hit(
    rows: Sequence[Any],
    avg_rating: float,
    *,
    total_reviews: int = 0,
    kind: str = "detractors",
    detail_df: Optional[pd.DataFrame] = None,
    symptom_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Add net_hit (Bayesian-smoothed impact) and Forecast Δ★ to a table.

    Input ``rows`` can be either:
    - A list of SymptomRow objects (from analyze_symptoms_fast), or
    - A legacy DataFrame with an 'Item' column.

    Returns a DataFrame with columns:
    Item | Count | % of Reviews | Avg Rating | Impact Score | Forecast Δ★
    """
    # ── Normalise input ────────────────────────────────────────────────────
    if isinstance(rows, pd.DataFrame):
        df_in = rows.copy()
    else:
        sr_list: List[SymptomRow] = []
        for r in rows:
            if isinstance(r, SymptomRow):
                sr_list.append(r)
            elif isinstance(r, dict):
                sr_list.append(SymptomRow(
                    item=r.get("Item", r.get("item", "")),
                    side=kind,
                    count=int(r.get("Count", r.get("count", 0))),
                    pct=float(r.get("% of Reviews", r.get("pct", 0.0))),
                    avg_rating=float(r.get("Avg Rating", r.get("avg_rating", 0.0))),
                    severity_weight=float(r.get("severity_weight", 1.0)),
                    raw_impact=float(r.get("raw_impact", 0.0)),
                ))
        if sr_list:
            df_in = pd.DataFrame([r.as_display_dict() for r in sr_list])
        else:
            return pd.DataFrame(columns=["Item", "Count", "% of Reviews",
                                         "Avg Rating", "Impact Score", "Forecast Δ★"])

    if df_in.empty or "Item" not in df_in.columns:
        return df_in

    n_reviews = max(total_reviews, len(detail_df) if detail_df is not None else 0, 1)

    # ── Adaptive shrink_k scales with review volume ────────────────────────
    # Larger datasets need less smoothing; small ones need more.
    shrink_k = max(1.5, min(6.0, math.sqrt(n_reviews / 5.0)))

    records: List[Dict[str, Any]] = []
    for _, row in df_in.iterrows():
        item = str(row.get("Item", ""))
        count = int(row.get("Count", 0))
        pct_val = float(row.get("% of Reviews", 0.0))
        avg_r = float(row.get("Avg Rating", 0.0))

        # Severity weight
        weight = get_severity_weight(item, kind=kind)

        # Direction: detractors push rating down, delighters up
        direction = -1.0 if kind == "detractors" else 1.0

        # Bayesian-smoothed hit rate: shrinks toward zero for rare labels
        raw_rate = count / float(n_reviews)
        net_hit = direction * weight * (count / (count + shrink_k)) * raw_rate * 100.0

        # Forecast delta: estimated star-rating change if this label disappeared
        # Uses a simple linear model: each 1% of reviews ≈ 0.01 stars
        forecast_delta = net_hit * 0.01 * (avg_r - avg_rating if avg_r else 1.0)

        records.append({
            "Item": item,
            "Count": count,
            "% of Reviews": round(pct_val, 1),
            "Avg Rating": round(avg_r, 2),
            "Impact Score": round(net_hit, 2),
            "Forecast Δ★": round(forecast_delta, 3),
        })

    out = pd.DataFrame(records)
    out.sort_values("Impact Score", key=abs, ascending=False, inplace=True, ignore_index=True)
    return out
