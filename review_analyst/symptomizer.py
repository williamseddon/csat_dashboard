"""
symptomizer.py — Evidence-first two-pass tagging engine (v3)
=============================================================
Architecture changes from v2 (inline in app.py)
-------------------------------------------------
* RATING-GATED POLARITY: Reviews are pre-filtered so 5★ reviews never
  receive detractor labels and 1-2★ reviews never receive delighter
  labels unless the review text contains explicit mixed-sentiment
  markers. This alone eliminates the largest cross-polarity hallucination
  vector.
* EVIDENCE-FIRST PROMPT: The prompt asks the AI to extract verbatim
  claims FIRST, then map each claim to catalog labels. This inverts
  the old flow where the AI picked a label and then searched for
  supporting text.
* ADAPTIVE BATCH SIZING: Batch size scales with review length — short
  reviews pack more per call, long reviews get smaller batches to
  stay within quality limits.
* CONFIDENCE SCORING: Each returned tag gets a confidence float (0–1)
  based on evidence quality, rating alignment, and catalog match
  tightness. Downstream consumers can threshold on confidence.
* AUTO-RETRY: Reviews that return zero tags on either expected side
  get re-processed with a simpler, focused single-review prompt.
* STRUCTURED RESULT: Returns SymptomResult dataclass instead of raw
  dict, making the pipeline type-safe and easier to extend.

Public API
----------
tag_review_batch(...)  → Dict[int, SymptomResult]
    Drop-in replacement for app.py's _call_symptomizer_batch.

estimate_batch_size(items) → int
    Adaptive batch sizing based on review lengths.

gate_polarity(rating, review_text) → Tuple[bool, bool]
    Returns (needs_det, needs_del) based on rating + text signals.
"""
from __future__ import annotations

import difflib
import hashlib
import json
import math
import re
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple


logger = logging.getLogger("starwalk.symptomizer")


# ---------------------------------------------------------------------------
# Result cache — avoids re-processing identical reviews
# ---------------------------------------------------------------------------

class ResultCache:
    """In-memory content-hash cache for symptomizer results.
    
    Key = hash(review_text + sorted_catalog_labels + model_name).
    Value = the result dict for that review.
    
    Survives within a single Streamlit session. Cleared on taxonomy change.
    """
    def __init__(self, max_size: int = 5000):
        self._store: Dict[str, Dict[str, Any]] = {}
        self._max = max_size
        self._hits = 0
        self._misses = 0
    
    def _key(self, review_text: str, catalog_hash: str, model: str = "") -> str:
        raw = f"{review_text.strip().lower()}|{catalog_hash}|{model}"
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:24]
    
    def get(self, review_text: str, catalog_hash: str, model: str = "") -> Optional[Dict[str, Any]]:
        result = self._store.get(self._key(review_text, catalog_hash, model))
        if result is not None:
            self._hits += 1
        else:
            self._misses += 1
        return result
    
    def put(self, review_text: str, catalog_hash: str, model: str, result: Dict[str, Any]) -> None:
        if len(self._store) >= self._max:
            # Evict oldest 20%
            keys = list(self._store.keys())
            for k in keys[:len(keys) // 5]:
                del self._store[k]
        self._store[self._key(review_text, catalog_hash, model)] = result
    
    def clear(self) -> None:
        self._store.clear()
        self._hits = 0
        self._misses = 0
    
    @property
    def stats(self) -> Dict[str, int]:
        return {"size": len(self._store), "hits": self._hits, "misses": self._misses}

    @staticmethod
    def catalog_hash(
        detractors: Sequence[str],
        delighters: Sequence[str],
        **context: Any,
    ) -> str:
        """Deterministic hash of the active tagging context for cache keying.

        The early cache only keyed on the flat label lists, which meant a run
        could incorrectly reuse stale outputs after changing aliases, guidance
        specs, product knowledge, prompt context, or evidence settings.
        Including those prompt-shaping inputs keeps the cache fast without
        leaking prior decisions into materially different runs.
        """

        def _stable(value: Any) -> Any:
            if isinstance(value, dict):
                return {str(key): _stable(value[key]) for key in sorted(value)}
            if isinstance(value, (list, tuple, set)):
                return [_stable(v) for v in value]
            if hasattr(value, "__dict__"):
                return _stable(value.__dict__)
            return str(value)

        payload = {
            "detractors": [str(d).strip().lower() for d in (detractors or []) if str(d).strip()],
            "delighters": [str(d).strip().lower() for d in (delighters or []) if str(d).strip()],
            "context": _stable(context),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:24]


# Module-level singleton — lives for the Streamlit session
_result_cache = ResultCache()


def prune_taxonomy(
    allowed: Sequence[str],
    historical_counts: Dict[str, int],
    *,
    min_reviews_for_prune: int = 30,
    max_labels: int = 25,
) -> Tuple[List[str], List[str]]:
    """Prune taxonomy labels before a tagging run.

    Returns ``(primary_labels, extended_labels)``. Primary labels go in the
    main prompt. Extended labels are available to follow-up or sparse-result
    recovery passes.
    """

    ordered = [str(label).strip() for label in (allowed or []) if str(label).strip()]
    if not ordered:
        return [], []

    total_signal = sum(int(historical_counts.get(label, 0) or 0) for label in ordered)
    approx_reviews = int(round(total_signal / max(len(ordered), 1)))
    if approx_reviews < int(min_reviews_for_prune):
        return ordered[:max_labels], ordered[max_labels:]

    active = [label for label in ordered if int(historical_counts.get(label, 0) or 0) > 0]
    pruned = [label for label in ordered if int(historical_counts.get(label, 0) or 0) <= 0]
    if not active:
        active = list(ordered)
        pruned = []

    active.sort(key=lambda label: (-int(historical_counts.get(label, 0) or 0), ordered.index(label)))
    primary = active[:max_labels]
    extended = active[max_labels:] + pruned

    if pruned:
        logger.info("Pruned %d zero-hit labels: %s", len(pruned), ", ".join(pruned[:5]))
    return primary, extended


def deduplicate_taxonomy_labels(
    labels: Sequence[str],
    aliases: Optional[Dict[str, List[str]]] = None,
    *,
    threshold: float = 0.55,
) -> Tuple[List[str], Dict[str, List[str]]]:
    """Merge near-duplicate labels before they enter the active catalog.

    Example: ``Loud Noise`` + ``Noisy Motor`` + ``Loud Motor`` can collapse
    into a single primary label while the extras become aliases.
    """

    alias_map: Dict[str, List[str]] = {
        str(key).strip(): [str(v).strip() for v in (vals or []) if str(v).strip()]
        for key, vals in dict(aliases or {}).items()
        if str(key).strip()
    }
    seen_stems: Dict[str, str] = {}
    out: List[str] = []

    for raw_label in (labels or []):
        label = str(raw_label).strip()
        if not label:
            continue
        stems = _tokenize_stemmed(label)
        if not stems:
            out.append(label)
            continue

        merged = False
        for existing_key, existing_label in list(seen_stems.items()):
            existing_stems = set(existing_key.split("|")) if existing_key else set()
            if not existing_stems:
                continue
            overlap = len(stems & existing_stems) / max(len(stems), len(existing_stems))
            if overlap >= threshold:
                alias_map.setdefault(existing_label, [])
                if label not in alias_map[existing_label]:
                    alias_map[existing_label].append(label)
                logger.info("Dedup: merged '%s' into '%s' (%.0f%% overlap)", label, existing_label, overlap * 100)
                merged = True
                break

        if not merged:
            out.append(label)
            seen_stems["|".join(sorted(stems))] = label

    return out, alias_map

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TagHit:
    """A single symptom tag with evidence and confidence."""
    label: str
    evidence: List[str] = field(default_factory=list)
    confidence: float = 1.0
    source: str = "ai"  # "ai" | "refined" | "universal" | "retry"


@dataclass
class SymptomResult:
    """Full symptomizer result for one review."""
    detractors: List[TagHit] = field(default_factory=list)
    delighters: List[TagHit] = field(default_factory=list)
    unlisted_detractors: List[str] = field(default_factory=list)
    unlisted_delighters: List[str] = field(default_factory=list)
    safety: str = "Not Mentioned"
    reliability: str = "Not Mentioned"
    sessions: str = "Unknown"
    confidence_avg: float = 0.0
    retried: bool = False

    # ── Convenience accessors for backward compatibility ──────────────
    @property
    def dets(self) -> List[str]:
        return [t.label for t in self.detractors]

    @property
    def dels(self) -> List[str]:
        return [t.label for t in self.delighters]

    @property
    def ev_det(self) -> Dict[str, List[str]]:
        return {t.label: t.evidence for t in self.detractors if t.evidence}

    @property
    def ev_del(self) -> Dict[str, List[str]]:
        return {t.label: t.evidence for t in self.delighters if t.evidence}

    def to_legacy_dict(self) -> Dict[str, Any]:
        """Convert to the dict format app.py currently expects."""
        return dict(
            dels=self.dels,
            dets=self.dets,
            ev_del=self.ev_del,
            ev_det=self.ev_det,
            unl_dels=self.unlisted_delighters,
            unl_dets=self.unlisted_detractors,
            safety=self.safety,
            reliability=self.reliability,
            sessions=self.sessions,
        )


@dataclass
class SymptomSpec:
    """Rich symptom definition with tagging guidance.

    When populated, these fields are injected into the system prompt so the AI
    knows exactly when to tag each symptom. This helps reduce polarity and
    ambiguity errors for labels like "Hair Damage" or "Noise" where negation
    and comparative phrasing matter.
    """

    name: str
    desc: str = ""
    detractor_signal: str = ""
    delighter_signal: str = ""
    ambiguity_rule: str = ""
    priority: str = "normal"
    aliases: List[str] = field(default_factory=list)
    side_lock: str = ""


def _coerce_symptom_spec(spec: Any) -> Optional[SymptomSpec]:
    """Coerce raw session payloads into :class:`SymptomSpec` objects.

    Streamlit session state may hold dataclasses, dicts, or plain strings. This
    helper keeps the prompt layer backward compatible while letting the app pass
    richer catalog objects without a separate conversion step.
    """

    if isinstance(spec, SymptomSpec):
        return spec
    if isinstance(spec, Mapping):
        name = str(spec.get("name", "")).strip()
        if not name:
            return None
        return SymptomSpec(
            name=name,
            desc=str(spec.get("desc", "")).strip(),
            detractor_signal=str(spec.get("detractor_signal", "")).strip(),
            delighter_signal=str(spec.get("delighter_signal", "")).strip(),
            ambiguity_rule=str(spec.get("ambiguity_rule", "")).strip(),
            priority=str(spec.get("priority", "normal")).strip() or "normal",
            aliases=[str(v).strip() for v in (spec.get("aliases") or []) if str(v).strip()],
            side_lock=str(spec.get("side_lock", "")).strip(),
        )
    if isinstance(spec, str) and spec.strip():
        return SymptomSpec(name=spec.strip())
    return None


def generate_symptom_guidance(
    *,
    client: Any,
    label: str,
    side: str,
    sample_evidence: List[str],
    product_context: str = "",
    chat_complete_fn: Optional[Callable] = None,
    safe_json_load_fn: Optional[Callable] = None,
    model_fn: Optional[Callable] = None,
) -> SymptomSpec:
    """Auto-generate rich :class:`SymptomSpec` guidance for a bare label.

    Uses sample evidence snippets from historical tagging to infer when this
    label should be tagged as a detractor, delighter, or skipped. This is a
    one-time catalog-enrichment helper, not something meant to run per review.
    """

    clean_label = str(label or "").strip()
    if not clean_label:
        return SymptomSpec(name="")
    if not chat_complete_fn:
        return SymptomSpec(name=clean_label)

    _jload = safe_json_load_fn or _default_json_load
    evidence_text = "\n".join(f'  - "{str(e).strip()}"' for e in (sample_evidence or [])[:10] if str(e).strip())

    prompt = f"""Generate tagging guidance for the symptom label \"{clean_label}\" ({side} side).

Product context: {product_context or 'consumer product'}

Example evidence snippets where this label was tagged:
{evidence_text or '  - (no evidence provided)'}

Generate:
1. desc: one sentence describing what this symptom covers
2. detractor_signal: describe when a reviewer is complaining about this (negative experience). Use \"always\" if this is always negative (e.g., a defect). Use \"never\" if this can never be negative.
3. delighter_signal: describe when a reviewer is praising this (positive experience). Use \"always\" if this is always positive. Use \"never\" if this can never be positive.
4. ambiguity_rule: describe edge cases where you would skip tagging (neutral mentions, unclear attribution, overlap with another symptom)

CRITICAL: The detractor and delighter signals must handle NEGATION correctly.
Example for \"Hair Damage\":
- detractor_signal: \"reviewer reports hair damage, dryness, breakage, or split ends after using the product\"
- delighter_signal: \"reviewer praises that the product did NOT damage their hair, or says hair feels healthy/protected after use\"
- ambiguity_rule: \"reviewer mentions general hair texture without attributing change to this product -> skip\"

JSON only: {{"desc":"...","detractor_signal":"...","delighter_signal":"...","ambiguity_rule":"..."}}"""

    try:
        result = chat_complete_fn(
            client,
            model=(model_fn or (lambda: "gpt-4o"))(),
            structured=True,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=400,
        )
        data = _jload(result)
        return SymptomSpec(
            name=clean_label,
            desc=str(data.get("desc", "")).strip(),
            detractor_signal=str(data.get("detractor_signal", "")).strip(),
            delighter_signal=str(data.get("delighter_signal", "")).strip(),
            ambiguity_rule=str(data.get("ambiguity_rule", "")).strip(),
        )
    except Exception as exc:
        logger.warning("Failed to generate guidance for '%s': %s", clean_label, exc)
        return SymptomSpec(name=clean_label)


# ---------------------------------------------------------------------------
# Polarity gating
# ---------------------------------------------------------------------------

_MIXED_SIGNAL = re.compile(
    r"\b(but|however|although|except|yet|while|wish|if only|"
    r"on the other hand|that said|pros and cons|"
    r"only complaint|only (issue|problem|downside)|"
    r"other than that|aside from|apart from|"
    r"would be (better|nice|great) if|my only)\b",
    re.IGNORECASE,
)

_EXPLICIT_NEGATIVE = re.compile(
    r"\b(broke|broken|fail|defect|issue|problem|stopped|won't|doesn't|"
    r"difficult|terrible|awful|hate|disappointed|returned|returning|"
    r"worst|horrible|useless|waste|junk|garbage|regret|refund|"
    r"burned|melted|smoke|smoking|shocked|dangerous)\b",
    re.IGNORECASE,
)

_EXPLICIT_POSITIVE = re.compile(
    r"\b(love|perfect|amazing|excellent|great|easy|recommend|best|"
    r"happy|satisfied|fantastic|wonderful|awesome|"
    r"impressed|obsessed|game.?changer|holy grail|worth every|"
    r"smooth|silky|shiny|beautiful|gorgeous|salon)\b",
    re.IGNORECASE,
)


def gate_polarity(
    rating: Any,
    review_text: str,
) -> Tuple[bool, bool]:
    """Always returns (True, True) — both sides are always eligible for tagging.

    Rating and text signals feed into confidence scoring downstream, not
    into hard gating.  A 5★ review that says "but the cord is too short"
    needs that detractor.  A 1★ review that says "love the color though"
    needs that delighter.

    Returns (needs_detractors, needs_delighters) — always (True, True).
    """
    return True, True


def polarity_confidence_modifier(
    rating: Any,
    review_text: str,
    side: str,
) -> float:
    """Soft confidence modifier based on rating + text alignment.

    Returns a float in [-0.25, +0.20] that adjusts tag confidence.
    Cross-polarity tags (detractor on 5★, delighter on 1★) get a penalty
    UNLESS the review text contains explicit signals for that side.

    This replaces the old hard gate — rating influences confidence, never
    blocks a tag entirely.
    """
    try:
        r = float(rating)
    except (TypeError, ValueError):
        return 0.0  # unknown rating → neutral

    text = str(review_text or "")
    has_mixed = bool(_MIXED_SIGNAL.search(text))
    has_negative = bool(_EXPLICIT_NEGATIVE.search(text))
    has_positive = bool(_EXPLICIT_POSITIVE.search(text))

    modifier = 0.0

    if side == "detractor":
        if r <= 2:
            modifier = +0.15          # expected — boost
        elif r <= 3:
            modifier = +0.05          # neutral territory
        elif r >= 5:
            if has_negative or has_mixed:
                modifier = -0.05      # cross-polarity but text supports it — mild penalty
            else:
                modifier = -0.18      # cross-polarity, no text support — stronger penalty
        elif r >= 4:
            if has_negative or has_mixed:
                modifier = 0.0
            else:
                modifier = -0.10
    elif side == "delighter":
        if r >= 4:
            modifier = +0.15          # expected — boost
        elif r <= 3 and r > 2:
            modifier = +0.05          # neutral territory
        elif r <= 2:
            if has_positive or has_mixed:
                modifier = -0.05      # cross-polarity but text supports it
            else:
                modifier = -0.18      # cross-polarity, no text support
        elif r <= 3:
            if has_positive or has_mixed:
                modifier = 0.0
            else:
                modifier = -0.10

    return modifier


# ---------------------------------------------------------------------------
# Adaptive batch sizing
# ---------------------------------------------------------------------------

def estimate_batch_size(
    items: Sequence[Mapping[str, Any]],
    *,
    max_batch: int = 10,
    min_batch: int = 2,
    target_tokens_per_batch: int = 4000,
) -> int:
    """Scale batch size based on total estimated review tokens."""
    if not items:
        return min_batch
    total_chars = sum(len(str(it.get("review", ""))) for it in items)
    avg_chars = total_chars / len(items)
    avg_tokens = max(1, int(avg_chars / 3.5))
    ideal = max(min_batch, min(max_batch, target_tokens_per_batch // avg_tokens))
    return ideal


# ---------------------------------------------------------------------------
# Label matching (improved)
# ---------------------------------------------------------------------------

_STEM_SUFFIXES = ["ation","tion","ness","ment","able","ible","ful","less","ous","ive","ing","ied","ies","ed","er","est","ly","al","ity","es","ey","y","e","s"]
_STOP_TOKENS = {"a","an","and","the","to","for","of","in","on","with","is","it","very","really","too","so","not","no"}

def _stem(word: str) -> str:
    w = word.lower().strip()
    for s in _STEM_SUFFIXES:
        if len(w) > len(s) + 2 and w.endswith(s): return w[:-len(s)]
    return w

def _tokenize_stemmed(text: str) -> set:
    return {_stem(t) for t in re.findall(r"[a-z]+", text.lower()) if t not in _STOP_TOKENS and len(t) > 2}

def _canon(s: Any) -> str:
    return " ".join(str(s or "").split()).lower().strip()

def _canon_alpha(s: Any) -> str:
    return "".join(ch for ch in _canon(s) if ch.isalnum())


def match_label(
    raw: str,
    allowed: Sequence[str],
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    cutoff: float = 0.72,
) -> Optional[str]:
    """Match a raw AI-returned label to the allowed catalog.

    Matching cascade:
    1. Exact match (case-insensitive, whitespace-normalized)
    2. Alias match
    3. Fuzzy match (difflib, lowered cutoff from 0.76 → 0.72 for better recall)
    4. Substring containment (bidirectional)
    5. Token overlap (≥60% of tokens shared)
    """
    if not raw or not allowed:
        return None
    raw_s = raw.strip()
    raw_canon = _canon_alpha(raw_s)

    # 1. Exact
    exact_map = {_canon_alpha(x): x for x in allowed}
    hit = exact_map.get(raw_canon)
    if hit:
        return hit

    # 2. Alias
    if aliases:
        for canonical, alias_list in aliases.items():
            if canonical not in allowed:
                continue
            for a in (alias_list or []):
                if _canon_alpha(raw_s) == _canon_alpha(a):
                    return canonical

    # 3. Stemmed exact
    raw_stems = _tokenize_stemmed(raw_s)
    if raw_stems:
        for label in allowed:
            if _tokenize_stemmed(label) == raw_stems:
                return label

    # 4. Fuzzy
    m = difflib.get_close_matches(raw_s, allowed, n=1, cutoff=cutoff)
    if m:
        return m[0]

    # 5. Substring containment (bidirectional)
    raw_lower = raw_s.lower()
    for label in allowed:
        if raw_lower in label.lower() or label.lower() in raw_lower:
            return label

    # 6. Word-boundary prefix/suffix: "Hair Damage Issues" → "Hair Damage"
    raw_words = [w for w in raw_lower.split() if w not in _STOP_TOKENS]
    if len(raw_words) >= 2:
        for label in allowed:
            label_words = [w.lower() for w in label.split() if w.lower() not in _STOP_TOKENS]
            if len(label_words) >= 2:
                # Check if all label words appear in raw in order (prefix match)
                if all(lw in raw_words for lw in label_words):
                    return label
                # Check if all raw words appear in label in order (suffix match)
                if all(rw in label_words for rw in raw_words):
                    return label

    # 7. Stemmed token overlap (≥55%)
    if raw_stems and len(raw_stems) >= 2:
        best_score, best_label = 0.0, None
        for label in allowed:
            label_stems = _tokenize_stemmed(label)
            if not label_stems: continue
            overlap = len(raw_stems & label_stems) / max(len(raw_stems), len(label_stems))
            if overlap > best_score: best_score, best_label = overlap, label
        if best_score >= 0.55 and best_label:
            return best_label

    return None


def rank_candidate_labels(
    raw: str,
    allowed: Sequence[str],
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    *,
    cutoff: float = 0.42,
    top_k: int = 5,
) -> List[Tuple[str, float]]:
    """Score likely catalog matches for free-text claim routing.

    This is intentionally more permissive than :func:`match_label`. The staged
    pipeline uses it to shortlist plausible taxonomy labels from an extracted
    claim or aspect phrase, then chooses the best candidate deterministically.
    """

    if not raw or not allowed:
        return []

    raw_s = str(raw or "").strip()
    if not raw_s:
        return []
    raw_lower = raw_s.lower()
    raw_canon = _canon_alpha(raw_s)
    raw_stems = _tokenize_stemmed(raw_s)
    scores: Dict[str, float] = {}
    order_map = {str(label): idx for idx, label in enumerate(list(allowed or []))}

    for label in allowed:
        canonical = str(label).strip()
        if not canonical:
            continue
        variants = [canonical]
        if aliases and canonical in aliases:
            variants.extend(str(v).strip() for v in (aliases.get(canonical) or []) if str(v).strip())

        best = scores.get(canonical, 0.0)
        for variant in variants:
            variant_lower = variant.lower()
            variant_canon = _canon_alpha(variant)
            variant_stems = _tokenize_stemmed(variant)

            if raw_canon and variant_canon and raw_canon == variant_canon:
                best = max(best, 1.0 if variant == canonical else 0.98)
                continue

            if raw_stems and variant_stems and raw_stems == variant_stems:
                best = max(best, 0.94 if variant == canonical else 0.92)

            if raw_lower in variant_lower or variant_lower in raw_lower:
                containment_bonus = min(len(raw_lower), len(variant_lower)) / max(len(raw_lower), len(variant_lower), 1)
                best = max(best, 0.80 + 0.15 * containment_bonus)

            if raw_stems and variant_stems:
                overlap = len(raw_stems & variant_stems) / max(len(raw_stems), len(variant_stems))
                if overlap > 0:
                    best = max(best, 0.45 + 0.40 * overlap)

            seq = difflib.SequenceMatcher(None, raw_canon or raw_lower, variant_canon or variant_lower).ratio()
            if seq > 0:
                best = max(best, 0.35 + 0.45 * seq)

        if best >= cutoff:
            scores[canonical] = max(scores.get(canonical, 0.0), best)

    ranked = sorted(scores.items(), key=lambda item: (-item[1], order_map.get(item[0], 9999)))
    return ranked[:max(int(top_k or 1), 1)]


def _best_claim_label(
    aspect: str,
    text: str,
    catalog: Sequence[str],
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
) -> Optional[str]:
    """Choose the best taxonomy label for a claim using weighted candidates."""

    weighted_scores: Dict[str, float] = {}

    def _add_candidates(raw: str, weight: float, cutoff: float) -> None:
        for label, score in rank_candidate_labels(raw, catalog, aliases=aliases, cutoff=cutoff, top_k=4):
            weighted_scores[label] = weighted_scores.get(label, 0.0) + (score * weight)

    _add_candidates(aspect, 1.30, 0.48)
    _add_candidates(str(text or "")[:96], 0.90, 0.42)

    tokens = [w for w in re.findall(r"[a-zA-Z']+", str(text or "").lower()) if w not in _STOP_TOKENS and len(w) > 2]
    for chunk_size, weight, cutoff in ((4, 0.55, 0.50), (3, 0.45, 0.52), (2, 0.30, 0.56)):
        if len(tokens) < chunk_size:
            continue
        for i in range(len(tokens) - chunk_size + 1):
            _add_candidates(" ".join(tokens[i:i + chunk_size]), weight, cutoff)

    if not weighted_scores:
        return None
    return max(weighted_scores.items(), key=lambda item: item[1])[0]


# ---------------------------------------------------------------------------
# Evidence validation (improved)
# ---------------------------------------------------------------------------

_NEGATION_PREFIX = re.compile(
    r"\b(no|not|never|don't|doesn't|won't|can't|isn't|wasn't|"
    r"wouldn't|shouldn't|without|hardly|barely|nor)\b",
    re.IGNORECASE,
)


_NEG_CONTEXT = re.compile(r"\b(not|no|never|don't|doesn't|won't|can't|isn't|wasn't|without|hardly|barely|nor)\s+", re.I)
_SARCASM_MARKERS = re.compile(r"(yeah right|sure|great[.!,]|wonderful[.!,]|fantastic[.!,]).*\b(broke|fail|terrible|awful|worst)", re.I)

# Try to import the sophisticated fragment-level NegationDetector and
# label-evidence coherence helper from tag_quality.
try:
    from review_analyst.tag_quality import (
        NegationDetector as _NegDetector,
        evidence_supports_label as _evidence_supports_label,
    )
    _HAS_NEG_DETECTOR = True
except Exception:
    _HAS_NEG_DETECTOR = False
    _evidence_supports_label = None

def validate_evidence(
    evidence_list: Sequence[str],
    review_text: str,
    max_chars: int = 120,
    *,
    label: str = "",
) -> List[str]:
    """Validate evidence with fuzzy matching, negation awareness, and quality checks.
    
    Matching cascade:
    1. Exact verbatim match (case-insensitive, whitespace-normalized)
    2. Fuzzy substring match — handles minor AI paraphrasing like
       "really loud" matching inside "really really loud"
    3. Negation filtering (NegationDetector or window-based fallback)
    
    Short reviews (< 30 words) get relaxed minimum evidence length (4 chars).
    """
    if not evidence_list or not review_text:
        return []
    rv_text = re.sub(r"\s+", " ", str(review_text or "").strip())
    rv_norm = rv_text.lower()
    rv_words = len(rv_norm.split())
    # Relax minimum for short reviews
    min_chars = 4 if rv_words < 30 else 6
    min_words = 1 if rv_words < 20 else 2
    out = []
    for e in evidence_list:
        e = str(e).strip()[:max_chars]
        if len(e) < min_chars or len(e.split()) < min_words:
            continue
        e_norm = re.sub(r"\s+", " ", e.lower())
        matched_span = ""
        
        # Tier 1: Exact verbatim match
        found_verbatim = e_norm in rv_norm
        if found_verbatim:
            exact_idx = rv_norm.find(e_norm)
            if exact_idx >= 0:
                matched_span = rv_text[exact_idx:exact_idx + len(e_norm)]
        
        # Tier 2: Fuzzy substring — find if all significant words appear nearby
        found_fuzzy = False
        if not found_verbatim:
            e_words = [w for w in e_norm.split() if len(w) > 2 and w not in _STOP_TOKENS]
            if e_words and len(e_words) >= 2:
                # Check if all content words appear within a 150-char window
                for start_pos in range(0, len(rv_norm) - 10, 20):
                    window = rv_norm[start_pos:start_pos + 150]
                    if all(w in window for w in e_words):
                        positions = []
                        for word in e_words:
                            pos = window.find(word)
                            if pos >= 0:
                                positions.append((start_pos + pos, start_pos + pos + len(word)))
                        if positions:
                            span_start = min(pos[0] for pos in positions)
                            span_end = max(pos[1] for pos in positions)
                            matched_span = rv_text[span_start:span_end].strip()[:max_chars]
                        found_fuzzy = True
                        break
            elif e_words and len(e_words) == 1 and len(e_words[0]) >= 5:
                # Single significant word — just check it exists
                if e_words[0] in rv_norm:
                    found_fuzzy = True
                    single_idx = rv_norm.find(e_words[0])
                    if single_idx >= 0:
                        span_end = min(len(rv_text), single_idx + len(e_words[0]) + 28)
                        matched_span = rv_text[single_idx:span_end].strip()[:max_chars]
        
        if not found_verbatim and not found_fuzzy:
            continue
        
        # Negation check — use sophisticated detector when available
        is_negated = False
        if _HAS_NEG_DETECTOR and len(e_norm.split()) <= 6:
            idx = rv_norm.find(e_norm) if found_verbatim else _find_fuzzy_position(e_norm, rv_norm)
            if idx >= 0:
                frag_start = max(0, rv_norm.rfind(".", 0, idx) + 1, rv_norm.rfind("!", 0, idx) + 1,
                                 rv_norm.rfind(";", 0, idx) + 1, rv_norm.rfind("—", 0, idx) + 1)
                frag_end = len(rv_norm)
                for delim in ".!;—":
                    pos = rv_norm.find(delim, idx + len(e_norm))
                    if pos > 0:
                        frag_end = min(frag_end, pos + 1)
                fragment = rv_norm[frag_start:frag_end].strip()
                if _NegDetector.is_negated(e_norm, fragment):
                    is_negated = True
                elif label and len(e_norm.split()) <= 3:
                    label_words = [w for w in label.lower().split() if len(w) > 3 and w not in _STOP_TOKENS]
                    for lw in label_words:
                        if lw in fragment and _NegDetector.is_negated(lw, fragment):
                            is_negated = True
                            break
        elif len(e_norm.split()) <= 4:
            idx = rv_norm.find(e_norm)
            if idx >= 0:
                preceding = rv_norm[max(0, idx - 40):idx]
                if _NEG_CONTEXT.search(preceding):
                    is_negated = True
                else:
                    end_idx = idx + len(e_norm)
                    following = rv_norm[end_idx:end_idx + 30]
                    if re.search(r"\b(not|no|never|at all|whatsoever|zero)\b", following):
                        if not re.search(r"\b(but|however|actually|still)\b", following):
                            is_negated = True
        if is_negated:
            continue
        out.append(matched_span or e)
    return out[:3]


def _find_fuzzy_position(evidence_norm: str, review_norm: str) -> int:
    """Find approximate position of evidence in review for negation checking.
    Returns the position of the first content word match, or -1."""
    words = [w for w in evidence_norm.split() if len(w) > 3 and w not in _STOP_TOKENS]
    if not words:
        return -1
    # Find position of first significant word
    pos = review_norm.find(words[0])
    return pos if pos >= 0 else -1


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

_HEDGING = re.compile(r"\b(kind of|sort of|somewhat|slightly|a (little|bit)|maybe|perhaps|not (too|that) bad)\b", re.I)
_EMPHASIS = re.compile(r"\b(very|extremely|incredibly|absolutely|completely|totally|SO|really really|worst|best ever|terrible|horrible|love|hate|perfect|amazing)\b", re.I)
_SEVERITY_HIGH = re.compile(r"\b(explod|fire|burn|shock|injur|hospital|danger|hazard|recall|broke.*first|DOA|caught fire|melted|smoking|sparks?|electr)\b", re.I)

def _score_tag_confidence(
    label: str,
    evidence: List[str],
    review_text: str,
    rating: Any,
    *,
    side: str,
    catalog_size: int,
) -> float:
    """Confidence scoring with hedging downgrade, emphasis boost, severity and repetition awareness."""
    score = 0.5
    ev_text = " ".join(evidence) if evidence else ""

    if evidence:
        rv_lower = review_text.lower()
        matched = sum(1 for e in evidence if e.lower() in rv_lower)
        if matched > 0:
            score += 0.25 * min(1.0, matched / max(len(evidence), 1))
        for ev in evidence:
            word_count = len(str(ev or "").split())
            if word_count >= 6:
                score += 0.08
            elif word_count >= 3:
                score += 0.04
            elif word_count > 0:
                score += 0.01
        # Repetition boost: evidence concept mentioned multiple times = stronger signal
        for e in evidence:
            e_lower = e.lower()
            if len(e_lower) >= 8:
                # Count approximate mentions of the core concept
                core_words = [w for w in e_lower.split() if len(w) > 3 and w not in _STOP_TOKENS]
                if core_words:
                    repeat_count = sum(1 for w in core_words if rv_lower.count(w) > 1)
                    if repeat_count >= 2:
                        score += 0.06  # Repeated complaint/praise = higher confidence
    else:
        score -= 0.15

    # Rating-polarity alignment: soft modifier replaces hard gating
    score += polarity_confidence_modifier(rating, review_text, side)

    # Hedging downgrade: "kind of loud" = lower confidence
    if _HEDGING.search(ev_text): score -= 0.08
    # Emphasis boost: "extremely loud" = higher confidence
    if _EMPHASIS.search(ev_text): score += 0.08
    # Severity boost: safety-critical language for detractors
    if side == "detractor" and _SEVERITY_HIGH.search(ev_text): score += 0.12

    # Sarcasm-rating cross-check: 1★ with positive words = sarcasm → boost detractor confidence
    try:
        r = float(rating)
        rv_lower_full = review_text.lower() if review_text else ""
        if r <= 2 and side == "detractor" and _EXPLICIT_POSITIVE.search(rv_lower_full):
            score += 0.06  # Likely sarcasm — boost detractor confidence
        elif r <= 2 and side == "delighter" and _EXPLICIT_POSITIVE.search(ev_text):
            score -= 0.10  # Positive words in 1-2★ context — likely sarcastic
        elif r >= 5 and side == "detractor" and _EXPLICIT_NEGATIVE.search(ev_text):
            score -= 0.08  # Negative words in 5★ context — likely "was worried but..."
    except (TypeError, ValueError):
        pass

    # Small catalog penalty
    if catalog_size < 8:
        score -= 0.05

    return max(0.0, min(1.0, score))


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SAFETY_ENUM = "Safe, Minor Concern, Safety Issue, Not Mentioned"
RELIABILITY_ENUM = "Reliable, Intermittent Issue, Failure, Not Mentioned"
SESSIONS_ENUM = "1-5, 6-20, 21-50, 50+, Unknown"


def _format_rich_catalog(
    specs: Sequence[SymptomSpec],
    side: str,
    fallback_labels: Sequence[str],
) -> str:
    """Format the symptom catalog for the system prompt.

    If rich :class:`SymptomSpec` entries are provided, emit guidance blocks.
    Otherwise fall back to the original flat label list format.
    """

    rich_specs = [spec for spec in (_coerce_symptom_spec(raw) for raw in (specs or [])) if spec]
    if rich_specs:
        lines: List[str] = []
        for spec in rich_specs:
            lines.append(f"[{spec.name}]")
            if spec.desc:
                lines.append(f"  desc: {spec.desc}")
            if side == "detractor" and spec.detractor_signal:
                if spec.detractor_signal == "always":
                    lines.append("  detractor: ALWAYS tag as detractor when mentioned")
                elif spec.detractor_signal == "never":
                    lines.append("  detractor: NEVER tag as detractor")
                else:
                    lines.append(f"  tag when: {spec.detractor_signal}")
            if side == "delighter" and spec.delighter_signal:
                if spec.delighter_signal == "always":
                    lines.append("  delighter: ALWAYS tag as delighter when mentioned")
                elif spec.delighter_signal == "never":
                    lines.append("  delighter: NEVER tag as delighter")
                else:
                    lines.append(f"  tag when: {spec.delighter_signal}")
            if spec.ambiguity_rule:
                lines.append(f"  skip when: {spec.ambiguity_rule}")
            if spec.priority == "high":
                lines.append("  priority: HIGH — always tag if present even when limit reached")
            if spec.side_lock:
                if spec.side_lock == "detractor_only":
                    lines.append("  side lock: detractor-only")
                elif spec.side_lock == "delighter_only":
                    lines.append("  side lock: delighter-only")
            if spec.aliases:
                lines.append("  aliases: " + ", ".join(spec.aliases[:8]))
            lines.append("")
        return "\n".join(lines).strip() or "  (none defined)"

    return "\n".join(f"  - {label}" for label in fallback_labels) or "  (none defined)"


def _build_system_prompt(
    *,
    allowed_detractors: Sequence[str],
    allowed_delighters: Sequence[str],
    detractor_specs: Optional[Sequence[SymptomSpec]] = None,
    delighter_specs: Optional[Sequence[SymptomSpec]] = None,
    product_profile: str = "",
    product_knowledge_text: str = "",
    taxonomy_context: str = "",
    category: str = "general",
    max_ev_chars: int = 120,
) -> str:
    """Build the system prompt with evidence-first architecture."""
    det_list = _format_rich_catalog(detractor_specs or [], "detractor", allowed_detractors)
    del_list = _format_rich_catalog(delighter_specs or [], "delighter", allowed_delighters)

    return f"""You are an expert consumer review analyst. Your job: tag reviews against a symptom taxonomy.

{f"Product: {product_profile[:600]}" if product_profile else ""}
{f"Product knowledge:\n{product_knowledge_text}" if product_knowledge_text else ""}
Category: {category}.
{taxonomy_context}

═══ DETRACTOR CATALOG (problems / complaints) ═══
{det_list}

═══ DELIGHTER CATALOG (positives / strengths) ═══
{del_list}

═══ EVIDENCE-FIRST METHOD ═══
For each review, work in TWO mental steps:
STEP 1: Read clause by clause. Extract every explicit claim about the product — quote verbatim.
STEP 2: For each claim, check if it maps to a catalog label. Only emit tags with evidence.

If a review has explicit "pros" and "cons" fields, treat pros as pre-confirmed positive claims (map to delighters) and cons as pre-confirmed negative claims (map to detractors). Still require evidence quotes.

═══ RULES (apply in order) ═══
1. READ FIRST: Read the entire review before tagging. Understand the overall sentiment arc.
2. EXACT LABELS: Use catalog label text exactly. No paraphrasing.
3. EVIDENCE REQUIRED: Every tag needs 1-3 verbatim quotes (6+ chars, 2+ words). More evidence = higher confidence. No evidence = no tag.
4. HIGH RECALL: Capture every applicable symptom. Long reviews can map to many labels.
5. BOTH SIDES: Tag BOTH detractors AND delighters for every review regardless of rating. A 5★ review can have minor complaints. A 1★ review can acknowledge positives. Let the evidence guide you.
6. NO INFERENCE: Only tag what is explicitly stated or clearly described.

═══ ACCURACY RULES ═══
7. NEGATION: "didn't damage my hair" and "no issues with noise" are POSITIVE signals, NOT detractors. "it's not loud" means quiet. Read the FULL clause before deciding polarity.
8. SARCASM: "Great, another broken product" is NEGATIVE. Mismatch between positive words and negative context. Rating is a strong signal — 1★ with "great" is sarcastic.
9. TEMPORAL: "Loved at first, now broken" — tag based on FINAL state. "worked for 2 weeks then died" = detractor.
10. COMPARATIVES: "Better than my old one" IS about this product. But only tag the product under review.
11. HEDGING: "Kind of loud" — still tag, but use specific labels over broad universals.
12. SEVERITY: Primary complaints (detailed, repeated) and passing mentions (brief) both get tagged.
13. MULTI-PRODUCT: Only tag symptoms about THIS product. Ignore claims about other products.
14. UNIVERSAL DISCIPLINE: Overall Satisfaction/Dissatisfaction are LAST RESORT. If ANY specific label exists on one side, DROP the universal.
15. CONTRADICTIONS: Don't assign Quiet AND Loud unless review discusses both in different contexts.
16. ZERO IS VALID: No tags better than forced matches.
17. UNLISTED: Add strong missing themes to unlisted arrays. Be conservative.
18. ALL IDS: Return a result for EVERY review id.

═══ COMMON MISTAKES TO AVOID ═══
WRONG: Review says "I was worried it would damage my hair but it didn't" → tagging "Hair Damage" as detractor.
RIGHT: This is a DELIGHTER — the reviewer is praising that no damage occurred.

WRONG: 5★ review "Love everything about this dryer" → tagging zero detractors AND zero delighters.
RIGHT: Tag the delighters! "Love everything" maps to Overall Satisfaction + any specific praise.

WRONG: Review says "cord is short but manageable" → NOT tagging "Short Cord".
RIGHT: Tag it — "cord is short" is an explicit complaint even if hedged. Hedging lowers confidence, not tagging.

WRONG: Review says "my OLD dryer was so loud, this one is quiet" → tagging "Loud" as detractor.
RIGHT: The loudness is about a DIFFERENT product. Tag "Quiet" as a delighter for THIS product.

═══ OUTPUT (strict JSON) ═══
{{"items":[{{
  "id":"<review_id>",
  "detractors":[{{"label":"<exact catalog label>","evidence":["<verbatim>"]}}],
  "delighters":[{{"label":"<exact catalog label>","evidence":["<verbatim>"]}}],
  "unlisted_detractors":["<2-5 word theme>"],
  "unlisted_delighters":["<2-5 word theme>"],
  "safety":"<{SAFETY_ENUM}>",
  "reliability":"<{RELIABILITY_ENUM}>",
  "sessions":"<{SESSIONS_ENUM}>"
}}]}}"""


def _build_user_payload(
    items: Sequence[Mapping[str, Any]],
) -> str:
    """Build the user message with structured pros/cons when available."""
    payload_items = []
    for it in items:
        review_text = str(it.get("review", ""))
        rating = it.get("rating")
        entry = dict(
            id=str(it["idx"]),
            review=review_text,
            rating=rating,
        )
        # Pass pros/cons as structured fields if available
        pros = str(it.get("pros", "")).strip()
        cons = str(it.get("cons", "")).strip()
        if pros and len(pros) > 3:
            entry["pros"] = pros
        if cons and len(cons) > 3:
            entry["cons"] = cons
        payload_items.append(entry)
    return json.dumps(dict(items=payload_items))


# ---------------------------------------------------------------------------
# Core tagging pipeline
# ---------------------------------------------------------------------------

def tag_review_batch(
    *,
    client: Any,
    items: Sequence[Mapping[str, Any]],
    allowed_delighters: Sequence[str],
    allowed_detractors: Sequence[str],
    detractor_specs: Optional[Sequence[SymptomSpec]] = None,
    delighter_specs: Optional[Sequence[SymptomSpec]] = None,
    product_profile: str = "",
    product_knowledge: Any = None,
    max_ev_chars: int = 120,
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    include_universal_neutral: bool = True,
    pre_category: str = "",  # Pre-computed category — skips inference API call
    # Injected callables from app.py (avoids circular import)
    chat_complete_fn: Optional[Callable] = None,
    safe_json_load_fn: Optional[Callable] = None,
    refine_fn: Optional[Callable] = None,
    model_fn: Optional[Callable] = None,
    reasoning_fn: Optional[Callable] = None,
    infer_category_fn: Optional[Callable] = None,
    taxonomy_context_fn: Optional[Callable] = None,
    product_knowledge_text_fn: Optional[Callable] = None,
    custom_universal_fn: Optional[Callable] = None,
    standardize_fn: Optional[Callable] = None,
) -> Dict[int, Dict[str, Any]]:
    """Evidence-first batch tagger. Drop-in replacement for _call_symptomizer_batch.

    Returns dict mapping review index → legacy result dict.
    """
    if not items:
        return {}

    # Resolve injected functions or use defaults
    _json_load = safe_json_load_fn or _default_json_load
    _model = (model_fn or (lambda: "gpt-4o"))()
    _reasoning = (reasoning_fn or (lambda: "none"))()

    merged_aliases: Dict[str, List[str]] = {
        str(key).strip(): [str(v).strip() for v in (vals or []) if str(v).strip()]
        for key, vals in dict(aliases or {}).items()
        if str(key).strip()
    }
    for spec in (_coerce_symptom_spec(raw) for raw in (detractor_specs or [])):
        if spec and spec.aliases:
            merged_aliases.setdefault(spec.name, [])
            for alias in spec.aliases:
                if alias not in merged_aliases[spec.name]:
                    merged_aliases[spec.name].append(alias)
    for spec in (_coerce_symptom_spec(raw) for raw in (delighter_specs or [])):
        if spec and spec.aliases:
            merged_aliases.setdefault(spec.name, [])
            for alias in spec.aliases:
                if alias not in merged_aliases[spec.name]:
                    merged_aliases[spec.name].append(alias)
    aliases = merged_aliases or None

    items_to_process = list(items)

    # Build prompt context — use pre-computed category if available (saves an API call)
    category = "general"
    if pre_category and pre_category != "general":
        category = pre_category
    elif infer_category_fn:
        _cat_cache_key = f"cat_{hashlib.sha256(product_profile.encode('utf-8', errors='replace')).hexdigest()[:12]}"
        _cached_cat = getattr(tag_review_batch, '_category_cache', {}).get(_cat_cache_key)
        if _cached_cat:
            category = _cached_cat
        else:
            try:
                cat_info = infer_category_fn(
                    product_profile,
                    [it.get("review", "") for it in items_to_process[:12]],
                )
                category = cat_info.get("category", "general")
                if not hasattr(tag_review_batch, '_category_cache'):
                    tag_review_batch._category_cache = {}
                tag_review_batch._category_cache[_cat_cache_key] = category
            except Exception:
                pass

    taxonomy_context = ""
    if taxonomy_context_fn:
        try:
            taxonomy_context = taxonomy_context_fn(category)
        except Exception:
            pass

    pk_text = ""
    if product_knowledge_text_fn and product_knowledge:
        try:
            pk_text = product_knowledge_text_fn(product_knowledge, limit_per_section=4)
        except Exception:
            pass

    # ── Cache check: skip items we've already processed ──────────────
    _cat_hash = ResultCache.catalog_hash(
        allowed_detractors,
        allowed_delighters,
        aliases=aliases,
        detractor_specs=detractor_specs,
        delighter_specs=delighter_specs,
        product_profile=product_profile,
        product_knowledge_text=pk_text,
        taxonomy_context=taxonomy_context,
        category=category,
        include_universal_neutral=include_universal_neutral,
        max_ev_chars=max_ev_chars,
    )
    cached_results: Dict[int, Dict[str, Any]] = {}
    uncached_items: List[Mapping[str, Any]] = []
    for it in items_to_process:
        cached = _result_cache.get(str(it.get("review", "")), _cat_hash, _model)
        if cached is not None:
            cached_results[int(it["idx"])] = cached
        else:
            uncached_items.append(it)

    if not uncached_items:
        logger.info("All %d items served from cache", len(items_to_process))
        return cached_results

    items_to_process = uncached_items

    system_prompt = _build_system_prompt(
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        detractor_specs=detractor_specs,
        delighter_specs=delighter_specs,
        product_profile=product_profile,
        product_knowledge_text=pk_text,
        taxonomy_context=taxonomy_context,
        category=category,
        max_ev_chars=max_ev_chars,
    )

    user_payload = _build_user_payload(items_to_process)
    # Dynamic token budget: scales with batch size AND catalog size
    # Large catalogs generate more tags per review → need more output tokens
    catalog_size = len(allowed_detractors) + len(allowed_delighters)
    catalog_multiplier = 1.0 + min(0.5, max(0, catalog_size - 20) / 80)  # up to 1.5x for 60+ labels
    base_tokens = int((200 * len(items_to_process) + 400) * catalog_multiplier)
    max_out = min(8000, max(1600, base_tokens))  # Higher ceiling prevents truncation on large batches

    # Call the AI
    if chat_complete_fn:
        result_text = chat_complete_fn(
            client,
            model=_model,
            structured=True,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=max_out,
            reasoning_effort=_reasoning,
        )
    else:
        return {}

    data = _json_load(result_text)
    items_out = data.get("items") or (data if isinstance(data, list) else [])
    by_id = {str(o.get("id")): o for o in items_out if isinstance(o, dict) and "id" in o}

    # Process results with confidence scoring
    out_by_idx: Dict[int, Dict[str, Any]] = {}

    for it in items_to_process:
        idx = int(it["idx"])
        review_text = it.get("review", "")
        rating = it.get("rating")
        obj = by_id.get(str(idx)) or {}

        # Extract and validate tags with confidence
        dels, ev_del = _extract_side_with_confidence(
            obj.get("delighters", []),
            allowed_delighters,
            review_text,
            rating,
            aliases=aliases,
            side="delighter",
            max_ev_chars=max_ev_chars,
        )
        dets, ev_det = _extract_side_with_confidence(
            obj.get("detractors", []),
            allowed_detractors,
            review_text,
            rating,
            aliases=aliases,
            side="detractor",
            max_ev_chars=max_ev_chars,
        )

        # NOTE: v2 refinement (TagRefiner from tag_quality.py) is intentionally
        # SKIPPED here. The v3 _extract_side_with_confidence pipeline already
        # handles everything the v2 refiner does — and does it better:
        #   - Confidence scoring with polarity_confidence_modifier
        #   - Aspect-level deduplication (_dedup_same_concept_labels)
        #   - Evidence-weighted contradiction resolution
        #   - Universal discipline (drop universals when 2+ specifics exist)
        #   - Fuzzy evidence validation with NegationDetector
        #   - Evidence coherence checking
        #
        # Running the v2 refiner AFTER v3 caused the "dual engine conflict":
        # the refiner used a different scoring algorithm (FragmentScorer) that
        # sometimes contradicted v3's decisions, AND it replaced v3's AI-sourced
        # evidence maps with its own snippet-based evidence. This made tag
        # decisions unpredictable and evidence display unreliable.
        #
        # The v2 refiner is still used by the inline fallback tagger (when v3
        # import fails) where it remains the only post-processing layer.

        # Cross-side coherence: check for contradictory evidence reuse
        dets, dels, ev_det, ev_del = _cross_side_coherence(dets, dels, ev_det, ev_del)

        # Enum validation
        safety = _validate_enum(obj.get("safety", "Not Mentioned"), SAFETY_ENUM)
        reliability = _validate_enum(obj.get("reliability", "Not Mentioned"), RELIABILITY_ENUM)
        sessions = _validate_enum(obj.get("sessions", "Unknown"), SESSIONS_ENUM)

        # Unlisted candidates
        unl_dels = _standardize_unlisted([str(x).strip() for x in (obj.get("unlisted_delighters") or []) if str(x).strip()])[:10]
        unl_dets = _standardize_unlisted([str(x).strip() for x in (obj.get("unlisted_detractors") or []) if str(x).strip()])[:10]
        if standardize_fn:
            try:
                std_result = standardize_fn(unl_dels, unl_dets)
                if isinstance(std_result, (list, tuple)) and len(std_result) >= 2:
                    unl_dels = list(std_result[0] or [])
                    unl_dets = list(std_result[1] or [])
            except Exception:
                pass

        out_by_idx[idx] = dict(
            dels=dels,
            dets=dets,
            ev_del=ev_del,
            ev_det=ev_det,
            unl_dels=list(unl_dels)[:10],
            unl_dets=list(unl_dets)[:10],
            safety=safety,
            reliability=reliability,
            sessions=sessions,
        )
        # Cache the result for this review
        _result_cache.put(it.get("review", ""), _cat_hash, _model, out_by_idx[idx])

    # Merge cached + fresh results
    if cached_results:
        logger.info("Cache: %d hits, %d fresh", len(cached_results), len(out_by_idx))
        out_by_idx.update(cached_results)

    return out_by_idx


def _merge_pipeline_results(
    staged_labels: List[str],
    staged_ev: Dict[str, List[str]],
    single_labels: List[str],
    single_ev: Dict[str, List[str]],
) -> Tuple[List[str], Dict[str, List[str]]]:
    """Merge results from two-stage and single-pass pipelines.

    - If both agree, keep the label.
    - If only one side finds it, still keep it.
    - Merge evidence from both sources and prefer the longer snippets.
    """

    all_labels = list(dict.fromkeys(list(staged_labels or []) + list(single_labels or [])))
    merged_ev: Dict[str, List[str]] = {}
    for label in all_labels:
        combined = list(dict.fromkeys(list(staged_ev.get(label, []) or []) + list(single_ev.get(label, []) or [])))
        combined.sort(key=lambda ev: -len(str(ev or "")))
        merged_ev[label] = [str(ev).strip() for ev in combined[:3] if str(ev).strip()]
    return all_labels[:10], merged_ev


def needs_verification(
    result: Dict[str, Any],
    review_text: str,
    rating: Any,
) -> bool:
    """Identify results that deserve a lightweight verification pass.

    The earlier gate was too rating-driven and over-verified nuanced mixed
    reviews. This version focuses on higher-risk symptoms: missing evidence,
    zero-tag substantive reviews, obvious over-tagging, and extreme-rating
    cross-polarity results that lack balancing evidence on the expected side.
    """

    dets = list(result.get("dets") or [])
    dels = list(result.get("dels") or [])
    ev_det = dict(result.get("ev_det") or {})
    ev_del = dict(result.get("ev_del") or {})
    review_words = len(str(review_text or "").split())
    total_tags = len(dets) + len(dels)

    if not dets and not dels and review_words > 50:
        return True

    weak_evidence_tags = 0
    for label in dets:
        evidence = [str(ev).strip() for ev in (ev_det.get(label) or []) if str(ev).strip()]
        if not evidence or max(len(ev.split()) for ev in evidence) < 3:
            weak_evidence_tags += 1
    for label in dels:
        evidence = [str(ev).strip() for ev in (ev_del.get(label) or []) if str(ev).strip()]
        if not evidence or max(len(ev.split()) for ev in evidence) < 3:
            weak_evidence_tags += 1
    if weak_evidence_tags >= 2:
        return True

    try:
        r = float(rating)
        if r >= 4.8 and dets and not dels:
            return True
        if r <= 1.2 and dels and not dets:
            return True
    except (TypeError, ValueError):
        pass

    if review_words < 40 and total_tags > 4:
        return True

    if total_tags >= 6 and review_words < 80:
        return True

    return False


_VERIFY_SYSTEM = """You are auditing symptom tags assigned to a product review. For each tag, decide KEEP or DROP.

DROP a tag if:
- The evidence doesn't support it (quote is about a different topic)
- The polarity is wrong (negative evidence tagged as delighter, or vice versa)
- The tag is about a DIFFERENT product mentioned in the review
- The evidence is negated ("no issues with X" should NOT be tagged as X detractor)

KEEP a tag if the evidence genuinely supports the label and polarity.

Output strict JSON: {"tags":[{"label":"...","verdict":"KEEP"|"DROP","reason":"one word"}]}"""


def verify_tags(
    *,
    client: Any,
    review_text: str,
    rating: Any,
    result: Dict[str, Any],
    chat_complete_fn: Optional[Callable] = None,
    safe_json_load_fn: Optional[Callable] = None,
    model_fn: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Run lightweight verification on a flagged result.

    This targets the small slice of results that are most likely to be wrong,
    which improves precision without paying for a second pass on every review.
    """

    if not chat_complete_fn:
        return result

    _jload = safe_json_load_fn or _default_json_load
    dets = list(result.get("dets") or [])
    dels = list(result.get("dels") or [])
    ev_det = dict(result.get("ev_det") or {})
    ev_del = dict(result.get("ev_del") or {})
    if not dets and not dels:
        return result

    tag_lines: List[str] = []
    for label in dets:
        ev = ev_det.get(label, [])
        tag_lines.append(f"  DETRACTOR: {label} — evidence: {'; '.join(ev[:2]) if ev else '(none)'}")
    for label in dels:
        ev = ev_del.get(label, [])
        tag_lines.append(f"  DELIGHTER: {label} — evidence: {'; '.join(ev[:2]) if ev else '(none)'}")

    user_msg = f"Review ({rating}★): \"{str(review_text or '')[:500]}\"\n\nTags assigned:\n" + "\n".join(tag_lines)

    try:
        raw = chat_complete_fn(
            client,
            model=(model_fn or (lambda: "gpt-4o"))(),
            structured=True,
            messages=[
                {"role": "system", "content": _VERIFY_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=300,
        )
        data = _jload(raw)
        verdicts = {
            str(tag.get("label", "")).strip(): str(tag.get("verdict", "KEEP")).upper()
            for tag in (data.get("tags") or [])
            if isinstance(tag, dict)
        }
        new_dets = [label for label in dets if verdicts.get(label, "KEEP") != "DROP"]
        new_dels = [label for label in dels if verdicts.get(label, "KEEP") != "DROP"]
        dropped = [label for label in dets + dels if verdicts.get(label) == "DROP"]
        if dropped:
            logger.info("Verification dropped %d tags: %s", len(dropped), ", ".join(dropped))
        return dict(
            dets=new_dets,
            dels=new_dels,
            ev_det={key: value for key, value in ev_det.items() if key in new_dets},
            ev_del={key: value for key, value in ev_del.items() if key in new_dels},
            unl_dets=list(result.get("unl_dets") or []),
            unl_dels=list(result.get("unl_dels") or []),
            safety=result.get("safety", "Not Mentioned"),
            reliability=result.get("reliability", "Not Mentioned"),
            sessions=result.get("sessions", "Unknown"),
        )
    except Exception as exc:
        logger.warning("Verification failed: %s", exc)
        return result


def tag_review_batch_v4(
    *,
    client: Any,
    items: Sequence[Mapping[str, Any]],
    allowed_delighters: Sequence[str],
    allowed_detractors: Sequence[str],
    long_review_threshold: int = 600,
    **kwargs: Any,
) -> Dict[int, Dict[str, Any]]:
    """v4 entry point: hybrid routing with verification.

    Short reviews go through the proven single-pass engine. Long reviews route
    through the staged claim-extraction pipeline and are then cross-validated
    against the single-pass output. Flagged outputs get a lightweight audit.
    """

    if not items:
        return {}

    aliases = {
        str(key).strip(): [str(v).strip() for v in (vals or []) if str(v).strip()]
        for key, vals in dict(kwargs.get("aliases") or {}).items()
        if str(key).strip()
    }

    deduped_detractors, aliases = deduplicate_taxonomy_labels(list(allowed_detractors or []), aliases)
    deduped_delighters, aliases = deduplicate_taxonomy_labels(list(allowed_delighters or []), aliases)

    det_counts = dict(kwargs.get("historical_detractor_counts") or {})
    del_counts = dict(kwargs.get("historical_delighter_counts") or {})
    primary_detractors, extended_detractors = prune_taxonomy(deduped_detractors, det_counts, max_labels=25)
    primary_delighters, extended_delighters = prune_taxonomy(deduped_delighters, del_counts, max_labels=25)

    active_detractors = primary_detractors or deduped_detractors
    active_delighters = primary_delighters or deduped_delighters

    logger.info(
        "v4 routing active — %d detractor labels (%d extended), %d delighter labels (%d extended)",
        len(active_detractors),
        len(extended_detractors),
        len(active_delighters),
        len(extended_delighters),
    )

    short_items = [item for item in items if len(str(item.get("review", ""))) < long_review_threshold]
    long_items = [item for item in items if len(str(item.get("review", ""))) >= long_review_threshold]
    results: Dict[int, Dict[str, Any]] = {}

    batch_kwargs = dict(kwargs)
    batch_kwargs["aliases"] = aliases
    batch_kwargs.pop("historical_detractor_counts", None)
    batch_kwargs.pop("historical_delighter_counts", None)

    if short_items:
        short_results = tag_review_batch(
            client=client,
            items=short_items,
            allowed_delighters=active_delighters,
            allowed_detractors=active_detractors,
            **batch_kwargs,
        )
        for item in short_items:
            idx = int(item["idx"])
            result = short_results.get(idx, {})
            if needs_verification(result, str(item.get("review", "")), item.get("rating")):
                result = verify_tags(
                    client=client,
                    review_text=str(item.get("review", "")),
                    rating=item.get("rating"),
                    result=result,
                    chat_complete_fn=batch_kwargs.get("chat_complete_fn"),
                    safe_json_load_fn=batch_kwargs.get("safe_json_load_fn"),
                    model_fn=batch_kwargs.get("model_fn"),
                )
            results[idx] = result

    for item in long_items:
        idx = int(item["idx"])
        review_text = str(item.get("review", ""))
        rating = item.get("rating")

        claims = extract_claims(
            client=client,
            review_text=review_text,
            rating=rating,
            chat_fn=batch_kwargs.get("chat_complete_fn"),
            json_fn=batch_kwargs.get("safe_json_load_fn"),
            model_fn=batch_kwargs.get("model_fn"),
            reasoning_fn=batch_kwargs.get("reasoning_fn"),
        )

        single_result = tag_review_batch(
            client=client,
            items=[item],
            allowed_delighters=active_delighters,
            allowed_detractors=active_detractors,
            **batch_kwargs,
        ).get(idx, {})

        if claims:
            staged_dets, staged_dels, staged_ev_det, staged_ev_del = map_claims_to_taxonomy(
                claims,
                active_detractors + list(extended_detractors),
                active_delighters + list(extended_delighters),
                aliases=aliases,
            )
            merged_dets, merged_ev_det = _merge_pipeline_results(
                staged_dets,
                staged_ev_det,
                list(single_result.get("dets") or []),
                dict(single_result.get("ev_det") or {}),
            )
            merged_dels, merged_ev_del = _merge_pipeline_results(
                staged_dels,
                staged_ev_del,
                list(single_result.get("dels") or []),
                dict(single_result.get("ev_del") or {}),
            )
            merged = dict(
                dets=merged_dets,
                dels=merged_dels,
                ev_det=merged_ev_det,
                ev_del=merged_ev_del,
                unl_dets=list(single_result.get("unl_dets") or []),
                unl_dels=list(single_result.get("unl_dels") or []),
                safety=single_result.get("safety", "Not Mentioned"),
                reliability=single_result.get("reliability", "Not Mentioned"),
                sessions=single_result.get("sessions", "Unknown"),
            )
        else:
            merged = single_result

        if needs_verification(merged, review_text, rating):
            merged = verify_tags(
                client=client,
                review_text=review_text,
                rating=rating,
                result=merged,
                chat_complete_fn=batch_kwargs.get("chat_complete_fn"),
                safe_json_load_fn=batch_kwargs.get("safe_json_load_fn"),
                model_fn=batch_kwargs.get("model_fn"),
            )

        results[idx] = merged

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_UNIVERSAL_LABELS = {"Overall Satisfaction", "Overall Dissatisfaction"}
_CONTRADICTION_PAIRS = [
    ({"Quiet","Quiet Operation","Low Noise"}, {"Loud","Loud Noise","Noisy","Noisy Motor","Loud Motor"}),
    ({"Easy To Use","Easy To Operate","User Friendly"}, {"Difficult To Use","Hard To Use","Confusing","Confusing Controls"}),
    ({"Fast Drying","Quick Results","Fast Results"}, {"Slow Drying","Takes Too Long","Slow Performance"}),
    ({"Lightweight","Light Weight","Light"}, {"Heavy","Too Heavy","Bulky"}),
    ({"Easy To Clean","Easy Cleanup"}, {"Hard To Clean","Difficult To Clean"}),
    ({"Long Battery Life","Good Battery","Battery Life"}, {"Battery Dies Fast","Short Battery Life","Poor Battery"}),
    ({"Durable","Well Built","Sturdy","Build Quality"}, {"Broke Quickly","Fragile","Poor Build Quality","Cheap Materials"}),
    # Hair care specific
    ({"Smooth Hair","Smoothing","Reduces Frizz","Frizz Control"}, {"Frizzy Hair","Causes Frizz","More Frizz"}),
    ({"Adds Shine","Shiny Hair","Glossy"}, {"Dull Hair","No Shine","Leaves Hair Dull"}),
    ({"No Hair Damage","Gentle On Hair","Hair Protection"}, {"Hair Damage","Damages Hair","Burns Hair","Dries Out Hair"}),
    ({"Cool Temperature","Temperature Control"}, {"Too Hot","Overheating","Burns Scalp","Gets Too Hot"}),
    ({"Compact","Portable","Travel Friendly"}, {"Too Large","Too Big","Not Portable"}),
    ({"Good Value","Worth The Price","Great Value"}, {"Overpriced","Not Worth The Price","Too Expensive"}),
    ({"Long Cord","Cord Length"}, {"Short Cord","Cord Too Short"}),
    ({"Multiple Attachments","Good Accessories"}, {"Missing Attachments","Needs More Attachments"}),
]

def _cross_side_coherence(
    dets: List[str], dels: List[str],
    ev_det: Dict[str, List[str]], ev_del: Dict[str, List[str]],
) -> Tuple[List[str], List[str], Dict[str, List[str]], Dict[str, List[str]]]:
    """Cross-side validation: catch contradictory labels across detractor/delighter sides.
    
    When a detractor and delighter are from the same contradiction pair (e.g.,
    "Loud Noise" as detractor AND "Quiet" as delighter), keep the one with
    stronger evidence. Also checks for shared evidence strings across sides.
    """
    if not dets or not dels:
        return dets, dels, ev_det, ev_del

    # 1. Contradiction pairs across sides
    for set_a, set_b in _CONTRADICTION_PAIRS:
        # set_a = positive side, set_b = negative side
        det_hits = set_b & set(dets)  # detractors matching negative side
        del_hits = set_a & set(dels)  # delighters matching positive side
        if det_hits and del_hits:
            # Both sides of a contradiction tagged — keep stronger evidence
            det_ev_score = sum(sum(len(e) for e in ev_det.get(l, [])) for l in det_hits)
            del_ev_score = sum(sum(len(e) for e in ev_del.get(l, [])) for l in del_hits)
            if det_ev_score >= del_ev_score:
                dels = [l for l in dels if l not in del_hits]
                for d in del_hits:
                    ev_del.pop(d, None)
            else:
                dets = [l for l in dets if l not in det_hits]
                for d in det_hits:
                    ev_det.pop(d, None)

    # 2. Shared evidence: same short snippet used on both sides is suspicious
    det_ev_strings = set()
    for evs in ev_det.values():
        for e in evs:
            if len(e) >= 10:
                det_ev_strings.add(e.lower().strip()[:50])
    for del_label in list(dels):
        for e in ev_del.get(del_label, []):
            if len(e) >= 10 and e.lower().strip()[:50] in det_ev_strings:
                # Same evidence used for both a detractor and delighter — drop the delighter tag
                # (detractors are typically more specific/actionable)
                dels = [l for l in dels if l != del_label]
                ev_del.pop(del_label, None)
                break

    return dets, dels, ev_det, ev_del


def _audit_tag_polarity(
    label: str,
    evidence: List[str],
    side: str,
    review_text: str,
) -> str:
    """Audit a single tag's polarity against its evidence in context.

    Returns ``correct``, ``inverted``, or ``ambiguous``.
    """

    if not evidence or not review_text:
        return "correct"

    rv_lower = str(review_text or "").lower()
    if side == "detractor" and _WORRY_THEN_POSITIVE.search(rv_lower):
        return "inverted"

    for ev in evidence:
        ev_lower = str(ev or "").lower().strip()
        if not ev_lower:
            continue
        idx = rv_lower.find(ev_lower)
        if idx < 0:
            continue

        ctx_start = max(0, idx - 100)
        ctx_end = min(len(rv_lower), idx + len(ev_lower) + 100)
        context = rv_lower[ctx_start:ctx_end]

        if _HAS_NEG_DETECTOR:
            try:
                if _NegDetector.is_negated(ev_lower, context):
                    return "inverted"
                label_terms = [term for term in re.findall(r"[a-z]+", str(label or "").lower()) if len(term) > 3 and term not in _STOP_TOKENS]
                for term in label_terms[:3]:
                    if _NegDetector.is_negated(term, context):
                        return "inverted"
            except Exception:
                pass
        else:
            preceding = rv_lower[max(0, idx - 50):idx]
            if _NEG_CONTEXT.search(preceding):
                return "inverted"
            label_terms = [term for term in re.findall(r"[a-z]+", str(label or "").lower()) if len(term) > 3 and term not in _STOP_TOKENS]
            for term in label_terms[:3]:
                term_idx = context.find(term)
                if term_idx > 0 and _NEG_CONTEXT.search(context[max(0, term_idx - 20):term_idx]):
                    return "inverted"

        but_pattern = re.compile(r"\b(but|however|although|except|yet|though)\b", re.I)
        but_match = but_pattern.search(context)
        if but_match:
            ev_pos = context.find(ev_lower)
            but_pos = but_match.start()
            if ev_pos > but_pos and side == "delighter":
                return "ambiguous"
            if ev_pos < but_pos and side == "detractor":
                following = context[but_pos:]
                if re.search(r"\b(fine|okay|ok|manageable|not\s+(?:a\s+)?big\s+deal|still\s+(?:love|like|recommend))\b", following, re.I):
                    return "ambiguous"

        if side == "detractor" and re.search(r"\b(old|older|previous|last|other)\b", context) and re.search(r"\b(this one|this product|new one|current one)\b", context):
            label_terms = [term for term in re.findall(r"[a-z]+", str(label or "").lower()) if len(term) > 3 and term not in _STOP_TOKENS]
            if any(term in context for term in label_terms[:3]):
                return "inverted"
    return "correct"


_WORRY_THEN_POSITIVE = re.compile(
    r"\b(worried|afraid|scared|concerned|nervous|thought|feared|expected)\b"
    r".*?\b(but|however|actually|turns?\s+out|surprisingly|thankfully|fortunately|glad)\b",
    re.I | re.DOTALL,
)


def _enforce_universal_discipline(labels, ev_map):
    """Drop universal labels when 1+ specific labels cover the sentiment.
    A review tagged with 'Loud Noise' + 'Overall Dissatisfaction' should drop
    the universal — the noise IS the dissatisfaction."""
    specific = [l for l in labels if l not in _UNIVERSAL_LABELS]
    if len(specific) >= 1:
        return [l for l in labels if l not in _UNIVERSAL_LABELS], {k:v for k,v in ev_map.items() if k not in _UNIVERSAL_LABELS}
    return labels, ev_map

def _check_contradictions_side(labels):
    """Rule 14: Remove weaker side of contradictory label pairs."""
    for set_a, set_b in _CONTRADICTION_PAIRS:
        has_a = set_a & set(labels)
        has_b = set_b & set(labels)
        if has_a and has_b:
            labels = [l for l in labels if l not in (has_b if len(has_a) >= len(has_b) else has_a)]
    return labels

def _evidence_coherent_with_label(label: str, evidence: List[str], review_text: str = "") -> bool:
    """Check if evidence text semantically relates to the label.
    Catches cases where AI picks a label and then grabs unrelated evidence.
    
    Uses three signals:
    1. Token overlap between label stems and evidence stems
    2. Evidence contains a keyword semantically related to the label's domain
    3. Long evidence (40+ chars) that at least shares one content word with label
    """
    if not evidence or not label:
        return True  # Give benefit of doubt if no evidence
    label_tokens = _tokenize_stemmed(label)
    if not label_tokens:
        return True
    ev_text = " ".join(e.lower() for e in evidence)
    ev_tokens = {_stem(w) for w in re.findall(r"[a-z]+", ev_text) if len(w) > 2}
    # Direct token overlap — strongest signal
    overlap = label_tokens & ev_tokens
    if overlap:
        return True
    # Long evidence must share at least one raw word (not stemmed) with the label
    label_words = {w.lower() for w in re.findall(r"[a-z]+", label.lower()) if len(w) > 2 and w.lower() not in _STOP_TOKENS}
    ev_words = {w.lower() for w in re.findall(r"[a-z]+", ev_text) if len(w) > 2}
    if any(len(e) >= 40 for e in evidence) and (label_words & ev_words):
        return True
    return False


def _standardize_unlisted(raw_list: List[str]) -> List[str]:
    """Deduplicate unlisted candidates using stemmed matching.
    E.g., 'Loud Motor' and 'Motor Noise' and 'Noisy Motor' → keep first."""
    seen_stems = []
    out = []
    for label in raw_list:
        stems = _tokenize_stemmed(label)
        if not stems:
            continue
        # Check overlap with already-seen labels
        is_dup = False
        for existing_stems in seen_stems:
            if existing_stems and stems:
                overlap = len(stems & existing_stems) / max(len(stems), len(existing_stems))
                if overlap >= 0.6:
                    is_dup = True
                    break
        if not is_dup:
            out.append(label)
            seen_stems.append(stems)
    return out


def _extract_side_with_confidence(
    raw_objs: Sequence[Any],
    allowed: Sequence[str],
    review_text: str,
    rating: Any,
    *,
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    side: str,
    max_ev_chars: int = 120,
) -> Tuple[List[str], Dict[str, List[str]]]:
    """Extract labels with confidence, universal discipline, contradiction checking,
    and aspect-level deduplication."""
    labels = []
    ev_map: Dict[str, List[str]] = {}
    coherence_flags: Dict[str, bool] = {}  # track coherence for confidence scoring

    for obj in (raw_objs or []):
        if not isinstance(obj, dict): continue
        raw = str(obj.get("label", "")).strip()
        lbl = match_label(raw, allowed, aliases=aliases)
        if not lbl or lbl in labels: continue  # Dedup
        raw_evs = [str(e) for e in (obj.get("evidence") or []) if isinstance(e, str)]
        validated = validate_evidence(raw_evs, review_text, max_ev_chars, label=lbl)
        if not validated:
            continue

        alias_vals = ((aliases or {}).get(lbl) or []) if isinstance(aliases, Mapping) else []
        if _evidence_supports_label:
            is_coherent = any(
                _evidence_supports_label(lbl, ev, side=side, aliases=alias_vals)
                for ev in validated
            )
        else:
            is_coherent = _evidence_coherent_with_label(lbl, validated, review_text=review_text)
        coherence_flags[lbl] = is_coherent
        if not is_coherent:
            logger.info("Coherence audit: '%s' evidence does not support %s side — skipping", lbl, side)
            continue
        labels.append(lbl)
        ev_map[lbl] = validated[:3]
        polarity_verdict = _audit_tag_polarity(lbl, validated, side, review_text)
        if polarity_verdict == "inverted":
            logger.info("Polarity audit: '%s' evidence is inverted for %s side — skipping", lbl, side)
            if labels:
                labels.pop()
            ev_map.pop(lbl, None)
            coherence_flags.pop(lbl, None)
            continue
        if polarity_verdict == "ambiguous":
            coherence_flags[lbl] = False
        if len(labels) >= 10: break

    # Aspect-level dedup: "Loud Noise" + "Noisy Motor" = same concept — keep best evidence
    labels, ev_map = _dedup_same_concept_labels(labels, ev_map)

    # Post-extraction intelligence
    labels, ev_map = _enforce_universal_discipline(labels, ev_map)
    labels = _check_contradictions_with_evidence(labels, ev_map)
    # Filter by confidence threshold — scales with catalog size
    if len(labels) >= 2:
        cat_size = len(allowed)
        review_words = len(str(review_text or "").split())
        if cat_size >= 50:
            conf_threshold = 0.18
        elif cat_size >= 30:
            conf_threshold = 0.22
        elif cat_size >= 15:
            conf_threshold = 0.25
        else:
            conf_threshold = 0.30

        if review_words < 30:
            conf_threshold *= 0.75
        elif review_words > 200:
            conf_threshold *= 1.10

        scored = []
        for lbl in labels:
            c = _score_tag_confidence(lbl, ev_map.get(lbl, []), review_text, rating,
                                       side=side, catalog_size=cat_size)
            # Coherence penalty: -0.12 for incoherent evidence
            if not coherence_flags.get(lbl, True):
                c -= 0.12
            scored.append((lbl, c))
        above_threshold = [(l, c) for l, c in scored if c >= conf_threshold]
        if above_threshold:
            labels = [l for l, c in above_threshold]
        else:
            scored.sort(key=lambda x: -x[1])
            labels = [l for l, c in scored[:3]]
        ev_map = {k: v for k, v in ev_map.items() if k in labels}
    return labels, ev_map


def _dedup_same_concept_labels(labels: List[str], ev_map: Dict[str, List[str]]) -> Tuple[List[str], Dict[str, List[str]]]:
    """Remove same-concept duplicates: 'Loud Noise' + 'Noisy Motor' → keep the one with better evidence.
    
    Sorts by evidence quality first so the strongest label for each concept
    is always the one that gets kept, regardless of AI return order.
    """
    if len(labels) < 2:
        return labels, ev_map
    # Sort by evidence quality (descending) so best labels are registered first
    sorted_labels = sorted(labels, key=lambda l: sum(len(e) for e in ev_map.get(l, [])), reverse=True)
    seen_stems: Dict[str, Tuple[str, int]] = {}  # stem_key → (label, evidence_score)
    out_labels = []
    for lbl in sorted_labels:
        stems = frozenset(_tokenize_stemmed(lbl))
        if not stems:
            out_labels.append(lbl)
            continue
        # Check overlap with already-seen labels
        ev_score = sum(len(e) for e in ev_map.get(lbl, []))
        merged = False
        for key, (existing_lbl, existing_score) in list(seen_stems.items()):
            existing_stems = frozenset(key.split("|"))
            overlap = len(stems & existing_stems) / max(len(stems), len(existing_stems))
            if overlap >= 0.6:
                # Same concept — keep the one with better evidence
                if ev_score > existing_score:
                    out_labels = [l for l in out_labels if l != existing_lbl]
                    out_labels.append(lbl)
                    seen_stems["|".join(sorted(stems))] = (lbl, ev_score)
                    # Merge evidence: combine best from both
                    combined_ev = list(ev_map.get(existing_lbl, [])) + list(ev_map.get(lbl, []))
                    ev_map[lbl] = sorted(set(combined_ev), key=lambda x: -len(x))[:3]
                    if existing_lbl in ev_map and existing_lbl != lbl:
                        del ev_map[existing_lbl]
                # else: keep existing, discard new
                merged = True
                break
        if not merged:
            out_labels.append(lbl)
            seen_stems["|".join(sorted(stems))] = (lbl, ev_score)
    return out_labels, ev_map


def _check_contradictions_with_evidence(labels: List[str], ev_map: Dict[str, List[str]]) -> List[str]:
    """Evidence-weighted contradiction resolution.
    
    When contradictory labels coexist (Quiet + Loud), keep the one with
    stronger evidence rather than just counting set sizes.
    """
    for set_a, set_b in _CONTRADICTION_PAIRS:
        has_a = set_a & set(labels)
        has_b = set_b & set(labels)
        if has_a and has_b:
            # Score each side by total evidence length
            score_a = sum(sum(len(e) for e in ev_map.get(l, [])) for l in has_a)
            score_b = sum(sum(len(e) for e in ev_map.get(l, [])) for l in has_b)
            # Drop the weaker side
            drop = has_b if score_a >= score_b else has_a
            labels = [l for l in labels if l not in drop]
            for d in drop:
                ev_map.pop(d, None)
    return labels


def _validate_enum(value: Any, enum_str: str) -> str:
    """Validate an enum value against a comma-separated enum string."""
    s = str(value or "").strip()
    allowed = [x.strip() for x in enum_str.split(",")]
    return s if s in allowed else allowed[-1]  # last is typically "Not Mentioned" / "Unknown"




# ---------------------------------------------------------------------------
# STAGE 1: Claim Extraction (for future staged pipeline)
# ---------------------------------------------------------------------------

_EXTRACT_SYSTEM = """You are a product review analyst. Extract every factual claim from this review.

For each claim:
- Quote EXACT text from the review (4-120 chars) — must appear verbatim
- Label polarity: positive, negative, neutral, or mixed
- Label aspect: 2-4 word noun phrase describing what the claim is about (e.g., "noise level", "hair damage", "ease of use", "build quality", "drying speed")

NEGATION AWARENESS: "didn't damage my hair" is POSITIVE polarity about "hair damage". "no issues with noise" is POSITIVE about "noise level". Read the full clause before assigning polarity.

SARCASM: "Great, another broken product" — the polarity is NEGATIVE despite positive words. Use rating as a signal.

Extract ALL claims — even minor mentions. A thorough extraction enables better downstream classification.

Output strict JSON: {"claims":[{"quote":"<verbatim>","polarity":"<polarity>","aspect":"<2-4 words>"}]}"""


def extract_claims(*, client, review_text, rating, chat_fn=None, json_fn=None, model_fn=None, reasoning_fn=None):
    """Stage 1: Extract factual claims from a review with no taxonomy context.
    This is the foundation of the staged pipeline — extract first, classify second."""
    if not chat_fn: return []
    _jload = json_fn or _default_json_load
    try:
        result = chat_fn(client, model=(model_fn or (lambda: "gpt-4o"))(), structured=True,
            messages=[{"role":"system","content":_EXTRACT_SYSTEM},
                      {"role":"user","content":json.dumps({"review":review_text,"rating":rating})}],
            temperature=0.0, response_format={"type":"json_object"}, max_tokens=1500,
            reasoning_effort=(reasoning_fn or (lambda: "none"))())
        data = _jload(result)
        return [{"text":str(c.get("quote","")).strip(),"polarity":str(c.get("polarity","neutral")).lower(),
                 "aspect":str(c.get("aspect","")).strip()}
                for c in (data.get("claims") or []) if len(str(c.get("quote","")).strip()) >= 4]
    except Exception as e:
        logger.warning(f"Claim extraction failed: {e}")
        return []


def map_claims_to_taxonomy(claims, allowed_detractors, allowed_delighters, aliases=None):
    """Stage 2: Map extracted claims to catalog labels deterministically (no AI call).

    The v4 version uses weighted candidate routing instead of the earlier
    first-hit cascade. This makes long-review mapping more deterministic and
    better at collapsing paraphrases onto the intended taxonomy label.
    """
    det_labels, del_labels = [], []
    det_ev, del_ev = {}, {}
    for claim in (claims or []):
        text = claim.get("text", "")
        aspect = claim.get("aspect", "")
        polarity = claim.get("polarity", "neutral")
        targets = []
        if polarity in ("negative", "mixed"):
            targets.append(("det", allowed_detractors, det_labels, det_ev))
        if polarity in ("positive", "mixed"):
            targets.append(("del", allowed_delighters, del_labels, del_ev))
        if polarity == "neutral":
            targets.append(("det", allowed_detractors, det_labels, det_ev))
            targets.append(("del", allowed_delighters, del_labels, del_ev))
        for side, catalog, labels, ev in targets:
            label = _best_claim_label(aspect, text, catalog, aliases=aliases)
            if label and label not in labels:
                labels.append(label)
                ev[label] = [text[:120]]
            elif label and label in ev and len(ev[label]) < 2:
                ev[label].append(text[:120])
    return det_labels[:10], del_labels[:10], det_ev, del_ev

def _default_json_load(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Auto-retry for zero-tag reviews
# ---------------------------------------------------------------------------

_RETRY_SYSTEM_PROMPT = """You are a consumer product review analyst. You are re-examining a review that an initial pass returned zero symptom tags for.

Read the review CLAUSE BY CLAUSE. For each clause:
1. Identify the claim being made about the product.
2. Determine polarity: is it positive, negative, or neutral? Watch for NEGATION — "didn't break" is positive, "no issues" is positive, "not loud" is a delighter.
3. Check if the claim matches a catalog label.
4. Quote the exact text as evidence.

{catalog_section}

Rules:
1. EVIDENCE REQUIRED: Every tag must have a verbatim quote from the review.
2. EXACT LABELS: Use catalog labels exactly as shown.
3. NEGATION AWARENESS: "it doesn't damage hair" is NOT a Hair Damage detractor. "no noise" is NOT a Noise detractor. Read carefully.
4. Be more generous than usual — this review was missed on first pass. Look for:
   - Implied sentiments: "had to return it" implies defect/disappointment
   - Hedged language: "it's okay I guess" = mild detractor
   - Comparative claims: "better than my old one" = delighter about this product
5. If truly nothing applies, return empty arrays.

Output strict JSON:
{{"detractors":[{{"label":"<exact>","evidence":["<verbatim>"]}}],"delighters":[{{"label":"<exact>","evidence":["<verbatim>"]}}],"safety":"<Safe|Minor Concern|Safety Issue|Not Mentioned>","reliability":"<Reliable|Intermittent Issue|Failure|Not Mentioned>"}}"
"""


def retry_zero_tag_reviews(
    *,
    client: Any,
    results: Dict[int, Dict[str, Any]],
    items: Sequence[Mapping[str, Any]],
    allowed_detractors: Sequence[str],
    allowed_delighters: Sequence[str],
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    max_ev_chars: int = 120,
    chat_complete_fn: Optional[Callable] = None,
    safe_json_load_fn: Optional[Callable] = None,
    model_fn: Optional[Callable] = None,
    reasoning_fn: Optional[Callable] = None,
    max_workers: int = 1,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[int, Dict[str, Any]]:
    """Re-process reviews that got zero tags on the expected side.

    A 2★ review with zero detractors, or a 5★ review with zero delighters,
    is likely a miss. This function sends those reviews individually with
    a simpler, more permissive prompt.
    """
    if not chat_complete_fn or not results:
        return results

    _json_load = safe_json_load_fn or _default_json_load
    _model = (model_fn or (lambda: "gpt-4o"))()
    _reasoning = (reasoning_fn or (lambda: "none"))()

    items_by_idx = {int(it["idx"]): it for it in items}
    retried = dict(results)

    def _label_list(payload: Mapping[str, Any], primary_key: str, fallback_key: str) -> List[str]:
        raw = payload.get(primary_key)
        if raw is None:
            raw = payload.get(fallback_key)
        if isinstance(raw, str):
            raw = [raw]
        return [str(value).strip() for value in (raw or []) if str(value).strip()]

    def _retry_flags(item: Mapping[str, Any], result: Mapping[str, Any]):
        rating = item.get("rating")
        review_text = item.get("review", "")
        dets_found = _label_list(result, "dets", "wrote_dets")
        dels_found = _label_list(result, "dels", "wrote_dels")
        try:
            r = float(rating)
        except (TypeError, ValueError):
            r = 3.0

        missed_det = False
        missed_del = False
        if not dets_found and not dels_found:
            missed_det = True
            missed_del = True
        else:
            if r <= 3 and not dets_found:
                missed_det = True
            if r >= 3 and not dels_found:
                missed_del = True
            if not dets_found and _EXPLICIT_NEGATIVE.search(review_text):
                missed_det = True
            if not dels_found and _EXPLICIT_POSITIVE.search(review_text):
                missed_del = True
        return missed_det, missed_del, rating, review_text

    candidates = []
    for idx, result in results.items():
        item = items_by_idx.get(int(idx))
        if not item:
            continue
        missed_det, missed_del, rating, review_text = _retry_flags(item, result)
        if missed_det or missed_del:
            candidates.append((int(idx), dict(result), item, rating, review_text, missed_det, missed_del))

    total_candidates = len(candidates)
    if progress_callback:
        try:
            progress_callback(0, total_candidates, "queued")
        except Exception:
            pass
    if total_candidates == 0:
        return retried

    def _process_candidate(job):
        idx, base_result, item, rating, review_text, missed_det, missed_del = job
        catalog_parts = []
        if missed_det:
            catalog_parts.append("DETRACTORS:\n" + "\n".join(f"  - {label}" for label in allowed_detractors))
        if missed_del:
            catalog_parts.append("DELIGHTERS:\n" + "\n".join(f"  - {label}" for label in allowed_delighters))

        system = _RETRY_SYSTEM_PROMPT.format(catalog_section="\n\n".join(catalog_parts))
        user_msg = json.dumps(dict(
            review=item.get("review", ""),
            rating=rating,
            retry_reason=f"{'missed detractors' if missed_det else ''}{' and ' if missed_det and missed_del else ''}{'missed delighters' if missed_del else ''}",
        ))

        updated = dict(base_result)
        changed = False
        try:
            result_text = chat_complete_fn(
                client,
                model=_model,
                structured=True,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.3,
                response_format={"type": "json_object"},
                max_tokens=1200,
                reasoning_effort=_reasoning,
            )
            data = _json_load(result_text)

            if missed_det and data.get("detractors"):
                new_dets, new_ev_det = _extract_side_with_confidence(
                    data["detractors"], allowed_detractors, review_text, rating,
                    aliases=aliases, side="detractor", max_ev_chars=max_ev_chars,
                )
                if new_dets:
                    updated["dets"] = new_dets
                    updated["ev_det"] = new_ev_det
                    changed = True

            if missed_del and data.get("delighters"):
                new_dels, new_ev_del = _extract_side_with_confidence(
                    data["delighters"], allowed_delighters, review_text, rating,
                    aliases=aliases, side="delighter", max_ev_chars=max_ev_chars,
                )
                if new_dels:
                    updated["dels"] = new_dels
                    updated["ev_del"] = new_ev_del
                    changed = True

            for field, enum_str in [("safety", SAFETY_ENUM), ("reliability", RELIABILITY_ENUM)]:
                retry_val = _validate_enum(data.get(field), enum_str)
                if retry_val not in ("Not Mentioned", "Unknown") and updated.get(field) in ("Not Mentioned", "Unknown", None):
                    updated[field] = retry_val
                    changed = True
        except Exception as exc:
            logger.debug("Retry failed for review %s: %s", idx, exc)
            return idx, None, False
        return idx, updated if changed else None, changed

    done = 0
    worker_count = max(1, min(int(max_workers or 1), total_candidates))
    if worker_count == 1:
        for job in candidates:
            idx, updated, changed = _process_candidate(job)
            if updated is not None:
                retried[idx] = updated
            done += 1
            if progress_callback:
                try:
                    progress_callback(done, total_candidates, "running")
                except Exception:
                    pass
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_process_candidate, job) for job in candidates]
            for future in as_completed(futures):
                idx, updated, changed = future.result()
                if updated is not None:
                    retried[idx] = updated
                done += 1
                if progress_callback:
                    try:
                        progress_callback(done, total_candidates, "running")
                    except Exception:
                        pass

    if progress_callback:
        try:
            progress_callback(total_candidates, total_candidates, "complete")
        except Exception:
            pass
    return retried



# ---------------------------------------------------------------------------
# Calibration pre-flight
# ---------------------------------------------------------------------------

def calibration_preflight(
    *,
    client: Any,
    sample_reviews: Sequence[str],
    allowed_detractors: Sequence[str],
    allowed_delighters: Sequence[str],
    product_profile: str = "",
    chat_complete_fn: Optional[Callable] = None,
    safe_json_load_fn: Optional[Callable] = None,
    model_fn: Optional[Callable] = None,
    reasoning_fn: Optional[Callable] = None,
    max_sample: int = 8,
) -> Dict[str, Any]:
    """Run a small calibration sample to validate the taxonomy before full batch.

    Tags a stratified sample of reviews and returns diagnostics:
    - hit_rate: fraction of reviews that got at least one tag
    - avg_tags: average tags per review
    - unused_labels: catalog labels that got zero hits
    - low_confidence_labels: labels that only matched weakly
    - recommendation: "ready" | "needs_tuning" | "review_taxonomy"
    """
    if not chat_complete_fn or not sample_reviews:
        return {"recommendation": "skipped", "reason": "no client or reviews"}

    _json_load = safe_json_load_fn or _default_json_load
    _model = (model_fn or (lambda: "gpt-4o"))()
    _reasoning = (reasoning_fn or (lambda: "none"))()

    # Take a stratified sample
    sample = list(sample_reviews[:max_sample])

    items = [
        {"idx": i, "review": r, "rating": None, "needs_det": True, "needs_del": True}
        for i, r in enumerate(sample)
    ]

    system = _build_system_prompt(
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        product_profile=product_profile,
    )

    try:
        result_text = chat_complete_fn(
            client,
            model=_model,
            structured=True,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": _build_user_payload(items)},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=3000,
            reasoning_effort=_reasoning,
        )
    except Exception as exc:
        return {"recommendation": "error", "reason": str(exc)}

    data = _json_load(result_text)
    items_out = data.get("items") or []

    # Compute diagnostics
    det_hits: Dict[str, int] = {}
    del_hits: Dict[str, int] = {}
    reviews_with_tags = 0
    total_tags = 0

    for obj in items_out:
        if not isinstance(obj, dict):
            continue
        has_tag = False
        for d in (obj.get("detractors") or []):
            label = match_label(str(d.get("label", "")), allowed_detractors)
            if label:
                det_hits[label] = det_hits.get(label, 0) + 1
                has_tag = True
                total_tags += 1
        for d in (obj.get("delighters") or []):
            label = match_label(str(d.get("label", "")), allowed_delighters)
            if label:
                del_hits[label] = del_hits.get(label, 0) + 1
                has_tag = True
                total_tags += 1
        if has_tag:
            reviews_with_tags += 1

    n = max(len(items_out), 1)
    hit_rate = reviews_with_tags / n
    avg_tags = total_tags / n

    used_det = set(det_hits.keys())
    used_del = set(del_hits.keys())
    unused_det = [l for l in allowed_detractors if l not in used_det]
    unused_del = [l for l in allowed_delighters if l not in used_del]

    # Recommendation
    if hit_rate >= 0.6 and avg_tags >= 1.5:
        recommendation = "ready"
    elif hit_rate >= 0.3:
        recommendation = "needs_tuning"
    else:
        recommendation = "review_taxonomy"

    return {
        "recommendation": recommendation,
        "sample_size": len(sample),
        "hit_rate": round(hit_rate, 3),
        "avg_tags_per_review": round(avg_tags, 2),
        "detractor_label_usage": det_hits,
        "delighter_label_usage": del_hits,
        "unused_detractors": unused_det,
        "unused_delighters": unused_del,
        "reviews_with_tags": reviews_with_tags,
        "total_tags": total_tags,
    }


# ---------------------------------------------------------------------------
# Cross-review audit
# ---------------------------------------------------------------------------

def audit_tag_distribution(
    results: Dict[int, Dict[str, Any]],
    *,
    min_occurrences: int = 2,
    items: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Audit the distribution of tags across all processed reviews.

    Flags:
    - singleton_labels: labels that appear exactly once (potential hallucinations)
    - dominant_labels: labels on >50% of reviews (might be too broad)
    - zero_evidence_tags: tags that were assigned without evidence
    - shared_evidence_labels: labels that reuse identical evidence strings (lazy AI)
    - polarity_mismatches: reviews where star rating contradicts tag polarity
    """
    if not results:
        return {}

    det_counts: Dict[str, int] = {}
    del_counts: Dict[str, int] = {}
    no_evidence: List[str] = []
    total = len(results)

    # Evidence reuse tracking
    evidence_to_labels: Dict[str, List[str]] = {}
    # Polarity mismatch tracking
    polarity_mismatches: List[Dict[str, Any]] = []
    items_by_idx = {int(it["idx"]): it for it in (items or [])} if items else {}

    for idx, result in results.items():
        dets = result.get("dets") or []
        dels = result.get("dels") or []
        for label in dets:
            det_counts[label] = det_counts.get(label, 0) + 1
            if label not in (result.get("ev_det") or {}):
                no_evidence.append(label)
            for ev in (result.get("ev_det") or {}).get(label, []):
                ev_key = ev.strip().lower()[:60]
                if ev_key and len(ev_key) >= 10:
                    evidence_to_labels.setdefault(ev_key, []).append(label)
        for label in dels:
            del_counts[label] = del_counts.get(label, 0) + 1
            if label not in (result.get("ev_del") or {}):
                no_evidence.append(label)
            for ev in (result.get("ev_del") or {}).get(label, []):
                ev_key = ev.strip().lower()[:60]
                if ev_key and len(ev_key) >= 10:
                    evidence_to_labels.setdefault(ev_key, []).append(label)

        # Polarity mismatch: 5★ with only detractors, or 1-2★ with only delighters
        item = items_by_idx.get(int(idx))
        if item:
            try:
                r = float(item.get("rating", 3))
                if r >= 5 and dets and not dels:
                    polarity_mismatches.append({"idx": idx, "rating": r, "issue": "5star_only_detractors", "dets": dets[:3]})
                elif r <= 2 and dels and not dets:
                    polarity_mismatches.append({"idx": idx, "rating": r, "issue": "low_star_only_delighters", "dels": dels[:3]})
            except (TypeError, ValueError):
                pass

    singleton_det = [l for l, c in det_counts.items() if c < min_occurrences]
    singleton_del = [l for l, c in del_counts.items() if c < min_occurrences]
    dominant_det = [l for l, c in det_counts.items() if c > total * 0.5]
    dominant_del = [l for l, c in del_counts.items() if c > total * 0.5]

    # Evidence reuse: same evidence string used for 3+ different labels = suspicious
    shared_evidence = {ev: labels for ev, labels in evidence_to_labels.items()
                       if len(set(labels)) >= 3}

    return {
        "total_reviews": total,
        "detractor_distribution": det_counts,
        "delighter_distribution": del_counts,
        "singleton_detractors": singleton_det,
        "singleton_delighters": singleton_del,
        "dominant_detractors": dominant_det,
        "dominant_delighters": dominant_del,
        "zero_evidence_tags": list(set(no_evidence)),
        "shared_evidence_labels": {ev: list(set(labels)) for ev, labels in shared_evidence.items()},
        "polarity_mismatches": polarity_mismatches[:20],
    }



# ---------------------------------------------------------------------------
# PHASE 3: Adaptive mid-run label tracking
# ---------------------------------------------------------------------------

class LabelTracker:
    """Tracks label performance during a symptomizer run.
    
    Flags labels that are hitting too often (>40% = too broad) or
    never hitting (0 in first N batches = possibly irrelevant).
    
    Can generate adaptive prompt hints for subsequent batches to
    steer the AI away from over-tagging dominant labels.
    """
    def __init__(self, allowed_detractors, allowed_delighters):
        self.det_counts = {l: 0 for l in allowed_detractors}
        self.del_counts = {l: 0 for l in allowed_delighters}
        self.total_reviews = 0
        self.zero_tag_reviews = 0  # Reviews with no tags at all
        self.warnings = []
        self._dominant_warned = set()  # Labels already warned about
    
    def record_batch(self, batch_results):
        """Record results from one batch."""
        for idx, result in (batch_results or {}).items():
            self.total_reviews += 1
            dets = result.get("dets") or []
            dels = result.get("dels") or []
            if not dets and not dels:
                self.zero_tag_reviews += 1
            for label in dets:
                if label in self.det_counts:
                    self.det_counts[label] += 1
            for label in dels:
                if label in self.del_counts:
                    self.del_counts[label] += 1
    
    def check_alerts(self, *, min_reviews=15):
        """Check for labels that are too broad or never hitting."""
        if self.total_reviews < min_reviews:
            return []
        alerts = []
        n = max(self.total_reviews, 1)
        for label, count in self.det_counts.items():
            pct = count / n
            if pct > 0.45:
                alerts.append({"label": label, "side": "detractor", "issue": "too_broad", 
                               "pct": round(pct * 100, 1), "count": count})
            elif count == 0 and self.total_reviews >= 20:
                alerts.append({"label": label, "side": "detractor", "issue": "zero_hits",
                               "pct": 0, "count": 0})
        for label, count in self.del_counts.items():
            pct = count / n
            if pct > 0.45:
                alerts.append({"label": label, "side": "delighter", "issue": "too_broad",
                               "pct": round(pct * 100, 1), "count": count})
            elif count == 0 and self.total_reviews >= 20:
                alerts.append({"label": label, "side": "delighter", "issue": "zero_hits",
                               "pct": 0, "count": 0})
        # Alert if zero-tag rate is too high
        if self.total_reviews >= 10 and self.zero_tag_reviews / n > 0.5:
            alerts.append({"label": "(overall)", "side": "both", "issue": "high_zero_rate",
                           "pct": round(self.zero_tag_reviews / n * 100, 1), "count": self.zero_tag_reviews})
        return alerts

    def get_prompt_hints(self) -> str:
        """Generate adaptive hints for subsequent batch prompts.
        
        When a label is hitting >50% of reviews, tell the AI to be more
        selective about it. When zero-tag rate is high, tell it to be more generous.
        Returns empty string if no adjustments needed.
        """
        if self.total_reviews < 15:
            return ""
        n = max(self.total_reviews, 1)
        hints = []
        for label, count in {**self.det_counts, **self.del_counts}.items():
            pct = count / n
            if pct > 0.50 and label not in self._dominant_warned:
                hints.append(f"- '{label}' is appearing in {pct:.0%} of reviews. Only tag it when evidence is strong and specific.")
                self._dominant_warned.add(label)
        if self.zero_tag_reviews / n > 0.4:
            hints.append("- Many reviews are returning zero tags. Be more generous — look for subtle signals and hedged language.")
        if not hints:
            return ""
        return "\n═══ MID-RUN ADJUSTMENTS ═══\n" + "\n".join(hints)


# ---------------------------------------------------------------------------
# PHASE 4: Post-run taxonomy recommendations
# ---------------------------------------------------------------------------

def generate_taxonomy_recommendations(
    results,
    *,
    allowed_detractors,
    allowed_delighters,
    min_occurrences=2,
    merge_threshold=0.55,
):
    """Generate actionable taxonomy recommendations after a symptomizer run.
    
    Returns a list of recommendations, each with:
    - action: 'remove' | 'merge' | 'promote' | 'split'
    - labels: affected labels
    - reason: human-readable explanation
    - priority: 'high' | 'medium' | 'low'
    """
    if not results:
        return []
    
    audit = audit_tag_distribution(results, min_occurrences=min_occurrences)
    total = audit.get("total_reviews", 0)
    if total < 5:
        return []
    
    recommendations = []
    
    # 1. Remove zero-hit labels
    det_dist = audit.get("detractor_distribution", {})
    del_dist = audit.get("delighter_distribution", {})
    used_dets = set(det_dist.keys())
    used_dels = set(del_dist.keys())
    
    for label in allowed_detractors:
        if label not in used_dets and total >= 15:
            recommendations.append({
                "action": "remove",
                "labels": [label],
                "side": "detractor",
                "reason": f"Zero hits across {total} reviews",
                "priority": "medium",
            })
    for label in allowed_delighters:
        if label not in used_dels and total >= 15:
            recommendations.append({
                "action": "remove",
                "labels": [label],
                "side": "delighter", 
                "reason": f"Zero hits across {total} reviews",
                "priority": "medium",
            })
    
    # 2. Flag dominant labels for potential splitting
    for label, count in det_dist.items():
        if count > total * 0.4:
            recommendations.append({
                "action": "split",
                "labels": [label],
                "side": "detractor",
                "reason": f"Appears in {count}/{total} reviews ({count/total*100:.0f}%) — may be too broad, consider splitting into more specific labels",
                "priority": "high",
            })
    for label, count in del_dist.items():
        if count > total * 0.4:
            recommendations.append({
                "action": "split",
                "labels": [label],
                "side": "delighter",
                "reason": f"Appears in {count}/{total} reviews ({count/total*100:.0f}%) — may be too broad",
                "priority": "high",
            })
    
    # 3. Suggest merging similar labels (same side, similar stems)
    for side, dist, allowed in [("detractor", det_dist, allowed_detractors), ("delighter", del_dist, allowed_delighters)]:
        labels_with_counts = [(l, dist.get(l, 0)) for l in allowed if dist.get(l, 0) > 0]
        for i, (l1, c1) in enumerate(labels_with_counts):
            stems1 = _tokenize_stemmed(l1)
            for l2, c2 in labels_with_counts[i+1:]:
                stems2 = _tokenize_stemmed(l2)
                if stems1 and stems2:
                    overlap = len(stems1 & stems2) / max(len(stems1), len(stems2))
                    if overlap >= merge_threshold:
                        keep = l1 if c1 >= c2 else l2
                        drop = l2 if c1 >= c2 else l1
                        recommendations.append({
                            "action": "merge",
                            "labels": [keep, drop],
                            "side": side,
                            "reason": f"'{keep}' ({max(c1,c2)} hits) and '{drop}' ({min(c1,c2)} hits) overlap {overlap:.0%} — merge into '{keep}'",
                            "priority": "medium",
                        })
    
    # 4. Promote unlisted candidates that appeared frequently
    unlisted_counts = {}
    for idx, result in results.items():
        for label in (result.get("unl_dets") or []):
            unlisted_counts.setdefault(("detractor", label), 0)
            unlisted_counts[("detractor", label)] += 1
        for label in (result.get("unl_dels") or []):
            unlisted_counts.setdefault(("delighter", label), 0)
            unlisted_counts[("delighter", label)] += 1
    
    for (side, label), count in sorted(unlisted_counts.items(), key=lambda x: -x[1]):
        if count >= max(3, total * 0.05):
            recommendations.append({
                "action": "promote",
                "labels": [label],
                "side": side,
                "reason": f"Unlisted theme appeared {count} times ({count/total*100:.1f}%) — promote to catalog",
                "priority": "high" if count >= total * 0.1 else "medium",
            })
    
    # Sort: high priority first, then by action type
    priority_order = {"high": 0, "medium": 1, "low": 2}
    action_order = {"promote": 0, "split": 1, "merge": 2, "remove": 3}
    recommendations.sort(key=lambda r: (priority_order.get(r["priority"], 9), action_order.get(r["action"], 9)))
    
    return recommendations
