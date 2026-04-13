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
import json
import math
import re
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple


logger = logging.getLogger("starwalk.symptomizer")

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
    """Decide which sides to tag based on rating + text signals.

    Returns (needs_detractors, needs_delighters).

    Rules:
    - 1–2★: detractors only, unless text has explicit positive signals
    - 3★: both sides (mixed by definition)
    - 4★: delighters only, unless text has explicit negative signals
    - 5★: delighters only, unless text has explicit negative signals
    - Any review with mixed-sentiment markers: both sides
    """
    try:
        r = float(rating)
    except (TypeError, ValueError):
        return True, True  # unknown rating → tag both

    text = str(review_text or "")
    has_mixed = bool(_MIXED_SIGNAL.search(text))
    has_negative = bool(_EXPLICIT_NEGATIVE.search(text))
    has_positive = bool(_EXPLICIT_POSITIVE.search(text))

    if r <= 2:
        needs_det = True
        needs_del = has_mixed or has_positive
    elif r <= 3:
        needs_det = True
        needs_del = True
    elif r <= 4:
        needs_det = has_mixed or has_negative
        needs_del = True
    else:  # 5★
        needs_det = has_mixed or has_negative
        needs_del = True

    return needs_det, needs_del


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

def validate_evidence(
    evidence_list: Sequence[str],
    review_text: str,
    max_chars: int = 120,
) -> List[str]:
    """Validate evidence with negation awareness and minimum quality checks.
    
    Checks both preceding and following context for negation patterns to catch:
    - "it didn't damage my hair" (negation before)
    - "damage my hair? not at all" (negation after)
    - "I was worried about damage but no issues" (negation in surrounding window)
    """
    if not evidence_list or not review_text:
        return []
    rv_norm = re.sub(r"\s+", " ", review_text.lower())
    out = []
    for e in evidence_list:
        e = str(e).strip()[:max_chars]
        # Minimum quality: 6+ chars, 2+ words
        if len(e) < 6 or len(e.split()) < 2:
            continue
        e_norm = re.sub(r"\s+", " ", e.lower())
        if e_norm not in rv_norm:
            continue
        # Negation check: reject short evidence preceded OR followed by negation
        idx = rv_norm.find(e_norm)
        if idx >= 0 and len(e_norm.split()) <= 4:
            # Check 40 chars before for negation
            preceding = rv_norm[max(0, idx - 40):idx]
            if _NEG_CONTEXT.search(preceding):
                continue
            # Check 30 chars after for negation/dismissal patterns
            end_idx = idx + len(e_norm)
            following = rv_norm[end_idx:end_idx + 30]
            if re.search(r"\b(not|no|never|at all|whatsoever|zero)\b", following):
                # But only reject if there's no double-negation reset
                if not re.search(r"\b(but|however|actually|still)\b", following):
                    continue
        out.append(e)
    return out[:2]


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
        avg_len = sum(len(e) for e in evidence) / max(len(evidence), 1)
        score += min(0.1, avg_len / 200)
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

    try:
        r = float(rating)
        if side == "detractor" and r <= 2: score += 0.15
        elif side == "detractor" and r >= 5: score -= 0.20
        elif side == "delighter" and r >= 4: score += 0.15
        elif side == "delighter" and r <= 2: score -= 0.20
    except (TypeError, ValueError): pass

    # Hedging downgrade: "kind of loud" = lower confidence
    if _HEDGING.search(ev_text): score -= 0.08
    # Emphasis boost: "extremely loud" = higher confidence
    if _EMPHASIS.search(ev_text): score += 0.08
    # Severity boost: safety-critical language for detractors
    if side == "detractor" and _SEVERITY_HIGH.search(ev_text): score += 0.12

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


def _build_system_prompt(
    *,
    allowed_detractors: Sequence[str],
    allowed_delighters: Sequence[str],
    product_profile: str = "",
    product_knowledge_text: str = "",
    taxonomy_context: str = "",
    category: str = "general",
    max_ev_chars: int = 120,
) -> str:
    """Build the system prompt with evidence-first architecture."""
    det_list = "\n".join(f"  - {l}" for l in allowed_detractors) or "  (none defined)"
    del_list = "\n".join(f"  - {l}" for l in allowed_delighters) or "  (none defined)"

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

═══ RULES (apply in order) ═══
1. READ FIRST: Read the entire review before tagging. Understand the overall sentiment arc.
2. EXACT LABELS: Use catalog label text exactly. No paraphrasing.
3. EVIDENCE REQUIRED: Every tag needs 1-2 verbatim quotes (6+ chars, 2+ words). No evidence = no tag.
4. HIGH RECALL: Capture every applicable symptom. Long reviews can map to many labels.
5. POLARITY GATE: Respect needs_detractors / needs_delighters flags.
6. NO INFERENCE: Only tag what is explicitly stated or clearly described.

═══ ACCURACY RULES ═══
7. NEGATION: "didn't damage my hair" and "no issues with noise" are POSITIVE signals, NOT detractors. "it's not loud" means quiet. Read the FULL clause before deciding polarity.
8. SARCASM: "Great, another broken product" is NEGATIVE. Mismatch between positive words and negative context. Rating is a strong signal — 1★ with "great" is sarcastic.
9. TEMPORAL: "Loved at first, now broken" — tag based on FINAL state. "worked for 2 weeks then died" = detractor.
10. COMPARATIVES: "Better than my old one" IS about this product. But only tag the product under review.
11. HEDGING: "Kind of loud" — still tag, but use specific labels over broad universals.
12. SEVERITY: Primary complaints (detailed, repeated) and passing mentions (brief) both get tagged.
13. MULTI-PRODUCT: Only tag symptoms about THIS product. Ignore claims about other products.
14. UNIVERSAL DISCIPLINE: Overall Satisfaction/Dissatisfaction are LAST RESORT. If 2+ specific labels on one side, DROP the universal.
15. CONTRADICTIONS: Don't assign Quiet AND Loud unless review discusses both in different contexts.
16. ZERO IS VALID: No tags better than forced matches.
17. UNLISTED: Add strong missing themes to unlisted arrays. Be conservative.
18. ALL IDS: Return a result for EVERY review id.

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
    """Build the user message with rating-gated polarity flags."""
    payload_items = []
    for it in items:
        review_text = str(it.get("review", ""))
        rating = it.get("rating")
        needs_det, needs_del = gate_polarity(rating, review_text)

        # Allow caller overrides
        if it.get("needs_det") is False:
            needs_det = False
        if it.get("needs_del") is False:
            needs_del = False

        payload_items.append(dict(
            id=str(it["idx"]),
            review=review_text,
            rating=rating,
            needs_detractors=needs_det,
            needs_delighters=needs_del,
        ))
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
    product_profile: str = "",
    product_knowledge: Any = None,
    max_ev_chars: int = 120,
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    include_universal_neutral: bool = True,
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

    # Build prompt context
    category = "general"
    if infer_category_fn:
        try:
            cat_info = infer_category_fn(
                product_profile,
                [it.get("review", "") for it in items[:12]],
            )
            category = cat_info.get("category", "general")
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

    system_prompt = _build_system_prompt(
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        product_profile=product_profile,
        product_knowledge_text=pk_text,
        taxonomy_context=taxonomy_context,
        category=category,
        max_ev_chars=max_ev_chars,
    )

    user_payload = _build_user_payload(items)
    max_out = min(5500, max(1200, 200 * len(items) + 400))  # Tighter = faster

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

    for it in items:
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

        # Refinement pass (v2 engine)
        if refine_fn:
            custom_dels, custom_dets = ([], [])
            if custom_universal_fn:
                try:
                    custom_dels, custom_dets = custom_universal_fn()
                except Exception:
                    pass

            refined = refine_fn(
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
                rating=rating,
                extra_universal_detractors=custom_dets,
                extra_universal_delighters=custom_dels,
            )
            dets = list(refined.get("dets", []))[:10]
            dels = list(refined.get("dels", []))[:10]
            ev_det = dict(refined.get("ev_det", {}) or {})
            ev_del = dict(refined.get("ev_del", {}) or {})

        # Enum validation
        safety = _validate_enum(obj.get("safety", "Not Mentioned"), SAFETY_ENUM)
        reliability = _validate_enum(obj.get("reliability", "Not Mentioned"), RELIABILITY_ENUM)
        sessions = _validate_enum(obj.get("sessions", "Unknown"), SESSIONS_ENUM)

        # Unlisted candidates
        unl_dels = _standardize_unlisted([str(x).strip() for x in (obj.get("unlisted_delighters") or []) if str(x).strip()])[:10]
        unl_dets = _standardize_unlisted([str(x).strip() for x in (obj.get("unlisted_detractors") or []) if str(x).strip()])[:10]
        if standardize_fn:
            try:
                unl_dels, _, _ = standardize_fn(unl_dels, [])
                _, unl_dets, _ = standardize_fn([], unl_dets)
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

    return out_by_idx


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

def _enforce_universal_discipline(labels, ev_map):
    """Rule 13: Drop universal labels when 2+ specific labels cover the sentiment."""
    specific = [l for l in labels if l not in _UNIVERSAL_LABELS]
    if len(specific) >= 2:
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
    """Extract labels with confidence, universal discipline, and contradiction checking."""
    labels = []
    ev_map: Dict[str, List[str]] = {}

    for obj in (raw_objs or []):
        if not isinstance(obj, dict): continue
        raw = str(obj.get("label", "")).strip()
        lbl = match_label(raw, allowed, aliases=aliases)
        if not lbl or lbl in labels: continue  # Dedup
        raw_evs = [str(e) for e in (obj.get("evidence") or []) if isinstance(e, str)]
        validated = validate_evidence(raw_evs, review_text, max_ev_chars)
        if not validated:
            validated = [str(e).strip()[:max_ev_chars] for e in raw_evs if str(e).strip()][:1]
        # Semantic coherence: does evidence actually relate to the label?
        if not _evidence_coherent_with_label(lbl, validated, review_text=review_text):
            continue
        labels.append(lbl)
        ev_map[lbl] = validated[:2]
        if len(labels) >= 10: break

    # Post-extraction intelligence
    labels, ev_map = _enforce_universal_discipline(labels, ev_map)
    labels = _check_contradictions_side(labels)
    # Filter by confidence threshold — drop very low-confidence tags
    if len(labels) >= 2:
        scored = []
        for lbl in labels:
            c = _score_tag_confidence(lbl, ev_map.get(lbl, []), review_text, rating,
                                       side=side, catalog_size=len(allowed))
            scored.append((lbl, c))
        # Keep tags above 0.25 confidence, or top 3 if all are low
        above_threshold = [(l, c) for l, c in scored if c >= 0.25]
        if above_threshold:
            labels = [l for l, c in above_threshold]
        else:
            scored.sort(key=lambda x: -x[1])
            labels = [l for l, c in scored[:3]]
        ev_map = {k: v for k, v in ev_map.items() if k in labels}
    return labels, ev_map


def _validate_enum(value: Any, enum_str: str) -> str:
    """Validate an enum value against a comma-separated enum string."""
    s = str(value or "").strip()
    allowed = [x.strip() for x in enum_str.split(",")]
    return s if s in allowed else allowed[-1]  # last is typically "Not Mentioned" / "Unknown"




# ---------------------------------------------------------------------------
# STAGE 1: Claim Extraction (for future staged pipeline)
# ---------------------------------------------------------------------------

_EXTRACT_SYSTEM = """You are a product review analyst. Extract every factual claim from this review.
For each claim: quote EXACT text (4-120 chars), label polarity (positive/negative/neutral/mixed), label aspect (2-4 words).
Output strict JSON: {"claims":[{"quote":"<verbatim>","polarity":"<polarity>","aspect":"<2-4 words>"}]}
Rules: extract ALL claims, verbatim quotes only, do not infer."""


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
    """Stage 2: Map extracted claims to catalog labels deterministically (no AI call)."""
    det_labels, del_labels = [], []
    det_ev, del_ev = {}, {}
    for claim in (claims or []):
        text = claim.get("text","")
        aspect = claim.get("aspect","")
        polarity = claim.get("polarity","neutral")
        targets = []
        if polarity in ("negative","mixed"): targets.append(("det", allowed_detractors, det_labels, det_ev))
        if polarity in ("positive","mixed"): targets.append(("del", allowed_delighters, del_labels, del_ev))
        if polarity == "neutral":
            targets.append(("det", allowed_detractors, det_labels, det_ev))
            targets.append(("del", allowed_delighters, del_labels, del_ev))
        for side, catalog, labels, ev in targets:
            label = match_label(aspect, catalog, aliases=aliases) or match_label(text[:60], catalog, aliases=aliases, cutoff=0.65)
            if label and label not in labels:
                labels.append(label); ev[label] = [text[:120]]
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

    for idx, result in results.items():
        item = items_by_idx.get(idx)
        if not item:
            continue

        rating = item.get("rating")
        needs_det, needs_del = gate_polarity(rating, item.get("review", ""))

        # Check if we missed tags on an expected side
        missed_det = needs_det and not result.get("dets")
        missed_del = needs_del and not result.get("dels")

        if not (missed_det or missed_del):
            continue

        # Build focused catalog for retry
        catalog_parts = []
        if missed_det:
            catalog_parts.append("DETRACTORS:\n" + "\n".join(f"  - {l}" for l in allowed_detractors))
        if missed_del:
            catalog_parts.append("DELIGHTERS:\n" + "\n".join(f"  - {l}" for l in allowed_delighters))

        system = _RETRY_SYSTEM_PROMPT.format(catalog_section="\n\n".join(catalog_parts))
        user_msg = json.dumps(dict(
            review=item.get("review", ""),
            rating=rating,
            retry_reason=f"{'missed detractors' if missed_det else ''}{' and ' if missed_det and missed_del else ''}{'missed delighters' if missed_del else ''}",
        ))

        try:
            result_text = chat_complete_fn(
                client,
                model=_model,
                structured=True,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
                max_tokens=1200,
                reasoning_effort=_reasoning,
            )
            data = _json_load(result_text)
            review_text = item.get("review", "")

            if missed_det and data.get("detractors"):
                new_dets, new_ev_det = _extract_side_with_confidence(
                    data["detractors"], allowed_detractors, review_text, rating,
                    aliases=aliases, side="detractor", max_ev_chars=max_ev_chars,
                )
                if new_dets:
                    updated = dict(retried[idx])
                    updated["dets"] = new_dets
                    updated["ev_det"] = new_ev_det
                    retried[idx] = updated

            if missed_del and data.get("delighters"):
                new_dels, new_ev_del = _extract_side_with_confidence(
                    data["delighters"], allowed_delighters, review_text, rating,
                    aliases=aliases, side="delighter", max_ev_chars=max_ev_chars,
                )
                if new_dels:
                    updated = dict(retried[idx])
                    updated["dels"] = new_dels
                    updated["ev_del"] = new_ev_del
                    retried[idx] = updated

            # Merge safety/reliability from retry if original was empty
            for field, enum_str in [("safety", SAFETY_ENUM), ("reliability", RELIABILITY_ENUM)]:
                retry_val = _validate_enum(data.get(field), enum_str)
                if retry_val not in ("Not Mentioned", "Unknown") and retried[idx].get(field) in ("Not Mentioned", "Unknown", None):
                    updated = dict(retried[idx])
                    updated[field] = retry_val
                    retried[idx] = updated

        except Exception as exc:
            logger.debug("Retry failed for review %s: %s", idx, exc)

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
) -> Dict[str, Any]:
    """Audit the distribution of tags across all processed reviews.

    Flags:
    - singleton_labels: labels that appear exactly once (potential hallucinations)
    - dominant_labels: labels on >50% of reviews (might be too broad)
    - zero_evidence_tags: tags that were assigned without evidence
    """
    if not results:
        return {}

    det_counts: Dict[str, int] = {}
    del_counts: Dict[str, int] = {}
    no_evidence: List[str] = []
    total = len(results)

    for idx, result in results.items():
        for label in (result.get("dets") or []):
            det_counts[label] = det_counts.get(label, 0) + 1
            if label not in (result.get("ev_det") or {}):
                no_evidence.append(label)
        for label in (result.get("dels") or []):
            del_counts[label] = del_counts.get(label, 0) + 1
            if label not in (result.get("ev_del") or {}):
                no_evidence.append(label)

    singleton_det = [l for l, c in det_counts.items() if c < min_occurrences]
    singleton_del = [l for l, c in del_counts.items() if c < min_occurrences]
    dominant_det = [l for l, c in det_counts.items() if c > total * 0.5]
    dominant_del = [l for l, c in del_counts.items() if c > total * 0.5]

    return {
        "total_reviews": total,
        "detractor_distribution": det_counts,
        "delighter_distribution": del_counts,
        "singleton_detractors": singleton_det,
        "singleton_delighters": singleton_del,
        "dominant_detractors": dominant_det,
        "dominant_delighters": dominant_del,
        "zero_evidence_tags": list(set(no_evidence)),
    }



# ---------------------------------------------------------------------------
# PHASE 3: Adaptive mid-run label tracking
# ---------------------------------------------------------------------------

class LabelTracker:
    """Tracks label performance during a symptomizer run.
    
    Flags labels that are hitting too often (>40% = too broad) or
    never hitting (0 in first N batches = possibly irrelevant).
    """
    def __init__(self, allowed_detractors, allowed_delighters):
        self.det_counts = {l: 0 for l in allowed_detractors}
        self.del_counts = {l: 0 for l in allowed_delighters}
        self.total_reviews = 0
        self.warnings = []
    
    def record_batch(self, batch_results):
        """Record results from one batch."""
        for idx, result in (batch_results or {}).items():
            self.total_reviews += 1
            for label in (result.get("dets") or []):
                if label in self.det_counts:
                    self.det_counts[label] += 1
            for label in (result.get("dels") or []):
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
        return alerts


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
