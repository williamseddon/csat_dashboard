"""
tag_quality.py  —  Symptomizer refinement engine (restructured v2)
==================================================================
Architecture changes from v1
------------------------------
* All configurable parameters live in TaggerConfig — serialisable to DB,
  tunable per product / use-case without touching algorithm code.
* ScoredTag dataclass replaces raw dicts throughout.  Every kept or dropped
  tag carries a human-readable `reason` field for debuggability.
* Evidence-first principle: a ScoredTag with evidence always beats one
  without, regardless of score magnitude.
* NegationDetector, FragmentScorer and TagRefiner are proper classes whose
  state is the config — easy to unit-test in isolation.
* Public API (refine_tag_assignment, compute_tag_edit_accuracy, …) is
  backward-compatible with v1 call sites.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Sentinel values
# ---------------------------------------------------------------------------

NON_VALUES = {"", "na", "n/a", "none", "null", "nan", "<na>", "not mentioned", "unknown"}

STOPWORDS = {
    "a", "an", "and", "the", "to", "for", "of", "in", "on", "with", "very", "really",
    "product", "item", "thing", "feature", "features", "overall",
}

GENERIC_SINGLE_WORD_CUES = {"easy", "simple", "hard", "difficult", "quick", "fast", "slow"}
GENERIC_LABEL_TOKENS = GENERIC_SINGLE_WORD_CUES | {"good", "poor", "high", "low"}

# ---------------------------------------------------------------------------
# Universal neutral labels
# ---------------------------------------------------------------------------

UNIVERSAL_NEUTRAL_DETRACTORS: List[str] = [
    "Overall Dissatisfaction", "Overpriced", "Poor Performance", "Poor Quality",
    "Difficult To Use", "Unreliable", "Hard To Clean", "Time Consuming",
]
UNIVERSAL_NEUTRAL_DELIGHTERS: List[str] = [
    "Overall Satisfaction", "Good Value", "Performs Well", "High Quality",
    "Easy To Use", "Reliable", "Easy To Clean", "Saves Time",
]

UNIVERSAL_DELIGHTER = UNIVERSAL_NEUTRAL_DELIGHTERS[0]
UNIVERSAL_DETRACTOR = UNIVERSAL_NEUTRAL_DETRACTORS[0]
UNIVERSAL_NEUTRAL_LABELS = set(UNIVERSAL_NEUTRAL_DETRACTORS + UNIVERSAL_NEUTRAL_DELIGHTERS)

# ---------------------------------------------------------------------------
# Canonical label patterns (most-specific → most-general; first match wins)
# ---------------------------------------------------------------------------

CANONICAL_LABEL_PATTERNS: Tuple[Tuple[str, str], ...] = (
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


def _canonicalize_tag_label(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip()).title()
    if not value:
        return ""
    for pattern, replacement in CANONICAL_LABEL_PATTERNS:
        if re.search(pattern, value, flags=re.IGNORECASE):
            return replacement
    return value


# ---------------------------------------------------------------------------
# Concept synonyms — 35 concept groups covering general + hair + vacuum/air
# (data unchanged from v1; architecture wraps it in ConceptLibrary below)
# ---------------------------------------------------------------------------

CONCEPT_SYNONYMS: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "overall_sentiment": {
        "positive": ("overall satisfaction","works great","it works great","it's great","love it","love this","amazing","excellent","awesome","perfect","fantastic","highly recommend","would recommend","happy","satisfied","very happy","best purchase","bought another","buy again","met expectations","worth buying","enjoying it","really enjoying","so far so good","pleased so far"),
        "negative": ("overall dissatisfaction","terrible","awful","hate it","disappointed","very disappointed","bad","poor","not good","doesn't work","does not work","would not recommend","wouldn't recommend","not worth it","regret buying","returned it","returning it","fell short"),
        "keywords": ("overall satisfaction","satisfaction","satisfied","happy","overall dissatisfaction","dissatisfaction","dissatisfied","disappointed","recommend","expectations"),
    },
    "noise": {
        "positive": ("quiet","silent","low noise","not loud","not noisy","runs quietly","quieter","quieter than","much quieter","a lot quieter"),
        "negative": ("loud","noisy","noise","too loud","too noisy","noise issue"),
        "keywords": ("quiet","silent","loud","noisy","noise"),
    },
    "ease_of_use": {
        "positive": ("easy to use","simple to use","straightforward to use","straightforward","intuitive","user friendly"),
        "negative": ("hard to use","difficult to use","confusing to use","complicated to use","awkward to use","not intuitive"),
        "keywords": ("easy","simple","straightforward","intuitive","hard","difficult","confusing","complicated"),
    },
    "cleaning": {
        "positive": ("easy to clean","easy cleanup","easy clean","quick cleanup"),
        "negative": ("hard to clean","difficult to clean","messy","cleanup is hard","cleanup takes forever"),
        "keywords": ("clean","cleanup","cleaning","messy","wash"),
    },
    "value": {
        "positive": ("good value","worth it","great value","affordable","priced right","worth the price","good for the money","worth every penny"),
        "negative": ("expensive","overpriced","pricey","too expensive","not worth it","cost too much"),
        "keywords": ("value","worth","affordable","expensive","overpriced","pricey","price"),
    },
    "performance": {
        "positive": ("performs well","works well","works great","effective","does the job","powerful","great results","good results"),
        "negative": ("poor performance","does not work well","doesn't work well","weak","underpowered","ineffective","poor results","bad results"),
        "keywords": ("performance","perform","result","results","effective","ineffective","powerful","weak"),
    },
    "quality": {
        "positive": ("high quality","good quality","well made","premium","solid","sturdy"),
        "negative": ("poor quality","low quality","cheap","cheaply made","feels cheap","bad quality","flimsy"),
        "keywords": ("quality","premium","cheap","flimsy","well made","sturdy","solid"),
    },
    "reliability": {
        "positive": ("reliable","durable","sturdy","holds up","well made"),
        "negative": ("broke","broken","breaks","stopped working","defective","flimsy","unreliable"),
        "keywords": ("reliable","durable","sturdy","broke","broken","defect","flimsy","unreliable"),
    },
    "time_efficiency": {
        "positive": ("saves time","time saver","quick","fast","quickly","faster"),
        "negative": ("time consuming","takes forever","slow","too slow","slower than expected"),
        "keywords": ("time","quick","fast","slow","forever"),
    },
    "size_weight": {
        "positive": ("compact","lightweight","light","small footprint","fits well","good size","right size"),
        "negative": ("bulky","heavy","too big","too large","too small","takes up space","does not fit"),
        "keywords": ("compact","lightweight","light","heavy","bulky","large","big","size","fit"),
    },
    "temperature": {
        "positive": ("stays cool","cool touch","safe temperature"),
        "negative": ("hot","too hot","overheats","burned","burnt","gets hot"),
        "keywords": ("hot","cool","overheat","burn","temperature"),
    },
    "setup": {
        "positive": ("easy setup","easy to set up","easy install","easy installation","easy assembly","assembled easily"),
        "negative": ("hard to set up","difficult setup","hard install","difficult installation","assembly was hard","setup issue"),
        "keywords": ("setup","set up","install","installation","assemble","assembly"),
    },
    "design": {
        "positive": ("looks great","nice design","stylish","beautiful","attractive","well designed"),
        "negative": ("poor design","bad design","awkward design","cheap looking","ugly","poorly designed"),
        "keywords": ("design","stylish","beautiful","attractive","ugly","awkward"),
    },
    "comfort": {
        "positive": ("comfortable","very comfortable","comfortable fit","feels good","supportive"),
        "negative": ("uncomfortable","not comfortable","painful","hurts","irritating to wear"),
        "keywords": ("comfortable","uncomfortable","painful","hurts","supportive"),
    },
    "instructions": {
        "positive": ("clear instructions","easy to follow instructions","clear directions","helpful instructions"),
        "negative": ("unclear instructions","bad directions","poor instructions","hard to follow instructions","instructions confusing"),
        "keywords": ("instructions","directions","manual","guide"),
    },
    "compatibility": {
        "positive": ("fits perfectly","compatible","works with","pairs easily"),
        "negative": ("doesn't fit","not compatible","won't pair","pairing issue","fit issue"),
        "keywords": ("fit","compatible","pair","pairing"),
    },
    "battery": {
        "positive": ("battery lasts","long battery life","holds a charge","holds charge well"),
        "negative": ("battery dies fast","short battery life","poor battery life","doesn't hold a charge","battery drains quickly"),
        "keywords": ("battery","charge","charging","drain"),
    },
    "charging": {
        "positive": ("charges quickly","fast charging","quick charge","fast to recharge"),
        "negative": ("charges slowly","slow charging","takes too long to charge","slow to recharge"),
        "keywords": ("charge","charging","recharge"),
    },
    "connectivity": {
        "positive": ("easy to connect","pairs quickly","connects easily","app setup was easy"),
        "negative": ("won't connect","connection issues","connectivity issues","bluetooth issues","wifi issues"),
        "keywords": ("connect","connection","connectivity","bluetooth","wifi","app"),
    },
    "taste": {
        "positive": ("tastes great","good flavor","great taste","delicious"),
        "negative": ("bad taste","tastes bad","awful flavor","off taste"),
        "keywords": ("taste","flavor","delicious"),
    },
    "scent": {
        "positive": ("smells great","pleasant scent","nice scent","pleasant fragrance"),
        "negative": ("strong smell","bad smell","weird scent","chemical smell","smells awful"),
        "keywords": ("smell","scent","fragrance","odor"),
    },
    "texture": {
        "positive": ("good texture","smooth texture","nice texture","great consistency"),
        "negative": ("bad texture","poor texture","grainy","sticky texture","weird consistency"),
        "keywords": ("texture","grainy","smooth","consistency","sticky"),
    },
    "packaging": {
        "positive": ("well packaged","arrived well packed","secure packaging"),
        "negative": ("arrived damaged","broken in box","damaged packaging","shipping issue","arrived broken"),
        "keywords": ("packaging","packaged","shipping","damaged box"),
    },
    # Hair care
    "frizz_control": {
        "positive": ("reduces frizz","frizz-free","no frizz","tames frizz","frizz free","eliminates frizz","smooth hair","sleek hair","controls frizz"),
        "negative": ("frizzy","makes frizz worse","still frizzy","didn't help frizz","more frizz","doesn't reduce frizz","no frizz control"),
        "keywords": ("frizz","frizzy","flyaway","flyaways","sleek"),
    },
    "heat_damage": {
        "positive": ("gentle on hair","no heat damage","doesn't damage hair","hair-friendly heat","safe for hair","won't damage hair","not damaging"),
        "negative": ("heat damage","damages hair","burns hair","fried my hair","hair breakage","too hot","scorching heat","burns scalp","hair is fried","caused damage"),
        "keywords": ("heat","damage","burn","fry","fried","breakage","scorching"),
    },
    "drying_speed": {
        "positive": ("fast drying","dries quickly","quick dry","fast dry time","dries fast","dried in minutes","quick drying"),
        "negative": ("slow drying","takes forever to dry","dries slowly","slow dry time","took forever to dry","takes too long to dry"),
        "keywords": ("dry","drying","dry time","dries"),
    },
    "attachment_fit": {
        "positive": ("easy attachment swap","attachments stay on","attachments click in","easy to attach","attachments are secure","stays on well"),
        "negative": ("attachment issues","attachments fall off","hard to attach","attachment doesn't stay","keeps coming off","attachment pops off","won't stay on","flies off"),
        "keywords": ("attachment","attachments","diffuser","concentrator","nozzle","accessory","accessories"),
    },
    "hair_results": {
        "positive": ("salon-quality results","salon results","professional results","like a blowout","smooth finish","shiny hair","glossy hair","great hair","beautiful results"),
        "negative": ("bad results on hair","doesn't work on my hair","no improvement to hair","worse hair"),
        "keywords": ("blowout","salon","shiny","glossy"),
    },
    "cord_length": {
        "positive": ("long cord","cord is long enough","good cord length","cord has plenty of length","long cable"),
        "negative": ("short cord","cord too short","not enough cord","limited reach","short cable","cord doesn't reach"),
        "keywords": ("cord","cable","reach"),
    },
    "weight_ergonomics": {
        "positive": ("lightweight","light to hold","easy to hold","not tiring to use","light in the hand","not heavy"),
        "negative": ("heavy","too heavy","arm gets tired","hand fatigue","wrist strain","tiring to hold","hard to hold"),
        "keywords": ("weight","heavy","wrist","fatigue"),
    },
    # Vacuum & cleaning
    "suction_power": {
        "positive": ("strong suction","powerful suction","great suction","amazing suction","incredible suction","fantastic suction","excellent suction","suction is great","suction is amazing","suction is incredible","suction is powerful","suction is strong","suction works great","picks up everything","picks up all","amazing pickup"),
        "negative": ("weak suction","poor suction","low suction power","no suction","doesn't pick up","leaves debris behind","misses dirt","loss of suction"),
        "keywords": ("suction","suction power","picks up","pickup","debris"),
    },
    "navigation_mapping": {
        "positive": ("good navigation","smart mapping","covers all areas","doesn't miss spots","good obstacle avoidance","great maneuverability","easy to steer"),
        "negative": ("gets stuck","poor navigation","misses spots","falls off ledge","bumps into furniture","mapping issues","poor obstacle avoidance","falls down stairs"),
        "keywords": ("navigation","mapping","stuck","obstacle","bump","steer","maneuver"),
    },
    "filtration_quality": {
        "positive": ("effective filtration","removes allergens","cleans air well","hepa works great","purifies air","captures particles","air feels cleaner"),
        "negative": ("filter issues","doesn't capture particles","poor filtration","filter falls out","filter doesn't work","air quality doesn't improve"),
        "keywords": ("filter","filtration","hepa","allergen","particle","purif"),
    },
    "vacuum_maintenance": {
        "positive": ("easy to empty","no mess emptying","clean dustbin","easy bin release","easy to maintain","washable filter","self-cleaning brush"),
        "negative": ("hard to empty","messy to empty","dust flies everywhere","clogs easily","hair tangles in brush","brush roll clogs","gets clogged","difficult to maintain"),
        "keywords": ("empty","dustbin","clog","clogs","brush roll","maintenance","tangle"),
    },
    "product_lifespan": {
        "positive": ("lasts for years","still going strong","held up well","built to last","durable over time","long-lasting","survived years of use"),
        "negative": ("died after months","only lasted weeks","broke down quickly","short lifespan","fell apart quickly","stopped working after","wore out fast","didn't last long","failed prematurely"),
        "keywords": ("lifespan","lasted","lasts","died after","broke after","longevity"),
    },
}

EXPLICIT_LABEL_CONCEPTS: Dict[str, str] = {
    "Overall Satisfaction": "overall_sentiment",      "Overall Dissatisfaction": "overall_sentiment",
    "Good Value": "value",                             "Overpriced": "value",
    "Performs Well": "performance",                    "Poor Performance": "performance",
    "High Quality": "quality",                         "Poor Quality": "quality",
    "Easy To Use": "ease_of_use",                      "Difficult To Use": "ease_of_use",
    "Reliable": "reliability",                         "Unreliable": "reliability",
    "Poor Durability": "reliability",                  "Durable": "reliability",
    "Easy To Clean": "cleaning",                       "Hard To Clean": "cleaning",
    "Saves Time": "time_efficiency",                   "Time Consuming": "time_efficiency",
    "Quiet": "noise",                                  "Loud": "noise",
    "Easy Setup": "setup",                             "Difficult Setup": "setup",
    "Clear Instructions": "instructions",              "Instructions Unclear": "instructions",
    "Right Size": "size_weight",                       "Wrong Size": "size_weight",
    "Long Battery Life": "battery",                    "Short Battery Life": "battery",
    "Fast Charging": "charging",                       "Slow Charging": "charging",
    "Easy Connectivity": "connectivity",               "Connectivity Issues": "connectivity",
    "Attractive Design": "design",                     "Poor Design": "design",
    "Comfortable": "comfort",                          "Uncomfortable": "comfort",
    "Cool Touch": "temperature",                       "Gets Hot": "temperature",
    "Pleasant Scent": "scent",                         "Unpleasant Scent": "scent",
    "Good Taste": "taste",                             "Bad Taste": "taste",
    "Good Texture": "texture",                         "Bad Texture": "texture",
    "Well Packaged": "packaging",                      "Damaged Packaging": "packaging",
    # Hair care
    "Salon-Quality Results": "hair_results",           "Heat Damage": "heat_damage",
    "Gentle On Hair": "heat_damage",                   "Too Hot": "heat_damage",
    "Reduces Frizz": "frizz_control",                  "Doesn't Reduce Frizz": "frizz_control",
    "Fast Drying": "drying_speed",                     "Slow Drying": "drying_speed",
    "Easy Attachment Swap": "attachment_fit",          "Attachment Issues": "attachment_fit",
    "Long Cord": "cord_length",                        "Short Cord": "cord_length",
    "Lightweight": "weight_ergonomics",                "Heavy": "weight_ergonomics",
    "Multiple Heat Settings": "ease_of_use",           "Poor Build Quality": "quality",
    # Vacuum & cleaning
    "Strong Suction": "suction_power",                 "Weak Suction": "suction_power",
    "Good Maneuverability": "navigation_mapping",      "Poor Navigation": "navigation_mapping",
    "Clogs Easily": "vacuum_maintenance",              "Tangled Brush Roll": "vacuum_maintenance",
    "Easy To Empty": "vacuum_maintenance",             "Hard To Empty": "vacuum_maintenance",
    "Easy To Maintain": "vacuum_maintenance",          "Picks Up Pet Hair": "suction_power",
    "Effective Filtration": "filtration_quality",      "Filter Issues": "filtration_quality",
    "Filter Expensive": "value",                       "Limited Coverage": "size_weight",
    "Short Filter Life": "reliability",                "Easy Filter Change": "cleaning",
    # Universal
    "Short Lifespan": "product_lifespan",              "Long-Lasting": "product_lifespan",
    "Dead On Arrival": "reliability",                  "Missing Parts": "packaging",
}

STEM_EQUIVS: Dict[str, str] = {
    "noisy":"noise","loud":"noise","louder":"noise","quiet":"quiet","silent":"quiet","quieter":"quiet",
    "cleanup":"clean","cleaning":"clean","cleaned":"clean","cleaner":"clean",
    "simple":"easy","easily":"easy","straightforward":"easy","intuitive":"easy",
    "difficult":"hard","confusing":"hard","complicated":"hard",
    "pricey":"price","expensive":"price","overpriced":"price","affordable":"value","worth":"value",
    "durable":"reliable","durability":"reliable","sturdy":"reliable","reliability":"reliable","reliable":"reliable",
    "broke":"break","broken":"break","breaks":"break","breaking":"break","lightweight":"light",
    "burned":"burn","burnt":"burn","overheats":"overheat",
    "performance":"perform","performs":"perform","performed":"perform",
    "quality":"quality","premium":"quality","flimsy":"quality","cheap":"quality",
    "faster":"fast","quickly":"quick","slower":"slow",
    "setup":"setup","installing":"install","installation":"install","assembled":"assemble","assembly":"assemble",
    "comfortable":"comfort","uncomfortable":"discomfort","supportive":"comfort",
    "instructions":"instruction","directions":"instruction","manual":"instruction",
    "connection":"connect","connectivity":"connect","connecting":"connect","paired":"pair","pairing":"pair",
    "battery":"battery","charging":"charge","charged":"charge","fragrance":"scent","smells":"smell","odor":"smell",
    "flavor":"taste","tastes":"taste","delicious":"taste","texture":"texture","consistency":"texture","grainy":"texture",
    "packaged":"packaging","shipping":"packaging",
    # Hair care
    "frizzy":"frizz","frizzing":"frizz","flyaway":"frizz","flyaways":"frizz","frizz-free":"frizz",
    "drying":"dry","dried":"dry","dries":"dry","fried":"fry","frying":"fry","breakage":"break",
    "scorching":"scorch","damaged":"damage","damages":"damage","diffuser":"attachment",
    "concentrator":"attachment","nozzle":"attachment","attachments":"attachment",
    "fatigued":"fatigue","tiring":"tire","cord":"cord","cable":"cord",
    # Vacuum & cleaning
    "suction":"suction","vacuums":"vacuum","vacuuming":"vacuum","clogging":"clog","clogged":"clog",
    "clogs":"clog","tangles":"tangle","tangled":"tangle","tangling":"tangle","debris":"debris",
    "dustbin":"dustbin","filtration":"filter","allergens":"allergen","purifies":"purify",
    "purifying":"purify","maneuvers":"maneuver","maneuvering":"maneuver","navigates":"navigate",
    "navigating":"navigate","mapping":"map",
    # Universal
    "lifespan":"lifespan","lasting":"last","longevity":"lifespan","premature":"early",
}


# ---------------------------------------------------------------------------
# Dataclasses — typed contracts throughout the pipeline
# ---------------------------------------------------------------------------

@dataclass
class TaggerConfig:
    """All tunable threshold parameters in one serialisable struct.

    Operators can create product-specific or use-case-specific configs:
    - QA monitoring: high precision  → raise keep_threshold, add_threshold
    - Consumer insights: high recall → lower both thresholds
    - Brand monitoring: balanced     → defaults (below)
    """
    # Keep thresholds (AI-selected labels)
    base_keep_threshold: float = 1.10
    base_keep_threshold_small_catalog: float = 1.35
    # Add thresholds (labels NOT in AI output but scoring high)
    base_add_threshold: float = 2.00
    base_add_threshold_small_catalog: float = 2.25
    # has_support gates
    has_support_coverage_score_threshold: float = 0.75   # score required if coverage >= 0.5
    has_support_hard_coverage: float = 0.80              # coverage alone → has_support
    has_support_score_floor: float = -0.50               # below this negation always wins
    # Scoring deltas
    evidence_boost: float = 1.75
    evidence_extra_per_item: float = 0.40
    evidence_cap: float = 1.00
    cue_hit_boost: float = 1.25
    cue_hit_extra_per_item: float = 0.55
    cue_hit_cap: float = 1.25
    coverage_half_boost: float = 0.90                    # coverage >= 0.5
    coverage_full_boost: float = 0.60                    # coverage >= 0.8
    opposite_penalty_with_evidence: float = 1.20
    opposite_penalty_without_evidence: float = 1.80
    negated_opposite_boost: float = 2.25
    negated_opposite_boost_with_cues: float = 1.50
    negation_penalty_with_cues: float = 2.50
    negation_penalty_without_cues: float = 2.00
    # Intensifier boosts
    intensifier_boost: float = 0.55                      # overall_sentiment
    intensifier_boost_specific: float = 0.35             # other concepts
    concept_keyword_extra_boost: float = 0.25
    # Star rating signal
    star_rating_overall_max: float = 0.85                # for overall_sentiment concept
    star_rating_specific_max: float = 0.20               # for all other concepts
    # Fragment length bonus (only when cue_hits or evidence present)
    fragment_long_boost: float = 0.20                    # >= 30 words
    fragment_medium_boost: float = 0.10                  # >= 15 words
    # Concept deduplication
    concept_dedup_margin: float = 1.50                   # score gap to displace a well-supported label
    # Catalog size threshold
    small_catalog_size: int = 6
    # Max labels per side in output
    max_per_side: int = 10


@dataclass
class TagEvidence:
    """A single piece of textual evidence supporting a tag."""
    text: str                        # verbatim snippet from the review
    source_cue: str = ""             # which cue/synonym triggered this
    confidence: float = 1.0          # 0–1; 1.0 = exact match, lower = fuzzy


@dataclass
class ScoredTag:
    """A label candidate with full scoring provenance.

    The `reason` field is the primary debugging handle: it records exactly
    why a tag was kept or dropped, enabling systematic accuracy analysis.
    """
    label: str
    concept: Optional[str]
    score: float
    evidence: List[TagEvidence] = field(default_factory=list)
    cue_hits: List[str] = field(default_factory=list)
    opposite_hits: List[str] = field(default_factory=list)
    coverage: float = 0.0
    has_support: bool = False
    kept: bool = False
    reason: str = ""
    rating_signal: float = 0.0

    @property
    def best_snippets(self) -> List[str]:
        return [e.text for e in self.evidence[:2]] if self.evidence else []


@dataclass
class RefinementResult:
    """Full output of the refinement pass — typed replacement for the raw dict."""
    dets: List[str]
    dels: List[str]
    ev_det: Dict[str, List[str]]
    ev_del: Dict[str, List[str]]
    scored_dets: Dict[str, ScoredTag]
    scored_dels: Dict[str, ScoredTag]
    added_dets: List[str]
    added_dels: List[str]
    removed_dets: List[str]
    removed_dels: List[str]

    def as_dict(self) -> Dict[str, Any]:
        """Backward-compatible dict for existing call sites."""
        return {
            "dets": self.dets, "dels": self.dels,
            "ev_det": self.ev_det, "ev_del": self.ev_del,
            "support_det": {k: {"score": v.score, "has_support": v.has_support,
                                "concept": v.concept, "snippets": v.best_snippets,
                                "cue_hits": v.cue_hits, "evidence": v.best_snippets,
                                "opposite_hits": v.opposite_hits,
                                "coverage": v.coverage, "rating_signal": v.rating_signal,
                                "review_norm": ""}
                            for k, v in self.scored_dets.items()},
            "support_del": {k: {"score": v.score, "has_support": v.has_support,
                                "concept": v.concept, "snippets": v.best_snippets,
                                "cue_hits": v.cue_hits, "evidence": v.best_snippets,
                                "opposite_hits": v.opposite_hits,
                                "coverage": v.coverage, "rating_signal": v.rating_signal,
                                "review_norm": ""}
                            for k, v in self.scored_dels.items()},
            "added_dets": self.added_dets, "added_dels": self.added_dels,
            "removed_dets": self.removed_dets, "removed_dels": self.removed_dels,
        }


# ---------------------------------------------------------------------------
# Pre-compiled sentiment intensifier patterns
# ---------------------------------------------------------------------------

_STRONG_POSITIVE_RE = re.compile(
    r"\b(?:absolutely\s+love|totally\s+(?:love|obsessed)|blown?\s+(?:me\s+)?away"
    r"|best\s+(?:purchase|buy|product|vacuum|appliance|thing)\s+(?:I'?ve?\s+ever|ever)"
    r"|couldn'?t\s+be\s+(?:happier|more\s+pleased|more\s+satisfied)"
    r"|exceeded\s+(?:my\s+)?expectations|phenomenal|exceptional|outstanding"
    r"|game\s+changer|life\s+changing|absolutely\s+(?:amazing|incredible|perfect|fantastic)"
    r"|10\s*/\s*10|five\s+stars?\s+(?:all\s+the\s+way)?|would\s+buy\s+again"
    r"|highly\s+recommend|love\s+love\s+love|obsessed\s+with|so\s+impressed"
    r"|wish\s+I\s+had\s+(?:bought|found)\s+this\s+sooner)\b",
    flags=re.IGNORECASE,
)

_STRONG_NEGATIVE_RE = re.compile(
    r"\b(?:absolutely\s+(?:terrible|awful|horrible|useless|hate)"
    r"|complete(?:ly)?\s+(?:useless|waste|garbage|junk|trash|disaster|disappointed)"
    r"|total(?:ly)?\s+(?:useless|waste|garbage|junk|trash|disaster)"
    r"|utter(?:ly)?\s+(?:useless|terrible|awful|disappointed)"
    r"|worst\s+(?:purchase|buy|product|vacuum|appliance|thing)\s+(?:I'?ve?\s+ever|ever)"
    r"|couldn'?t\s+be\s+(?:worse|more\s+disappointed|more\s+frustrated)"
    r"|fell\s+apart|piece\s+of\s+(?:junk|garbage|trash)"
    r"|absolute\s+(?:garbage|junk|disaster|waste|rubbish)"
    r"|do\s+not\s+(?:buy|waste\s+your\s+money)|save\s+your\s+money"
    r"|returning\s+(?:this|it)\s+immediately|threw\s+it\s+(?:away|in\s+the\s+trash)"
    r"|zero\s+stars?\s+if\s+(?:I\s+could|possible))\b",
    flags=re.IGNORECASE,
)

# Pre-compiled concept-specific friction / delight overrides
# Each entry: concept → {side: [(compiled_re, score_delta)]}
_CONCEPT_PATTERN_OVERRIDES: Dict[str, Dict[str, List[Tuple[re.Pattern, float]]]] = {}

_RAW_CONCEPT_PATTERN_OVERRIDES: Dict[str, Dict[str, List[Tuple[str, float]]]] = {
    "cleaning": {
        "detractor": [(r"\bcleanup\s+takes\s+forever\b|\btakes\s+forever\s+to\s+clean\b|\bcleanup\s+is\s+a\s+(?:pain|nightmare|chore)\b|\bsuch\s+a\s+pain\s+to\s+clean\b|\bso\s+hard\s+to\s+clean\b", 2.2)],
        "delighter": [(r"\bcleanup\s+takes\s+forever\b|\btakes\s+forever\s+to\s+clean\b|\bcleanup\s+is\s+a\s+(?:pain|nightmare|chore)\b", -2.0)],
    },
    "setup": {
        "detractor": [(r"\b(?:once|after)\s+you\s+figure\s+out\s+(?:the\s+)?setup\b|\bfigure\s+out\s+(?:the\s+)?setup\b|\btook\s+(?:me\s+)?(?:forever|hours|a\s+long\s+time)\s+to\s+set\s+up\b|\bsetup\s+(?:was|is|took)\s+(?:a\s+)?(?:nightmare|struggle|pain|hassle)\b", 2.1)],
        "delighter": [(r"\b(?:once|after)\s+you\s+figure\s+out\s+(?:the\s+)?setup\b|\bfigure\s+out\s+(?:the\s+)?setup\b", -1.9)],
    },
    "noise": {
        "detractor": [(r"\bwakes?\s+(?:me|us|everyone)\s+up\b|\bcan\s+(?:hear|heard)\s+(?:it\s+)?from\s+(?:another|the\s+other)\s+room\b|\bso\s+loud\s+(?:you|i)\s+can't\s+(?:hear|talk|watch)\b", 2.0)],
        "delighter": [(r"\bwakes?\s+(?:me|us|everyone)\s+up\b", -1.8)],
    },
    "reliability": {
        "detractor": [(r"\bstop(?:ped|s)?\s+work(?:ing)?\s+(?:after|within)\s+(?:a\s+)?(?:week|month|day|few)\b|\bbreak(?:s|ing)?\s+down\b|\bfell\s+apart\b|\bstopped\s+working\s+(?:completely|suddenly|randomly)\b", 2.3)],
        "delighter": [(r"\bstop(?:ped|s)?\s+work(?:ing)?\b|\bfell\s+apart\b", -2.0)],
    },
    "temperature": {
        "detractor": [(r"\bget(?:s|ting)?\s+(?:extremely|very|so|dangerously)\s+hot\b|\bburi?n(?:t|ed|s)?\s+(?:my|the)\b|\bscalding\b|\bsmok(?:e|es|ing)\b", 2.4)],
        "delighter": [(r"\bscalding\b|\bburi?n(?:t|ed)?\b|\bsmok(?:e|ing)\b", -2.2)],
    },
    "heat_damage": {
        "detractor": [(r"\bfried\s+my\s+hair\b|\bhair\s+(?:is\s+(?:\w+\s+)?)?fried\b"
                       r"|\bmy\s+hair\s+is\s+(?:\w+\s+)?fried\b"
                       r"|\bhair\s+fell\s+out\b|\bhair\s+broke\s+off\b"
                       r"|\bscalp\s+burn(?:s|ed)?\b|\bgave\s+me\s+a\s+burn\b"
                       r"|\bburnt?\s+(?:my\s+)?(?:hair|scalp)\b"
                       r"|\bcaused\s+(?:heat\s+)?damage\b|\bdamaged?\s+my\s+hair\b"
                       r"|\bheat\s+damage(?:d)?\b", 2.5)],
        "delighter": [(r"\bfried\s+my\s+hair\b|\bhair\s+broke\s+off\b"
                       r"|\bhair\s+fell\s+out\b|\bburnt?\s+(?:my\s+)?(?:hair|scalp)\b", -2.3)],
    },
    "frizz_control": {
        "detractor": [(r"\bstill\s+(?:so\s+)?frizzy\b|\bhair\s+(?:is\s+)?(?:still\s+)?(?:a\s+)?frizzy\s+mess\b|\bdid(?:n'?t)?\s+(?:help|reduce|tame|control)\s+(?:the\s+|my\s+)?frizz\b|\bmade\s+(?:my\s+hair\s+)?(?:even\s+)?(?:more\s+)?frizzy\b|\bworse\s+frizz\b", 2.0)],
        "delighter": [(r"\bstill\s+(?:so\s+)?frizzy\b|\bmade\s+(?:my\s+hair\s+)?(?:more\s+)?frizzy\b|\bworse\s+frizz\b", -2.0)],
    },
    "attachment_fit": {
        "detractor": [(r"\bkeeps?\s+(?:falling|coming)\s+off\b|\bwon'?t\s+stay\s+on\b|\bflies?\s+off\b|\bpopped?\s+off\b|\bdidn'?t\s+(?:stay|fit|lock)\s+(?:on|in|properly)\b|\bkept\s+(?:falling|flying|popping)\s+off\b", 2.2)],
        "delighter": [(r"\bkeeps?\s+(?:falling|coming)\s+off\b|\bwon'?t\s+stay\s+on\b|\bflies?\s+off\b", -2.0)],
    },
    "drying_speed": {
        "detractor": [(r"\btook\s+(?:forever|hours?|so\s+long)\s+(?:to\s+dry|drying)\b|\bstill\s+(?:wet|damp)\s+after\b|\bdry\s+time\s+(?:is\s+)?(?:terrible|awful|horrible|way\s+too\s+long)\b", 2.0)],
        "delighter": [(r"\btook\s+(?:forever|hours?)\s+(?:to\s+dry|drying)\b|\bstill\s+(?:wet|damp)\s+after\b", -1.8)],
    },
    "cord_length": {
        "detractor": [(r"\bcord\s+(?:is\s+)?(?:way\s+)?too\s+short\b|\bcan'?t\s+(?:reach|use\s+it)\s+(?:from|without\s+standing\s+right\s+next\s+to)\b|\blimited\s+(?:by\s+(?:the\s+)?)?cord\s+(?:length|reach)\b", 1.8)],
        "delighter": [(r"\bcord\s+(?:is\s+)?(?:way\s+)?too\s+short\b", -1.8)],
    },
    "suction_power": {
        "detractor": [(r"\bleaves?\s+(?:all\s+)?(?:the\s+)?(?:dirt|debris|dust|crumbs?)\s+behind\b|\bcan'?t\s+pick\s+up\b|\bmisses?\s+(?:half|all|most\s+of)\s+(?:the\s+)?dirt\b|\bno\s+(?:suction|power)\s+(?:at\s+all|left|whatsoever)\b|\blost\s+(?:all\s+)?suction\b", 2.2)],
        "delighter": [(r"\bleaves?\s+(?:dirt|debris)\s+behind\b|\bno\s+suction\s+(?:at\s+all|left)\b", -2.0)],
    },
    "navigation_mapping": {
        "detractor": [(r"\bgot\s+stuck\s+(?:under|behind|in)\b|\bfell\s+(?:down|off)\s+(?:the\s+)?(?:stairs?|steps?|ledge|edge)\b|\bkeeps?\s+(?:getting\s+)?stuck\b|\bdrove\s+off\s+the\s+(?:ledge|stair)\b", 2.2)],
        "delighter": [(r"\bgot\s+stuck|\bfell\s+(?:down|off)\b", -2.0)],
    },
    "vacuum_maintenance": {
        "detractor": [(r"\bhair\s+wraps?\s+(?:all\s+)?around\s+(?:the\s+)?(?:brush|roller)\b|\bbrush\s+roll\s+(?:clogs?|jams?|gets?\s+tangled)\b|\bcompletely\s+clogged\b|\bclogged\s+after\s+(?:one|every|each)\s+use\b|\bdust\s+flies?\s+everywhere\s+when\s+(?:emptying|empty)\b", 2.2)],
        "delighter": [(r"\bhair\s+wraps?\s+around\s+(?:the\s+)?brush\b|\bbrush\s+roll\s+(?:clogs?|jams?)\b", -2.0)],
    },
    "product_lifespan": {
        "detractor": [(r"\bdied\s+after\s+(?:only\s+)?(?:\d+\s+)?(?:days?|weeks?|months?|uses?)\b|\bonly\s+lasted\s+(?:\d+\s+)?(?:days?|weeks?|months?)\b|\bstopped\s+working\s+after\s+(?:\d+\s+)?(?:days?|weeks?|months?)\b|\bbroke\s+(?:down\s+)?after\s+(?:just\s+)?(?:\d+\s+)?(?:uses?|days?|weeks?|months?)\b|\bwore\s+out\s+(?:after|in)\s+(?:just\s+)?(?:\d+\s+)?(?:months?|weeks?)\b", 2.5)],
        "delighter": [(r"\bstill\s+going\s+strong\s+after\b|\bheld\s+up\s+(?:great|well)\s+after\s+(?:\d+\s+)?(?:years?|months?)\b", 2.0)],
    },
    "filtration_quality": {
        "detractor": [(r"\bdoesn'?t\s+(?:clean|purify|filter)\s+(?:the\s+)?air\b|\bair\s+quality\s+(?:hasn'?t|didn'?t)\s+improved?\b|\bstill\s+sneezing\s+(?:with|after|around)\b|\ballergens?\s+(?:still|not)\b", 2.1)],
        "delighter": [(r"\bno\s+longer\s+sneezing\b|\ballergies?\s+(?:gone|improved|better)\b|\bair\s+feels?\s+(?:so\s+much\s+)cleaner\b", 2.1)],
    },
}

# Lazy-compile overrides on first use
def _get_concept_overrides() -> Dict[str, Dict[str, List[Tuple[re.Pattern, float]]]]:
    global _CONCEPT_PATTERN_OVERRIDES
    if not _CONCEPT_PATTERN_OVERRIDES:
        for concept, sides in _RAW_CONCEPT_PATTERN_OVERRIDES.items():
            compiled: Dict[str, List[Tuple[re.Pattern, float]]] = {}
            for side_key, entries in sides.items():
                compiled[side_key] = [
                    (re.compile(pattern, flags=re.IGNORECASE), delta)
                    for pattern, delta in entries
                ]
            _CONCEPT_PATTERN_OVERRIDES[concept] = compiled
    return _CONCEPT_PATTERN_OVERRIDES


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def _canon(text: Any) -> str:
    value = str(text or "").strip().lower()
    value = value.replace("\u2018", "'").replace("\u2019", "'").replace("`", "'")
    value = value.replace("\u201c", '"').replace("\u201d", '"')
    value = re.sub(r"[\u2010-\u2015]", "-", value)
    return re.sub(r"\s+", " ", value)


def _clean_tag(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip())
    if not value or _canon(value) in NON_VALUES:
        return ""
    return value.title()


def _stem(token: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "", token.lower())
    if not token:
        return ""
    token = STEM_EQUIVS.get(token, token)
    for suffix in ("ing", "ers", "ier", "est", "er", "ies", "ied", "ed", "es", "s", "ly"):
        if len(token) > 4 and token.endswith(suffix):
            token = token[:-len(suffix)]
            break
    return STEM_EQUIVS.get(token, token)


def _tokenize(text: str) -> List[str]:
    tokens = [_stem(m) for m in re.findall(r"[a-z0-9']+", _canon(text))]
    return [t for t in tokens if t and t not in STOPWORDS and len(t) > 1]


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set = set()
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _coerce_rating(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _first_review_snippet(review_text: str, max_chars: int = 120) -> Optional[str]:
    raw = re.sub(r"\s+", " ", str(review_text or "").strip())
    if not raw:
        return None
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+|\n+", raw) if p.strip()]
    snippet = parts[0] if parts else raw
    return (snippet[:max_chars].rstrip() or None)


# ---------------------------------------------------------------------------
# NegationDetector — sentence-aware negation scoping
# ---------------------------------------------------------------------------

class NegationDetector:
    """Detects negated phrase occurrences in text.

    Operates at the fragment level so "I had no noise issues — the
    attachment falls off" doesn't cancel the attachment detractor.
    """

    _PATTERNS: List[re.Pattern] = []  # compiled lazily

    @classmethod
    def _get_patterns(cls, phrase_pattern: str) -> List[re.Pattern]:
        return [
            re.compile(p, re.IGNORECASE) for p in [
                rf"\b(?:not|never|hardly|barely|isn't|wasn't|weren't|ain't|without)\s+(?:really\s+|very\s+|that\s+|quite\s+|at\s+all\s+)?{phrase_pattern}\b",
                rf"\bnot\s+as\s+{phrase_pattern}\s+as\b",
                rf"\bnot\s+as\s+{phrase_pattern}\s+as\s+(?:I\s+)?(?:expected|hoped|thought|wanted|advertised|described|promised)\b",
                rf"\b(?:less|fewer)\s+{phrase_pattern}\b",
                rf"\b(?:less|lacking|missing|needs?\s+more|could\s+use\s+more)\s+{phrase_pattern}\b",
                rf"\bno\s+(?:issue|issues|problem|problems|complaint|complaints|trouble|concern|concerns)\s+(?:with|from|regarding|about)\s+(?:the\s+|any\s+|this\s+)?(?:[^.!?;,]{{0,30}}\s+)?{phrase_pattern}\b",
                rf"\bfree\s+of\s+{phrase_pattern}\b",
                rf"{phrase_pattern}-free\b",
                rf"\bzero\s+(?:[^.!?;,]{{0,20}}\s+)?{phrase_pattern}\b",
                rf"\bno\s+{phrase_pattern}(?:\s+\S+){{0,4}}\s+(?:at\s+all|whatsoever|to\s+speak\s+of)\b",
                rf"\bnever\s+(?:had|experienced|noticed|encountered|seen|had\s+any)\s+(?:[^.!?;,]{{0,20}}\s+)?{phrase_pattern}\b",
                rf"\bwithout\s+(?:any\s+|a\s+)?{phrase_pattern}\b",
                rf"\b(?:would|could|should)\s+(?:be|have\s+been)\s+(?:more|better|nicer)\s+.*{phrase_pattern}\b",
                rf"\b(?:supposed|expected|meant|claimed|advertised)\s+to\s+(?:be\s+)?{phrase_pattern}\b.*\b(?:but|however|yet|instead|unfortunately)\b",
                rf"\b(?:only|just)\s+{phrase_pattern}\s+(?:on|at|when|during|in)\b",
            ]
        ]

    @classmethod
    def is_negated(cls, phrase: str, text: str) -> bool:
        if not phrase or not text:
            return False
        parts = [p for p in re.split(r"\s+", str(phrase).strip()) if p]
        escaped_parts = []
        for p in parts:
            ep = re.escape(p)
            ep = ep.replace("\\'", "['\u2018\u2019" + chr(96) + "]")
            ep = ep.replace("\\-", "[-\u2013\u2014]")
            escaped_parts.append(ep)
        phrase_pattern = r"\s+".join(escaped_parts)
        return any(p.search(text) for p in cls._get_patterns(phrase_pattern))


# ---------------------------------------------------------------------------
# ConceptLibrary — wraps CONCEPT_SYNONYMS with query methods
# ---------------------------------------------------------------------------

class ConceptLibrary:
    """Provides concept inference and cue expansion.

    Centralises all CONCEPT_SYNONYMS lookups so FragmentScorer doesn't
    have to import the raw dict directly.
    """

    def infer(self, label: str, aliases: Optional[Sequence[str]] = None) -> Optional[str]:
        candidates = [_canonicalize_tag_label(label)]
        if aliases:
            candidates += [_canonicalize_tag_label(a) for a in aliases]
        candidates = [c for c in candidates if c]
        for c in candidates:
            explicit = EXPLICIT_LABEL_CONCEPTS.get(c)
            if explicit:
                return explicit
        hay_tokens: set = set()
        for c in candidates:
            hay_tokens.update(_tokenize(c))
        haystacks = [_canon(c) for c in candidates]
        best_concept: Optional[str] = None
        best_score = 0.0
        for concept, spec in CONCEPT_SYNONYMS.items():
            score = 0.0
            for keyword in spec.get("keywords", ()):
                kn = _canon(keyword)
                kt = set(_tokenize(keyword))
                if kn and any(kn == h or kn in h for h in haystacks):
                    score = max(score, 3.0 + 0.15 * len(kt or {kn}))
                    continue
                if kt and hay_tokens:
                    overlap = len(kt & hay_tokens)
                    if overlap:
                        score = max(score, (overlap / float(len(kt))) + 0.2 * len(kt))
            if score > best_score:
                best_score, best_concept = score, concept
        return best_concept

    def cues_for(self, label: str, side: str, aliases: Optional[Sequence[str]] = None) -> Tuple[List[str], Optional[str]]:
        cleaned = _canonicalize_tag_label(label)
        alias_vals = [_canonicalize_tag_label(a) for a in (aliases or []) if _canonicalize_tag_label(a)]
        concept = self.infer(cleaned, alias_vals)
        cues: List[str] = [cleaned] + alias_vals
        if concept:
            polarity = "positive" if side == "delighter" else "negative"
            cues.extend(CONCEPT_SYNONYMS.get(concept, {}).get(polarity, ()))
        label_tokens = _tokenize(cleaned)
        if label_tokens:
            cues.append(" ".join(label_tokens))
        return _dedupe_keep_order(_filter_generic_cues(cues, concept, cleaned)), concept

    def opposite_phrases(self, concept: Optional[str], side: str) -> Tuple[str, ...]:
        if not concept:
            return ()
        polarity = "negative" if side == "delighter" else "positive"
        return CONCEPT_SYNONYMS.get(concept, {}).get(polarity, ())

    def concept_keywords(self, concept: Optional[str]) -> Tuple[str, ...]:
        if not concept:
            return ()
        return CONCEPT_SYNONYMS.get(concept, {}).get("keywords", ())


_concept_lib = ConceptLibrary()   # module-level singleton


def _filter_generic_cues(cues: Sequence[str], concept: Optional[str], cleaned_label: str) -> List[str]:
    if not concept:
        return [str(c) for c in cues if str(c).strip()]
    label_norm = _canon(cleaned_label)
    kept: List[str] = []
    for cue in cues:
        cue_text = re.sub(r"\s+", " ", str(cue or "").strip().lower())
        if not cue_text:
            continue
        if cue_text in GENERIC_SINGLE_WORD_CUES and cue_text not in label_norm:
            continue
        kept.append(cue_text)
    return kept


def _effective_label_tokens(label: str, concept: Optional[str]) -> List[str]:
    tokens = _tokenize(label)
    if not concept or len(tokens) <= 1:
        return tokens
    filtered = [t for t in tokens if t not in GENERIC_LABEL_TOKENS]
    return filtered or tokens


_GENERIC_POSITIVE_SENTIMENT_RE = re.compile(
    r"\b(?:enjoy(?:ed|ing)?|happy|pleased|satisfied|so\s+far\s+so\s+good|works?\s+great|working\s+great|"
    r"lov(?:e|ed)|recommend|awesome|amazing|fantastic|excellent)\b",
    flags=re.IGNORECASE,
)

_GENERIC_NEGATIVE_SENTIMENT_RE = re.compile(
    r"\b(?:disappointed|unhappy|frustrated|hate|regret|return(?:ed|ing)?|terrible|awful|"
    r"doesn'?t\s+work|does\s+not\s+work|not\s+worth)\b",
    flags=re.IGNORECASE,
)

_MISSING_COMPONENT_RE = re.compile(
    r"\b(?:missing|not\s+included|didn'?t\s+come\s+with|did\s+not\s+come\s+with|"
    r"left\s+out|wasn'?t\s+included|without\s+the|without\s+any)\b",
    flags=re.IGNORECASE,
)



def _supports_variety_label(label: str, evidence_text: str) -> bool:
    label_norm = _canon(label)
    ev_norm = _canon(evidence_text)
    if not label_norm or not ev_norm:
        return False
    if not re.search(r"\b(?:versatile|multiple|multi|variety|different)\b", label_norm):
        return False
    if not re.search(r"\b(?:made|make|making|cook(?:ed|ing)?|bake(?:d|ing)?|fry(?:ing)?|use(?:d|ing)?|works?\s+for|great\s+for|good\s+for)\b", ev_norm):
        return False
    separators = ev_norm.count(",") + len(re.findall(r"\band\b", ev_norm))
    content_words = [w for w in re.findall(r"[a-z]+", ev_norm) if len(w) > 3 and w not in STOPWORDS]
    return separators >= 2 and len(set(content_words)) >= 4



def evidence_supports_label(
    label: str,
    evidence_text: str,
    *,
    concept: Optional[str] = None,
    side: str = "delighter",
    aliases: Optional[Sequence[str]] = None,
) -> bool:
    """Return whether a piece of evidence genuinely supports a label.

    This is intentionally stricter than plain verbatim matching. A quoted span
    from the review only counts when it is semantically related to the label,
    its aliases, or the label's concept keywords. This blocks the common
    hallucination failure mode where the model picks a label and then attaches
    an unrelated sentence just because it is verbatim.
    """
    cleaned_label = _canonicalize_tag_label(label)
    evidence_raw = re.sub(r"\s+", " ", str(evidence_text or "").strip())
    if not cleaned_label or not evidence_raw:
        return False

    alias_vals = [_canonicalize_tag_label(a) for a in (aliases or []) if _canonicalize_tag_label(a)]
    inferred_concept = concept or _concept_lib.infer(cleaned_label, alias_vals)
    evidence_tokens = set(_tokenize(evidence_raw))

    # 1) Direct label or alias phrase / token overlap.
    label_phrases = [cleaned_label] + alias_vals
    if any(_phrase_in_text(phrase, evidence_raw) for phrase in label_phrases if phrase):
        return True

    label_tokens: set[str] = set()
    for phrase in label_phrases:
        label_tokens.update(
            token
            for token in _effective_label_tokens(phrase, inferred_concept)
            if token and token not in GENERIC_LABEL_TOKENS
        )
    if label_tokens and (label_tokens & evidence_tokens):
        return True

    # 2) Concept-aware support: exact cue phrases or concept keywords.
    if inferred_concept:
        concept_keywords = _concept_lib.concept_keywords(inferred_concept)
        if any(_phrase_in_text(keyword, evidence_raw) for keyword in concept_keywords if keyword):
            return True
        keyword_tokens = {token for keyword in concept_keywords for token in _tokenize(keyword)}
        if keyword_tokens and (keyword_tokens & evidence_tokens):
            return True

        polarity = "positive" if side == "delighter" else "negative"
        concept_phrases = CONCEPT_SYNONYMS.get(inferred_concept, {}).get(polarity, ())
        for phrase in concept_phrases:
            matched = _phrase_in_text(phrase, evidence_raw)
            if matched and not NegationDetector.is_negated(phrase, evidence_raw):
                return True

        if inferred_concept == "overall_sentiment":
            if side == "delighter" and (_STRONG_POSITIVE_RE.search(evidence_raw) or _GENERIC_POSITIVE_SENTIMENT_RE.search(evidence_raw)):
                return True
            if side == "detractor" and (_STRONG_NEGATIVE_RE.search(evidence_raw) or _GENERIC_NEGATIVE_SENTIMENT_RE.search(evidence_raw)):
                return True

    # 3) Special-case labels that rely on structural evidence rather than token overlap.
    if re.search(r"\bmissing\b", cleaned_label.lower()) and _MISSING_COMPONENT_RE.search(evidence_raw):
        return True
    if _supports_variety_label(cleaned_label, evidence_raw):
        return True

    return False


# ---------------------------------------------------------------------------
# phrase-in-text (word-boundary aware)
# ---------------------------------------------------------------------------

def _phrase_in_text(phrase: str, review_text: str) -> Optional[str]:
    """Return matched substring if *phrase* appears as a whole token-sequence
    in *review_text*; ``None`` otherwise.  Uses word boundaries to prevent
    ``"noise issue"`` from matching ``"noise issues"``.
    """
    if not phrase or not review_text:
        return None
    phrase_text = str(phrase).strip()
    escaped = re.escape(phrase_text)
    escaped = escaped.replace("\\\\'", "['\u2018\u2019" + chr(96) + "]")
    escaped = escaped.replace("\\\\-", "[-\u2013\u2014]")
    prefix = r"\b" if phrase_text and phrase_text[0].isalnum() else ""
    suffix = r"\b" if phrase_text and phrase_text[-1].isalnum() else ""
    match = re.search(prefix + escaped + suffix, review_text, re.IGNORECASE)
    if match:
        return match.group(0)
    pn = _canon(phrase_text)
    if pn:
        pp = r"\b" if pn[0].isalnum() else ""
        ps = r"\b" if pn[-1].isalnum() else ""
        if re.search(pp + re.escape(pn) + ps, _canon(review_text), re.IGNORECASE):
            return phrase_text
    return None


# ---------------------------------------------------------------------------
# FragmentScorer — scores a single text fragment for a label
# ---------------------------------------------------------------------------

class FragmentScorer:
    """Scores one text fragment against a label and returns a ScoredTag.

    All scoring parameters come from TaggerConfig.  This class is
    stateless per fragment — instantiate once per TaggerConfig, call
    many times.
    """

    def __init__(self, cfg: TaggerConfig) -> None:
        self.cfg = cfg

    def score(
        self,
        fragment: str,
        *,
        cues: Sequence[str],
        label_tokens: Sequence[str],
        concept: Optional[str],
        side: str,
        provided_evidence: Sequence[str] = (),
        label: str = "",
        aliases: Sequence[str] = (),
    ) -> ScoredTag:
        cfg = self.cfg
        fragment_tokens = set(_tokenize(fragment))
        label_tokens_set = set(label_tokens)

        # ── Evidence validation ────────────────────────────────────────────
        valid_evidence: List[TagEvidence] = []
        cleaned_label = _canonicalize_tag_label(label or (cues[0] if cues else ""))
        alias_vals = [_canonicalize_tag_label(a) for a in (aliases or []) if _canonicalize_tag_label(a)]
        for ev_text in provided_evidence:
            matched = _phrase_in_text(str(ev_text).strip(), fragment)
            if not matched:
                continue
            if evidence_supports_label(
                cleaned_label,
                matched,
                concept=concept,
                side=side,
                aliases=alias_vals,
            ):
                valid_evidence.append(TagEvidence(text=matched, source_cue="provided", confidence=1.0))

        # ── Cue matching ───────────────────────────────────────────────────
        cue_hits: List[str] = []
        negated_hits: List[str] = []
        cue_snippets: List[TagEvidence] = []
        for cue in cues:
            matched = _phrase_in_text(cue, fragment)
            if not matched:
                continue
            if NegationDetector.is_negated(cue, fragment):
                negated_hits.append(cue)
            else:
                cue_hits.append(cue)
                cue_snippets.append(TagEvidence(text=matched, source_cue=cue, confidence=0.9))

        # ── Token coverage ─────────────────────────────────────────────────
        coverage = 0.0
        if label_tokens_set:
            overlap = len(label_tokens_set & fragment_tokens)
            coverage = overlap / float(max(len(label_tokens_set), 1))

        # ── Base score ─────────────────────────────────────────────────────
        score = 0.0
        if valid_evidence:
            score += cfg.evidence_boost + min(cfg.evidence_cap, cfg.evidence_extra_per_item * len(valid_evidence))
        if cue_hits:
            score += cfg.cue_hit_boost + min(cfg.cue_hit_cap, cfg.cue_hit_extra_per_item * len(cue_hits))
        if coverage >= 0.5:
            score += cfg.coverage_half_boost
        if coverage >= 0.8:
            score += cfg.coverage_full_boost

        # ── Opposite-polarity detection ────────────────────────────────────
        opposite_hits: List[str] = []
        negated_opposite_hits: List[str] = []
        for phrase in _concept_lib.opposite_phrases(concept, side):
            is_neg = NegationDetector.is_negated(phrase, fragment)
            matched = _phrase_in_text(phrase, fragment)
            if matched and not is_neg:
                opposite_hits.append(_canon(matched))
            elif is_neg:
                negated_opposite_hits.append(phrase)

        if opposite_hits:
            score -= cfg.opposite_penalty_with_evidence if valid_evidence else cfg.opposite_penalty_without_evidence
        if negated_opposite_hits:
            boost = cfg.negated_opposite_boost_with_cues if cue_hits else cfg.negated_opposite_boost
            score += boost
            for phrase in negated_opposite_hits:
                m = _phrase_in_text(phrase, fragment)
                if m:
                    cue_snippets.append(TagEvidence(text=m, source_cue=f"neg:{phrase}", confidence=0.8))

        if negated_hits:
            score -= cfg.negation_penalty_with_cues if cue_hits else cfg.negation_penalty_without_cues

        # ── Concept-specific pattern overrides ─────────────────────────────
        overrides = _get_concept_overrides()
        if concept and concept in overrides:
            for pattern, delta in overrides[concept].get(side, []):
                if pattern.search(fragment):
                    score += delta
                    if delta > 0:
                        snippet = _first_review_snippet(fragment) or fragment[:120]
                        if snippet:
                            cue_snippets.append(TagEvidence(text=snippet, source_cue="pattern_override", confidence=0.85))

        # ── Sentiment intensifier boost ─────────────────────────────────────
        is_overall = (concept == "overall_sentiment")
        if is_overall or score > 0 or coverage >= 0.3 or cue_hits or valid_evidence:
            if side == "delighter" and _STRONG_POSITIVE_RE.search(fragment):
                score += cfg.intensifier_boost if is_overall else cfg.intensifier_boost_specific
                kws = _concept_lib.concept_keywords(concept)
                if any(kw and kw in fragment.lower() for kw in kws):
                    score += cfg.concept_keyword_extra_boost
            elif side == "detractor" and _STRONG_NEGATIVE_RE.search(fragment):
                score += cfg.intensifier_boost if is_overall else cfg.intensifier_boost_specific
                kws = _concept_lib.concept_keywords(concept)
                if any(kw and kw in fragment.lower() for kw in kws):
                    score += cfg.concept_keyword_extra_boost

        # ── Fragment length bonus ───────────────────────────────────────────
        if cue_hits or valid_evidence:
            wc = len(fragment.split())
            if wc >= 30:
                score += cfg.fragment_long_boost
            elif wc >= 15:
                score += cfg.fragment_medium_boost

        all_evidence = valid_evidence + cue_snippets
        return ScoredTag(
            label="",  # filled in by caller
            concept=concept,
            score=round(score, 4),
            evidence=_dedupe_evidence(all_evidence),
            cue_hits=_dedupe_keep_order(cue_hits),
            opposite_hits=_dedupe_keep_order(opposite_hits + negated_opposite_hits),
            coverage=coverage,
            has_support=False,  # evaluated by TagScorer
        )


def _dedupe_evidence(items: List[TagEvidence]) -> List[TagEvidence]:
    seen: set[str] = set()
    out: List[TagEvidence] = []

    def _norm(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip().lower())

    def _score(item: TagEvidence) -> tuple[int, int, float]:
        text = _norm(item.text)
        return (len(text), len(text.split()), float(getattr(item, "confidence", 0.0) or 0.0))

    ranked = sorted(list(items or []), key=_score, reverse=True)
    for item in ranked:
        key = _norm(item.text)
        if not key or key in seen:
            continue
        # Suppress shorter snippets when a longer kept snippet already covers them.
        if any(key in _norm(kept.text) for kept in out):
            continue
        out.append(item)
        seen.add(key)
        if len(out) >= 2:
            break
    return out


def _split_fragments(review_text: str) -> List[str]:
    raw = re.sub(r"\s+", " ", str(review_text or "").strip())
    if not raw:
        return []
    sentences = [p.strip(" ,;") for p in re.split(r"(?<=[.!?])\s+|\n+", raw) if p.strip()]
    _CONTRAST_RE = re.compile(
        r"\s+(?:but|however|although|though|even\s+though|except|while|yet|whereas|"
        r"despite|on\s+the\s+other\s+hand|that\s+said|then\s+again|after\s+all|"
        r"having\s+said\s+that|with\s+that\s+said)\s+|[;•|]|\u2014|\u2013",
        flags=re.IGNORECASE,
    )
    fragments: List[str] = []
    for sentence in sentences or [raw]:
        for piece in _CONTRAST_RE.split(sentence):
            frag = piece.strip(" ,;")
            if len(frag) >= 8:
                fragments.append(frag)
    return _dedupe_keep_order(fragments)


# ---------------------------------------------------------------------------
# TagScorer — scores a label against the full review using best-fragment logic
# ---------------------------------------------------------------------------

class TagScorer:
    """Scores one label against the full review text.

    Uses FragmentScorer across all fragments and selects the best score,
    then applies star-rating signal and has_support logic.
    """

    def __init__(self, cfg: TaggerConfig) -> None:
        self.cfg = cfg
        self._fragment_scorer = FragmentScorer(cfg)

    def score(
        self,
        label: str,
        review_text: str,
        side: str,
        aliases: Optional[Sequence[str]] = None,
        provided_evidence: Optional[Sequence[str]] = None,
        rating: Any = None,
    ) -> ScoredTag:
        cfg = self.cfg
        cues, concept = _concept_lib.cues_for(label, side, aliases)
        review_text = str(review_text or "")
        review_norm = _canon(review_text)
        label_tokens = list(set(_effective_label_tokens(label, concept)))

        # Score full review and each fragment; take the best
        candidates: List[ScoredTag] = []
        for frag in [review_text] + _split_fragments(review_text):
            if _canon(frag) == review_norm and frag != review_text:
                continue
            scored = self._fragment_scorer.score(
                frag, cues=cues, label_tokens=label_tokens,
                concept=concept, side=side,
                provided_evidence=provided_evidence or [],
                label=label,
                aliases=aliases or [],
            )
            candidates.append(scored)

        best = max(
            candidates,
            key=lambda t: (t.score, t.coverage, len(t.evidence), len(t.cue_hits)),
            default=ScoredTag(label=label, concept=concept, score=0.0),
        )

        # Apply star-rating signal
        rating_value = _coerce_rating(rating)
        rating_signal = 0.0
        if rating_value is not None:
            if concept == "overall_sentiment":
                if side == "delighter" and rating_value > 3.0:
                    rating_signal = round(min(cfg.star_rating_overall_max, (rating_value - 3.0) / 2.0 * cfg.star_rating_overall_max), 3)
                elif side == "detractor" and rating_value < 3.0:
                    rating_signal = round(min(cfg.star_rating_overall_max, (3.0 - rating_value) / 2.0 * cfg.star_rating_overall_max), 3)
                best.score += rating_signal
            elif best.score > 0:
                if side == "delighter" and rating_value >= 4.5:
                    best.score += round((rating_value - 4.0) / 1.0 * cfg.star_rating_specific_max, 3)
                elif side == "detractor" and rating_value <= 1.5:
                    best.score += round((2.0 - rating_value) / 1.0 * cfg.star_rating_specific_max, 3)

        # Compute has_support
        has_support = bool(
            best.evidence
            or best.cue_hits
            or (best.coverage >= 0.5 and best.score >= cfg.has_support_coverage_score_threshold)
            or (best.coverage >= cfg.has_support_hard_coverage)
        ) and best.score > cfg.has_support_score_floor

        # overall_sentiment: strong emotional language alone is enough
        if concept == "overall_sentiment" and best.score > 0.4 and best.score > cfg.has_support_score_floor:
            if side == "delighter" and _STRONG_POSITIVE_RE.search(review_text):
                has_support = True
                if rating_signal > 0 and not best.evidence:
                    fallback = _first_review_snippet(review_text)
                    if fallback:
                        best.evidence = [TagEvidence(text=fallback, source_cue="sentiment", confidence=0.7)]
            elif side == "detractor" and _STRONG_NEGATIVE_RE.search(review_text):
                has_support = True

        best.label = _clean_tag(label)
        best.has_support = has_support
        best.rating_signal = rating_signal
        return best


# ---------------------------------------------------------------------------
# TagRefiner — orchestrates the full refinement pipeline
# ---------------------------------------------------------------------------

class TagRefiner:
    """Orchestrates the evidence-first refinement pipeline.

    Pipeline:
      1. Score every candidate label against the review.
      2. Decide which AI-selected labels to keep (keep_threshold or has_support).
      3. Opportunistically add unselected labels that score above add_threshold.
      4. Run concept-level conflict resolution (detractor vs delighter).
      5. Dedup same-side concept duplicates (keep best-supported label).
      6. Strip overall fallback when specific labels are present.
    """

    def __init__(self, cfg: Optional[TaggerConfig] = None) -> None:
        self.cfg = cfg or TaggerConfig()
        self._scorer = TagScorer(self.cfg)

    def refine(
        self,
        review_text: str,
        detractors: Sequence[str],
        delighters: Sequence[str],
        *,
        allowed_detractors: Optional[Sequence[str]] = None,
        allowed_delighters: Optional[Sequence[str]] = None,
        evidence_det: Optional[Mapping[str, Sequence[str]]] = None,
        evidence_del: Optional[Mapping[str, Sequence[str]]] = None,
        aliases: Optional[Mapping[str, Sequence[str]]] = None,
        max_per_side: Optional[int] = None,
        include_universal_neutral: bool = True,
        rating: Any = None,
        extra_universal_detractors: Optional[Sequence[str]] = None,
        extra_universal_delighters: Optional[Sequence[str]] = None,
    ) -> RefinementResult:
        cfg = self.cfg
        max_per = max_per_side if max_per_side is not None else cfg.max_per_side
        selected_dets = normalize_tag_list(detractors or [])
        selected_dels = normalize_tag_list(delighters or [])

        if include_universal_neutral:
            allowed_dets, allowed_dels = ensure_universal_taxonomy(
                allowed_detractors or selected_dets,
                allowed_delighters or selected_dels,
                include_universal_neutral=True,
                extra_universal_detractors=extra_universal_detractors,
                extra_universal_delighters=extra_universal_delighters,
            )
        else:
            allowed_dets = normalize_tag_list(allowed_detractors or selected_dets)
            allowed_dels = normalize_tag_list(allowed_delighters or selected_dels)

        alias_map = {str(k).title(): list(v or []) for k, v in (aliases or {}).items()}
        small_catalog = (len(allowed_dets) + len(allowed_dels)) <= cfg.small_catalog_size
        base_keep = cfg.base_keep_threshold_small_catalog if small_catalog else cfg.base_keep_threshold
        base_add  = cfg.base_add_threshold_small_catalog  if small_catalog else cfg.base_add_threshold

        # Score every candidate
        scored_dets: Dict[str, ScoredTag] = {}
        scored_dels: Dict[str, ScoredTag] = {}
        for label in allowed_dets:
            scored_dets[label] = self._scorer.score(
                label, review_text, "detractor",
                aliases=alias_map.get(label),
                provided_evidence=(evidence_det or {}).get(label) or (),
                rating=rating,
            )
            scored_dets[label].label = label
        for label in allowed_dels:
            scored_dels[label] = self._scorer.score(
                label, review_text, "delighter",
                aliases=alias_map.get(label),
                provided_evidence=(evidence_del or {}).get(label) or (),
                rating=rating,
            )
            scored_dels[label].label = label

        # Per-label adaptive thresholds
        def _keep_thresh(tag: ScoredTag) -> float:
            thresh = base_keep
            if tag.evidence:
                thresh -= 0.20
            if tag.coverage >= 0.4:
                thresh -= 0.10
            if not tag.concept:
                thresh += 0.15
            return max(thresh, 0.70)

        def _add_thresh(tag: ScoredTag) -> float:
            thresh = base_add
            if tag.evidence:
                thresh -= 0.25
            if not tag.concept:
                thresh += 0.25
            return max(thresh, 1.50)

        # Keep AI-selected labels that pass threshold or have_support
        refined_dets = [
            label for label in selected_dets
            if scored_dets.get(label, ScoredTag("", None, 0)).score >= _keep_thresh(scored_dets.get(label, ScoredTag("", None, 0)))
            or scored_dets.get(label, ScoredTag("", None, 0)).has_support
        ]
        refined_dels = [
            label for label in selected_dels
            if scored_dels.get(label, ScoredTag("", None, 0)).score >= _keep_thresh(scored_dels.get(label, ScoredTag("", None, 0)))
            or scored_dels.get(label, ScoredTag("", None, 0)).has_support
        ]

        # Add unsourced labels with strong scores
        for label in allowed_dets:
            if label not in refined_dets and scored_dets[label].score >= _add_thresh(scored_dets[label]):
                refined_dets.append(label)
        for label in allowed_dels:
            if label not in refined_dels and scored_dels[label].score >= _add_thresh(scored_dels[label]):
                refined_dels.append(label)

        # overall_sentiment rescue pass
        for label in allowed_dets:
            tag = scored_dets.get(label, ScoredTag("", None, 0))
            if label not in refined_dets and tag.concept == "overall_sentiment":
                if tag.score >= max(2.0, base_keep) and (tag.cue_hits or tag.coverage >= 0.5 or tag.rating_signal > 0):
                    refined_dets.append(label)
        for label in allowed_dels:
            tag = scored_dels.get(label, ScoredTag("", None, 0))
            if label not in refined_dels and tag.concept == "overall_sentiment":
                if tag.score >= max(2.0, base_keep) and (tag.cue_hits or tag.coverage >= 0.5 or tag.rating_signal > 0):
                    refined_dels.append(label)

        # Concept-level cross-side conflict resolution
        refined_dets, refined_dels = self._resolve_cross_side_conflicts(
            refined_dets, refined_dels, scored_dets, scored_dels, review_text
        )

        # Same-side concept deduplication
        refined_dets = self._dedup_same_concept(refined_dets, scored_dets)
        refined_dels = self._dedup_same_concept(refined_dels, scored_dels)

        # Normalise + strip fallback
        refined_dets = normalize_tag_list(refined_dets)
        refined_dels = normalize_tag_list(refined_dels)
        refined_dets, refined_dels = _strip_overall_fallback_when_specific_present(refined_dets, refined_dels)

        refined_dets = refined_dets[:max_per]
        refined_dels = refined_dels[:max_per]

        if not include_universal_neutral:
            refined_dets, refined_dels = strip_universal_neutral_tags(
                refined_dets, refined_dels,
                extra_universal_detractors=extra_universal_detractors,
                extra_universal_delighters=extra_universal_delighters,
            )
            refined_dets = refined_dets[:max_per]
            refined_dels = refined_dels[:max_per]

        # Build evidence maps
        ev_det_out: Dict[str, List[str]] = {
            label: [e.text for e in scored_dets[label].evidence[:2]]
            for label in refined_dets if scored_dets.get(label, ScoredTag("", None, 0)).evidence
        }
        ev_del_out: Dict[str, List[str]] = {
            label: [e.text for e in scored_dels[label].evidence[:2]]
            for label in refined_dels if scored_dels.get(label, ScoredTag("", None, 0)).evidence
        }

        return RefinementResult(
            dets=refined_dets, dels=refined_dels,
            ev_det=ev_det_out, ev_del=ev_del_out,
            scored_dets=scored_dets, scored_dels=scored_dels,
            added_dets=[l for l in refined_dets if l not in selected_dets],
            added_dels=[l for l in refined_dels if l not in selected_dels],
            removed_dets=[l for l in selected_dets if l not in refined_dets],
            removed_dels=[l for l in selected_dels if l not in refined_dels],
        )

    def _resolve_cross_side_conflicts(
        self,
        dets: List[str], dels: List[str],
        scored_dets: Dict[str, ScoredTag], scored_dels: Dict[str, ScoredTag],
        review_text: str,
    ) -> Tuple[List[str], List[str]]:
        det_by_concept: Dict[str, List[str]] = {}
        del_by_concept: Dict[str, List[str]] = {}
        for label in dets:
            c = scored_dets.get(label, ScoredTag("", None, 0)).concept
            if c:
                det_by_concept.setdefault(c, []).append(label)
        for label in dels:
            c = scored_dels.get(label, ScoredTag("", None, 0)).concept
            if c:
                del_by_concept.setdefault(c, []).append(label)

        for concept in sorted(set(det_by_concept) & set(del_by_concept)):
            d_labels = det_by_concept[concept]
            l_labels = del_by_concept[concept]
            best_det = max(d_labels, key=lambda l: scored_dets[l].score)
            best_del = max(l_labels, key=lambda l: scored_dels[l].score)
            d_tag = scored_dets[best_det]
            l_tag = scored_dels[best_del]

            # Allow both if snippets differ or review has contrast markers
            if self._conflict_allowed(d_tag, l_tag, review_text):
                continue
            if d_tag.score >= l_tag.score + 0.75:
                dels = [l for l in dels if scored_dels.get(l, ScoredTag("", None, 0)).concept != concept]
            elif l_tag.score >= d_tag.score + 0.75:
                dets = [l for l in dets if scored_dets.get(l, ScoredTag("", None, 0)).concept != concept]
            else:
                # Tie: keep the more evidence-rich side
                det_spec = len(d_tag.evidence) + len(d_tag.cue_hits)
                del_spec = len(l_tag.evidence) + len(l_tag.cue_hits)
                if det_spec >= del_spec:
                    dels = [l for l in dels if scored_dels.get(l, ScoredTag("", None, 0)).concept != concept]
                else:
                    dets = [l for l in dets if scored_dets.get(l, ScoredTag("", None, 0)).concept != concept]

        return dets, dels

    @staticmethod
    def _conflict_allowed(det_tag: ScoredTag, del_tag: ScoredTag, review_text: str) -> bool:
        det_snips = {e.text.lower() for e in det_tag.evidence}
        del_snips = {e.text.lower() for e in del_tag.evidence}
        if det_snips and del_snips and det_snips != del_snips:
            return True
        _CONTRAST_MARKERS = (
            " but ", " however ", " although ", " though ", " even though ",
            " while ", " whereas ", " yet ", " despite ", " except ",
            " on the other hand ", " on low ", " on high ",
            " at low speed ", " at high speed ", " used to ", " at first ",
            " after a while ", " over time ",
        )
        rn = _canon(review_text)
        return any(m in rn for m in _CONTRAST_MARKERS)

    @staticmethod
    def _dedup_same_concept(labels: List[str], scored: Dict[str, ScoredTag]) -> List[str]:
        """Keep both labels when both are well-supported; drop weaker only when
        the stronger clearly dominates and the weaker has no evidence."""
        concept_best: Dict[str, str] = {}
        keep_both: set = set()
        for label in labels:
            tag = scored.get(label, ScoredTag("", None, 0))
            concept = tag.concept
            if not concept or concept == "overall_sentiment":
                keep_both.add(label)
                continue
            current = concept_best.get(concept)
            if current is None:
                concept_best[concept] = label
            else:
                s_new = scored.get(label, ScoredTag("", None, 0)).score
                s_cur = scored.get(current, ScoredTag("", None, 0)).score
                hs_new = scored.get(label, ScoredTag("", None, 0)).has_support
                hs_cur = scored.get(current, ScoredTag("", None, 0)).has_support
                if hs_new and hs_cur:
                    keep_both.add(label)
                    keep_both.add(current)
                    del concept_best[concept]
                elif s_new > s_cur + 1.5 and not hs_cur:
                    concept_best[concept] = label
        kept = set(concept_best.values()) | keep_both
        return [l for l in labels if l in kept]


# ---------------------------------------------------------------------------
# Tag list utilities (used by TagRefiner and public API)
# ---------------------------------------------------------------------------

def normalize_tag_list(values: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen: set = set()
    for v in values or []:
        cleaned = _canonicalize_tag_label(v)
        if not cleaned or cleaned.lower() in NON_VALUES:
            continue
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _universal_label_set(
    *,
    extra_universal_detractors: Optional[Sequence[Any]] = None,
    extra_universal_delighters: Optional[Sequence[Any]] = None,
) -> set:
    return set(normalize_tag_list(
        list(UNIVERSAL_NEUTRAL_DETRACTORS)
        + list(UNIVERSAL_NEUTRAL_DELIGHTERS)
        + list(extra_universal_detractors or [])
        + list(extra_universal_delighters or [])
    ))


def is_universal_neutral_label(
    label: Any,
    *,
    extra_universal_detractors: Optional[Sequence[Any]] = None,
    extra_universal_delighters: Optional[Sequence[Any]] = None,
) -> bool:
    return _clean_tag(label) in _universal_label_set(
        extra_universal_detractors=extra_universal_detractors,
        extra_universal_delighters=extra_universal_delighters,
    )


def ensure_universal_taxonomy(
    detractors: Optional[Sequence[Any]] = None,
    delighters: Optional[Sequence[Any]] = None,
    *,
    include_universal_neutral: bool = True,
    extra_universal_detractors: Optional[Sequence[Any]] = None,
    extra_universal_delighters: Optional[Sequence[Any]] = None,
) -> Tuple[List[str], List[str]]:
    dets = list(detractors or [])
    dels = list(delighters or [])
    if include_universal_neutral:
        dets = list(UNIVERSAL_NEUTRAL_DETRACTORS) + list(extra_universal_detractors or []) + dets
        dels = list(UNIVERSAL_NEUTRAL_DELIGHTERS) + list(extra_universal_delighters or []) + dels
    return normalize_tag_list(dets), normalize_tag_list(dels)


def strip_universal_neutral_tags(
    detractors: Optional[Sequence[Any]] = None,
    delighters: Optional[Sequence[Any]] = None,
    *,
    extra_universal_detractors: Optional[Sequence[Any]] = None,
    extra_universal_delighters: Optional[Sequence[Any]] = None,
) -> Tuple[List[str], List[str]]:
    universal = _universal_label_set(
        extra_universal_detractors=extra_universal_detractors,
        extra_universal_delighters=extra_universal_delighters,
    )
    return (
        [l for l in normalize_tag_list(detractors or []) if l not in universal],
        [l for l in normalize_tag_list(delighters or []) if l not in universal],
    )


def _strip_overall_fallback_when_specific_present(
    detractors: Optional[Sequence[Any]] = None,
    delighters: Optional[Sequence[Any]] = None,
) -> Tuple[List[str], List[str]]:
    dets = normalize_tag_list(detractors or [])
    dels = normalize_tag_list(delighters or [])
    if any(l != UNIVERSAL_DETRACTOR for l in dets):
        dets = [l for l in dets if l != UNIVERSAL_DETRACTOR]
    if any(l != UNIVERSAL_DELIGHTER for l in dels):
        dels = [l for l in dels if l != UNIVERSAL_DELIGHTER]
    return dets, dels


# ---------------------------------------------------------------------------
# Public API — backward-compatible wrappers
# ---------------------------------------------------------------------------

_default_refiner = TagRefiner()


def refine_tag_assignment(
    review_text: str,
    detractors: Optional[Sequence[str]],
    delighters: Optional[Sequence[str]],
    *,
    allowed_detractors: Optional[Sequence[str]] = None,
    allowed_delighters: Optional[Sequence[str]] = None,
    evidence_det: Optional[Mapping[str, Sequence[str]]] = None,
    evidence_del: Optional[Mapping[str, Sequence[str]]] = None,
    aliases: Optional[Mapping[str, Sequence[str]]] = None,
    max_per_side: int = 10,
    include_universal_neutral: bool = True,
    rating: Any = None,
    extra_universal_detractors: Optional[Sequence[str]] = None,
    extra_universal_delighters: Optional[Sequence[str]] = None,
    config: Optional[TaggerConfig] = None,
) -> Dict[str, Any]:
    """Backward-compatible wrapper returning a plain dict (v1 shape)."""
    refiner = TagRefiner(config) if config else _default_refiner
    result = refiner.refine(
        review_text, detractors or [], delighters or [],
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        evidence_det=evidence_det,
        evidence_del=evidence_del,
        aliases=aliases,
        max_per_side=max_per_side,
        include_universal_neutral=include_universal_neutral,
        rating=rating,
        extra_universal_detractors=extra_universal_detractors,
        extra_universal_delighters=extra_universal_delighters,
    )
    return result.as_dict()


# Legacy name aliases kept for call-site compatibility
def build_label_cues(label: str, side: str, aliases: Optional[Sequence[str]] = None) -> Tuple[List[str], Optional[str]]:
    return _concept_lib.cues_for(label, side, aliases)


def _support_details(
    label: str,
    review_text: str,
    side: str,
    aliases: Optional[Sequence[str]] = None,
    evidence: Optional[Sequence[str]] = None,
    *,
    rating: Any = None,
) -> Dict[str, Any]:
    """Legacy dict-returning wrapper used by existing call sites."""
    tag = _default_refiner._scorer.score(
        label, review_text, side,
        aliases=aliases, provided_evidence=evidence or (), rating=rating,
    )
    return {
        "label": tag.label or _clean_tag(label),
        "concept": tag.concept,
        "score": round(tag.score, 4),
        "cue_hits": tag.cue_hits,
        "opposite_hits": tag.opposite_hits,
        "evidence": [e.text for e in tag.evidence[:2]],
        "snippets": [e.text for e in tag.evidence[:2]],
        "coverage": tag.coverage,
        "has_support": tag.has_support,
        "review_norm": _canon(review_text),
        "rating_signal": tag.rating_signal,
    }


def compute_tag_edit_accuracy(
    baseline_map: Mapping[Any, Mapping[str, Sequence[str]]],
    current_map: Mapping[Any, Mapping[str, Sequence[str]]],
) -> Dict[str, Any]:
    def _norm(m: Mapping[Any, Mapping[str, Sequence[str]]]) -> Dict[str, Dict[str, set]]:
        out: Dict[str, Dict[str, set]] = {}
        for k, v in (m or {}).items():
            rk = str(k)
            out[rk] = {
                "detractors": set(normalize_tag_list((v or {}).get("detractors") or [])),
                "delighters": set(normalize_tag_list((v or {}).get("delighters") or [])),
            }
        return out

    baseline = _norm(baseline_map)
    current  = _norm(current_map)
    all_keys = sorted(set(baseline) | set(current))
    baseline_total_tags = added_tags = removed_tags = changed_reviews = 0
    for key in all_keys:
        bd = (baseline.get(key) or {}).get("detractors", set())
        bl = (baseline.get(key) or {}).get("delighters", set())
        cd = (current.get(key) or {}).get("detractors", set())
        cl = (current.get(key) or {}).get("delighters", set())
        baseline_total_tags += len(bd) + len(bl)
        added   = len(cd - bd) + len(cl - bl)
        removed = len(bd - cd) + len(bl - cl)
        added_tags   += added
        removed_tags += removed
        if added or removed:
            changed_reviews += 1
    total_changes = added_tags + removed_tags
    if baseline_total_tags <= 0:
        accuracy_pct = 100.0 if total_changes == 0 else 0.0
    else:
        accuracy_pct = max(0.0, 100.0 * (1.0 - total_changes / float(baseline_total_tags)))
    return {
        "baseline_total_tags": baseline_total_tags,
        "added_tags": added_tags,
        "removed_tags": removed_tags,
        "total_changes": total_changes,
        "changed_reviews": changed_reviews,
        "accuracy_pct": round(accuracy_pct, 1),
    }
