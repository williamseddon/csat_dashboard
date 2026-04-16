from __future__ import annotations
from dataclasses import dataclass, field

import difflib
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

NON_VALUES = {"", "na", "n/a", "none", "null", "nan", "<na>", "not mentioned", "unknown"}
STOPWORDS = {
    "a", "an", "and", "the", "to", "for", "of", "in", "on", "with", "very", "really",
    "product", "item", "thing", "feature", "features", "issue", "issues", "problem", "problems",
}


def _canon(text: Any) -> str:
    value = str(text or "").strip().lower()
    value = value.replace("’", "'").replace("`", "'").replace("‘", "'")
    value = value.replace("“", '"').replace("”", '"')
    value = re.sub(r"[‐-―]", "-", value)
    return re.sub(r"\s+", " ", value)


def _clean_label(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip().replace("&", " And "))
    value = re.sub(r"\s+", " ", value).strip(" ,;|/")
    if not value or _canon(value) in NON_VALUES:
        return ""
    return value.title()


def _stem(token: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "", token.lower())
    if not token:
        return ""
    for suffix in ("ing", "ers", "ier", "est", "er", "ies", "ied", "ed", "es", "s", "ly"):
        if len(token) > 4 and token.endswith(suffix):
            token = token[: -len(suffix)]
            break
    return token


def _tokenize(text: Any) -> List[str]:
    tokens = [_stem(match) for match in re.findall(r"[a-z0-9']+", _canon(text))]
    return [tok for tok in tokens if tok and tok not in STOPWORDS and len(tok) > 1]


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        clean = _clean_label(value)
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


SYSTEMATIC_RULES: List[Dict[str, Any]] = [
    {"side": "delighter", "canonical": "Overall Satisfaction", "aliases": ["happy", "satisfied", "love it", "great overall", "excellent overall", "would recommend", "met expectations", "works great", "great experience"], "family": "Overall Sentiment"},
    {"side": "detractor", "canonical": "Overall Dissatisfaction", "aliases": ["disappointed", "unhappy", "hate it", "bad overall", "would not recommend", "fell short", "regret buying", "not satisfied"], "family": "Overall Sentiment"},
    {"side": "delighter", "canonical": "Good Value", "aliases": ["great value", "worth it", "worth the price", "value for money", "good for the price", "good price"], "family": "Value"},
    {"side": "detractor", "canonical": "Overpriced", "aliases": ["too expensive", "pricey", "cost too much", "not worth it", "over priced"], "family": "Value"},
    {"side": "delighter", "canonical": "Performs Well", "aliases": ["works well", "works great", "effective", "does the job", "strong performance", "great results", "good results"], "family": "Performance"},
    {"side": "detractor", "canonical": "Poor Performance", "aliases": ["does not work well", "doesn't work well", "weak performance", "ineffective", "poor results", "bad results", "underpowered"], "family": "Performance"},
    {"side": "delighter", "canonical": "High Quality", "aliases": ["good quality", "well made", "premium quality", "solid build", "sturdy build", "durable build"], "family": "Quality"},
    {"side": "detractor", "canonical": "Poor Quality", "aliases": ["low quality", "bad quality", "feels cheap", "cheaply made", "flimsy", "poor build quality"], "family": "Quality"},
    {"side": "delighter", "canonical": "Easy To Use", "aliases": ["simple to use", "user friendly", "easy use", "easy operation", "intuitive", "straightforward"], "family": "Usability"},
    {"side": "detractor", "canonical": "Difficult To Use", "aliases": ["hard to use", "complicated", "confusing", "not intuitive", "awkward to use"], "family": "Usability"},
    {"side": "delighter", "canonical": "Reliable", "aliases": ["dependable", "consistent", "holds up", "durable", "trustworthy"], "family": "Reliability"},
    {"side": "detractor", "canonical": "Unreliable", "aliases": ["inconsistent", "breaks easily", "stopped working", "defective", "doesn't last"], "family": "Reliability"},
    {"side": "delighter", "canonical": "Easy To Clean", "aliases": ["easy cleanup", "easy clean", "quick cleanup", "simple to clean"], "family": "Cleaning"},
    {"side": "detractor", "canonical": "Hard To Clean", "aliases": ["difficult to clean", "messy", "cleanup is hard", "cleanup takes forever", "hard cleanup"], "family": "Cleaning"},
    {"side": "delighter", "canonical": "Saves Time", "aliases": ["time saver", "quick", "fast", "quickly", "faster than expected"], "family": "Time Efficiency"},
    {"side": "detractor", "canonical": "Time Consuming", "aliases": ["takes forever", "slow", "too slow", "slower than expected"], "family": "Time Efficiency"},
    {"side": "delighter", "canonical": "Quiet", "aliases": ["quiet operation", "silent", "low noise", "not loud", "not noisy", "runs quietly"], "family": "Noise"},
    {"side": "detractor", "canonical": "Loud", "aliases": ["noisy", "too loud", "too noisy", "noise issue", "noise level", "loud operation"], "family": "Noise"},
    {"side": "delighter", "canonical": "Easy Setup", "aliases": ["easy to set up", "easy install", "easy installation", "easy to assemble", "quick setup"], "family": "Setup"},
    {"side": "detractor", "canonical": "Difficult Setup", "aliases": ["hard to set up", "difficult install", "hard installation", "difficult assembly", "assembly is hard", "setup issues"], "family": "Setup"},
    {"side": "delighter", "canonical": "Right Size", "aliases": ["good size", "fits well", "perfect size", "compact size", "just the right size"], "family": "Size / Fit"},
    {"side": "detractor", "canonical": "Wrong Size", "aliases": ["too big", "too small", "does not fit", "takes up too much space", "bulky", "heavy", "size issue"], "family": "Size / Fit"},
    {"side": "delighter", "canonical": "Attractive Design", "aliases": ["looks great", "nice design", "beautiful", "stylish", "well designed", "good looking"], "family": "Design"},
    {"side": "detractor", "canonical": "Poor Design", "aliases": ["ugly", "bad design", "poorly designed", "awkward design", "cheap looking"], "family": "Design"},
    {"side": "delighter", "canonical": "Comfortable", "aliases": ["very comfortable", "comfortable fit", "feels good", "comfortable to wear", "comfortable to use"], "family": "Comfort"},
    {"side": "detractor", "canonical": "Uncomfortable", "aliases": ["not comfortable", "hurts", "painful", "irritating to wear", "uncomfortable fit"], "family": "Comfort"},
    {"side": "delighter", "canonical": "Clear Instructions", "aliases": ["easy to follow instructions", "clear directions", "helpful instructions", "good instructions"], "family": "Instructions"},
    {"side": "detractor", "canonical": "Instructions Unclear", "aliases": ["unclear instructions", "poor instructions", "bad directions", "hard to follow instructions", "instructions confusing"], "family": "Instructions"},
    {"side": "delighter", "canonical": "Good Packaging", "aliases": ["well packaged", "arrived well packed", "secure packaging"], "family": "Packaging"},
    {"side": "detractor", "canonical": "Shipping Damage", "aliases": ["arrived damaged", "broken in box", "damaged packaging", "shipping issue", "arrived broken"], "family": "Packaging"},
    {"side": "delighter", "canonical": "Compatible", "aliases": ["fits perfectly", "works with", "pairs easily", "compatible with"], "family": "Compatibility"},
    {"side": "detractor", "canonical": "Compatibility Issue", "aliases": ["doesn't fit", "not compatible", "won't pair", "pairing issue", "fit issue"], "family": "Compatibility"},
    {"side": "delighter", "canonical": "Long Battery Life", "aliases": ["battery lasts", "holds charge well", "long-lasting battery", "great battery life"], "family": "Battery"},
    {"side": "detractor", "canonical": "Short Battery Life", "aliases": ["battery dies fast", "doesn't hold a charge", "poor battery life", "battery drains quickly"], "family": "Battery"},
    {"side": "delighter", "canonical": "Fast Charging", "aliases": ["charges quickly", "quick charge", "fast to recharge"], "family": "Charging"},
    {"side": "detractor", "canonical": "Slow Charging", "aliases": ["charges slowly", "takes too long to charge", "slow to recharge"], "family": "Charging"},
    {"side": "delighter", "canonical": "Easy Connectivity", "aliases": ["easy to connect", "pairs quickly", "connects easily", "app setup was easy"], "family": "Connectivity"},
    {"side": "detractor", "canonical": "Connectivity Issues", "aliases": ["won't connect", "connection issues", "app won't connect", "bluetooth issues", "wifi issues"], "family": "Connectivity"},
    {"side": "delighter", "canonical": "Pleasant Scent", "aliases": ["smells great", "nice scent", "pleasant fragrance", "good smell"], "family": "Scent"},
    {"side": "detractor", "canonical": "Unpleasant Scent", "aliases": ["strong smell", "bad smell", "weird scent", "chemical smell", "smells awful"], "family": "Scent"},
    {"side": "delighter", "canonical": "Great Taste", "aliases": ["tastes great", "good flavor", "delicious", "great tasting"], "family": "Taste"},
    {"side": "detractor", "canonical": "Bad Taste", "aliases": ["tastes bad", "awful taste", "bad flavor", "off taste"], "family": "Taste"},
    {"side": "delighter", "canonical": "Good Texture", "aliases": ["smooth texture", "nice texture", "great consistency"], "family": "Texture"},
    {"side": "detractor", "canonical": "Poor Texture", "aliases": ["bad texture", "grainy", "sticky texture", "weird consistency"], "family": "Texture"},
    {"side": "delighter", "canonical": "Gentle On Skin", "aliases": ["non irritating", "doesn't irritate", "soft on skin", "gentle"], "family": "Skin / Formula"},
    {"side": "detractor", "canonical": "Irritating", "aliases": ["irritates skin", "caused irritation", "burns", "itchy", "rash"], "family": "Skin / Formula"},
    # ── Vacuum & Cleaning ──────────────────────────────────────────────────
    {"side": "delighter", "canonical": "Strong Suction",       "aliases": ["great suction power", "powerful suction", "picks up everything", "amazing suction"], "family": "Suction Power"},
    {"side": "detractor", "canonical": "Weak Suction",         "aliases": ["poor suction", "low suction power", "doesn't pick up", "leaves debris", "misses dirt"], "family": "Suction Power"},
    {"side": "delighter", "canonical": "Easy To Empty",        "aliases": ["dustbin easy to empty", "no mess emptying", "easy bin release"], "family": "Ease Of Use"},
    {"side": "detractor", "canonical": "Hard To Empty",        "aliases": ["messy to empty", "dust flies everywhere", "difficult bin release", "messy emptying"], "family": "Ease Of Use"},
    {"side": "delighter", "canonical": "Good Maneuverability", "aliases": ["easy to steer", "turns well", "reaches corners", "moves smoothly"], "family": "Design & Ergonomics"},
    {"side": "detractor", "canonical": "Poor Navigation",      "aliases": ["gets stuck", "bumps into furniture", "falls off ledge", "misses spots", "poor obstacle avoidance"], "family": "Design & Ergonomics"},
    {"side": "detractor", "canonical": "Clogs Easily",         "aliases": ["gets clogged", "hair wraps brush", "blocks frequently", "clogs up fast"], "family": "Reliability"},
    {"side": "detractor", "canonical": "Tangled Brush Roll",   "aliases": ["hair tangles in brush", "brush gets tangled", "roller clogs with hair", "brush jams"], "family": "Ease Of Use"},
    {"side": "delighter", "canonical": "Picks Up Pet Hair",    "aliases": ["great for pet hair", "removes dog hair", "cat hair no problem", "excellent on fur"], "family": "Performance"},
    # ── Air Quality ─────────────────────────────────────────────────────────
    {"side": "delighter", "canonical": "Effective Filtration", "aliases": ["removes allergens", "cleans air well", "HEPA works great", "purifies air", "captures particles"], "family": "Performance"},
    {"side": "detractor", "canonical": "Filter Expensive",     "aliases": ["replacement filters costly", "filters overpriced", "expensive to maintain"], "family": "Value"},
    {"side": "detractor", "canonical": "Limited Coverage",     "aliases": ["not powerful enough for room", "too small for space", "poor room coverage"], "family": "Size / Fit"},
    {"side": "detractor", "canonical": "Filter Issues",        "aliases": ["filter indicator wrong", "filter hard to install", "filter falls out", "filter seal poor"], "family": "Ease Of Use"},
    {"side": "detractor", "canonical": "Short Filter Life",    "aliases": ["filter needs changing too often", "filter wears out fast", "filter life disappointing"], "family": "Reliability"},
    # ── Universal cross-category ─────────────────────────────────────────────
    {"side": "detractor", "canonical": "Short Lifespan",       "aliases": ["died after months", "only lasted weeks", "broke down quickly", "very short lifespan", "fell apart quickly", "stopped working after"], "family": "Reliability"},
    {"side": "delighter", "canonical": "Long-Lasting",         "aliases": ["lasts for years", "still going strong", "held up well", "durable over time", "built to last"], "family": "Reliability"},
    {"side": "detractor", "canonical": "Dead On Arrival",      "aliases": ["arrived broken", "didn't work out of box", "broken on arrival", "DOA", "wouldn't turn on"], "family": "Quality & Durability"},
    {"side": "detractor", "canonical": "Missing Parts",        "aliases": ["parts missing", "incomplete package", "wrong parts included", "missing piece"], "family": "Packaging & Delivery"},
    {"side": "delighter", "canonical": "Easy To Maintain",     "aliases": ["low maintenance", "easy to service", "simple upkeep", "easy to keep clean", "maintenance free"], "family": "Ease Of Use"},
    # ── Hair Care ──────────────────────────────────────────────────────────
    {"side": "delighter", "canonical": "Salon-Quality Results", "aliases": ["salon results", "professional results", "like a blowout", "smooth finish", "shiny hair", "frizz free"], "family": "Hair Results"},
    {"side": "detractor", "canonical": "Heat Damage", "aliases": ["damages hair", "burns hair", "hair breakage", "causes heat damage", "fried my hair"], "family": "Hair Results"},
    {"side": "delighter", "canonical": "Reduces Frizz", "aliases": ["eliminates frizz", "controls frizz", "no frizz", "frizz-free", "tames frizz"], "family": "Hair Results"},
    {"side": "detractor", "canonical": "Doesn't Reduce Frizz", "aliases": ["still frizzy", "made frizz worse", "no frizz control", "didn't help frizz"], "family": "Hair Results"},
    {"side": "delighter", "canonical": "Fast Drying", "aliases": ["dries quickly", "quick dry", "dries fast", "fast dry time"], "family": "Time Efficiency"},
    {"side": "detractor", "canonical": "Slow Drying", "aliases": ["takes too long to dry", "dries slowly", "slow dry time", "took forever to dry"], "family": "Time Efficiency"},
    {"side": "detractor", "canonical": "Too Hot", "aliases": ["runs too hot", "gets very hot", "overheats", "scorching heat", "burns scalp"], "family": "Safety"},
    {"side": "delighter", "canonical": "Lightweight", "aliases": ["light to hold", "not heavy", "easy to hold", "not tiring to use"], "family": "Design & Ergonomics"},
    {"side": "detractor", "canonical": "Heavy", "aliases": ["too heavy", "arm gets tired", "hand fatigue", "wrist strain"], "family": "Design & Ergonomics"},
    {"side": "delighter", "canonical": "Easy Attachment Swap", "aliases": ["easy to switch attachments", "attachments click in easily", "attachment swaps easily"], "family": "Ease Of Use"},
    {"side": "detractor", "canonical": "Attachment Issues", "aliases": ["attachments fall off", "attachment doesn't stay", "hard to attach", "attachments don't fit"], "family": "Ease Of Use"},
    {"side": "delighter", "canonical": "Long Cord", "aliases": ["cord is long enough", "good cord length", "long cable"], "family": "Design & Ergonomics"},
    {"side": "detractor", "canonical": "Short Cord", "aliases": ["cord too short", "not enough cord", "short cable", "limited reach"], "family": "Design & Ergonomics"},
    {"side": "delighter", "canonical": "Multiple Heat Settings", "aliases": ["adjustable heat", "heat settings", "temperature control", "heat options"], "family": "Ease Of Use"},
    {"side": "detractor", "canonical": "Poor Build Quality", "aliases": ["feels cheap", "flimsy plastic", "broke quickly", "cheaply made", "poor construction"], "family": "Quality & Durability"},
]


CATEGORY_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "kitchen_appliance": ("air fryer", "blender", "coffee", "espresso", "oven", "toaster", "cook", "cooking", "basket", "dishwasher", "preheat", "recipe", "fryer"),
    "beauty_personal_care": (
        "shampoo", "conditioner", "serum", "cream", "lotion", "fragrance",
        "makeup", "mascara", "lip gloss", "face wash", "moisturizer",
        "cleanser", "toner", "skincare", "foundation", "concealer", "eyeshadow",
        "moisturizing", "vitamin c", "hyaluronic", "retinol", "spf", "sunscreen",
        "nourishing", "anti-aging", "exfoliat", "collagen", "niacinamide",
        "body lotion", "body wash", "face mask", "eye cream", "lip balm",
    ),
    "hair_care": (
        "hair dryer", "blow dryer", "hair styler", "flat iron", "curling iron",
        "diffuser", "concentrator", "hair tool", "frizz", "heat damage",
        "blowout", "hair styling", "airwrap", "straightener",
        "hair care", "shark flexstyle", "dyson airwrap", "hair wand",
    ),
    "vacuum_cleaning": (
        "vacuum", "suction", "dustbin", "dust bin", "brush roll", "brush bar",
        "cordless vacuum", "robot vacuum", "robovac", "steam mop",
        "bagless", "canister vacuum", "pet hair", "shark vacuum",
        "clog", "carpet cleaning", "debris", "dirt pickup", "self-emptying",
        "swivel steering", "hardwood floors",
    ),
    "air_quality": (
        "air purifier", "hepa", "humidifier", "dehumidifier", "air filter",
        "allergen", "air quality", "purification", "air cleaner", "ionizer",
        "tower fan", "space heater", "air circulation", "pollen", "dust particles",
        "smoke removal", "odor removal", "carbon filter", "true hepa",
    ),
    "electronics": ("battery", "charge", "charging", "screen", "display", "bluetooth", "wifi", "app", "usb", "speaker", "camera", "headphones", "laptop", "phone", "electronic"),
    "apparel_footwear": ("shirt", "pants", "dress", "shoe", "shoes", "sneaker", "boot", "sock", "fabric", "size", "fit", "wear", "waist", "heel"),
    "furniture_home": ("chair", "table", "desk", "mattress", "sofa", "couch", "drawer", "shelf", "furniture", "wood", "assembly", "assembled", "room"),
    "tools_outdoors": ("drill", "saw", "mower", "trimmer", "blade", "tool", "motor", "cordless", "torque", "garage", "outdoor"),
    "food_beverage": ("taste", "flavor", "drink", "snack", "coffee beans", "tea", "fresh", "recipe", "delicious", "beverage"),
    "pet": ("dog", "cat", "pet", "puppy", "kitten", "litter", "chew", "collar", "bowl", "pet food", "treat"),
}


CATEGORY_PACKS: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
    "general": {
        "delighters": [
            {"label": "Performs Well",    "aliases": ["works great", "works well", "effective", "does the job", "as advertised"]},
            {"label": "High Quality",     "aliases": ["well made", "solid build", "durable", "great construction", "premium feel"]},
            {"label": "Easy To Use",      "aliases": ["user friendly", "intuitive", "simple to use", "straightforward", "no learning curve"]},
            {"label": "Good Value",       "aliases": ["worth it", "great value", "good for the price", "worth every penny"]},
            {"label": "Reliable",         "aliases": ["lasts long", "consistent", "dependable", "holds up over time"]},
            {"label": "Easy Setup",       "aliases": ["easy to assemble", "quick setup", "ready out of box", "no tools needed"]},
            {"label": "Attractive Design","aliases": ["looks great", "stylish", "nice design", "well designed", "sleek"]},
            {"label": "Good Packaging",   "aliases": ["arrived safely", "well packaged", "secure packaging", "arrived in perfect condition"]},
        ],
        "detractors": [
            {"label": "Poor Performance", "aliases": ["doesn't work well", "ineffective", "poor results", "not as described", "underwhelming"]},
            {"label": "Poor Quality",     "aliases": ["cheaply made", "flimsy", "feels cheap", "fell apart", "poor build quality"]},
            {"label": "Difficult To Use", "aliases": ["hard to use", "confusing", "not intuitive", "steep learning curve", "awkward"]},
            {"label": "Overpriced",       "aliases": ["too expensive", "not worth it", "overpriced", "better options for less"]},
            {"label": "Unreliable",       "aliases": ["stopped working", "broke early", "inconsistent", "defective", "died quickly"]},
            {"label": "Difficult Setup",  "aliases": ["hard to assemble", "poor instructions", "setup nightmare", "confusing setup"]},
            {"label": "Poor Design",      "aliases": ["bad design", "poorly designed", "awkward design", "frustrating to use"]},
            {"label": "Shipping Damage",  "aliases": ["arrived damaged", "broken in box", "damaged packaging", "missing parts on arrival"]},
        ],
    },
    "kitchen_appliance": {
        "delighters": [
            {"label": "Easy Setup", "aliases": ["easy to set up", "easy to assemble"]},
            {"label": "Quiet", "aliases": ["quiet operation", "silent"]},
            {"label": "Easy To Clean", "aliases": ["easy cleanup", "easy clean"]},
            {"label": "Performs Well", "aliases": ["cooks evenly", "works great", "great results"]},
            {"label": "Right Size", "aliases": ["good size", "good capacity"]},
            {"label": "Saves Time", "aliases": ["quick cooking", "fast cooking"]},
        ],
        "detractors": [
            {"label": "Difficult Setup", "aliases": ["hard to set up", "assembly is hard"]},
            {"label": "Loud", "aliases": ["noisy", "too loud"]},
            {"label": "Hard To Clean", "aliases": ["difficult to clean", "messy cleanup"]},
            {"label": "Poor Performance", "aliases": ["uneven cooking", "poor results"]},
            {"label": "Wrong Size", "aliases": ["too small", "too bulky"]},
            {"label": "Time Consuming", "aliases": ["takes too long", "slow to cook"]},
        ],
    },
    "beauty_personal_care": {
        "delighters": [
            {"label": "Pleasant Scent", "aliases": ["smells great", "nice scent"]},
            {"label": "Gentle On Skin", "aliases": ["non irritating", "gentle"]},
            {"label": "Performs Well", "aliases": ["works well", "great results"]},
            {"label": "Easy To Use", "aliases": ["easy to apply", "simple to use"]},
            {"label": "Good Texture", "aliases": ["smooth texture", "nice texture"]},
        ],
        "detractors": [
            {"label": "Unpleasant Scent", "aliases": ["bad smell", "chemical smell"]},
            {"label": "Irritating", "aliases": ["caused irritation", "burns", "itchy"]},
            {"label": "Poor Performance", "aliases": ["did not work", "poor results"]},
            {"label": "Difficult To Use", "aliases": ["hard to apply", "messy to use"]},
            {"label": "Poor Texture", "aliases": ["grainy", "sticky texture"]},
        ],
    },
    "electronics": {
        "delighters": [
            {"label": "Long Battery Life", "aliases": ["battery lasts", "holds charge well"]},
            {"label": "Fast Charging", "aliases": ["charges quickly", "quick charge"]},
            {"label": "Easy Connectivity", "aliases": ["pairs quickly", "connects easily"]},
            {"label": "Performs Well", "aliases": ["responsive", "works well"]},
            {"label": "High Quality", "aliases": ["solid build", "well made"]},
        ],
        "detractors": [
            {"label": "Short Battery Life", "aliases": ["battery dies fast", "poor battery life"]},
            {"label": "Slow Charging", "aliases": ["charges slowly", "takes too long to charge"]},
            {"label": "Connectivity Issues", "aliases": ["won't connect", "connection issues"]},
            {"label": "Poor Performance", "aliases": ["laggy", "slow", "unresponsive"]},
            {"label": "Poor Quality", "aliases": ["feels cheap", "flimsy"]},
        ],
    },
    "apparel_footwear": {
        "delighters": [
            {"label": "Comfortable", "aliases": ["comfortable fit", "feels great"]},
            {"label": "Right Size", "aliases": ["true to size", "fits well"]},
            {"label": "Attractive Design", "aliases": ["looks great", "stylish"]},
            {"label": "High Quality", "aliases": ["good material", "well made"]},
        ],
        "detractors": [
            {"label": "Uncomfortable", "aliases": ["hurts", "not comfortable"]},
            {"label": "Wrong Size", "aliases": ["runs small", "runs large", "does not fit"]},
            {"label": "Poor Design", "aliases": ["awkward design", "cheap looking"]},
            {"label": "Poor Quality", "aliases": ["poor material", "fell apart"]},
        ],
    },
    "furniture_home": {
        "delighters": [
            {"label": "Easy Setup", "aliases": ["easy assembly", "assembled easily"]},
            {"label": "Comfortable", "aliases": ["comfortable", "supportive"]},
            {"label": "Attractive Design", "aliases": ["looks great", "beautiful"]},
            {"label": "High Quality", "aliases": ["sturdy", "solid"]},
            {"label": "Right Size", "aliases": ["fits perfectly", "good size"]},
        ],
        "detractors": [
            {"label": "Difficult Setup", "aliases": ["hard assembly", "assembly is difficult"]},
            {"label": "Uncomfortable", "aliases": ["not comfortable", "painful"]},
            {"label": "Poor Design", "aliases": ["bad design", "awkward design"]},
            {"label": "Poor Quality", "aliases": ["flimsy", "cheap"]},
            {"label": "Wrong Size", "aliases": ["too large", "too small"]},
        ],
    },
    "tools_outdoors": {
        "delighters": [
            {"label": "Performs Well", "aliases": ["powerful", "great results"]},
            {"label": "Reliable", "aliases": ["holds up", "durable"]},
            {"label": "Easy To Use", "aliases": ["easy handling", "simple to use"]},
            {"label": "Long Battery Life", "aliases": ["battery lasts"]},
        ],
        "detractors": [
            {"label": "Poor Performance", "aliases": ["underpowered", "weak"]},
            {"label": "Unreliable", "aliases": ["stopped working", "breaks easily"]},
            {"label": "Difficult To Use", "aliases": ["awkward to use", "hard to handle"]},
            {"label": "Short Battery Life", "aliases": ["battery dies fast"]},
        ],
    },
    "food_beverage": {
        "delighters": [
            {"label": "Great Taste", "aliases": ["delicious", "good flavor"]},
            {"label": "Good Texture", "aliases": ["nice texture", "smooth texture"]},
            {"label": "Good Value", "aliases": ["worth it", "great value"]},
        ],
        "detractors": [
            {"label": "Bad Taste", "aliases": ["tastes bad", "awful flavor"]},
            {"label": "Poor Texture", "aliases": ["grainy", "weird texture"]},
            {"label": "Overpriced", "aliases": ["too expensive", "not worth it"]},
        ],
    },
    "pet": {
        "delighters": [
            {"label": "Performs Well", "aliases": ["works well", "effective"]},
            {"label": "Easy To Clean", "aliases": ["easy cleanup", "easy to wash"]},
            {"label": "High Quality", "aliases": ["durable", "well made"]},
            {"label": "Right Size", "aliases": ["good size", "fits well"]},
        ],
        "detractors": [
            {"label": "Poor Performance", "aliases": ["didn't work", "ineffective"]},
            {"label": "Hard To Clean", "aliases": ["hard cleanup", "difficult to wash"]},
            {"label": "Poor Quality", "aliases": ["flimsy", "falls apart"]},
            {"label": "Wrong Size", "aliases": ["too small", "too big"]},
        ],
    },
    "vacuum_cleaning": {
        "delighters": [
            {"label": "Strong Suction",       "aliases": ["great suction power", "powerful suction", "picks up everything", "amazing pickup"]},
            {"label": "Easy To Empty",        "aliases": ["dustbin easy to empty", "no mess emptying", "easy bin release", "clean emptying"]},
            {"label": "Long Battery Life",    "aliases": ["battery lasts long", "good run time", "holds charge well", "runs for hours"]},
            {"label": "Quiet",                "aliases": ["quiet operation", "low noise", "not loud", "whisper quiet"]},
            {"label": "Lightweight",          "aliases": ["easy to carry", "light to push", "not heavy", "effortless to use"]},
            {"label": "Easy To Maintain",     "aliases": ["easy filter clean", "simple maintenance", "brush roll easy to clean", "washable filter"]},
            {"label": "Good Maneuverability", "aliases": ["easy to steer", "turns well", "reaches under furniture", "moves smoothly", "great on corners"]},
            {"label": "Picks Up Pet Hair",    "aliases": ["great for pet hair", "removes dog hair", "cat hair no problem", "excellent on pet fur"]},
        ],
        "detractors": [
            {"label": "Weak Suction",         "aliases": ["poor suction", "low suction power", "doesn't pick up", "leaves debris behind", "misses dirt"]},
            {"label": "Hard To Empty",        "aliases": ["messy to empty", "dust flies everywhere", "difficult bin release", "messy emptying"]},
            {"label": "Short Battery Life",   "aliases": ["battery dies fast", "poor run time", "doesn't last a room", "need to recharge constantly"]},
            {"label": "Loud",                 "aliases": ["too loud", "very noisy", "noise level high", "disruptively loud"]},
            {"label": "Heavy",                "aliases": ["too heavy", "hard to push", "tiring to use", "arm gets tired"]},
            {"label": "Clogs Easily",         "aliases": ["gets clogged", "hair wraps brush", "blocks frequently", "clogs up fast", "tangles badly"]},
            {"label": "Poor Navigation",      "aliases": ["gets stuck", "bumps into furniture", "falls off ledge", "misses spots", "poor obstacle avoidance"]},
            {"label": "Tangled Brush Roll",   "aliases": ["hair tangles in brush", "brush gets tangled", "roller clogs with hair", "brush jams"]},
        ],
    },
    "air_quality": {
        "delighters": [
            {"label": "Effective Filtration", "aliases": ["removes allergens", "cleans air well", "HEPA works great", "purifies air beautifully", "captures particles"]},
            {"label": "Quiet",               "aliases": ["quiet operation", "runs silently", "whisper quiet", "low fan noise"]},
            {"label": "Easy Filter Change",  "aliases": ["filter easy to replace", "simple filter swap", "filter change is easy", "straightforward maintenance"]},
            {"label": "Right Size",          "aliases": ["good room coverage", "perfect for room size", "right capacity", "covers my space well"]},
            {"label": "Saves Time",          "aliases": ["auto mode works well", "set and forget", "smart sensing", "adjusts automatically"]},
            {"label": "Easy To Use",         "aliases": ["simple controls", "easy to operate", "intuitive settings", "app works well"]},
        ],
        "detractors": [
            {"label": "Filter Expensive",    "aliases": ["replacement filters costly", "ongoing filter cost", "filters overpriced", "expensive to maintain"]},
            {"label": "Loud",               "aliases": ["too loud on high", "noisy fan", "disruptive on high setting", "loud at speed"]},
            {"label": "Limited Coverage",   "aliases": ["not powerful enough for room", "too small for space", "poor room coverage", "can't handle large room"]},
            {"label": "Filter Issues",      "aliases": ["filter indicator wrong", "filter hard to install", "filter falls out", "filter seal poor"]},
            {"label": "Poor Performance",   "aliases": ["doesn't clean air", "air quality doesn't improve", "ineffective filtration", "doesn't capture particles"]},
            {"label": "Short Filter Life",  "aliases": ["filter needs changing too often", "filter wears out fast", "filter life disappointing"]},
        ],
    },
    "hair_care": {
        "delighters": [
            {"label": "Salon-Quality Results",  "aliases": ["salon results", "professional results", "like a blowout", "smooth finish", "shiny hair", "frizz free"]},
            {"label": "Reduces Frizz",          "aliases": ["eliminates frizz", "controls frizz", "no frizz", "frizz-free"]},
            {"label": "Fast Drying",            "aliases": ["dries quickly", "quick dry", "dries fast", "fast dry time"]},
            {"label": "Gentle On Hair",         "aliases": ["no heat damage", "doesn't damage hair", "hair-friendly heat", "not damaging", "safe for hair"]},
            {"label": "Easy To Use",            "aliases": ["simple to use", "easy to style", "easy to operate", "intuitive controls"]},
            {"label": "Easy Attachment Swap",   "aliases": ["easy to switch attachments", "attachments click in easily", "attachment swaps easily"]},
            {"label": "Lightweight",            "aliases": ["light to hold", "not heavy", "easy to hold", "not tiring to use"]},
            {"label": "Quiet",                  "aliases": ["quiet operation", "not loud", "low noise", "runs quietly"]},
            {"label": "Long Cord",              "aliases": ["cord is long enough", "good cord length", "long cable"]},
            {"label": "Multiple Heat Settings", "aliases": ["adjustable heat", "heat settings", "temperature control", "heat options"]},
        ],
        "detractors": [
            {"label": "Heat Damage",            "aliases": ["damages hair", "burns hair", "hair breakage", "causes heat damage", "fried my hair"]},
            {"label": "Doesn't Reduce Frizz",  "aliases": ["still frizzy", "made frizz worse", "no frizz control", "didn't help frizz"]},
            {"label": "Slow Drying",            "aliases": ["takes too long to dry", "dries slowly", "slow dry time", "took forever to dry"]},
            {"label": "Too Hot",                "aliases": ["runs too hot", "gets very hot", "overheats", "scorching heat", "burns scalp"]},
            {"label": "Difficult To Use",       "aliases": ["hard to use", "awkward to hold", "hard to maneuver", "hard to style with"]},
            {"label": "Attachment Issues",      "aliases": ["attachments fall off", "attachment doesn't stay", "hard to attach", "attachments don't fit", "attachment keeps coming off"]},
            {"label": "Heavy",                  "aliases": ["too heavy", "arm gets tired", "hand fatigue", "wrist strain"]},
            {"label": "Loud",                   "aliases": ["too loud", "very noisy", "loud operation", "noise level"]},
            {"label": "Short Cord",             "aliases": ["cord too short", "not enough cord", "short cable", "limited reach"]},
            {"label": "Poor Build Quality",     "aliases": ["feels cheap", "flimsy plastic", "broke quickly", "cheaply made", "poor construction"]},
        ],
    },
}


UNIVERSAL_NEUTRAL_DELIGHTERS: Tuple[str, ...] = (
    "Overall Satisfaction",
    "Good Value",
    "Performs Well",
    "High Quality",
    "Easy To Use",
    "Reliable",
    "Easy To Clean",
    "Saves Time",
)
UNIVERSAL_NEUTRAL_DETRACTORS: Tuple[str, ...] = (
    "Overall Dissatisfaction",
    "Overpriced",
    "Poor Performance",
    "Poor Quality",
    "Difficult To Use",
    "Unreliable",
    "Hard To Clean",
    "Time Consuming",
)



# ---------------------------------------------------------------------------
# TaxonomyRegistry — wraps all taxonomy data in a queryable class
#
# Benefits over raw dicts:
#   • Single object to pass into functions / serialise to a DB row
#   • clear() + reload() enables hot-reloading from Airtable / Snowflake
#   • from_rules() creates a registry from SYSTEMATIC_RULES (current default)
#   • All existing free functions are thin wrappers that call the default
#     registry, so no call sites need updating
# ---------------------------------------------------------------------------


@dataclass
class LabelDef:
    """A single taxonomy entry."""
    canonical: str
    side: str                           # "delighter" | "detractor"
    aliases: list = field(default_factory=list)
    family: str = ""
    bucket: str = "Product Specific"   # Category Driver | Universal Neutral | Product Specific
    severity_weight: float = 1.0       # multiplier in impact scoring

    @property
    def all_phrases(self) -> list:
        return [self.canonical] + self.aliases


class TaxonomyRegistry:
    """Loads, indexes and queries taxonomy labels.

    Usage::

        registry = TaxonomyRegistry.from_rules()

        # Look up a label
        ldef = registry.get_label("Heat Damage")

        # Bucket a free-form label string
        bucket = registry.bucket_for("heat damage", side="detractor")

        # Score support across reviews
        result = registry.score_support("Heat Damage", sample_reviews, side="detractor")

        # Hot-reload from external source (Airtable / Snowflake)
        registry.load_from_dicts([{"canonical": "...", "side": "...", ...}])
    """

    def __init__(self) -> None:
        self._by_canonical: dict = {}       # canonical_lower → LabelDef
        self._by_alias: dict = {}           # alias_lower → LabelDef
        self._by_side: dict = {"delighter": [], "detractor": []}

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    @classmethod
    def from_rules(cls) -> "TaxonomyRegistry":
        """Build from the module-level SYSTEMATIC_RULES (default)."""
        registry = cls()
        registry._load_systematic_rules()
        return registry

    def _load_systematic_rules(self) -> None:
        self._by_canonical.clear()
        self._by_alias.clear()
        self._by_side = {"delighter": [], "detractor": []}
        for rule in SYSTEMATIC_RULES:
            ldef = LabelDef(
                canonical=str(rule.get("canonical", "")),
                side=str(rule.get("side", "")),
                aliases=list(rule.get("aliases", [])),
                family=str(rule.get("family", "")),
            )
            if not ldef.canonical or not ldef.side:
                continue
            self._index(ldef)

    def load_from_dicts(self, rows: list) -> None:
        """Replace current registry with rows from an external source."""
        self._by_canonical.clear()
        self._by_alias.clear()
        self._by_side = {"delighter": [], "detractor": []}
        for row in rows:
            ldef = LabelDef(
                canonical=str(row.get("canonical", "")),
                side=str(row.get("side", "")),
                aliases=list(row.get("aliases") or []),
                family=str(row.get("family", "")),
                bucket=str(row.get("bucket", "Product Specific")),
                severity_weight=float(row.get("severity_weight", 1.0)),
            )
            if ldef.canonical and ldef.side:
                self._index(ldef)

    def _index(self, ldef: LabelDef) -> None:
        key = _canon(ldef.canonical)
        self._by_canonical[key] = ldef
        self._by_side.setdefault(ldef.side, []).append(ldef)
        for alias in ldef.aliases:
            ak = _canon(str(alias))
            if ak:
                self._by_alias[ak] = ldef

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_label(self, label: str) -> "LabelDef | None":
        key = _canon(str(label or ""))
        return self._by_canonical.get(key) or self._by_alias.get(key)

    def bucket_for(self, label: str, *, side: str = "") -> str:
        return bucket_symptom_label(label, side=side or None)

    def score_support(self, label: str, reviews: list, *, side: str = "") -> dict:
        return score_symptom_support(label, reviews, side=side or None)

    def infer_category(self, product_description: str = "", reviews: list = None) -> dict:
        return infer_category(product_description, reviews or [])

    def starter_pack(self, category: str) -> dict:
        return starter_pack_for_category(category)

    def infer_theme(self, label: str, *, side: str = "", category: str = "") -> str:
        return infer_l1_theme(label, side=side, category=category)

    def all_labels(self, side: str = "") -> list:
        if side:
            return [ld.canonical for ld in self._by_side.get(side, [])]
        return [ld.canonical for ld in list(self._by_canonical.values())]

    def __len__(self) -> int:
        return len(self._by_canonical)

    def __repr__(self) -> str:
        return (f"TaxonomyRegistry({len(self._by_side.get('delighter',[]))} delighters, "
                f"{len(self._by_side.get('detractor',[]))} detractors)")


# Module-level default registry (lazy-initialised on first use)
_default_registry: "TaxonomyRegistry | None" = None


def get_registry() -> TaxonomyRegistry:
    """Return the module-level default TaxonomyRegistry."""
    global _default_registry
    if _default_registry is None:
        _default_registry = TaxonomyRegistry.from_rules()
    return _default_registry

def bucket_symptom_label(label: Any, *, side: Optional[str] = None, category: Any = "general") -> str:
    cleaned = standardize_symptom_label(label, side=side)
    if not cleaned:
        return "Product Specific"
    if side == "delighter" and cleaned in set(UNIVERSAL_NEUTRAL_DELIGHTERS):
        return "Universal Neutral"
    if side == "detractor" and cleaned in set(UNIVERSAL_NEUTRAL_DETRACTORS):
        return "Universal Neutral"
    if side is None and cleaned in set(UNIVERSAL_NEUTRAL_DELIGHTERS) | set(UNIVERSAL_NEUTRAL_DETRACTORS):
        return "Universal Neutral"
    pack = starter_pack_for_category(category)
    if side == "delighter":
        if cleaned in set(pack.get("delighters", [])):
            return "Category Driver"
    elif side == "detractor":
        if cleaned in set(pack.get("detractors", [])):
            return "Category Driver"
    elif cleaned in set(pack.get("delighters", [])) | set(pack.get("detractors", [])):
        return "Category Driver"
    return "Product Specific"


def _phrase_matches_review(phrase: str, review_norm: str) -> bool:
    phrase_norm = _canon(phrase)
    if not phrase_norm:
        return False
    if " " in phrase_norm:
        return phrase_norm in review_norm
    if len(phrase_norm) < 5:
        return False
    return bool(re.search(rf"\b{re.escape(phrase_norm)}\b", review_norm))


def _first_matching_sentence(review_text: str, phrases: Sequence[str]) -> str:
    raw = re.sub(r"\s+", " ", str(review_text or "").strip())
    if not raw:
        return ""
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", raw) if part.strip()]
    review_norm = _canon(raw)
    for sentence in sentences or [raw]:
        sentence_norm = _canon(sentence)
        if any(_phrase_matches_review(phrase, sentence_norm) for phrase in phrases if phrase):
            return sentence
    if phrases:
        phrase_norm = _canon(phrases[0])
        if phrase_norm and phrase_norm in review_norm:
            return raw
    return sentences[0] if sentences else raw


def score_symptom_support(
    label: Any,
    sample_reviews: Sequence[str] | None = None,
    *,
    aliases: Sequence[str] | None = None,
    side: Optional[str] = None,
) -> Dict[str, Any]:
    cleaned = standardize_symptom_label(label, side=side) or _clean_label(label)
    reviews = [str(text or "").strip() for text in (sample_reviews or []) if str(text or "").strip()]
    alias_values = [_clean_label(alias) for alias in (aliases or []) if _clean_label(alias)]
    rule = _best_matching_rule(cleaned, side=side)
    rule_aliases = [_clean_label(alias) for alias in (rule or {}).get("aliases", []) if _clean_label(alias)]
    phrases = _dedupe_keep_order([cleaned] + alias_values + rule_aliases)
    label_tokens = set(_tokenize(cleaned))
    review_hits = 0
    example_snippets: List[str] = []
    phrase_hit_count = 0
    for review in reviews:
        review_norm = _canon(review)
        review_tokens = set(_tokenize(review))
        matched = False
        for phrase in phrases:
            if _phrase_matches_review(phrase, review_norm):
                matched = True
                phrase_hit_count += 1
                break
        if not matched and label_tokens:
            overlap = len(label_tokens & review_tokens) / float(max(len(label_tokens), 1))
            if overlap >= 0.8 or (len(label_tokens) >= 2 and label_tokens.issubset(review_tokens)):
                matched = True
        if not matched:
            continue
        review_hits += 1
        if len(example_snippets) < 3:
            example = _first_matching_sentence(review, phrases or [cleaned])
            if example and example not in example_snippets:
                example_snippets.append(example[:140])
    review_count = max(len(reviews), 1)
    support_ratio = review_hits / float(review_count)
    specificity = float(min(max(len(label_tokens), 1), 5))
    score = float(review_hits) + support_ratio + (0.12 * specificity) + min(0.35, 0.05 * phrase_hit_count)
    return {
        "label": cleaned,
        "review_hits": int(review_hits),
        "support_ratio": round(float(support_ratio), 4),
        "score": round(float(score), 4),
        "specificity": round(float(specificity), 2),
        "examples": example_snippets,
    }


def prioritize_ai_taxonomy_items(
    items: Sequence[Any] | None,
    *,
    side: str,
    sample_reviews: Sequence[str] | None,
    category: Any = "general",
    min_review_hits: int = 1,
    max_keep: int = 18,
    exclude_universal: bool = True,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    bucket_priority = {"Category Driver": 0, "Product Specific": 1, "Universal Neutral": 2}
    for raw in items or []:
        if isinstance(raw, Mapping):
            label = raw.get("label")
            aliases = list(raw.get("aliases") or [])
            family = _clean_label(raw.get("family"))
            theme = _clean_label(raw.get("theme") or raw.get("l1_theme") or raw.get("l1"))
            rationale = str(raw.get("rationale") or "").strip()
            declared_bucket = str(raw.get("bucket") or "").strip()
            seeded = bool(raw.get("seeded"))
        else:
            label = raw
            aliases = []
            family = ""
            theme = ""
            rationale = ""
            declared_bucket = ""
            seeded = False
        canonical = standardize_symptom_label(label, side=side)
        if not canonical:
            continue
        bucket = declared_bucket or bucket_symptom_label(canonical, side=side, category=category)
        if exclude_universal and bucket == "Universal Neutral":
            continue
        rec = merged.setdefault(canonical, {
            "label": canonical,
            "aliases": [],
            "family": family,
            "theme": theme,
            "rationale": rationale,
            "bucket": bucket,
            "seeded": seeded,
        })
        for alias in aliases:
            clean_alias = _clean_label(alias)
            if clean_alias and clean_alias != canonical and clean_alias not in rec["aliases"]:
                rec["aliases"].append(clean_alias)
        if family and not rec.get("family"):
            rec["family"] = family
        if theme and not rec.get("theme"):
            rec["theme"] = theme
        if rationale and not rec.get("rationale"):
            rec["rationale"] = rationale
        if bucket_priority.get(bucket, 9) < bucket_priority.get(str(rec.get("bucket") or "Product Specific"), 9):
            rec["bucket"] = bucket
        rec["seeded"] = bool(rec.get("seeded")) or seeded

    kept: List[Dict[str, Any]] = []
    fallback_product: List[Dict[str, Any]] = []
    for rec in merged.values():
        support = score_symptom_support(rec["label"], sample_reviews, aliases=rec.get("aliases"), side=side)
        combined = dict(rec)
        combined.update(support)
        combined["theme"] = combined.get("theme") or infer_l1_theme(combined.get("label"), side=side, family=combined.get("family"), category=category)
        if combined["bucket"] == "Category Driver":
            if combined["review_hits"] >= 1:
                kept.append(combined)
        elif combined["bucket"] == "Product Specific":
            if combined["review_hits"] >= 1:
                fallback_product.append(combined)
            if combined["review_hits"] >= max(int(min_review_hits), 1):
                kept.append(combined)
        elif combined["bucket"] == "Universal Neutral" and not exclude_universal:
            kept.append(combined)

    if not any(item.get("bucket") == "Product Specific" for item in kept) and fallback_product:
        fallback_product = sorted(
            fallback_product,
            key=lambda item: (int(item.get("review_hits", 0)), float(item.get("score", 0.0)), float(item.get("specificity", 0.0)), item.get("label", "")),
            reverse=True,
        )
        kept.extend(fallback_product[: min(6, max_keep)])

    kept = sorted(
        kept,
        key=lambda item: (
            bucket_priority.get(str(item.get("bucket") or "Product Specific"), 9),
            -int(item.get("review_hits", 0)),
            -float(item.get("score", 0.0)),
            -float(item.get("specificity", 0.0)),
            str(item.get("label") or ""),
        ),
    )
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in kept:
        label = str(item.get("label") or "")
        if not label or label in seen:
            continue
        seen.add(label)
        out.append(item)
        if len(out) >= max_keep:
            break
    return out


def _label_matches_rule(cleaned_label: str, side: Optional[str], rule: Mapping[str, Any]) -> bool:
    if side and rule.get("side") not in {side, "any", "both"}:
        return False
    label_norm = _canon(cleaned_label)
    candidates = [_canon(rule.get("canonical"))] + [_canon(alias) for alias in rule.get("aliases", [])]
    for cand in candidates:
        if not cand:
            continue
        if label_norm == cand:
            return True
        if len(label_norm) >= 5 and (label_norm in cand or cand in label_norm):
            return True
    label_tokens = set(_tokenize(cleaned_label))
    for cand in candidates:
        cand_tokens = set(_tokenize(cand))
        if label_tokens and cand_tokens and (len(label_tokens & cand_tokens) / max(len(label_tokens), len(cand_tokens))) >= 0.8:
            return True
    return False


def _best_matching_rule(label: Any, side: Optional[str] = None) -> Optional[Dict[str, Any]]:
    cleaned = _clean_label(label)
    if not cleaned:
        return None
    matches: List[Tuple[float, Dict[str, Any]]] = []
    label_norm = _canon(cleaned)
    label_tokens = set(_tokenize(cleaned))
    for rule in SYSTEMATIC_RULES:
        if side and rule.get("side") not in {side, "any", "both"}:
            continue
        if not _label_matches_rule(cleaned, side, rule):
            continue
        best = 0.0
        for cand in [rule.get("canonical", "")] + list(rule.get("aliases", []) or []):
            cand_norm = _canon(cand)
            ratio = difflib.SequenceMatcher(None, label_norm, cand_norm).ratio()
            cand_tokens = set(_tokenize(cand))
            if label_tokens and cand_tokens:
                ratio = max(ratio, len(label_tokens & cand_tokens) / float(max(len(label_tokens), len(cand_tokens))))
            best = max(best, ratio)
        matches.append((best, rule))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], len(str(item[1].get("canonical", "")))), reverse=True)
    return dict(matches[0][1])


def standardize_symptom_label(label: Any, side: Optional[str] = None) -> str:
    cleaned = _clean_label(label)
    if not cleaned:
        return ""

    direct_patterns = []
    if side == "delighter":
        direct_patterns = [
            (r"\bEasy Cleanup\b|\bEasy Clean\b", "Easy To Clean"),
            (r"\bSimple To Use\b|\bStraightforward To Use\b|\bUser Friendly\b", "Easy To Use"),
            (r"\bQuiet Operation\b|\bRuns Quietly\b", "Quiet"),
            (r"\bFast Charge\b|\bQuick Charge\b", "Fast Charging"),
            (r"\bGreat Value\b|\bValue For Money\b|\bWorth The Price\b", "Good Value"),
            (r"\bWorks Great\b|\bWorks Well\b|\bDoes The Job\b", "Performs Well"),
            (r"\bLooks Great\b|\bNice Design\b|\bStylish\b", "Attractive Design"),
            (r"\bTrue To Size\b|\bFits Well\b|\bPerfect Size\b", "Right Size"),
            (r"\bEasy Install\b|\bEasy Installation\b|\bEasy Assembly\b", "Easy Setup"),
            (r"\bClear Directions\b|\bHelpful Instructions\b", "Clear Instructions"),
            # ── Hair care ─────────────────────────────────────────────────────
            (r"\bSalon.?Quality\s+(?:Blowout|Results?|Finish|Hair)\b"
             r"|\bProfessional\s+(?:Results?|Blowout)\b"
             r"|\bLike\s+A\s+(?:Salon\s+)?Blowout\b", "Salon-Quality Results"),
            (r"\bNo\s+(?:More\s+)?Frizz\b|\bFrizz.?Free\b|\bEliminates?\s+Frizz\b"
             r"|\bTames?\s+(?:The\s+)?Frizz\b|\bReduces?\s+Frizz\b"
             r"|\bControls?\s+(?:My\s+)?Frizz\b", "Reduces Frizz"),
            (r"\bDries?\s+(?:Hair\s+)?(?:In\s+Minutes|Quickly|Fast|Super\s+Fast)\b"
             r"|\bFast\s+Dry(?:ing)?\b|\bQuick\s+Dry(?:ing)?\b", "Fast Drying"),
            (r"\bGentle\s+On\s+(?:My\s+)?Hair\b|\bNo\s+Heat\s+Damage\b"
             r"|\bDoesn'?t?\s+(?:Cause|Damage)\s+(?:My\s+)?Hair\b"
             r"|\bHair.?Friendly\b|\bSafe\s+(?:For|On)\s+(?:My\s+)?Hair\b", "Gentle On Hair"),
            (r"\bAttachments?\s+(?:Click|Lock|Snap|Stay)\s+(?:In|On)\s+(?:Easily|Well|Perfectly|Securely)\b"
             r"|\bEasy\s+(?:To\s+)?Swap\s+Attachments?\b"
             r"|\bAttachments?\s+Are\s+(?:Easy\s+To\s+)?Secure\b", "Easy Attachment Swap"),
            (r"\bLong(?:er)?\s+Cord\b|\bCord\s+(?:Is\s+)?Long\s+Enough\b"
             r"|\bPlenty\s+Of\s+Cord\b|\bGood\s+Cord\s+Length\b", "Long Cord"),
            (r"\bLightweight\b|\bLight\s+(?:To\s+Hold|Weight|In\s+(?:My\s+)?Hand)\b"
             r"|\bNot\s+(?:Too\s+)?Heavy\b|\bNot\s+Tiring\b", "Lightweight"),
            (r"\bMultiple\s+Heat\s+Settings?\b|\bAdjustable\s+Heat\b"
             r"|\bTemperature\s+Control\b|\bHeat\s+Options?\b", "Multiple Heat Settings"),
            # ── Vacuum & Cleaning ──────────────────────────────────────────
            (r"\bStrong(?:er)?\s+Suction\b|\bPowerful\s+Suction\b|\bGreat\s+Suction\b"
             r"|\bPicks?\s+Up\s+Everything\b|\bAmazing\s+Pickup\b", "Strong Suction"),
            (r"\bEasy\s+To\s+Empty\b|\bDustbin\s+Easy\b|\bNo\s+Mess\s+Emptying\b"
             r"|\bClean\s+Emptying\b|\bEasy\s+Bin\s+Release\b", "Easy To Empty"),
            (r"\bGood\s+Maneuverability\b|\bEasy\s+To\s+Steer\b|\bTurns?\s+Well\b"
             r"|\bReaches?\s+(?:Under|Corners)\b|\bMoves?\s+Smoothly\b", "Good Maneuverability"),
            (r"\bPicks?\s+Up\s+Pet\s+Hair\b|\bGreat\s+(?:For|On)\s+Pet\s+Hair\b"
             r"|\bRemoves?\s+(?:Dog|Cat)\s+Hair\b", "Picks Up Pet Hair"),
            (r"\bEffective\s+Filtration\b|\bRemoves?\s+Allergens?\b"
             r"|\bCleans?\s+Air\s+Well\b|\bHEPA\s+Works?\b|\bPurifies?\s+Air\b", "Effective Filtration"),
            (r"\bEasy\s+Filter\s+Change\b|\bFilter\s+Easy\s+To\s+Replace\b"
             r"|\bSimple\s+Filter\s+Swap\b", "Easy Filter Change"),
            # ── Universal cross-category ─────────────────────────────────────
            (r"\bLong.Lasting\b|\bLasts?\s+For\s+Years?\b|\bStill\s+Going\s+Strong\b"
             r"|\bHeld\s+Up\s+Well\b|\bBuilt\s+To\s+Last\b", "Long-Lasting"),
            (r"\bEasy\s+To\s+Maintain\b|\bLow\s+Maintenance\b|\bSimple\s+Upkeep\b"
             r"|\bMinimal\s+Maintenance\b|\bMaintenance\s+Free\b", "Easy To Maintain"),
        ]
    elif side == "detractor":
        direct_patterns = [
            (r"\bHard Cleanup\b|\bMessy Cleanup\b|\bDifficult To Clean\b", "Hard To Clean"),
            (r"\bNoisy\b|\bNoise Issue\b|\bToo Noisy\b|\bExcess Noise\b|\bToo Loud\b", "Loud"),
            (r"\bConnectivity Problem(s)?\b|\bConnection Problem(s)?\b", "Connectivity Issues"),
            (r"\bBattery Life Issue(s)?\b", "Short Battery Life"),
            (r"\bSlow Charge\b", "Slow Charging"),
            (r"\bBad Smell\b|\bChemical Smell\b", "Unpleasant Scent"),
            (r"\bBad Flavor\b|\bAwful Taste\b", "Bad Taste"),
            (r"\bRuns Small\b|\bRuns Large\b|\bDoes Not Fit\b", "Wrong Size"),
            (r"\bBad Directions\b|\bConfusing Instructions\b", "Instructions Unclear"),
            (r"\bHard Install\b|\bHard Installation\b|\bAssembly Is Hard\b", "Difficult Setup"),
            (r"\bHard To Use\b|\bConfusing To Use\b|\bAwkward To Use\b", "Difficult To Use"),
            (r"\bDoesn'?T Work\b|\bDidn'?T Work\b|\bPoor Results\b", "Poor Performance"),
            (r"\bDisappointed\b|\bWould Not Recommend\b|\bRegret Buying\b", "Overall Dissatisfaction"),
            # ── Hair care ─────────────────────────────────────────────────────
            (r"\bFrizzy\s+Hair\b|\bHair\s+(?:Is\s+)?Frizzy\b|\bStill\s+Frizzy\b"
             r"|\bMakes?\s+(?:My\s+)?Hair\s+(?:More\s+)?Frizzy\b"
             r"|\bFrizzy\s+Mess\b", "Doesn't Reduce Frizz"),
            (r"\bHair\s+(?:Is\s+)?Fried\b|\bFried\s+(?:My\s+)?Hair\b"
             r"|\bHeat\s+Damag(?:e|ed|es|ing)\b|\bCaused?\s+(?:Heat\s+)?Damage\b"
             r"|\bDamaged?\s+(?:My\s+)?Hair\b|\bHair\s+Breakage\b", "Heat Damage"),
            (r"\bGets?\s+(?:Too\s+|Very\s+|Dangerously\s+)?Hot\b"
             r"|\bRuns?\s+(?:Too\s+)?Hot\b|\bBurns?\s+(?:My\s+)?(?:Hair|Scalp)\b"
             r"|\bScalding\b|\bScorching\b", "Too Hot"),
            (r"\bAttachments?\s+(?:(?:Keep(?:s)?|Kept)\s+)?(?:Fall(?:s|ing)?|Come?s?\s+Off|Pop(?:s|ped)?\s+Off"
             r"|Fly(?:ing)?\s+Off|Won'?t?\s+Stay)\b"
             r"|\bHard\s+To\s+(?:Attach|Connect)\s+(?:The\s+)?Attachments?\b", "Attachment Issues"),
            (r"\bCord\s+(?:Is\s+)?(?:Too\s+)?Short\b|\bShort\s+Cord\b"
             r"|\bNot\s+Enough\s+Cord\b|\bLimited\s+(?:By\s+(?:The\s+)?)?Cord\b"
             r"|\bCord\s+Doesn'?t\s+Reach\b", "Short Cord"),
            (r"\bToo\s+Heavy\b|\bHeavy\s+(?:To\s+Hold|(?:For\s+)?Daily\s+Use)\b"
             r"|\bArm\s+(?:Gets?\s+Tired|Fatigue)\b|\bWrist\s+Strain\b"
             r"|\bHand\s+Fatigue\b|\bTiring\s+To\s+(?:Hold|Use)\b", "Heavy"),
            (r"\bSlow(?:er)?\s+Dry(?:ing)?\b|\bTakes?\s+(?:Forever|Too\s+Long)\s+To\s+Dry\b"
             r"|\bSlow\s+Dry\s+Time\b|\bStill\s+(?:Wet|Damp)\s+After\b", "Slow Drying"),
            (r"\bPoor\s+Build\s+Quality\b|\bFeels?\s+Cheap\b|\bFlimsy\s+Plastic\b"
             r"|\bCheaply\s+Made\b|\bPoor\s+Construction\b"
             r"|\bBroke\s+(?:After|Within|In)\b", "Poor Build Quality"),
            # ── Vacuum & Cleaning ──────────────────────────────────────────
            (r"\bWeak(?:er)?\s+Suction\b|\bPoor\s+Suction\b|\bLow\s+Suction\b"
             r"|\bNo\s+Suction\b|\bLoss\s+Of\s+Suction\b", "Weak Suction"),
            (r"\bGets?\s+Stuck\b|\bPoor\s+Navigation\b"
             r"|\bFalls?\s+Off\s+(?:Ledge|Stairs?|Edge)\b|\bMisses?\s+Spots?\b", "Poor Navigation"),
            (r"\bClogs?\s+(?:Easily|Up|Fast|Quickly)\b"
             r"|\bKeeps?\s+(?:Getting\s+)?Clogged\b", "Clogs Easily"),
            (r"\bHair\s+(?:Tangles?|Wraps?)\s+(?:In|Around)\s+(?:The\s+)?Brush\b"
             r"|\bBrush\s+(?:Roll\s+)?(?:Gets?|Is)\s+Tangled\b", "Tangled Brush Roll"),
            (r"\bHard\s+To\s+Empty\b|\bMessy\s+To\s+Empty\b|\bDust\s+Flies\b"
             r"|\bDifficult\s+(?:To\s+)?Empty\b", "Hard To Empty"),
            (r"\bFilter\s+(?:Is\s+)?Expensive\b|\bExpensive\s+Filters?\b"
             r"|\bFilters?\s+(?:Cost|Are)\s+Too\s+(?:Much|Pricey|Expensive)\b", "Filter Expensive"),
            (r"\bLimited\s+Coverage\b|\bNot\s+Powerful\s+Enough\s+For\s+(?:The\s+)?Room\b"
             r"|\bToo\s+Small\s+For\s+(?:The\s+)?Room\b", "Limited Coverage"),
            (r"\bShort\s+Filter\s+Life\b|\bFilter\s+(?:Wears?|Runs?)\s+Out\s+(?:Fast|Quickly)\b", "Short Filter Life"),
            # ── Universal cross-category ─────────────────────────────────────
            (r"\bStopped\s+Working\s+After\b"
             r"|\bDied\s+After\s+(?:Only\s+)?(?:\d+\s+)?(?:Days?|Weeks?|Months?)\b"
             r"|\bOnly\s+Lasted\s+(?:\d+\s+)?(?:Days?|Weeks?|Months?)\b"
             r"|\bShort\s+Lifespan\b", "Short Lifespan"),
            (r"\bDead\s+On\s+Arrival\b|\bDOA\b|\bArrived\s+(?:Broken|Not\s+Working|Dead)\b"
             r"|\bDidn'?t\s+Work\s+Out\s+Of\s+(?:The\s+)?Box\b"
             r"|\bWon'?t\s+Turn\s+On\b|\bWouldn'?t\s+Turn\s+On\b", "Dead On Arrival"),
            (r"\bMissing\s+Parts?\b|\bParts?\s+Missing\b|\bIncomplete\s+Package\b"
             r"|\bWrong\s+Parts?\s+Included\b|\bMissing\s+Piece\b", "Missing Parts"),
        ]

    for pattern, replacement in direct_patterns:
        if re.search(pattern, cleaned, flags=re.IGNORECASE):
            return replacement

    rule = _best_matching_rule(cleaned, side=side)
    if rule is not None:
        return _clean_label(rule.get("canonical"))

    cleaned = re.sub(r"\bUsability\b", "Use", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bEasy Cleanup\b", "Easy To Clean", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bHard Cleanup\b", "Hard To Clean", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bNoisy\b", "Loud", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bQuiet Operation\b", "Quiet", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bConnectivity Problem(s)?\b", "Connectivity Issues", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bBattery Life Issue(s)?\b", "Short Battery Life", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bFast Charge\b", "Fast Charging", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bSlow Charge\b", "Slow Charging", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;|/")
    return _clean_label(cleaned)


def _labels_equivalent(a: str, b: str) -> bool:
    if not a or not b:
        return False
    a_norm = _canon(a)
    b_norm = _canon(b)
    if a_norm == b_norm:
        return True
    a_tokens = set(_tokenize(a))
    b_tokens = set(_tokenize(b))
    if a_tokens and a_tokens == b_tokens:
        return True
    ratio = difflib.SequenceMatcher(None, a_norm, b_norm).ratio()
    if ratio >= 0.95:
        return True
    if a_tokens and b_tokens:
        overlap = len(a_tokens & b_tokens) / float(max(len(a_tokens), len(b_tokens)))
        if overlap >= 0.9:
            return True
    return False


def _merge_label_stream(values: Sequence[Any], side: Optional[str]) -> Tuple[List[str], Dict[str, List[str]]]:
    out: List[str] = []
    alias_map: Dict[str, List[str]] = {}
    for value in values or []:
        original = _clean_label(value)
        if not original:
            continue
        canonical = standardize_symptom_label(original, side=side)
        if not canonical:
            continue
        existing = next((label for label in out if _labels_equivalent(label, canonical)), None)
        target = existing or canonical
        if existing is None:
            out.append(canonical)
        if original != target:
            alias_map.setdefault(target, [])
            if original not in alias_map[target]:
                alias_map[target].append(original)
    return out, alias_map


def merge_alias_maps(*maps: Mapping[str, Sequence[str]] | None) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for amap in maps:
        for key, values in (amap or {}).items():
            canonical = _clean_label(key)
            if not canonical:
                continue
            bucket = out.setdefault(canonical, [])
            for value in values or []:
                alias = _clean_label(value)
                if alias and alias != canonical and alias not in bucket:
                    bucket.append(alias)
    return out


def build_alias_map_for_labels(
    delighters: Sequence[Any] | None = None,
    detractors: Sequence[Any] | None = None,
    *,
    extra_aliases: Mapping[str, Sequence[str]] | None = None,
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for side, labels in (("delighter", delighters or []), ("detractor", detractors or [])):
        for label in labels:
            canonical = standardize_symptom_label(label, side=side)
            if not canonical:
                continue
            bucket = out.setdefault(canonical, [])
            rule = _best_matching_rule(canonical, side=side)
            if rule is not None:
                for alias in rule.get("aliases", []) or []:
                    clean_alias = _clean_label(alias)
                    if clean_alias and clean_alias != canonical and clean_alias not in bucket:
                        bucket.append(clean_alias)
    return merge_alias_maps(out, extra_aliases)


def canonicalize_symptom_catalog(
    delighters: Sequence[Any] | None = None,
    detractors: Sequence[Any] | None = None,
) -> Tuple[List[str], List[str], Dict[str, List[str]]]:
    dels, del_aliases = _merge_label_stream(delighters or [], side="delighter")
    dets, det_aliases = _merge_label_stream(detractors or [], side="detractor")
    aliases = merge_alias_maps(del_aliases, det_aliases, build_alias_map_for_labels(dels, dets))
    return dels, dets, aliases


def infer_category(
    product_description: str = "",
    sample_reviews: Sequence[str] | None = None,
) -> Dict[str, Any]:
    """Infer the product category from the description and sample reviews.

    Product description keywords are weighted 2.5× over review keywords so
    that the declared product type is authoritative — a face-serum product
    doesn't route to vacuum_cleaning just because a review mentions "suction".
    """
    desc_norm = _canon(str(product_description or ""))
    review_texts = [str(text or "") for text in (sample_reviews or [])[:40]]
    review_norm  = _canon(" \n ".join(review_texts))

    scores: Dict[str, float] = {}
    hits:   Dict[str, List[str]] = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        score    = 0.0
        cat_hits: List[str] = []
        per_kw   = 1.0 if any(" " in _canon(kw) for kw in keywords) else 0.6

        for keyword in keywords:
            key_norm = _canon(keyword)
            if not key_norm:
                continue
            base_pts = 1.0 if " " in key_norm else 0.6
            if desc_norm and key_norm in desc_norm:
                # Product description is authoritative: 3.5× weight
                score += base_pts * 3.5
                cat_hits.append(keyword)
            elif review_norm and key_norm in review_norm:
                score += base_pts
                if keyword not in cat_hits:
                    cat_hits.append(keyword)

        if score > 0:
            scores[category] = score
            hits[category]   = cat_hits

    if not scores:
        return {"category": "general", "confidence": 0.0, "signals": []}

    ranked     = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_cat, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence   = max(0.15, min(0.98, best_score / max(best_score + second_score + 0.5, 1.0)))
    return {"category": best_cat, "confidence": round(confidence, 2), "signals": hits.get(best_cat, [])[:6]}


def category_label(category: Any) -> str:
    mapping = {
        "general": "General Merchandise",
        "kitchen_appliance": "Kitchen Appliance",
        "beauty_personal_care": "Beauty / Personal Care",
        "hair_care": "Hair Care",
        "vacuum_cleaning": "Vacuum & Cleaning",
        "air_quality": "Air Quality & Climate",
        "electronics": "Electronics",
        "apparel_footwear": "Apparel / Footwear",
        "furniture_home": "Furniture / Home",
        "tools_outdoors": "Tools / Outdoors",
        "food_beverage": "Food / Beverage",
        "pet": "Pet",
    }
    return mapping.get(str(category or "general"), str(category or "general").replace("_", " ").title())


def starter_pack_for_category(category: Any) -> Dict[str, Any]:
    key = str(category or "general")
    pack = CATEGORY_PACKS.get(key) or CATEGORY_PACKS["general"]
    dels = [_clean_label(item.get("label")) for item in pack.get("delighters", []) if _clean_label(item.get("label"))]
    dets = [_clean_label(item.get("label")) for item in pack.get("detractors", []) if _clean_label(item.get("label"))]
    aliases: Dict[str, List[str]] = {}
    for side_key in ("delighters", "detractors"):
        for item in pack.get(side_key, []):
            label = _clean_label(item.get("label"))
            if not label:
                continue
            aliases[label] = _dedupe_keep_order(item.get("aliases", []) or [])
    return {"delighters": dels, "detractors": dets, "aliases": aliases, "category": key}


def select_supported_category_pack(
    category: Any,
    sample_reviews: Sequence[str] | None,
    *,
    min_hits: int = 1,
    max_per_side: int = 6,
) -> Dict[str, Any]:
    pack = starter_pack_for_category(category)
    reviews = [str(text or "") for text in (sample_reviews or []) if str(text or "").strip()]
    if not reviews:
        return {"delighters": [], "detractors": [], "aliases": {}, "category": pack.get("category", "general")}

    def _count_hits(label: str, aliases: Sequence[str]) -> int:
        phrases = [_canon(label)] + [_canon(alias) for alias in aliases]
        count = 0
        for review in reviews:
            review_norm = _canon(review)
            if any(phrase and phrase in review_norm for phrase in phrases):
                count += 1
        return count

    out_dels: List[str] = []
    out_dets: List[str] = []
    alias_map: Dict[str, List[str]] = {}
    for label in pack.get("delighters", []):
        aliases = list(pack.get("aliases", {}).get(label, []))
        if _count_hits(label, aliases) >= min_hits:
            out_dels.append(label)
            alias_map[label] = aliases
    for label in pack.get("detractors", []):
        aliases = list(pack.get("aliases", {}).get(label, []))
        if _count_hits(label, aliases) >= min_hits:
            out_dets.append(label)
            alias_map[label] = aliases
    return {
        "delighters": out_dels[:max_per_side],
        "detractors": out_dets[:max_per_side],
        "aliases": alias_map,
        "category": pack.get("category", "general"),
    }


THEME_ALIASES: Dict[str, Tuple[str, ...]] = {
    "Overall Sentiment": ("Overall", "Satisfaction", "Dissatisfaction", "Sentiment"),
    "Value": ("Price", "Pricing", "Value For Money", "Affordability"),
    "Performance": ("Results", "Effectiveness", "Output", "Power"),
    "Quality & Durability": ("Quality", "Build Quality", "Durability", "Materials"),
    "Ease Of Use": ("Usability", "Ease Of Use", "Ease", "Controls"),
    "Reliability": ("Consistency", "Dependability", "Longevity"),
    "Cleaning & Maintenance": ("Cleaning", "Maintenance", "Cleanup", "Care"),
    "Time Efficiency": ("Speed", "Time", "Convenience"),
    "Noise": ("Sound", "Volume", "Quietness"),
    "Setup & Instructions": ("Setup", "Assembly", "Installation", "Instructions", "Documentation"),
    "Size & Fit": ("Size", "Fit", "Capacity", "Portability"),
    "Design & Ergonomics": ("Design", "Ergonomics", "Handling", "Look", "Appearance"),
    "Comfort": ("Comfort", "Feel", "Wearability"),
    "Packaging & Delivery": ("Packaging", "Delivery", "Shipping"),
    "Compatibility & Connectivity": ("Compatibility", "Connectivity", "Pairing", "Integration", "App"),
    "Power & Battery": ("Battery", "Charging", "Power Management"),
    "Sensory Experience": ("Scent", "Taste", "Texture", "Flavor", "Smell"),
    "Skin / Formula": ("Skin", "Formula", "Ingredients"),
    "Safety": ("Safety", "Burn", "Leak", "Damage", "Overheating"),
    "Hair Results": (
        "Frizz", "Frizz Control", "Heat Damage", "Blowout", "Smoothness",
        "Shine", "Hair Damage", "Thermal Damage",
    ),
    "Hair Tool Ergonomics": (
        "Cord Length", "Weight", "Ergonomics", "Attachment Fit", "Attachments",
    ),
    "Suction & Pickup": (
        "Suction Power", "Suction", "Pickup Power", "Cleaning Performance",
        "Pet Hair", "Debris Pickup",
    ),
    "Navigation & Mapping": (
        "Navigation", "Mapping", "Robot Navigation", "Obstacle Avoidance",
        "Floor Coverage", "Smart Mapping",
    ),
    "Filtration & Air Quality": (
        "Filtration", "Air Filtration", "HEPA", "Air Purification",
        "Filter", "Allergen Removal", "Air Quality",
    ),
    "Maintenance & Upkeep": (
        "Maintenance", "Filter Life", "Upkeep", "Filter Cost",
        "Brush Roll", "Clogging", "Self-Cleaning",
    ),
    "Product Lifespan": (
        "Lifespan", "Durability Over Time", "Longevity", "Long-Lasting",
        "Short Lifespan", "Premature Failure",
    ),
    "Product Specific": ("Product Specific", "Other"),
}

THEME_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "Overall Sentiment": ("overall", "satisfaction", "dissatisfaction", "recommend", "expectations", "happy", "disappointed"),
    "Value": ("value", "price", "priced", "expensive", "worth", "affordable", "overpriced"),
    "Performance": ("perform", "performance", "results", "effective", "effectiveness", "powerful", "works well", "poor performance"),
    "Quality & Durability": ("quality", "durable", "durability", "sturdy", "flimsy", "cheap", "break", "broken", "well made"),
    "Ease Of Use": ("easy", "difficult", "confusing", "intuitive", "awkward", "controls", "button", "handle", "hard to use"),
    "Reliability": ("reliable", "unreliable", "consistent", "inconsistent", "stopped working", "defective", "lasts", "holds up"),
    "Cleaning & Maintenance": ("clean", "cleanup", "maintenance", "filter", "wash", "messy", "grease", "lint"),
    "Time Efficiency": ("time", "quick", "fast", "slow", "takes too long", "time saving", "time saver"),
    "Noise": ("noise", "noisy", "loud", "quiet", "silent"),
    "Setup & Instructions": ("setup", "set up", "assembly", "install", "installation", "instructions", "manual", "directions", "guide"),
    "Size & Fit": ("size", "fit", "fits", "capacity", "big", "small", "bulky", "compact", "heavy", "lightweight"),
    "Design & Ergonomics": ("design", "ergonomic", "look", "appearance", "beautiful", "stylish", "attachment", "lid", "door", "basket", "cord", "handle"),
    "Comfort": ("comfortable", "uncomfortable", "supportive", "hurts", "painful"),
    "Packaging & Delivery": ("packaging", "package", "shipping", "arrived", "box", "delivery", "damaged"),
    "Compatibility & Connectivity": ("connect", "connection", "connectivity", "pair", "pairing", "compatible", "app", "wifi", "bluetooth"),
    "Power & Battery": ("battery", "charge", "charging", "recharge", "power", "cordless"),
    "Sensory Experience": ("scent", "smell", "taste", "flavor", "texture", "fragrance", "consistency"),
    "Skin / Formula": ("skin", "formula", "ingredients", "irritat", "burns", "itchy", "rash", "gentle"),
    "Safety": ("safety", "danger", "dangerous", "burn", "hot", "overheat", "smoke", "fire", "leak", "sharp", "scorching", "scalding"),
    "Hair Results": (
        "frizz", "frizzy", "flyaway", "blowout", "salon", "smooth hair",
        "shiny", "glossy", "heat damage", "hair damage", "fried",
        "reduces frizz", "frizz-free", "frizz free", "gentle on hair",
    ),
    "Hair Tool Ergonomics": (
        "attachment", "attachments", "diffuser", "concentrator", "nozzle",
        "cord", "cable", "heavy", "lightweight", "arm fatigue", "wrist",
    ),
    "Suction & Pickup": (
        "suction", "pick up", "pickup", "debris", "dirt", "pet hair",
        "strong suction", "weak suction", "suction power", "leaves behind",
    ),
    "Navigation & Mapping": (
        "navigation", "mapping", "stuck", "obstacle", "bump", "cliff",
        "gets stuck", "misses spots", "robot", "autonomous", "sensors",
    ),
    "Filtration & Air Quality": (
        "filter", "filtration", "hepa", "allergen", "particle", "pm2",
        "purification", "air quality", "captures", "removes particles",
    ),
    "Maintenance & Upkeep": (
        "maintenance", "filter change", "filter life", "clog", "clogs",
        "brush roll", "tangles", "upkeep", "service", "replacement",
    ),
    "Product Lifespan": (
        "lifespan", "lasted", "died after", "stopped working after",
        "broke after", "only lasted", "fell apart", "long-lasting",
    ),
}



def canonical_theme_name(theme: Any) -> str:
    cleaned = _clean_label(theme)
    if not cleaned:
        return ""
    norm = _canon(cleaned)
    exact_lookup = {_canon(canonical): canonical for canonical in THEME_ALIASES}
    if norm in exact_lookup:
        return exact_lookup[norm]
    for canonical, aliases in THEME_ALIASES.items():
        alias_norms = {_canon(canonical)} | {_canon(alias) for alias in aliases}
        if norm in alias_norms:
            return canonical
    theme_tokens = set(_tokenize(cleaned))
    best: Tuple[float, str] = (0.0, "")
    for canonical, aliases in THEME_ALIASES.items():
        candidates = [canonical] + list(aliases)
        for cand in candidates:
            ratio = difflib.SequenceMatcher(None, norm, _canon(cand)).ratio()
            cand_tokens = set(_tokenize(cand))
            if theme_tokens and cand_tokens:
                ratio = max(ratio, len(theme_tokens & cand_tokens) / float(max(len(theme_tokens), len(cand_tokens))))
            if ratio > best[0]:
                best = (ratio, canonical)
    if best[0] >= 0.72:
        return best[1]
    return cleaned




# ---------------------------------------------------------------------------
# Pre-compiled regexes for infer_l1_theme fast-paths
# (defined at module scope so they compile once, not on every function call)
# ---------------------------------------------------------------------------
_VACUUM_SUCTION_RE = re.compile(
    r"\b(?:strong\s+suction|weak\s+suction|suction\s+power|suction\s+loss"
    r"|picks?\s+up\s+(?:everything|debris|pet\s+hair|dirt)"
    r"|leaves?\s+(?:debris|dirt)\s+behind|doesn'?t\s+pick\s+up"
    r"|pet\s+hair|debris\s+pickup|cleaning\s+performance"
    r"|no\s+suction|loss\s+of\s+suction)\b",
    flags=re.IGNORECASE,
)
_VACUUM_NAVIGATE_RE = re.compile(
    r"\b(?:gets?\s+stuck|poor\s+navigation|good\s+maneuverability"
    r"|bumps?\s+into|falls?\s+off\s+(?:ledge|stair|edge)"
    r"|misses?\s+spots?|obstacle\s+avoidance|smart\s+mapping"
    r"|tangled?\s+brush|brush\s+roll|poor\s+obstacle\s+avoidance)\b",
    flags=re.IGNORECASE,
)
_VACUUM_MAINT_RE = re.compile(
    r"\b(?:filter\s+(?:expensive|life|issues?|change|cost|too\s+often)"
    r"|clogs?\s+(?:easily|fast|up|frequently)|dustbin"
    r"|easy\s+to\s+(?:empty|maintain)|hard\s+to\s+empty"
    r"|short\s+filter\s+life|brush\s+gets?\s+tangled|tangled\s+brush\s+roll)\b",
    flags=re.IGNORECASE,
)
_FILTRATION_RE = re.compile(
    r"\b(?:effective\s+filtration|hepa|air\s+(?:quality|purification|filtration)"
    r"|removes?\s+allergens?|captures?\s+particles?|air\s+cleaner"
    r"|filter\s+(?:issues?|expensive|life)|limited\s+coverage)\b",
    flags=re.IGNORECASE,
)
_LIFESPAN_RE = re.compile(
    r"\b(?:short\s+lifespan|long.lasting|died\s+after|only\s+lasted"
    r"|stopped\s+working\s+after|broke\s+(?:after|down)|premature\s+failure"
    r"|dead\s+on\s+arrival|missing\s+parts?)\b",
    flags=re.IGNORECASE,
)


def infer_l1_theme(
    label: Any,
    *,
    side: Optional[str] = None,
    family: Any = "",
    theme: Any = "",
    category: Any = "general",
) -> str:
    for raw in (theme, family):
        canonical = canonical_theme_name(raw)
        if canonical:
            return canonical
    cleaned = standardize_symptom_label(label, side=side) or _clean_label(label)
    if not cleaned:
        return "Product Specific"
    label_norm = _canon(cleaned)

    # ── Fast-path: hair-care label routing ─────────────────────────────────
    # These must be checked before the generic rule/keyword scoring because
    # "Heat Damage" would otherwise route to "Performance" (via _best_matching_rule
    # → "Performance" family) and "Reduces Frizz" / "Frizz" fall through to
    # "Product Specific" with no keyword match.
    _HAIR_RESULTS_RE = re.compile(
        r"\b(?:heat\s+damage|hair\s+damage|thermal\s+damage|fried.*hair|hair.*fried"
        r"|frizz(?:y)?|frizz.free|reduces\s+frizz|doesn.?t\s+reduce\s+frizz"
        r"|no\s+frizz|flyaway|blowout|salon.quality|salon\s+results"
        r"|shiny\s+hair|smooth\s+(?:finish|hair)|glossy|gentle\s+on\s+hair"
        r"|no\s+heat\s+damage|fast\s+dry(?:ing)?|dries\s+quickly|slow\s+dry(?:ing)?)\b",
        flags=re.IGNORECASE,
    )
    _HAIR_ERGONOMICS_RE = re.compile(
        r"\b(?:attachment\s+issues?|easy\s+attachment|short\s+cord|long\s+cord"
        r"|cord\s+(?:length|too\s+short)|diffuser|concentrator|nozzle"
        r"|(?:too\s+)?heavy|lightweight|arm\s+(?:gets?\s+tired|fatigue)"
        r"|wrist\s+(?:strain|fatigue)|hand\s+fatigue)\b",
        flags=re.IGNORECASE,
    )
    _HAIR_SAFETY_RE = re.compile(
        r"\b(?:heat\s+damage|too\s+hot|dangerously\s+hot|burns?\s+(?:hair|scalp)"
        r"|scorching|thermal\s+damage)\b",
        flags=re.IGNORECASE,
    )

    if _HAIR_SAFETY_RE.search(label_norm):
        return "Safety"
    if _HAIR_RESULTS_RE.search(label_norm):
        return "Hair Results"
    if _HAIR_ERGONOMICS_RE.search(label_norm):
        return "Hair Tool Ergonomics"

    # ── Vacuum / cleaning fast-paths (module-level constants defined below) ───
    if _VACUUM_SUCTION_RE.search(label_norm):
        return "Suction & Pickup"
    if _VACUUM_NAVIGATE_RE.search(label_norm):
        return "Navigation & Mapping"
    if _VACUUM_MAINT_RE.search(label_norm):
        return "Maintenance & Upkeep"
    if _FILTRATION_RE.search(label_norm):
        return "Filtration & Air Quality"
    if _LIFESPAN_RE.search(label_norm):
        return "Product Lifespan"

        # ── Generic fast-paths ──────────────────────────────────────────────────
    if re.search(r"\b(hard|difficult|awkward|confusing)\b.*\b(open|close|remove|attach|detach|press|reach|use)\b", label_norm):
        return "Ease Of Use"
    if re.search(r"\b(filter|door|lid|attachment|button|control|handle)\b", label_norm) and re.search(r"\b(open|close|remove|attach|detach|press|reach)\b", label_norm):
        return "Ease Of Use"
    rule = _best_matching_rule(cleaned, side=side)
    if rule is not None:
        canonical = canonical_theme_name(rule.get("family"))
        if canonical:
            return canonical
    label_tokens = set(_tokenize(cleaned))
    best: Tuple[float, str] = (0.0, "Product Specific")
    for canonical, keywords in THEME_KEYWORDS.items():
        score = 0.0
        for keyword in keywords:
            key_norm = _canon(keyword)
            if not key_norm:
                continue
            if " " in key_norm and key_norm in label_norm:
                score += 1.35
            elif key_norm in label_norm:
                score += 0.85
            else:
                key_tokens = set(_tokenize(keyword))
                if label_tokens and key_tokens:
                    overlap = len(label_tokens & key_tokens) / float(max(len(label_tokens), len(key_tokens)))
                    if overlap >= 0.5:
                        score += overlap
        if score > best[0]:
            best = (score, canonical)
    if best[0] >= 0.85:
        return best[1]
    bucket = bucket_symptom_label(cleaned, side=side, category=category)
    if bucket == "Universal Neutral":
        matched = canonical_theme_name(family) or canonical_theme_name(theme)
        if matched:
            return matched
    return "Product Specific"



def build_structured_taxonomy_rows(
    delighters: Sequence[Any] | None = None,
    detractors: Sequence[Any] | None = None,
    *,
    aliases: Mapping[str, Sequence[str]] | None = None,
    category: Any = "general",
    preview_items: Sequence[Mapping[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    alias_map = merge_alias_maps(aliases)
    preview_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for raw in preview_items or []:
        side_key = str(raw.get("side") or "").strip().lower()
        if side_key not in {"delighter", "detractor"}:
            continue
        canonical = standardize_symptom_label(raw.get("label"), side=side_key)
        if not canonical:
            continue
        preview_lookup[(side_key, canonical)] = dict(raw)

    rows: List[Dict[str, Any]] = []
    bucket_priority = {"Universal Neutral": 0, "Category Driver": 1, "Product Specific": 2}
    for side_key, labels in (("delighter", delighters or []), ("detractor", detractors or [])):
        seen = set()
        for raw_label in labels:
            label = standardize_symptom_label(raw_label, side=side_key)
            if not label or label in seen:
                continue
            seen.add(label)
            meta = preview_lookup.get((side_key, label), {})
            family = _clean_label(meta.get("family"))
            declared_theme = _clean_label(meta.get("theme") or meta.get("l1_theme") or meta.get("l1"))
            l1_theme = infer_l1_theme(label, side=side_key, family=family, theme=declared_theme, category=category)
            bucket = str(meta.get("bucket") or bucket_symptom_label(label, side=side_key, category=category) or "Product Specific")
            merged_aliases = _dedupe_keep_order(list(alias_map.get(label, [])) + list(meta.get("aliases") or []))
            rows.append({
                "L1 Theme": l1_theme,
                "L2 Symptom": label,
                "Side": "Delighter" if side_key == "delighter" else "Detractor",
                "Bucket": bucket,
                "Family": family or l1_theme,
                "Aliases": ", ".join(merged_aliases) if merged_aliases else "—",
                "Alias Count": len(merged_aliases),
                "Review Hits": int(meta.get("review_hits", 0) or 0),
                "Support %": round(float(meta.get("support_ratio", 0.0) or 0.0) * 100.0, 1),
                "Rationale": str(meta.get("rationale") or "").strip(),
                "Example": str((meta.get("examples") or [""])[0] or "").strip(),
                "side_key": side_key,
                "label": label,
            })
    rows.sort(
        key=lambda row: (
            0 if row.get("Side") == "Detractor" else 1,
            str(row.get("L1 Theme") or ""),
            bucket_priority.get(str(row.get("Bucket") or "Product Specific"), 9),
            -int(row.get("Review Hits", 0) or 0),
            str(row.get("L2 Symptom") or ""),
        )
    )
    return rows



def suggest_taxonomy_merges(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    max_suggestions: int = 12,
) -> List[Dict[str, Any]]:
    candidates = [dict(row) for row in (rows or []) if str(row.get("L2 Symptom") or "").strip()]
    suggestions: List[Dict[str, Any]] = []
    seen_pairs = set()
    for idx, left in enumerate(candidates):
        left_label = str(left.get("L2 Symptom") or "").strip()
        left_side = str(left.get("side_key") or str(left.get("Side") or "").lower()).strip().lower()
        left_theme = str(left.get("L1 Theme") or "").strip()
        left_hits = int(left.get("Review Hits", 0) or 0)
        left_norm = _canon(left_label)
        left_tokens = set(_tokenize(left_label))
        for right in candidates[idx + 1:]:
            right_label = str(right.get("L2 Symptom") or "").strip()
            right_side = str(right.get("side_key") or str(right.get("Side") or "").lower()).strip().lower()
            right_theme = str(right.get("L1 Theme") or "").strip()
            if not right_label or left_side != right_side or left_theme != right_theme:
                continue
            pair_key = tuple(sorted((left_label, right_label)))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            right_hits = int(right.get("Review Hits", 0) or 0)
            right_norm = _canon(right_label)
            ratio = difflib.SequenceMatcher(None, left_norm, right_norm).ratio()
            right_tokens = set(_tokenize(right_label))
            overlap = 0.0
            if left_tokens and right_tokens:
                overlap = len(left_tokens & right_tokens) / float(max(len(left_tokens), len(right_tokens)))
            subset_match = bool(left_norm in right_norm or right_norm in left_norm)
            if ratio < 0.84 and overlap < 0.75 and not subset_match:
                continue
            keep_label, merge_label = left_label, right_label
            keep_hits, merge_hits = left_hits, right_hits
            if (right_hits, -len(right_label), right_label) > (left_hits, -len(left_label), left_label):
                keep_label, merge_label = right_label, left_label
                keep_hits, merge_hits = right_hits, left_hits
            reason_bits = []
            if ratio >= 0.9:
                reason_bits.append("very similar wording")
            elif overlap >= 0.75:
                reason_bits.append("high token overlap")
            if subset_match:
                reason_bits.append("one label contains the other")
            suggestions.append({
                "L1 Theme": left_theme or "Product Specific",
                "Side": "Delighter" if left_side == "delighter" else "Detractor",
                "Keep": keep_label,
                "Merge": merge_label,
                "Keep Hits": keep_hits,
                "Merge Hits": merge_hits,
                "Why": ", ".join(reason_bits) or "similar concept",
                "Similarity": round(max(ratio, overlap), 2),
            })
    suggestions.sort(key=lambda row: (row.get("Similarity", 0), row.get("Keep Hits", 0), -len(str(row.get("Keep") or ""))), reverse=True)
    return suggestions[:max_suggestions]


def taxonomy_prompt_context(
    category: Any,
    *,
    include_pack: bool = True,
    max_labels_per_side: int = 6,
) -> str:
    pack = starter_pack_for_category(category)
    parts = [f"Likely category: {category_label(category)}."]
    if include_pack:
        del_hint = ", ".join(pack.get("delighters", [])[:max_labels_per_side]) or "None"
        det_hint = ", ".join(pack.get("detractors", [])[:max_labels_per_side]) or "None"
        parts.append(f"Category-general delighter patterns to consider: {del_hint}.")
        parts.append(f"Category-general detractor patterns to consider: {det_hint}.")
    parts.append(
        "Use systematic label naming. Do not output the same concept twice with different wording. "
        "Prefer concise Title Case labels, broad paired labels for universal themes, and specific product labels only when they describe a recurring concrete issue or strength."
    )
    return " ".join(parts)
