"""Taxonomy intelligence: AI-powered taxonomy building, product knowledge,
category inference, and seed candidate generation.

Extracted from app.py for cleaner organization.
"""
from __future__ import annotations
import textwrap
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
import sys
def _app():
    return sys.modules.get('__main__', sys.modules.get('app'))

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _coerce_product_knowledge_list(items, max_items=20):
    if not items: return []
    if isinstance(items, str):
        items = [s.strip() for s in items.split(",") if s.strip()]
    return [str(x).strip() for x in list(items)[:max_items] if str(x).strip()]

def _titleize_theme_label(label):
    s = str(label or "").strip()
    if not s: return ""
    return " ".join(w.capitalize() for w in s.split())

def _normalize_tag_list(tags):
    if not tags: return []
    seen = set()
    out = []
    for t in tags:
        s = str(t).strip()
        if not s or s.upper() in NON_VALUES: continue
        key = s.lower()
        if key not in seen:
            seen.add(key)
            out.append(s)
    return out

def _tokenize(text):
    return set(re.findall(r"[a-z]{2,}", str(text or "").lower()))


def _ai_build_symptom_list(*, client, product_description, sample_reviews, product_knowledge=None):
    category_info = _app()._infer_taxonomy_category(product_description, sample_reviews)
    category = category_info.get("category", "general")
    sample_reviews = [_trunc(str(review or "").strip(), 420) for review in list(sample_reviews or [])[:60] if str(review or "").strip()]
    min_hits = 1 if len(sample_reviews) <= 15 else 2
    product_knowledge = _app()._normalize_product_knowledge(product_knowledge)
    supported_pack = _app()._select_supported_category_pack(category, sample_reviews, min_hits=min_hits, max_per_side=10)
    generic_seed_pack = _derive_csat_seed_candidates(product_description, sample_reviews, product_knowledge, category)
    # Augment with knowledge-driven candidates
    try:
        knowledge_candidates = _knowledge_driven_taxonomy_candidates(product_knowledge, product_description, sample_reviews, category)
        # Merge knowledge candidates into the seed pack
        for label in knowledge_candidates.get("delighters", []):
            if label not in generic_seed_pack.get("delighters", []):
                generic_seed_pack.setdefault("delighters", []).append(label)
        for label in knowledge_candidates.get("detractors", []):
            if label not in generic_seed_pack.get("detractors", []):
                generic_seed_pack.setdefault("detractors", []).append(label)
        for k, v in knowledge_candidates.get("aliases", {}).items():
            if k not in generic_seed_pack.get("aliases", {}):
                generic_seed_pack.setdefault("aliases", {})[k] = v
    except Exception:
        pass
    neutral_dels, neutral_dets = _app()._universal_neutral_catalog()
    excluded_universal = {
        "delighters": list(neutral_dels),
        "detractors": list(neutral_dets),
    }
    knowledge_context = _app()._product_knowledge_context_text(product_knowledge, limit_per_section=6)
    sys = textwrap.dedent(f"""
        You are a consumer insights expert building a reusable first-pass symptom taxonomy for product review analysis.
        The taxonomy must work for ANY consumer product category and stay useful to consumer insights, quality engineers, product developers, CX, and brand teams.
        DELIGHTERS = praised features, strengths, positive outcomes.
        DETRACTORS = problems, failures, frustrations, negative outcomes.

        {_app()._taxonomy_prompt_context(category)}

        PRODUCT KNOWLEDGE RULES:
        - The GENERIC SEED CANDIDATES below were systematically derived from structured product knowledge:
          • Each product_area generated component-level symptom pairs (e.g., Filter → "Easy Filter Cleaning" / "Filter Hard To Clean")
          • Each desired_outcome generated achievement/failure pairs
          • Each likely_failure_mode became a direct detractor candidate
          • Each workflow_step generated usability pairs
        - These seeds are your starting point — validate each against the sample reviews and KEEP labels that have review support.
        - ADD new labels for patterns you see in reviews that the seeds missed.
        - REMOVE seeds that have zero review support and don't match any review pattern.
        - When the product clearly has important outcomes, create paired labels when supported, such as Fits As Expected vs Fit Does Not Match Expectation, Long Battery Life vs Battery Dies Fast, Strong Suction vs Weak Suction, Hydrates Well vs Not Hydrating Enough, or Keeps Drinks Cold vs Loses Cold Quickly.
        - Prefer concrete customer-facing labels that are still useful to cross-functional teams.
        - When in doubt, prefer component + mode labels such as Filter Door Hard To Open, Lid Leaks In Bag, App Mapping Is Confusing, or Steam Wand Hard To Clean over vague catch-alls.

        SYSTEMATIC LABELING RULES:
        - Do not call the same concept two different things. Collapse near-duplicates to one canonical label.
        - Use concise Title Case labels, usually 2-6 words.
        - Universal Neutral Symptoms are managed elsewhere, so do NOT spend slots returning these labels: {', '.join(excluded_universal['delighters'] + excluded_universal['detractors'])}.
        - Separate your output into Category Drivers vs Product Specific labels.
        - Category Drivers should represent the major reasons customers are satisfied or dissatisfied in this category.
        - Product Specific labels should be concrete, engineer-usable recurring issues or strengths tied to components, workflow, setup, cleaning, fit, packaging, formula, or failure modes.
        - Keep the catalog MECE-ish in practice: avoid overlapping labels that describe the same issue at different levels of abstraction when one sharper label will do.
        - Keep each label cross-functional and actionable for consumer insights, quality, product, CX, and brand teams.
        - Avoid vague duplicates like Great Product, Good Features, Bad Experience, Poor Item.
        - Include aliases for common alternate phrasings reviewers use.
        - Keep the final list clean, deduplicated, and useful for downstream analytics tables.

        PRODUCT KNOWLEDGE SNAPSHOT:
        {knowledge_context or 'No structured product knowledge available.'}

        GENERIC FIRST-CUT SEED CANDIDATES TO CONSIDER:
        Delighters: {', '.join(generic_seed_pack.get('delighters', [])[:18]) or 'None'}
        Detractors: {', '.join(generic_seed_pack.get('detractors', [])[:22]) or 'None'}

        OUTPUT — strict JSON only:
        {{
          "category":"<short category>",
          "delighters":{{
            "category_drivers":[{{"label":"<Title Case 2-6 words>","theme":"<L1 theme>","aliases":["<alternate phrase>"],"family":"<short family>","rationale":"<why>"}}],
            "product_specific":[{{"label":"<Title Case 2-6 words>","theme":"<L1 theme>","aliases":["<alternate phrase>"],"family":"<short family>","rationale":"<why>"}}]
          }},
          "detractors":{{
            "category_drivers":[{{"label":"<Title Case 2-6 words>","theme":"<L1 theme>","aliases":["<alternate phrase>"],"family":"<short family>","rationale":"<why>"}}],
            "product_specific":[{{"label":"<Title Case 2-6 words>","theme":"<L1 theme>","aliases":["<alternate phrase>"],"family":"<short family>","rationale":"<why>"}}]
          }},
          "notes":"<short note>"
        }}
        Aim for roughly 6-10 category drivers and 12-24 product-specific labels per side based on actual review patterns. Favor broader coverage only when labels stay concrete, non-overlapping, and review-backed.
    """).strip()
    payload = dict(
        product_description=product_description or "General consumer product",
        product_knowledge=product_knowledge,
        category_hint=category,
        category_signals=category_info.get("signals", []),
        sample_reviews=sample_reviews,
        supported_category_drivers={
            "delighters": supported_pack.get("delighters", []),
            "detractors": supported_pack.get("detractors", []),
        },
        generic_seed_candidates={
            "delighters": generic_seed_pack.get("delighters", []),
            "detractors": generic_seed_pack.get("detractors", []),
        },
        universal_neutral_managed_separately=excluded_universal,
    )
    result_text = _app()._chat_complete_with_fallback_models(
        client,
        model=_app()._shared_model(),
        structured=True,
        messages=[{"role": "system", "content": sys}, {"role": "user", "content": json.dumps(payload)}],
        temperature=0.0,
        response_format={"type": "json_object"},
        max_tokens=4200,
        reasoning_effort=_app()._shared_reasoning(),
    )
    data = _safe_json_load(result_text)

    def _coerce_taxonomy_item(obj, *, bucket, seeded=False):
        if isinstance(obj, dict):
            label = str(obj.get("label", "")).strip()
            aliases = [str(a).strip() for a in (obj.get("aliases") or []) if str(a).strip()]
            family = _safe_text(obj.get("family"))
            theme = _safe_text(obj.get("theme") or obj.get("l1_theme") or obj.get("l1"))
            rationale = _safe_text(obj.get("rationale"))
        else:
            label = str(obj or "").strip()
            aliases = []
            family = ""
            theme = ""
            rationale = ""
        if not label:
            return None
        return {
            "label": label,
            "aliases": aliases,
            "family": family,
            "theme": theme,
            "rationale": rationale,
            "bucket": bucket,
            "seeded": bool(seeded),
        }

    def _parse_side(section):
        parsed = []
        if isinstance(section, dict):
            for bucket_key, bucket_name in (("category_drivers", "Category Driver"), ("product_specific", "Product Specific")):
                for raw in (section.get(bucket_key) or []):
                    item = _coerce_taxonomy_item(raw, bucket=bucket_name)
                    if item:
                        parsed.append(item)
            if not parsed:
                for raw in (section.get("items") or []):
                    item = _coerce_taxonomy_item(raw, bucket="Product Specific")
                    if item:
                        parsed.append(item)
        else:
            for raw in (section or []):
                item = _coerce_taxonomy_item(raw, bucket="Product Specific")
                if item:
                    parsed.append(item)
        return parsed

    seeded_delighters = [
        {
            "label": label,
            "aliases": list((supported_pack.get("aliases") or {}).get(label, [])) + list((generic_seed_pack.get("aliases") or {}).get(label, [])),
            "family": "",
            "theme": _app()._infer_taxonomy_l1_theme(label, side="delighter", category=category),
            "rationale": "Supported CSAT or category-general pattern from product knowledge and the review sample.",
            "bucket": "Category Driver",
            "seeded": True,
        }
        for label in _app()._dedupe_keep_order(list(supported_pack.get("delighters") or []) + list(generic_seed_pack.get("delighters") or []))
    ]
    seeded_detractors = [
        {
            "label": label,
            "aliases": list((supported_pack.get("aliases") or {}).get(label, [])) + list((generic_seed_pack.get("aliases") or {}).get(label, [])),
            "family": "",
            "theme": _app()._infer_taxonomy_l1_theme(label, side="detractor", category=category),
            "rationale": "Supported CSAT or category-general pattern from product knowledge and the review sample.",
            "bucket": "Category Driver",
            "seeded": True,
        }
        for label in _app()._dedupe_keep_order(list(supported_pack.get("detractors") or []) + list(generic_seed_pack.get("detractors") or []))
    ]

    ai_del_items = _parse_side(data.get("delighters") or [])
    ai_det_items = _parse_side(data.get("detractors") or [])

    prioritized_dels = _app()._prioritize_ai_taxonomy_items(
        seeded_delighters + ai_del_items,
        side="delighter",
        sample_reviews=sample_reviews,
        category=category,
        min_review_hits=min_hits,
        max_keep=36,
        exclude_universal=True,
    )
    prioritized_dets = _app()._prioritize_ai_taxonomy_items(
        seeded_detractors + ai_det_items,
        side="detractor",
        sample_reviews=sample_reviews,
        category=category,
        min_review_hits=min_hits,
        max_keep=42,
        exclude_universal=True,
    )

    merged_aliases = {}
    for item in prioritized_dels + prioritized_dets:
        label = _safe_text(item.get("label"))
        aliases = [str(v).strip() for v in (item.get("aliases") or []) if str(v).strip()]
        if label and aliases:
            merged_aliases[label] = aliases

    canon_dels, canon_dets, canon_aliases = _app()._standardize_symptom_lists(
        [item.get("label") for item in prioritized_dels],
        [item.get("label") for item in prioritized_dets],
    )
    alias_map = _app()._alias_map_for_catalog(
        canon_dels,
        canon_dets,
        extra_aliases=_app()._merge_taxonomy_alias_maps(
            canon_aliases,
            merged_aliases,
            supported_pack.get("aliases", {}),
            generic_seed_pack.get("aliases", {}),
        ),
    )

    def _finalize_preview(items, *, side):
        lookup = {str(item.get("label") or ""): dict(item) for item in items}
        ordered = canon_dels if side == "delighter" else canon_dets
        preview_rows = []
        for label in ordered:
            base = dict(lookup.get(label, {}))
            base.setdefault("label", label)
            base["bucket"] = base.get("bucket") or _app()._bucket_taxonomy_label(label, side=side, category=category)
            base["aliases"] = list(alias_map.get(label, base.get("aliases") or []))
            base.setdefault("family", "")
            base["l1_theme"] = _app()._infer_taxonomy_l1_theme(label, side=side, family=base.get("family"), theme=base.get("theme") or base.get("l1_theme"), category=category)
            base["side"] = side
            base.setdefault("rationale", "")
            base.setdefault("review_hits", 0)
            base.setdefault("support_ratio", 0.0)
            base.setdefault("score", 0.0)
            base.setdefault("specificity", 0.0)
            base.setdefault("examples", [])
            preview_rows.append(base)
        return preview_rows[:36]

    preview_dels = _finalize_preview(prioritized_dels, side="delighter")
    preview_dets = _finalize_preview(prioritized_dets, side="detractor")

    note = str(data.get("notes", "")).strip()
    if not note:
        note = (
            f"Detected category: {category}. Universal neutral labels were held out, CSAT and outcome drivers were seeded from product knowledge plus review evidence, "
            f"and product-specific labels were ranked by review support to keep the catalog systematic and useful."
        )
    return dict(
        delighters=list(canon_dels)[:36],
        detractors=list(canon_dets)[:42],
        aliases=alias_map,
        category=category,
        category_confidence=category_info.get("confidence", 0.0),
        taxonomy_note=note,
        preview_delighters=preview_dels,
        preview_detractors=preview_dets,
        product_knowledge=product_knowledge,
    )



def _knowledge_driven_taxonomy_candidates(product_knowledge, product_description="", sample_reviews=None, category="general"):
    """Systematically derive symptom candidates from structured product knowledge.

    Instead of dumping knowledge as text context, this function exploits each
    knowledge field to generate specific, actionable symptom pairs:

    - product_areas    → Component-level detractor/delighter pairs
    - desired_outcomes → Outcome achievement / failure pairs
    - likely_failure_modes → Direct detractor labels
    - workflow_steps   → Usability symptom pairs
    - use_cases        → Context-specific symptom candidates
    - csat_drivers     → Category-level satisfaction drivers
    - watchouts        → Pre-flagged risk areas as detractors

    Returns a dict with 'delighters', 'detractors', 'aliases', and 'generation_log'.
    """
    info = _app()._normalize_product_knowledge(product_knowledge)
    delighters, detractors = [], []
    aliases = {}
    generation_log = []
    corpus = "\n".join([_safe_text(product_description)] + [str(r) for r in (sample_reviews or [])[:40]]).lower()

    def _add(side, label, *, from_field="", aliases_list=None):
        label = _titleize_theme_label(label)
        if not label: return
        target = delighters if side == "del" else detractors
        if label not in target:
            target.append(label)
            generation_log.append({"label": label, "side": "delighter" if side == "del" else "detractor", "source": from_field})
        if aliases_list:
            aliases[label] = [str(a).strip() for a in aliases_list if str(a).strip()][:6]

    def _has_support(keywords):
        return any(kw.lower() in corpus for kw in keywords if kw)

    # ── 1. Product Areas → Component symptom pairs ──────────────────────
    component_templates = {
        "filter":      ("Easy Filter Cleaning", "Filter Hard To Clean", ["filter maintenance", "clogged filter"]),
        "motor":       ("Powerful Motor", "Motor Issues", ["motor noise", "motor failure"]),
        "battery":     ("Long Battery Life", "Battery Dies Fast", ["charge", "battery drain"]),
        "cord":        ("Cord Length Adequate", "Cord Too Short", ["cord", "power cord"]),
        "heater":      ("Heats Up Fast", "Slow To Heat", ["heat up time", "warm up"]),
        "brush":       ("Brush Works Well", "Brush Issues", ["bristle", "brush head"]),
        "nozzle":      ("Nozzle Design", "Nozzle Issues", ["nozzle", "concentrator"]),
        "attachment":  ("Attachments Work Well", "Attachment Issues", ["attachment", "accessory"]),
        "display":     ("Clear Display", "Display Hard To Read", ["screen", "display"]),
        "lid":         ("Secure Lid", "Lid Leaks", ["lid", "seal", "leak"]),
        "handle":      ("Comfortable Grip", "Handle Uncomfortable", ["grip", "handle", "ergonomic"]),
        "blade":       ("Sharp Blade", "Blade Dulls Quickly", ["blade", "cutting"]),
        "pump":        ("Pump Works Well", "Pump Issues", ["pump", "dispenser"]),
        "wheel":       ("Wheels Roll Smoothly", "Wheel Issues", ["wheel", "caster", "roll"]),
        "sensor":      ("Sensor Accuracy", "Sensor Issues", ["sensor", "detect"]),
        "app":         ("App Works Well", "App Issues", ["app", "bluetooth", "wifi", "connect"]),
    }
    for area in (info.get("product_areas") or []):
        area_lower = area.lower()
        for key, (del_label, det_label, signals) in component_templates.items():
            if key in area_lower or any(s in area_lower for s in signals):
                if _has_support(signals + [key]):
                    _add("del", del_label, from_field=f"product_areas:{area}")
                    _add("det", det_label, from_field=f"product_areas:{area}")
                break
        else:
            # Generic component pair
            clean = _titleize_theme_label(area)
            if clean:
                _add("del", f"{clean} Works Well", from_field=f"product_areas:{area}")
                _add("det", f"{clean} Issues", from_field=f"product_areas:{area}")

    # ── 2. Desired Outcomes → Achievement / failure pairs ───────────────
    for outcome in (info.get("desired_outcomes") or []):
        clean = _titleize_theme_label(outcome)
        if not clean: continue
        # Generate the positive and negative version
        _add("del", clean, from_field=f"desired_outcomes:{outcome}", aliases_list=[outcome])
        # Generate the negative (failure to achieve)
        neg = clean
        for pos_word, neg_word in [("Good","Poor"),("Fast","Slow"),("Easy","Difficult"),("Quiet","Loud"),
                                     ("Strong","Weak"),("Smooth","Rough"),("Long","Short"),("Clean","Dirty"),
                                     ("Clear","Unclear"),("Comfortable","Uncomfortable")]:
            if pos_word.lower() in clean.lower():
                neg = clean.replace(pos_word, neg_word).replace(pos_word.lower(), neg_word.lower())
                break
        else:
            neg = f"Poor {clean}" if not clean.startswith("Poor") else clean
        if neg != clean:
            _add("det", neg, from_field=f"desired_outcomes:{outcome}")

    # ── 3. Failure Modes → Direct detractors ────────────────────────────
    for mode in (info.get("likely_failure_modes") or []):
        _add("det", mode, from_field=f"likely_failure_modes:{mode}")

    # ── 4. Workflow Steps → Usability pairs ─────────────────────────────
    for step in (info.get("workflow_steps") or []):
        clean = _titleize_theme_label(step)
        if not clean: continue
        _add("del", f"Easy {clean}", from_field=f"workflow_steps:{step}")
        _add("det", f"Difficult {clean}", from_field=f"workflow_steps:{step}")

    # ── 5. CSAT Drivers → High-level satisfaction labels ────────────────
    for driver in (info.get("csat_drivers") or []):
        _add("del", driver, from_field=f"csat_drivers:{driver}")

    # ── 6. Watchouts → Pre-flagged detractors ───────────────────────────
    for watchout in (info.get("watchouts") or []):
        _add("det", watchout, from_field=f"watchouts:{watchout}")

    # ── 7. Explicit theme lists from knowledge ──────────────────────────
    for theme in (info.get("likely_delighter_themes") or []):
        _add("del", theme, from_field="likely_delighter_themes")
    for theme in (info.get("likely_detractor_themes") or []):
        _add("det", theme, from_field="likely_detractor_themes")

    # ── 8. Validate against reviews — flag unsupported candidates ───────
    validated_dels, validated_dets = [], []
    for label in delighters:
        tokens = _tokenize(label)
        if tokens and any(t in corpus for t in tokens):
            validated_dels.append(label)
        elif len(delighters) < 12:  # Keep if catalog is small
            validated_dels.append(label)
    for label in detractors:
        tokens = _tokenize(label)
        if tokens and any(t in corpus for t in tokens):
            validated_dets.append(label)
        elif len(detractors) < 14:
            validated_dets.append(label)

    return {
        "delighters": _normalize_tag_list(validated_dels)[:20],
        "detractors": _normalize_tag_list(validated_dets)[:25],
        "aliases": aliases,
        "generation_log": generation_log,
        "total_candidates": len(delighters) + len(detractors),
        "validated_count": len(validated_dels) + len(validated_dets),
    }



def _derive_csat_seed_candidates(product_description, sample_reviews, product_knowledge, category):
    archetype = _infer_generic_archetype(product_description, product_knowledge)
    corpus = "\n".join([_safe_text(product_description)] + [str(r) for r in (sample_reviews or [])[:60]] + [str(v) for v in (_app()._product_knowledge_context_text(product_knowledge).splitlines())]).lower()
    delighters, detractors = [], []
    aliases = {}

    def add_item(side_key, label, *, aliases_list=None, theme="Performance", family="CSAT Driver"):
        clean_label = _titleize_theme_label(label)
        if not clean_label:
            return
        target = delighters if side_key == "delighter" else detractors
        if clean_label not in target:
            target.append(clean_label)
        aliases[clean_label] = _coerce_product_knowledge_list(aliases_list or [], max_items=8)

    for driver_name, payload in _GENERIC_DRIVER_LIBRARY.items():
        if any(sig in corpus for sig in payload.get("signals", ())):
            add_item("delighter", payload["delighter"]["label"], aliases_list=payload["delighter"].get("aliases"), theme=payload["delighter"].get("theme", "Performance"), family=payload["delighter"].get("family", "CSAT Driver"))
            add_item("detractor", payload["detractor"]["label"], aliases_list=payload["detractor"].get("aliases"), theme=payload["detractor"].get("theme", "Performance"), family=payload["detractor"].get("family", "CSAT Driver"))

    for item in _specific_generic_pairs(product_knowledge, product_description, category):
        add_item("delighter" if item.get("side") == "delighter" else "detractor", item.get("label"), aliases_list=item.get("aliases") or [], theme=item.get("theme", "Performance"), family=item.get("family", "CSAT Driver"))

    for raw in _coerce_product_knowledge_list((_app()._normalize_product_knowledge(product_knowledge).get("likely_delighter_themes") or []), max_items=12):
        add_item("delighter", raw, aliases_list=[], theme="Performance", family="Product Knowledge Theme")
    for raw in _coerce_product_knowledge_list((_app()._normalize_product_knowledge(product_knowledge).get("likely_detractor_themes") or []) + (_app()._normalize_product_knowledge(product_knowledge).get("likely_failure_modes") or []), max_items=14):
        add_item("detractor", raw, aliases_list=[], theme="Performance", family="Product Knowledge Theme")

    archetype_pack = {
        "wireless_audio": {
            "delighters": ["Clear Sound Quality", "Comfortable Fit", "Long Battery Life"],
            "detractors": ["Battery Dies Fast", "Bluetooth Connection Drops", "Uncomfortable Fit"],
        },
        "vacuum_floorcare": {
            "delighters": ["Strong Suction", "Easy Bin Emptying", "Smart Navigation"],
            "detractors": ["Weak Suction", "Gets Stuck Often", "Bin Hard To Empty"],
        },
        "coffee_espresso": {
            "delighters": ["Rich Coffee Flavor", "Fast Heat Up", "Consistent Brew Results"],
            "detractors": ["Coffee Lacks Flavor", "Takes Too Long To Heat Up", "Inconsistent Brew Results"],
        },
        "air_fryer_oven": {
            "delighters": ["Crisps Food Well", "Fast Preheat", "Easy Basket Cleanup"],
            "detractors": ["Food Cooks Unevenly", "Long Preheat Time", "Basket Hard To Clean"],
        },
        "apparel": {
            "delighters": ["Fits As Expected", "Comfortable To Wear", "Fabric Feels Premium"],
            "detractors": ["Fit Does Not Match Expectation", "Feels Uncomfortable In Use", "Fabric Feels Cheap"],
        },
        "footwear": {
            "delighters": ["Comfortable To Wear", "Fits As Expected", "Good Arch Support"],
            "detractors": ["Fit Does Not Match Expectation", "Hurts After Long Wear", "Lacks Arch Support"],
        },
        "mattress_bedding": {
            "delighters": ["Comfortable Support", "Sleeps Cooler", "Good Motion Isolation"],
            "detractors": ["Too Firm", "Too Soft", "Sleeps Hot"],
        },
        "drinkware": {
            "delighters": ["Keeps Drinks Cold", "Leakproof Lid", "Fits Cup Holder"],
            "detractors": ["Loses Cold Quickly", "Leaks In Bag", "Does Not Fit Cup Holder"],
        },
        "skincare_topical": {
            "delighters": ["Hydrates Well", "Gentle On Skin", "Absorbs Quickly"],
            "detractors": ["Not Hydrating Enough", "Causes Irritation", "Feels Sticky"],
        },
        "oral_care": {
            "delighters": ["Leaves Teeth Feeling Clean", "Long Battery Life", "Gentle Yet Effective"],
            "detractors": ["Reservoir Leaks", "Battery Dies Fast", "Pressure Feels Too Strong"],
        },
    }.get(archetype, {})
    for label in archetype_pack.get("delighters", []):
        add_item("delighter", label)
    for label in archetype_pack.get("detractors", []):
        add_item("detractor", label)

    return {
        "delighters": _normalize_tag_list(delighters)[:18],
        "detractors": _normalize_tag_list(detractors)[:22],
        "aliases": aliases,
        "archetype": archetype,
    }



def _ai_generate_product_description(*, client, sample_reviews, existing_description=""):
    sys = textwrap.dedent("""
        You are a product marketing and consumer-insights analyst writing a concise product description from customer reviews.
        Use only facts and recurring capabilities clearly supported by the review sample.
        Do not invent capacities, dimensions, accessories, or retailer-specific claims not grounded in the reviews.
        Write 2-4 concise sentences describing what the product is, what it is mainly used for, and the most repeated strengths or caveats.
        Also extract structured product knowledge that will help downstream symptom generation create a sharper, more CSAT-driven first-pass taxonomy for ANY consumer product.
        Focus on the outcomes customers are trying to achieve, the workflow steps that matter, likely failure modes, comparison benchmarks, and user contexts.
        OUTPUT — strict JSON only:
        {
          "description":"<2-4 sentence product description>",
          "confidence_note":"<short note on confidence>",
          "product_archetype":"<short archetype such as wireless audio, vacuum floorcare, coffee espresso, drinkware, skincare topical, footwear>",
          "product_areas":["<component, workflow area, or product part>"],
          "use_cases":["<main use case or job to be done>"],
          "desired_outcomes":["<customer outcome the product should deliver>"],
          "comparison_set":["<brand, premium alternative, or comparison set>"],
          "workflow_steps":["<important step in setup, use, maintenance, or switching workflow>"],
          "user_contexts":["<skill level, environment, hair type, skin type, family size, commute context, etc.>"],
          "csat_drivers":["<core reason a customer would be satisfied or dissatisfied>"],
          "likely_failure_modes":["<important failure mode or friction point>"],
          "likely_themes":["<probable first-pass theme>"],
          "likely_delighter_themes":["<positive theme>"],
          "likely_detractor_themes":["<negative theme or failure mode>"],
          "watchouts":["<important caution, limitation, or risk>" ]
        }
    """).strip()
    compact_reviews = [_trunc(str(review or "").strip(), 420) for review in list(sample_reviews or [])[:60] if str(review or "").strip()]
    payload = dict(existing_description=existing_description or "", sample_reviews=compact_reviews)
    result_text = _app()._chat_complete_with_fallback_models(
        client,
        model=_app()._shared_model(),
        structured=True,
        messages=[{"role": "system", "content": sys}, {"role": "user", "content": json.dumps(payload)}],
        temperature=0.0,
        response_format={"type": "json_object"},
        max_tokens=1800,
        reasoning_effort=_app()._shared_reasoning(),
    )
    data = _safe_json_load(result_text)
    product_knowledge = _app()._normalize_product_knowledge(data)
    return {
        "description": _safe_text(data.get("description")),
        "confidence_note": _safe_text(data.get("confidence_note")),
        "product_knowledge": product_knowledge,
    }



def _infer_generic_archetype(product_description, product_knowledge):
    corpus = "\n".join([_safe_text(product_description)] + [str(v) for v in _coerce_product_knowledge_list(_app()._product_knowledge_context_text(product_knowledge).splitlines(), max_items=30)])
    corpus_norm = corpus.lower()
    best = "general"
    best_score = 0.0
    for archetype, keywords in _GENERIC_ARCHETYPE_KEYWORDS.items():
        score = 0.0
        for keyword in keywords:
            if str(keyword).lower() in corpus_norm:
                score += 1.0 if " " in str(keyword) else 0.65
        if score > best_score:
            best = archetype
            best_score = score
    return best



def _specific_generic_pairs(product_knowledge, product_description, category):
    info = _app()._normalize_product_knowledge(product_knowledge)
    items = []
    seeds = list(info.get("desired_outcomes") or []) + list(info.get("workflow_steps") or []) + list(info.get("csat_drivers") or []) + list(info.get("likely_failure_modes") or [])
    corpus = " ".join([_safe_text(product_description)] + [str(v) for v in seeds]).lower()

    def add_pair(pos_label, neg_label, *, signals, pos_aliases=None, neg_aliases=None, theme="Performance", family="CSAT Driver"):
        if not any(sig in corpus for sig in signals):
            return
        items.append({"side": "delighter", "label": pos_label, "aliases": pos_aliases or [], "theme": theme, "family": family, "bucket": "Category Driver", "seeded": True})
        items.append({"side": "detractor", "label": neg_label, "aliases": neg_aliases or [], "theme": theme, "family": family, "bucket": "Category Driver", "seeded": True})

    add_pair("Keeps Drinks Cold", "Loses Cold Quickly", signals=["cold", "keeps cold", "temperature retention", "insulated"], pos_aliases=["stays cold all day"], neg_aliases=["doesn't stay cold", "ice melts quickly"], theme="Reliability", family="Outcome Longevity")
    add_pair("Fits Cup Holder", "Does Not Fit Cup Holder", signals=["cup holder", "car"], pos_aliases=["fits my cup holder"], neg_aliases=["too big for cup holder"], theme="Size & Fit", family="Size & Fit")
    add_pair("Hydrates Well", "Not Hydrating Enough", signals=["hydrate", "hydrating", "moistur"], pos_aliases=["skin feels hydrated"], neg_aliases=["skin feels dry", "not moisturizing enough"], theme="Performance", family="Results & Outcome")
    add_pair("Strong Suction", "Weak Suction", signals=["suction", "pet hair", "pickup"], pos_aliases=["picks up everything"], neg_aliases=["doesn't pick up enough"], theme="Performance", family="Results & Outcome")
    add_pair("Rich Coffee Flavor", "Coffee Lacks Flavor", signals=["coffee", "espresso", "flavor", "taste"], pos_aliases=["great tasting coffee"], neg_aliases=["coffee tastes weak"], theme="Performance", family="Results & Outcome")
    add_pair("Fast Preheat", "Long Preheat Time", signals=["preheat", "heats up", "heat up"], pos_aliases=["preheats quickly"], neg_aliases=["slow to preheat"], theme="Time Efficiency", family="Time Efficiency")
    add_pair("Fits As Expected", "Fit Does Not Match Expectation", signals=["fit", "size", "runs small", "runs large", "true to size"], pos_aliases=["true to size"], neg_aliases=["runs small", "runs large"], theme="Size & Fit", family="Size & Fit")
    add_pair("Long Battery Life", "Battery Dies Fast", signals=["battery", "charge", "charging", "runtime"], pos_aliases=["battery lasts a long time"], neg_aliases=["battery drains quickly"], theme="Reliability", family="Power & Connectivity")
    return items


