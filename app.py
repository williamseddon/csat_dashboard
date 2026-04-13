"""
SharkNinja Review Analyst + Symptomizer — Updated and optimized
Updated with:
- Live sidebar filter system with timeframe, stars, core filters, dynamic extra filters, and active filter summary
- Symptom filters shown only when symptom data exists
- Short hoverable Reference tiles (AI Analyst citations only)
- More stable model fallbacks for batch / structured operations
- Symptomizer result cards now show detractors and delighters at the bottom like Review Explorer
"""
from __future__ import annotations

import difflib
import gc
import html
import io
import json
import math
import os
import random
import re
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.worksheet.worksheet import Worksheet
from plotly.subplots import make_subplots

try:
    from review_analyst.connectors import (
        load_multiple_product_reviews as _package_load_multiple_product_reviews,
        load_product_reviews as _package_load_product_reviews,
        load_uploaded_files as _package_load_uploaded_files,
    )
except Exception:
    _package_load_product_reviews = None
    _package_load_multiple_product_reviews = None
    _package_load_uploaded_files = None

try:
    from review_analyst.tag_quality import (
        UNIVERSAL_DETRACTOR as _UNIVERSAL_DETRACTOR,
        UNIVERSAL_DELIGHTER as _UNIVERSAL_DELIGHTER,
        UNIVERSAL_NEUTRAL_DETRACTORS as _UNIVERSAL_NEUTRAL_DETRACTORS,
        UNIVERSAL_NEUTRAL_DELIGHTERS as _UNIVERSAL_NEUTRAL_DELIGHTERS,
        compute_tag_edit_accuracy as _compute_tag_edit_accuracy,
        ensure_universal_taxonomy as _ensure_universal_taxonomy,
        normalize_tag_list as _normalize_tag_list,
        refine_tag_assignment as _refine_tag_assignment,
        strip_universal_neutral_tags as _strip_universal_neutral_tags,
    )
except Exception:
    _UNIVERSAL_DELIGHTER = "Overall Satisfaction"
    _UNIVERSAL_DETRACTOR = "Overall Dissatisfaction"
    _UNIVERSAL_NEUTRAL_DETRACTORS = [
        "Overall Dissatisfaction",
        "Overpriced",
        "Poor Performance",
        "Poor Quality",
        "Difficult To Use",
        "Unreliable",
        "Hard To Clean",
        "Time Consuming",
    ]
    _UNIVERSAL_NEUTRAL_DELIGHTERS = [
        "Overall Satisfaction",
        "Good Value",
        "Performs Well",
        "High Quality",
        "Easy To Use",
        "Reliable",
        "Easy To Clean",
        "Saves Time",
    ]

    def _normalize_tag_list(values):
        out = []
        seen = set()
        for value in values or []:
            item = re.sub(r"\s+", " ", str(value or "").strip())
            if not item or item.lower() in {"", "na", "n/a", "none", "null", "nan", "<na>", "not mentioned", "unknown"}:
                continue
            item = item.title()
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    def _ensure_universal_taxonomy(detractors=None, delighters=None, include_universal_neutral=True):
        det_seed = list(detractors or [])
        del_seed = list(delighters or [])
        if include_universal_neutral:
            det_seed = list(_UNIVERSAL_NEUTRAL_DETRACTORS) + det_seed
            del_seed = list(_UNIVERSAL_NEUTRAL_DELIGHTERS) + del_seed
        dets = _normalize_tag_list(det_seed)
        dels = _normalize_tag_list(del_seed)
        return dets, dels

    def _strip_universal_neutral_tags(detractors=None, delighters=None):
        dets = [tag for tag in _normalize_tag_list(detractors or []) if tag not in set(_UNIVERSAL_NEUTRAL_DETRACTORS + _UNIVERSAL_NEUTRAL_DELIGHTERS)]
        dels = [tag for tag in _normalize_tag_list(delighters or []) if tag not in set(_UNIVERSAL_NEUTRAL_DETRACTORS + _UNIVERSAL_NEUTRAL_DELIGHTERS)]
        return dets, dels

    def _refine_tag_assignment(review_text, detractors, delighters, **kwargs):
        return {
            "dets": _normalize_tag_list(detractors),
            "dels": _normalize_tag_list(delighters),
            "ev_det": {k: list(v or [])[:2] for k, v in (kwargs.get("evidence_det") or {}).items() if k in _normalize_tag_list(detractors)},
            "ev_del": {k: list(v or [])[:2] for k, v in (kwargs.get("evidence_del") or {}).items() if k in _normalize_tag_list(delighters)},
            "support_det": {},
            "support_del": {},
            "added_dets": [],
            "added_dels": [],
            "removed_dets": [],
            "removed_dels": [],
        }

    def _compute_tag_edit_accuracy(baseline_map, current_map):
        baseline_total = 0
        added = 0
        removed = 0
        changed = 0
        for key in set((baseline_map or {}).keys()) | set((current_map or {}).keys()):
            base = baseline_map.get(key, {}) if isinstance(baseline_map, dict) else {}
            cur = current_map.get(key, {}) if isinstance(current_map, dict) else {}
            base_det = set(_normalize_tag_list((base or {}).get("detractors") or []))
            base_del = set(_normalize_tag_list((base or {}).get("delighters") or []))
            cur_det = set(_normalize_tag_list((cur or {}).get("detractors") or []))
            cur_del = set(_normalize_tag_list((cur or {}).get("delighters") or []))
            baseline_total += len(base_det) + len(base_del)
            delta_add = len(cur_det - base_det) + len(cur_del - base_del)
            delta_rem = len(base_det - cur_det) + len(base_del - cur_del)
            added += delta_add
            removed += delta_rem
            if delta_add or delta_rem:
                changed += 1
        total_changes = added + removed
        accuracy = 100.0 if baseline_total <= 0 and total_changes == 0 else max(0.0, 100.0 * (1.0 - total_changes / float(max(baseline_total, 1))))
        return {
            "baseline_total_tags": baseline_total,
            "added_tags": added,
            "removed_tags": removed,
            "total_changes": total_changes,
            "changed_reviews": changed,
            "accuracy_pct": round(accuracy, 1),
        }

try:
    from review_analyst.taxonomy import (
        bucket_symptom_label as _bucket_taxonomy_label,
        build_alias_map_for_labels as _build_taxonomy_alias_map,
        build_structured_taxonomy_rows as _build_structured_taxonomy_rows,
        canonical_theme_name as _canonical_theme_name,
        canonicalize_symptom_catalog as _canonicalize_taxonomy_catalog,
        infer_category as _infer_taxonomy_category,
        infer_l1_theme as _infer_taxonomy_l1_theme,
        merge_alias_maps as _merge_taxonomy_alias_maps,
        prioritize_ai_taxonomy_items as _prioritize_ai_taxonomy_items,
        select_supported_category_pack as _select_supported_category_pack,
        suggest_taxonomy_merges as _suggest_taxonomy_merges,
        taxonomy_prompt_context as _taxonomy_prompt_context,
    )
except Exception:
    def _canonicalize_taxonomy_catalog(delighters=None, detractors=None):
        return _normalize_tag_list(delighters or []), _normalize_tag_list(detractors or []), {}

    def _canonical_theme_name(theme):
        return re.sub(r"\s+", " ", str(theme or "").strip()).title()

    def _infer_taxonomy_l1_theme(label, side=None, family="", theme="", category="general"):
        return _canonical_theme_name(theme or family) or "Product Specific"

    def _build_structured_taxonomy_rows(delighters=None, detractors=None, aliases=None, category="general", preview_items=None):
        rows = []
        for side_key, labels in (("delighter", delighters or []), ("detractor", detractors or [])):
            for raw in labels:
                label = re.sub(r"\s+", " ", str(raw or "").strip()).title()
                if not label:
                    continue
                rows.append({
                    "L1 Theme": "Product Specific",
                    "L2 Symptom": label,
                    "Side": "Delighter" if side_key == "delighter" else "Detractor",
                    "Bucket": _bucket_taxonomy_label(label, side=side_key, category=category),
                    "Aliases": ", ".join((aliases or {}).get(label, [])) if label in (aliases or {}) else "—",
                    "Review Hits": 0,
                    "Support %": 0.0,
                    "Rationale": "",
                    "Example": "",
                    "side_key": side_key,
                    "label": label,
                })
        return rows

    def _suggest_taxonomy_merges(rows, max_suggestions=12):
        return []

    def _build_taxonomy_alias_map(delighters=None, detractors=None, extra_aliases=None):
        merged = {}
        for label in _normalize_tag_list(list(delighters or []) + list(detractors or [])):
            merged.setdefault(label, [])
        for key, values in (extra_aliases or {}).items():
            label = re.sub(r"\s+", " ", str(key or "").strip()).title()
            if not label:
                continue
            bucket = merged.setdefault(label, [])
            for value in values or []:
                alias = re.sub(r"\s+", " ", str(value or "").strip()).title()
                if alias and alias != label and alias not in bucket:
                    bucket.append(alias)
        return merged

    def _merge_taxonomy_alias_maps(*maps):
        merged = {}
        for amap in maps:
            for key, values in (amap or {}).items():
                label = re.sub(r"\s+", " ", str(key or "").strip()).title()
                if not label:
                    continue
                bucket = merged.setdefault(label, [])
                for value in values or []:
                    alias = re.sub(r"\s+", " ", str(value or "").strip()).title()
                    if alias and alias != label and alias not in bucket:
                        bucket.append(alias)
        return merged

    def _infer_taxonomy_category(product_description="", sample_reviews=None):
        return {"category": "general", "confidence": 0.0, "signals": []}

    def _taxonomy_prompt_context(category, include_pack=True, max_labels_per_side=6):
        return ""

    def _select_supported_category_pack(category, sample_reviews, min_hits=1, max_per_side=6):
        return {"delighters": [], "detractors": [], "aliases": {}, "category": str(category or "general")}

    def _bucket_taxonomy_label(label, side=None, category="general"):
        return "Product Specific"

    def _prioritize_ai_taxonomy_items(items, side, sample_reviews, category="general", min_review_hits=1, max_keep=18, exclude_universal=True):
        out = []
        seen = set()
        for raw in items or []:
            if isinstance(raw, dict):
                label = str(raw.get("label", "")).strip()
                aliases = [str(v).strip() for v in (raw.get("aliases") or []) if str(v).strip()]
                family = str(raw.get("family", "")).strip()
                rationale = str(raw.get("rationale", "")).strip()
                bucket = str(raw.get("bucket", "") or "Product Specific")
            else:
                label = str(raw or "").strip()
                aliases = []
                family = ""
                rationale = ""
                bucket = "Product Specific"
            label = re.sub(r"\s+", " ", label).title()
            if not label or label in seen:
                continue
            seen.add(label)
            out.append({
                "label": label,
                "aliases": aliases,
                "family": family,
                "rationale": rationale,
                "bucket": bucket,
                "review_hits": 0,
                "support_ratio": 0.0,
                "score": 0.0,
                "specificity": 0.0,
                "examples": [],
                "seeded": bool(isinstance(raw, dict) and raw.get("seeded")),
            })
            if len(out) >= max_keep:
                break
        return out

try:
    from openai import OpenAI
    _HAS_OPENAI = True
except ImportError:
    OpenAI = None
    _HAS_OPENAI = False

try:
    import tiktoken
    _TIKTOKEN_ENC = tiktoken.get_encoding("cl100k_base")
    _HAS_TIKTOKEN = True
except Exception:
    tiktoken = None
    _TIKTOKEN_ENC = None
    _HAS_TIKTOKEN = False

try:
    from review_analyst.css import APP_CSS as _APP_CSS
except Exception:
    _APP_CSS = ""

try:
    from review_analyst.symptomizer import (
        tag_review_batch as _v3_tag_review_batch,
        gate_polarity as _v3_gate_polarity,
        estimate_batch_size as _v3_estimate_batch_size,
        match_label as _v3_match_label,
        validate_evidence as _v3_validate_evidence,
        retry_zero_tag_reviews as _v3_retry_zero_tags,
        calibration_preflight as _v3_calibration_preflight,
        audit_tag_distribution as _v3_audit_distribution,
        LabelTracker as _v3_LabelTracker,
        score_taxonomy_health as _v3_score_taxonomy_health,
        generate_taxonomy_recommendations as _v3_generate_recommendations,
    )
    _HAS_SYMPTOMIZER_V3 = True
except Exception:
    _HAS_SYMPTOMIZER_V3 = False

try:
    from review_analyst.workspace_store import (
        save_workspace_record as _ws_save,
        list_workspace_records as _ws_list,
        load_workspace_record as _ws_load,
        delete_workspace_record as _ws_delete,
        rename_workspace_record as _ws_rename,
        touch_workspace_loaded as _ws_touch,
        count_workspace_records as _ws_count,
    )
    _HAS_WORKSPACE_STORE = True
except Exception:
    _HAS_WORKSPACE_STORE = False

try:
    from review_analyst.logging_config import setup_logging, get_logger
    setup_logging()
    _log = get_logger("app")
except Exception:
    import logging as _logging
    _log = _logging.getLogger("starwalk.app")

try:
    from review_analyst.social_listening import _render_social_listening_tab as _pkg_render_social_listening_tab
    _HAS_SOCIAL_PKG = True
except Exception:
    _HAS_SOCIAL_PKG = False

st.set_page_config(page_title="StarWalk Review Analyst Beta", layout="wide")

if _APP_CSS:
    st.markdown(_APP_CSS, unsafe_allow_html=True)
else:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
:root{--navy:#0f172a;--slate-500:#64748b;--accent:#6366f1;--page-bg:#eef0f4;--surface:#ffffff;--border:#dde1e8;}
html,body,.stApp{font-family:'Inter',system-ui,sans-serif;color:var(--navy);background:var(--page-bg)!important;}
.block-container{max-width:1500px!important;}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
# ── AI Service (extracted to core/ai.py) ──────────────────────────────
try:
    from core.ai import (
        _get_api_key, _api_key_source, _get_client, _safe_json_load,
        _chat_complete, _chat_complete_with_fallback_models,
        _shared_model, _shared_reasoning, _estimate_tokens,
        _coerce_ai_target_words, _ai_target_token_budget,
        _strip_review_citations,
    )
    _AI_SERVICE_EXTRACTED = True
except ImportError:
    _AI_SERVICE_EXTRACTED = False

# ── Export (extracted to core/export.py) ──────────────────────────────
try:
    from core.export import (
        _autosize_cell_text, _autosize_ws, _filter_criteria_df,
        _build_master_excel, _get_master_bundle, _safe_get_master_bundle,
        _gen_symptomized_workbook,
    )
    _EXPORT_EXTRACTED = True
except ImportError:
    _EXPORT_EXTRACTED = False

# ── Workspace (extracted to core/workspace.py) ────────────────────────
try:
    from core.workspace import (
        _auto_discover_product, _reset_workspace_state,
        _save_taxonomy_to_memory, _load_taxonomy_from_memory,
    )
    _WORKSPACE_EXTRACTED = True
except ImportError:
    _WORKSPACE_EXTRACTED = False

# ── Review cards (extracted to core/review_cards.py) ──────────────────
try:
    from core.review_cards import (
        _sort_reviews, _highlight_keywords_in_text, _highlight_evidence,
        _symptom_tags_html, _render_review_card,
    )
    _REVIEW_CARDS_EXTRACTED = True
except ImportError:
    _REVIEW_CARDS_EXTRACTED = False

# ── Dashboard analytics (extracted to core/dashboard.py) ──────────────
try:
    from core.dashboard import (
        _compute_metrics_cached, _compute_metrics_direct, _get_metrics,
        _rating_dist_cached, _rating_dist, _monthly_trend_cached, _monthly_trend,
        _cumulative_avg_region_trend, _render_reviews_over_time_chart,
        _render_dashboard_snapshot, _top_theme_summary, _compute_detailed_symptom_impact,
    )
    _DASHBOARD_EXTRACTED = True
except ImportError:
    _DASHBOARD_EXTRACTED = False

# ── Taxonomy builder (extracted to core/taxonomy_builder.py) ──────────
try:
    from core.taxonomy_builder import (
        _ai_build_symptom_list, _knowledge_driven_taxonomy_candidates,
        _derive_csat_seed_candidates, _ai_generate_product_description,
        _infer_taxonomy_category, _infer_generic_archetype, _specific_generic_pairs,
    )
    _TAXONOMY_BUILDER_EXTRACTED = True
except ImportError:
    _TAXONOMY_BUILDER_EXTRACTED = False

# ── Symptom batch (extracted to core/symptom_batch.py) ────────────────
try:
    from core.symptom_batch import (
        _call_symptomizer_batch, _ensure_ai_cols, _upsert_processed_symptom_record, _detect_sym_cols, _detect_missing, _prioritize_for_symptomization, _sample_reviews_for_symptomizer, _symptomizer_review_text,
    )
    _SYMPTOM_BATCH_EXTRACTED = True
except ImportError:
    _SYMPTOM_BATCH_EXTRACTED = False

# ── Catalog management (extracted to core/catalog.py) ─────────────────
try:
    from core.catalog import (
        _normalize_product_knowledge, _product_knowledge_context_text, _render_product_knowledge_panel, _get_symptom_whitelists, _custom_universal_catalog, _alias_map_for_catalog, _taxonomy_preview_items_with_side, _render_structured_taxonomy_table,
    )
    _CATALOG_EXTRACTED = True
except ImportError:
    _CATALOG_EXTRACTED = False

# ── Connectors dispatch (extracted to core/connectors_dispatch.py) ────
try:
    from core.connectors_dispatch import (_finalize_df, _flatten_review, _flatten_powerreviews_review, _normalize_uploaded_df, _extract_candidate_tokens_from_html, _probe_bazaarvoice_candidates, _load_powerreviews_api_url, _fetch_all_bazaarvoice_for_candidate, _fetch_all_powerreviews_for_candidate, _load_product_reviews, _load_product_reviews_dispatch, _load_multiple_product_reviews,)
    _CONNECTORS_EXTRACTED = True
except ImportError:
    _CONNECTORS_EXTRACTED = False

# ── Filters (extracted to core/filters.py) ────────────────────────────
try:
    from core.filters import (_init_state, _render_sidebar, _apply_live_review_filters, _collect_active_filter_items, _reset_review_filters,)
    _FILTERS_EXTRACTED = True
except ImportError:
    _FILTERS_EXTRACTED = False

# ── Symptom display (extracted to core/symptom_display.py) ────────────
try:
    from core.symptom_display import (_render_symptom_dashboard, _render_interactive_symptom_table, _opp_scatter, _add_net_hit, _prepare_symptom_long, _build_theme_impact_table, _render_ai_taxonomy_preview_table, _render_symptomizer_taxonomy_workbench, _candidate_rows_for_side, _save_inline_symptom_edit, _build_inline_tag_suggestions,)
    _SYMPTOM_DISPLAY_EXTRACTED = True
except ImportError:
    _SYMPTOM_DISPLAY_EXTRACTED = False

# ── Analyst (extracted to core/analyst.py) ────────────────────────────
try:
    from core.analyst import (_select_relevant, _snippet_rows, _build_ai_context, _call_analyst, _product_name, _persona_instructions,)
    _ANALYST_EXTRACTED = True
except ImportError:
    _ANALYST_EXTRACTED = False
except ImportError:
    _WORKSPACE_EXTRACTED = False
except ImportError:
    _EXPORT_EXTRACTED = False
except ImportError:
    _AI_SERVICE_EXTRACTED = False

# ── Missing imports from extractions ──────────────────────────────────

APP_TITLE           = "StarWalk Review Analyst Beta"
DEFAULT_PASSKEY     = "caC6wVBHos09eVeBkLIniLUTzrNMMH2XMADEhpHe1ewUw"
DEFAULT_DISPLAYCODE = "15973_3_0-en_us"
DEFAULT_API_VERSION = "5.5"
DEFAULT_PAGE_SIZE   = 100
DEFAULT_SORT        = "SubmissionTime:desc"
DEFAULT_CONTENT_LOCALES = (
    "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,"
    "id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,"
    "tr*,vi*,en_AU,en_CA,en_GB"
)
BAZAARVOICE_ENDPOINT = "https://api.bazaarvoice.com/data/reviews.json"
POWERREVIEWS_BASE = "https://display.powerreviews.com"
POWERREVIEWS_MAX_PAGE_SIZE = 25
UK_EU_BV_PASSKEY = "capxzF3xnCmhSCHhkomxF1sQkZmh2zK2fNb8D1VDNl3hY"
COSTCO_BV_PASSKEY = "bai25xto36hkl5erybga10t99"
SEPHORA_BV_PASSKEY = "calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus"
ULTA_POWERREVIEWS_API_KEY = "daa0f241-c242-4483-afb7-4449942d1a2b"
HOKA_POWERREVIEWS_API_KEY = "ea283fa2-3fdc-4127-863c-b1e2397f7a77"

SITE_REVIEW_CONFIGS = [
    {
        "key": "sharkninja_us",
        "provider": "bazaarvoice",
        "bv_style": "revstats",
        "label": "SharkNinja US",
        "domains": ["sharkninja.com", "www.sharkninja.com", "sharkclean.com", "www.sharkclean.com", "ninjakitchen.com", "www.ninjakitchen.com"],
        "passkey": DEFAULT_PASSKEY,
        "displaycode": DEFAULT_DISPLAYCODE,
        "api_version": DEFAULT_API_VERSION,
        "content_locales": DEFAULT_CONTENT_LOCALES,
        "sort": DEFAULT_SORT,
        "retailer": "SharkNinja US",
    },
    {
        "key": "sharkninja_uk_eu",
        "provider": "bazaarvoice",
        "bv_style": "simple",
        "label": "SharkNinja UK/EU",
        "domains": [
            "sharkninja.co.uk", "www.sharkninja.co.uk",
            "sharkninja.eu", "www.sharkninja.eu",
            "sharkninja.de", "www.sharkninja.de",
            "sharkninja.fr", "www.sharkninja.fr",
            "sharkninja.es", "www.sharkninja.es",
            "sharkninja.it", "www.sharkninja.it",
            "sharkninja.nl", "www.sharkninja.nl",
            "sharkclean.co.uk", "www.sharkclean.co.uk", "ninjakitchen.co.uk", "www.ninjakitchen.co.uk",
            "sharkclean.eu", "www.sharkclean.eu", "ninjakitchen.eu", "www.ninjakitchen.eu",
            "sharkclean.de", "www.sharkclean.de", "ninjakitchen.de", "www.ninjakitchen.de",
            "sharkclean.fr", "www.sharkclean.fr", "ninjakitchen.fr", "www.ninjakitchen.fr",
            "sharkclean.es", "www.sharkclean.es", "ninjakitchen.es", "www.ninjakitchen.es",
            "sharkclean.it", "www.sharkclean.it", "ninjakitchen.it", "www.ninjakitchen.it",
            "sharkclean.nl", "www.sharkclean.nl", "ninjakitchen.nl", "www.ninjakitchen.nl",
        ],
        "passkey": UK_EU_BV_PASSKEY,
        "api_version": "5.4",
        "sort": "SubmissionTime:desc",
        "locale": "en_GB",
        "retailer": "SharkNinja UK/EU",
    },
    {
        "key": "costco",
        "provider": "bazaarvoice",
        "bv_style": "revstats",
        "label": "Costco",
        "domains": ["costco.com", "www.costco.com"],
        "passkey": COSTCO_BV_PASSKEY,
        "displaycode": "2070_2_0-en_us",
        "api_version": "5.5",
        "content_locales": "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,tr*,vi*",
        "sort": "SubmissionTime:desc",
        "retailer": "Costco",
    },
    {
        "key": "sephora",
        "provider": "bazaarvoice",
        "bv_style": "simple",
        "label": "Sephora",
        "domains": ["sephora.com", "www.sephora.com"],
        "passkey": SEPHORA_BV_PASSKEY,
        "api_version": "5.4",
        "sort": "SubmissionTime:desc",
        "locale": "en_US",
        "retailer": "Sephora",
        "extra_filters": ["contentlocale:en*"],
    },
    {
        "key": "ulta",
        "provider": "powerreviews",
        "label": "Ulta",
        "domains": ["ulta.com", "www.ulta.com"],
        "merchant_id": "6406",
        "locale": "en_US",
        "page_locale": "en_US",
        "api_key": ULTA_POWERREVIEWS_API_KEY,
        "sort": "Newest",
        "retailer": "Ulta",
    },
    {
        "key": "hoka",
        "provider": "powerreviews",
        "label": "Hoka",
        "domains": ["hoka.com", "www.hoka.com"],
        "merchant_id": "437772",
        "locale": "en_US",
        "page_locale": "en_US",
        "api_key": HOKA_POWERREVIEWS_API_KEY,
        "sort": "Newest",
        "retailer": "Hoka",
    },
]

POWERREVIEWS_ENDPOINT = "https://display.powerreviews.com"
SHARK_EUUK_BV_PASSKEY = "capxzF3xnCmhSCHhkomxF1sQkZmh2zK2fNb8D1VDNl3hY"
SHARK_EUUK_BV_API_VERSION = "5.4"
SHARK_EUUK_BV_CONFIG = {
    "style": "simple",
    "passkey": SHARK_EUUK_BV_PASSKEY,
    "api_version": SHARK_EUUK_BV_API_VERSION,
    "locale": "en_GB",
    "sort": "SubmissionTime:desc",
    "extra_filters": [],
}
COSTCO_BV_CONFIG = {
    "style": "revstats",
    "passkey": "bai25xto36hkl5erybga10t99",
    "displaycode": "2070_2_0-en_us",
    "api_version": "5.5",
    "sort": "relevancy:a1",
    "content_locales": "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,tr*,vi*",
}
SEPHORA_BV_CONFIG = {
    "style": "simple",
    "passkey": "calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus",
    "api_version": "5.4",
    "locale": "en_US",
    "sort": "SubmissionTime:desc",
    "extra_filters": ["contentlocale:en*"],
}
ULTA_POWERREVIEWS_CONFIG = {
    "merchant_id": "6406",
    "locale": "en_US",
    "page_locale": "en_US",
    "apikey": "daa0f241-c242-4483-afb7-4449942d1a2b",
    "sort": "Newest",
}
HOKA_POWERREVIEWS_CONFIG = {
    "merchant_id": "437772",
    "locale": "en_US",
    "page_locale": "en_US",
    "apikey": "ea283fa2-3fdc-4127-863c-b1e2397f7a77",
    "sort": "Newest",
}
POWERREVIEWS_ENDPOINT_TEMPLATE = "https://display.powerreviews.com/m/{merchant_id}/l/{locale}/product/{product_id}/reviews"
SHARKNINJA_UK_EU_BV_CONFIG = {
    "passkey": UK_EU_BV_PASSKEY,
    "api_version": "5.4",
    "locale": "en_GB",
    "include": "Products,Comments",
    "sort": "SubmissionTime:desc",
    "content_locale": "en*",
    "retailer": "SharkNinja UK/EU",
}
ULTA_PR_CONFIG = {
    **ULTA_POWERREVIEWS_CONFIG,
    "apikey": ULTA_POWERREVIEWS_CONFIG.get("apikey") or ULTA_POWERREVIEWS_API_KEY,
    "retailer": "Ulta",
}
HOKA_PR_CONFIG = {
    **HOKA_POWERREVIEWS_CONFIG,
    "apikey": HOKA_POWERREVIEWS_CONFIG.get("apikey") or HOKA_POWERREVIEWS_API_KEY,
    "retailer": "Hoka",
}

DEFAULT_PRODUCT_URL = "https://www.sharkninja.com/ninja-air-fryer-pro-xl-6-in-1/AF181.html"
SOURCE_MODE_URL = "Product / review URL"
SOURCE_MODE_FILE = "Uploaded review file"

TAB_DASHBOARD = "📊  Insights"
TAB_REVIEW_EXPLORER = "🔍  Reviews"
TAB_AI_ANALYST = "🤖  AI Analyst"
TAB_REVIEW_PROMPT = "🏷️  Review Prompt"
TAB_SYMPTOMIZER = "⚙️  Configure"
TAB_SOCIAL_LISTENING = "📣  Social Listening Beta"
WORKSPACE_TABS = [
    TAB_DASHBOARD,
    TAB_REVIEW_EXPLORER,
    TAB_AI_ANALYST,
    TAB_SYMPTOMIZER,
]

MODEL_OPTIONS = [
    "gpt-5.4-mini",
    "gpt-5.4",
    "gpt-5.4-pro",
    "gpt-5.4-nano",
    "gpt-5-chat-latest",
    "gpt-5-mini",
    "gpt-5",
    "gpt-5-nano",
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1",
]
DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_REASONING = "none"
STRUCTURED_FALLBACK_MODEL = "gpt-5.4-mini"
AI_VISIBLE_CHAT_MESSAGES = 2
AI_CONTEXT_TOKEN_BUDGET = 10_000
NON_VALUES = {"<NA>","NA","N/A","NONE","-","","NAN","NULL"}
STOPWORDS = {
    "a","about","after","again","all","also","am","an","and","any","are","as","at",
    "be","because","been","before","being","best","better","but","by","can","could",
    "did","do","does","don","down","even","every","for","from","get","got","great",
    "had","has","have","he","her","here","hers","him","his","how","i","if","in",
    "into","is","it","its","just","like","love","made","make","many","me","more",
    "most","much","my","new","no","not","now","of","on","one","only","or","other",
    "our","out","over","product","really","so","some","than","that","the","their",
    "them","then","there","these","they","this","to","too","use","used","using",
    "very","was","we","well","were","what","when","which","while","with","would",
    "you","your",
}
PERSONAS: Dict[str, Dict[str, Any]] = {
    "Product Development": {
        "blurb": "Translates reviews into product and feature decisions.",
        "prompt": "Create a report for the product development team. Highlight what customers love, unmet needs, feature gaps, usability friction, and concrete roadmap opportunities. End with the top 5 product actions ranked by impact.",
        "instructions": (
            "You are a senior product strategy analyst specialising in consumer appliances.\n"
            "Structure your response with these exact sections:\n"
            "## What Customers Love\n## Unmet Needs & Feature Gaps\n"
            "## Usability Friction\n## Roadmap Opportunities\n## Top 5 Actions (ranked)\n"
            "Cite review IDs inline as (review_ids: 12345, 67890) for every material claim.\n"
            "Be specific — name exact features, not vague categories.\n"
            "Keep each section to 3-5 bullet points. Total response ≤ 500 words."
        ),
    },
    "Quality Engineer": {
        "blurb": "Focuses on failure modes, defects, durability, and root-cause signals.",
        "prompt": "Create a report for a quality engineer. Identify defect patterns, reliability risks, cleaning issues, performance inconsistencies, and probable root-cause hypotheses. Separate confirmed evidence from inference.",
        "instructions": (
            "You are a senior quality and reliability analyst for consumer appliances.\n"
            "Sections:\n"
            "## Confirmed Defect Patterns\n## Reliability & Durability Risks\n"
            "## Root-Cause Hypotheses\n## Cleaning & Maintenance Issues\n"
            "## Risk Severity Matrix (High/Med/Low)\n"
            "Mark speculative claims as [INFERRED]. Cite review IDs for every confirmed finding.\n"
            "Prioritise by frequency × severity. Total ≤ 500 words."
        ),
    },
    "Consumer Insights": {
        "blurb": "Extracts sentiment drivers, purchase motivations, and voice-of-customer insights.",
        "prompt": "Create a report for the consumer insights team. Summarize key sentiment drivers, barriers to adoption, purchase motivations, key use cases, and how tone changes across star ratings and incentivized vs non-incentivized reviews.",
        "instructions": (
            "You are a consumer insights lead specialising in VoC analysis.\n"
            "Sections:\n"
            "## Top Sentiment Drivers (positive)\n## Top Sentiment Drivers (negative)\n"
            "## Purchase Motivations & Jobs-to-be-Done\n## Barriers to Satisfaction\n"
            "## Organic vs Incentivized Tone Differences\n## Key Verbatim Quotes (3-5)\n"
            "Use plain, executive-ready language. Cite review IDs for quotes. Total ≤ 500 words."
        ),
    },
}
DET_LETTERS  = ["K","L","M","N","O","P","Q","R","S","T"]
DEL_LETTERS  = ["U","V","W","X","Y","Z","AA","AB","AC","AD"]
DET_INDEXES  = [column_index_from_string(c) for c in DET_LETTERS]
DEL_INDEXES  = [column_index_from_string(c) for c in DEL_LETTERS]
META_ORDER   = [("Safety","AE"),("Reliability","AF"),("# of Sessions","AG")]
META_INDEXES = {name: column_index_from_string(col) for name,col in META_ORDER}
AI_DET_HEADERS  = [f"AI Symptom Detractor {i}" for i in range(1,11)]
AI_DEL_HEADERS  = [f"AI Symptom Delighter {i}" for i in range(1,11)]
AI_META_HEADERS = ["AI Safety","AI Reliability","AI # of Sessions"]
SAFETY_ENUM      = ["Not Mentioned","Concern","Positive"]
RELIABILITY_ENUM = ["Not Mentioned","Negative","Neutral","Positive"]
SESSIONS_ENUM    = ["0","1","2–3","4–9","10+","Unknown"]
DEFAULT_PRIORITY_DELIGHTERS = ["Overall Satisfaction","Ease Of Use","Effective Results",
    "Visible Improvement","Time Saver","Comfort","Value","Reliability"]
DEFAULT_PRIORITY_DETRACTORS = ["Overall Dissatisfaction","Poor Results","Ease Of Use","Reliability Issue","High Cost",
    "Irritation","Battery Problem","High Noise","Cleaning Difficulty",
    "Setup Issue","Connectivity Issue","Safety Concern"]
REVIEW_PROMPT_STARTER_ROWS = [
    {"column_name":"perceived_loudness",
     "prompt":"How is product loudness described? Positive, Negative, Neutral, or Not Mentioned.",
     "labels":"Positive, Negative, Neutral, Not Mentioned"},
    {"column_name":"reliability_risk_signal",
     "prompt":"Does the review mention a product reliability or durability risk? Risk Mentioned, Positive Reliability, or Not Mentioned.",
     "labels":"Risk Mentioned, Positive Reliability, Not Mentioned"},
    {"column_name":"product_usage_sessions_if_mentioned",
     "prompt":"If mentioned, how many times has the customer used the product? Choose the closest bucket. Use Unknown if the review does not say.",
     "labels":"0, 1, 2–3, 4–9, 10+, Unknown"},
    {"column_name":"safety_signal_if_mentioned",
     "prompt":"Does the review mention a product safety concern or a positive safety statement? Concern, Positive, or Not Mentioned.",
     "labels":"Concern, Positive, Not Mentioned"},
    {"column_name":"ownership_period_if_mentioned",
     "prompt":"If mentioned, how long has the customer owned or used the product? Choose the closest bucket. Use Unknown if not stated.",
     "labels":"First Use, Under 1 Week, 1–4 Weeks, 1–3 Months, 3+ Months, Unknown"},
]

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
class ReviewDownloaderError(Exception):
    pass


@dataclass
class ReviewBatchSummary:
    product_url: str
    product_id: str
    total_reviews: int
    page_size: int
    requests_needed: int
    reviews_downloaded: int


def _safe_text(v, default=""):
    if v is None:
        return default
    if isinstance(v, (list, tuple, set, dict, pd.Series, pd.DataFrame, pd.Index)):
        return default
    try:
        m = pd.isna(v)
    except Exception:
        m = False
    if isinstance(m, bool) and m:
        return default
    t = str(v).strip()
    return default if t.lower() in {"nan", "none", "null", "<na>"} else t


def _safe_int(v, d=0):
    try:
        return int(float(v))
    except Exception:
        return d


def _safe_bool(v, d=False):
    if v is None:
        return d
    if isinstance(v, bool):
        return v
    t = _safe_text(v).lower()
    if t in {"true", "1", "yes", "y", "t"}:
        return True
    if t in {"false", "0", "no", "n", "f", ""}:
        return False
    return d


def _safe_mean(s):
    if s.empty:
        return None
    n = pd.to_numeric(s, errors="coerce").dropna()
    return float(n.mean()) if not n.empty else None


def _safe_pct(num, den):
    return 0.0 if not den else float(num) / float(den)


def _fmt_secs(sec):
    sec = max(0.0, float(sec or 0))
    m = int(sec // 60)
    s = int(round(sec - m * 60))
    return f"{m}:{s:02d}"


def _canon(s):
    return " ".join(str(s).split()).lower().strip()


def _canon_simple(s):
    return "".join(ch for ch in _canon(s) if ch.isalnum())


def _esc(s):
    return html.escape(str(s or ""))


def _chip_html(items):
    if not items:
        return "<span class='chip gray'>No active filters</span>"
    return "<div class='chip-wrap'>" + "".join(f"<span class='chip {c}'>{_esc(t)}</span>" for t, c in items) + "</div>"


def _is_missing(v):
    if v is None:
        return True
    if isinstance(v, (list, tuple, set, dict, pd.Series, pd.DataFrame, pd.Index)):
        return False
    try:
        m = pd.isna(v)
    except Exception:
        return False
    return bool(m) if isinstance(m, (bool, int)) else False


def _fmt_num(v, d=2):
    if v is None or _is_missing(v):
        return "n/a"
    return f"{v:.{d}f}"


def _fmt_pct(v, d=1):
    if v is None or _is_missing(v):
        return "n/a"
    return f"{100 * float(v):.{d}f}%"


def _trunc(text, max_chars=420):
    text = re.sub(r"\s+", " ", _safe_text(text)).strip()
    return text if len(text) <= max_chars else text[:max_chars - 3].rstrip() + "…"


def _norm_text(text):
    return re.sub(r"\s+", " ", str(text).lower()).strip()


def _tokenize(text):
    return [t for t in re.findall(r"[a-z0-9']+", _norm_text(text)) if len(t) > 2 and t not in STOPWORDS]


def _slugify(text, fallback="custom"):
    c = re.sub(r"[^a-zA-Z0-9]+", "_", _safe_text(text).lower())
    c = re.sub(r"_+", "_", c).strip("_") or fallback
    return ("prompt_" + c if c[0].isdigit() else c)[:64]


def _first_non_empty(series):
    for v in series.astype(str):
        v = _safe_text(v)
        if v and v.lower() != "nan":
            return v
    return ""


def _clean_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def _make_openai_client(api_key: str):
    if not (_HAS_OPENAI and api_key):
        return None
    try:
        return OpenAI(api_key=api_key, timeout=60, max_retries=3)
    except TypeError:
        try:
            return OpenAI(api_key=api_key)
        except Exception:
            return None


def _reasoning_options_for_model(model: str) -> List[str]:
    m = _safe_text(model).lower()
    if not m.startswith("gpt-5"):
        return ["none"]
    if m.startswith("gpt-5.4") or m in {"gpt-5-chat-latest", "gpt-5.2", "gpt-5.2-pro"}:
        return ["none", "low", "medium", "high", "xhigh"]
    if m in {"gpt-5", "gpt-5-mini", "gpt-5-nano"}:
        return ["minimal", "low", "medium", "high"]
    return ["none", "low", "medium", "high"]


def _current_ai_target_words() -> int:
    return _coerce_ai_target_words(st.session_state.get("ai_response_words", 1200))


def _model_supports_reasoning(model: str) -> bool:
    return _safe_text(model).lower().startswith("gpt-5")


def _normalize_reasoning_effort_for_model(model: str, reasoning_effort: Optional[str]) -> Optional[str]:
    if not _model_supports_reasoning(model):
        return None
    allowed = _reasoning_options_for_model(model)
    effort = _safe_text(reasoning_effort).lower()
    if effort in allowed:
        return effort
    if not effort:
        return allowed[0] if allowed else None
    if effort == "none" and "minimal" in allowed:
        return "minimal"
    if effort == "minimal" and "none" in allowed:
        return "none"
    if effort == "xhigh" and "high" in allowed:
        return "high"
    if effort == "high" and "xhigh" in allowed:
        return "high"
    return allowed[0] if allowed else None


def _model_accepts_temperature(model: str, reasoning_effort: Optional[str]) -> bool:
    m = _safe_text(model).lower()
    eff = _safe_text(reasoning_effort).lower()
    if not m.startswith("gpt-5"):
        return True
    if m.startswith("gpt-5.4") or m in {"gpt-5-chat-latest", "gpt-5.2", "gpt-5.2-pro"}:
        return eff in {"", "none"}
    return False


def _split_chat_messages(messages, keep_last=AI_VISIBLE_CHAT_MESSAGES):
    items = list(messages or [])
    keep = max(1, int(keep_last or 1))
    if len(items) <= keep:
        return [], items
    return items[:-keep], items[-keep:]


def _show_thinking(msg):
    ph = st.empty()
    ph.markdown(f"""<div class="thinking-overlay"><div class="thinking-card">
      <div class="thinking-spinner"></div>
      <div class="thinking-title">Working…</div>
      <div class="thinking-sub">{_esc(msg)}</div>
    </div></div>""", unsafe_allow_html=True)
    return ph


def _prepare_messages_for_model(model: str, messages):
    prepared = []
    use_developer = _safe_text(model).lower().startswith("gpt-5")
    for msg in list(messages or []):
        if not isinstance(msg, dict):
            continue
        item = dict(msg)
        if use_developer and item.get("role") == "system":
            item["role"] = "developer"
        prepared.append(item)
    return prepared


def _build_completion_token_kwargs(max_tokens):
    try:
        limit = int(max_tokens) if max_tokens is not None else None
    except Exception:
        limit = None
    if limit is None or limit <= 0:
        return {}
    return {"max_completion_tokens": limit}


def _model_candidates_for_task(selected_model: str, *, structured: bool = False) -> List[str]:
    preferred = _safe_text(selected_model) or DEFAULT_MODEL
    fallbacks = [preferred]
    if structured:
        fallbacks += [STRUCTURED_FALLBACK_MODEL, DEFAULT_MODEL, "gpt-4.1"]
    else:
        fallbacks += [DEFAULT_MODEL]
    out = []
    seen = set()
    for m in fallbacks:
        if m and m not in seen:
            out.append(m)
            seen.add(m)
    return out


def _get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


def _domain_matches(host: str, domain: str) -> bool:
    host = _safe_text(host).lower()
    domain = _safe_text(domain).lower()
    return bool(host == domain or host.endswith("." + domain))


def _is_powerreviews_api_url(url: str) -> bool:
    parsed = urlparse(_safe_text(url))
    return "display.powerreviews.com" in parsed.netloc.lower() and "/product/" in parsed.path.lower() and parsed.path.lower().endswith("/reviews")


def _extract_pid_from_url(url):
    parsed = urlparse(url.strip())
    path = parsed.path
    m = re.search(r"/([A-Za-z0-9_-]+)\.html(?:$|[?#])", path)
    if m:
        c = m.group(1).strip().upper()
        if re.fullmatch(r"[A-Z0-9_-]{3,}", c):
            return c
    return None


def _extract_pid_from_html(h):
    for pat in [
        r'Item\s*No\.?\s*([A-Z0-9_-]{3,})',
        r'"productId"\s*:\s*"([A-Z0-9_-]{3,})"',
        r'"sku"\s*:\s*"([A-Z0-9_-]{3,})"',
        r'"model"\s*:\s*"([A-Z0-9_-]{3,})"',
    ]:
        m = re.search(pat, h, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().upper()
    soup = BeautifulSoup(h, "html.parser")
    text = soup.get_text(" ", strip=True)
    for pat in [r"Item\s*No\.?\s*([A-Z0-9_-]{3,})", r"Model\s*:?\s*([A-Z0-9_-]{3,})"]:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().upper()
    return None


def _fetch_reviews_page(session, *, product_id, passkey, displaycode, api_version,
                        page_size, offset, sort, content_locales):
    params = dict(
        resource="reviews",
        action="REVIEWS_N_STATS",
        filter=[
            f"productid:eq:{product_id}",
            f"contentlocale:eq:{content_locales}",
            "isratingsonly:eq:false",
        ],
        filter_reviews=f"contentlocale:eq:{content_locales}",
        include="authors,products,comments",
        filteredstats="reviews",
        Stats="Reviews",
        limit=int(page_size),
        offset=int(offset),
        limit_comments=3,
        sort=sort,
        passkey=passkey,
        apiversion=api_version,
        displaycode=displaycode,
    )
    resp = session.get(BAZAARVOICE_ENDPOINT, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def _fetch_bv_simple_page(session, *, product_id, passkey, api_version,
                          page_size, offset, sort, content_locale="en*",
                          locale="en_US", include="Products,Comments"):
    params: Dict[str, Any] = {
        "apiversion": api_version,
        "passkey": passkey,
        "Include": include,
        "Stats": "Reviews",
        "Limit": int(page_size),
        "Offset": int(offset),
        "Sort": sort,
        "Filter": [f"ProductId:{product_id}"],
    }
    if content_locale:
        params["Filter"].insert(0, f"contentlocale:{content_locale}")
    if locale:
        params["Locale"] = locale
    resp = session.get(BAZAARVOICE_ENDPOINT, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def _fetch_bazaarvoice_raw_page(session, *, api_url, params):
    resp = session.get(api_url, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def _fetch_powerreviews_page(session, *, merchant_id, locale, product_id,
                             apikey, paging_from=0, page_size=POWERREVIEWS_MAX_PAGE_SIZE,
                             sort="Newest", filters="", search="",
                             image_only=False):
    endpoint = POWERREVIEWS_ENDPOINT_TEMPLATE.format(
        merchant_id=merchant_id,
        locale=locale,
        product_id=product_id,
    )
    safe_page_size = max(1, min(int(page_size or POWERREVIEWS_MAX_PAGE_SIZE), POWERREVIEWS_MAX_PAGE_SIZE))
    params = {
        "paging.from": int(paging_from),
        "paging.size": safe_page_size,
        "filters": filters or "",
        "search": search or "",
        "sort": sort or "Newest",
        "image_only": "true" if image_only else "false",
        "page_locale": locale,
        "_noconfig": "true",
        "apikey": apikey,
    }
    resp = session.get(endpoint, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("errors"):
        raise ReviewDownloaderError(f"PowerReviews error: {payload.get('errors')}")
    return payload


def _find_ci_key(mapping, candidates):
    wanted = {str(c).lower() for c in candidates}
    for k in mapping.keys():
        if str(k).lower() in wanted:
            return k
    return None


def _query_first_ci(mapping, candidates, default=None):
    key = _find_ci_key(mapping, candidates)
    if key is None:
        return default
    val = mapping.get(key)
    if isinstance(val, list):
        return val[0] if val else default
    return val if val not in (None, "") else default


def _set_ci_param(mapping, candidates, value):
    key = _find_ci_key(mapping, candidates) or list(candidates)[0]
    mapping[key] = value
    return key


def _clone_params(mapping):
    out = {}
    for k, v in mapping.items():
        out[k] = list(v) if isinstance(v, list) else v
    return out


def _dedupe_keep_order(values):
    out = []
    seen = set()
    for v in values or []:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _normalize_input_url(product_url: str) -> str:
    product_url = (product_url or "").strip()
    if not re.match(r"^https?://", product_url, flags=re.IGNORECASE):
        product_url = "https://" + product_url
    return product_url


def _strip_www(host: str) -> str:
    host = (host or "").lower().strip()
    return host[4:] if host.startswith("www.") else host


def _is_bazaarvoice_api_url(url: str) -> bool:
    parsed = urlparse(url)
    return "api.bazaarvoice.com" in (parsed.netloc or "").lower() and parsed.path.endswith("/reviews.json")


def _is_powerreviews_api_url(url: str) -> bool:
    parsed = urlparse(url)
    return "display.powerreviews.com" in (parsed.netloc or "").lower() and "/product/" in (parsed.path or "") and parsed.path.endswith("/reviews")


def _looks_like_sharkninja_uk_eu(host: str) -> bool:
    h = _strip_www(host)
    if h in {
        "sharkninja.co.uk", "sharkninja.eu", "sharkninja.de", "sharkninja.fr", "sharkninja.es", "sharkninja.it", "sharkninja.nl", "sharkninja.ie",
        "sharkclean.co.uk", "ninjakitchen.co.uk", "sharkclean.eu", "ninjakitchen.eu",
        "sharkclean.de", "ninjakitchen.de", "sharkclean.fr", "ninjakitchen.fr",
        "sharkclean.nl", "ninjakitchen.nl", "sharkclean.ie", "ninjakitchen.ie",
    }:
        return True
    return ("sharkclean" in h or "ninjakitchen" in h or "sharkninja" in h) and not h.endswith(".com")


def _looks_like_sharkninja_us(host: str) -> bool:
    h = _strip_www(host)
    return h in {"sharkclean.com", "www.sharkclean.com", "ninjakitchen.com", "www.ninjakitchen.com", "sharkninja.com", "www.sharkninja.com"} or h.endswith("sharkclean.com") or h.endswith("ninjakitchen.com")


def _safe_candidate_token(raw: Any) -> Optional[str]:
    s = _safe_text(raw)
    s = s.strip().strip("\"' ")
    s = re.sub(r"\.html?$", "", s, flags=re.IGNORECASE)
    s = s.strip()
    if not s or len(s) < 3 or len(s) > 80:
        return None
    if re.search(r"\s", s):
        return None
    if s.lower() in {"product", "products", "reviews", "review", "home", "en", "us", "uk", "gb"}:
        return None
    return s


def _extract_embedded_review_api_urls(html: str) -> List[str]:
    text = (html or "")
    if not text:
        return []
    norm = (
        text.replace(r"\/", "/")
            .replace("&amp;", "&")
            .replace(r"\u0026", "&")
            .replace(r"\x26", "&")
    )
    hits: List[str] = []
    patterns = [
        r"https://api\.bazaarvoice\.com/data/reviews\.json[^\"'<>\s]+",
        r"https://display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*",
        r"//display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*",
        r"/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*apikey=[^\"'<>\s]+",
    ]
    for pat in patterns:
        for match in re.findall(pat, norm, flags=re.IGNORECASE):
            url = str(match).strip().strip('"\' ,)')
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/m/"):
                url = "https://display.powerreviews.com" + url
            hits.append(url)
    return _dedupe_keep_order(hits)


def _extract_powerreviews_embeds(html: str) -> List[Dict[str, str]]:
    text = (html or "")
    if not text:
        return []
    norm = (
        text.replace(r"\/", "/")
            .replace("&amp;", "&")
            .replace(r"\u0026", "&")
            .replace(r"\x26", "&")
    )
    found: List[Dict[str, str]] = []
    for m in re.finditer(r"(?:https?:)?//display\.powerreviews\.com/m/(\d+)/l/([A-Za-z_]+)/product/([^/?\"'&<>]+)/reviews([^\"'<>]*)", norm, flags=re.IGNORECASE):
        merchant_id, locale, product_id, rest = m.groups()
        api_match = re.search(r"[?&]apikey=([^&#\"']+)", rest or "", flags=re.IGNORECASE)
        if merchant_id and locale and product_id and api_match:
            found.append({
                "merchant_id": merchant_id,
                "locale": locale,
                "product_id": product_id,
                "apikey": api_match.group(1),
            })
    return found


def _extract_bazaarvoice_product_id_from_params(params: Dict[str, Any]) -> Optional[str]:
    direct = _query_first_ci(params, ["productid", "productId", "ProductId", "pid", "id"])
    if direct:
        return _safe_candidate_token(direct)
    for key, vals in params.items():
        if str(key).lower() not in {"filter", "filter_reviews", "filterreviews"}:
            continue
        vals_list = vals if isinstance(vals, list) else [vals]
        for val in vals_list:
            txt = str(val)
            m = re.search(r"productid(?::eq)?:([^,&]+)", txt, flags=re.IGNORECASE)
            if m:
                return _safe_candidate_token(m.group(1))
    return None


def _extract_candidate_tokens_from_url(url: str) -> List[str]:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    cands: List[str] = []

    for key, values in params.items():
        lk = key.lower()
        if lk in {"productid", "product_id", "itemid", "item_id", "pid", "id", "sku"}:
            cands.extend(values)
        dw = re.match(r"dwvar_([^_]+)_", key, flags=re.IGNORECASE)
        if dw:
            cands.append(dw.group(1))

    path = parsed.path or ""
    zid_match = re.search(r"[_\-]zid([A-Za-z0-9\-_]+)$", path, flags=re.IGNORECASE)
    if zid_match:
        cands.append(zid_match.group(1))

    segments = [s for s in path.split("/") if s]
    if segments:
        last = re.sub(r"\.html?$", "", segments[-1], flags=re.IGNORECASE)
        cands.append(last)
        for token in re.findall(r"([A-Za-z0-9]{4,30})", last):
            cands.append(token)
        if len(segments) >= 2:
            cands.append(re.sub(r"\.html?$", "", segments[-2], flags=re.IGNORECASE))

    out = []
    for raw in cands:
        tok = _safe_candidate_token(raw)
        if tok:
            out.append(tok)
    return _dedupe_keep_order(out)


def _extract_generic_bv_product_id(url: str, html: str) -> Optional[str]:
    parsed = urlparse(url.strip())
    params = parse_qs(parsed.query)
    for key in ["productId", "product_id", "itemId", "item_id", "pid", "id", "sku"]:
        if key in params and params[key]:
            tok = _safe_candidate_token(params[key][0])
            if tok:
                return tok
    zid_match = re.search(r"[_\-]zid([A-Z0-9\-_]+)$", parsed.path, re.IGNORECASE)
    if zid_match:
        tok = _safe_candidate_token(zid_match.group(1))
        if tok:
            return tok
    html_cands = _extract_candidate_tokens_from_html(html)
    for tok in html_cands:
        if re.fullmatch(r"[A-Za-z0-9_-]{4,40}", tok):
            return tok
    segments = [s for s in parsed.path.split("/") if s]
    if segments:
        last = re.sub(r"\.html?$", "", segments[-1], flags=re.IGNORECASE)
        trailing = re.search(r"([A-Z0-9]{4,20})$", last, re.IGNORECASE)
        if trailing:
            return trailing.group(1)
        tok = _safe_candidate_token(last)
        if tok:
            return tok
    return None


def _is_incentivized(r):
    badges = [str(b).lower() for b in (r.get("BadgesOrder") or [])]
    if any("incentivized" in b for b in badges):
        return True
    ctx = r.get("ContextDataValues") or {}
    if isinstance(ctx, dict):
        for k, v in ctx.items():
            if "incentivized" in str(k).lower():
                flag = str((v.get("Value", "") if isinstance(v, dict) else v)).strip().lower()
                if flag in {"", "true", "1", "yes"}:
                    return True
    return False


def _ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _extract_age_group(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    payload = val
    if isinstance(payload, str):
        stripped = payload.strip()
        if not stripped:
            return None
        try:
            payload = json.loads(stripped)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    for k, raw in payload.items():
        if "age" not in str(k).lower():
            continue
        candidate = raw.get("Value") or raw.get("Label") if isinstance(raw, dict) else raw
        candidate = _safe_text(candidate)
        if candidate and candidate.lower() not in {"nan", "none", "null", "unknown", "prefer not to say"}:
            return candidate
    return None


def _pick_col(df, aliases):
    lk = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        c = lk.get(str(a).strip().lower())
        if c:
            return c
    return None


UPLOAD_REVIEW_ID_ALIASES = ["Event Id", "Event ID", "Review ID", "Review Id", "Id", "review_id"]
UPLOAD_REVIEW_TEXT_ALIASES = ["Review Text", "Review", "Body", "Content", "review_text"]
UPLOAD_TITLE_ALIASES = ["Title", "Review Title", "Headline", "title"]
UPLOAD_RATING_ALIASES = ["Rating (num)", "Rating", "Stars", "Star Rating", "rating"]
UPLOAD_DATE_ALIASES = ["Opened date", "Opened Date", "Submission Time", "Review Date", "Date", "submission_time"]
LOCAL_SYMPTOM_META_ALIASES = {
    "AI Safety": ["AI Safety", "Safety"],
    "AI Reliability": ["AI Reliability", "Reliability"],
    "AI # of Sessions": ["AI # of Sessions", "# of Sessions", "Number of Sessions", "Sessions"],
}
SYMPTOM_NON_VALUES = set(NON_VALUES) | {"NOT MENTIONED"}


def _series_alias(df, aliases):
    c = _pick_col(df, aliases)
    if c is None:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[c]


def _parse_flag(v, *, pos, neg):
    t = _safe_text(v).lower()
    if t in {"", "nan", "none", "null", "n/a"}:
        return pd.NA
    if any(t == x.lower() for x in neg):
        return False
    if any(t == x.lower() for x in pos):
        return True
    if t.startswith(("not ", "non ")):
        return False
    return True


def _local_symptom_columns(columns):
    out = []
    for col in columns:
        name = str(col).strip()
        lower = name.lower()
        if lower.startswith("ai symptom detractor") or lower.startswith("ai symptom delighter"):
            out.append(name)
            continue
        if re.fullmatch(r"symptom\s+(?:[1-9]|10|1[1-9]|20)", lower):
            out.append(name)
    return out


def _normalize_symptom_series(series):
    text = series.astype("string").fillna("").str.strip()
    valid = (text != "") & (~text.str.upper().isin(SYMPTOM_NON_VALUES)) & (~text.str.startswith("<"))
    return text.where(valid, pd.NA).str.title()


def _score_uploaded_sheet(columns):
    lowered = {str(col).strip().lower() for col in columns}
    score = 0
    if any(alias.lower() in lowered for alias in UPLOAD_REVIEW_TEXT_ALIASES):
        score += 4
    if any(alias.lower() in lowered for alias in UPLOAD_RATING_ALIASES):
        score += 3
    if any(alias.lower() in lowered for alias in UPLOAD_REVIEW_ID_ALIASES):
        score += 3
    if any(alias.lower() in lowered for alias in UPLOAD_TITLE_ALIASES):
        score += 1
    if any(alias.lower() in lowered for alias in UPLOAD_DATE_ALIASES):
        score += 1
    if _local_symptom_columns(list(columns)):
        score += 2
    return score


def _read_best_uploaded_excel_sheet(raw_bytes):
    bio = io.BytesIO(raw_bytes)
    xls = pd.ExcelFile(bio)
    if not xls.sheet_names:
        return pd.DataFrame(), ""
    best_name = xls.sheet_names[0]
    best_score = -1
    for sheet_name in xls.sheet_names:
        try:
            headers = list(pd.read_excel(xls, sheet_name=sheet_name, nrows=0).columns)
        except Exception:
            headers = []
        score = _score_uploaded_sheet(headers)
        if score > best_score:
            best_score = score
            best_name = sheet_name
    return pd.read_excel(xls, sheet_name=best_name), best_name


def _best_uploaded_excel_sheet_name(raw_bytes):
    try:
        _, sheet_name = _read_best_uploaded_excel_sheet(raw_bytes)
        return sheet_name
    except Exception:
        return ""


def _read_uploaded_file(f, *, include_local_symptomization=False):
    fname = getattr(f, "name", "uploaded_file")
    raw = f.getvalue()
    suffix = fname.lower().rsplit(".", 1)[-1] if "." in fname else "csv"
    if suffix == "csv":
        try:
            raw_df = pd.read_csv(io.BytesIO(raw))
        except UnicodeDecodeError:
            raw_df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
    elif suffix in {"xlsx", "xls", "xlsm"}:
        raw_df, sheet_name = _read_best_uploaded_excel_sheet(raw)
        raw_df.attrs["source_sheet_name"] = sheet_name
    else:
        raise ReviewDownloaderError(f"Unsupported: {fname}")
    if raw_df.empty:
        raise ReviewDownloaderError(f"{fname} is empty.")
    normalized = _normalize_uploaded_df(raw_df, source_name=fname, include_local_symptomization=include_local_symptomization)
    source_sheet_name = raw_df.attrs.get("source_sheet_name")
    if source_sheet_name:
        normalized.attrs["source_sheet_name"] = source_sheet_name
    return normalized


def _load_uploaded_files(files, *, include_local_symptomization=False):
    if not files:
        raise ReviewDownloaderError("Upload at least one file.")
    with st.spinner("Reading files…"):
        frames = [_read_uploaded_file(f, include_local_symptomization=include_local_symptomization) for f in files]
    combined = pd.concat(frames, ignore_index=True)
    combined["review_id"] = combined["review_id"].astype(str)
    combined = combined.drop_duplicates(subset=["review_id"], keep="first").reset_index(drop=True)
    combined = _finalize_df(combined)
    pid = (
        _first_non_empty(combined["base_sku"].fillna("")) or
        _first_non_empty(combined["product_id"].fillna("")) or
        "UPLOADED_REVIEWS"
    )
    names = [getattr(f, "name", "file") for f in files]
    src = names[0] if len(names) == 1 else f"{len(names)} uploaded files"
    summary = ReviewBatchSummary(
        product_url="",
        product_id=pid,
        total_reviews=len(combined),
        page_size=max(len(combined), 1),
        requests_needed=0,
        reviews_downloaded=len(combined),
    )
    source_sheet_name = frames[0].attrs.get("source_sheet_name") if len(frames) == 1 else ""
    return dict(summary=summary, reviews_df=combined, source_type="uploaded", source_label=src, source_sheet_name=source_sheet_name)


def _apply_source_metadata(df: pd.DataFrame, *, retailer: str = "", source_system: str = "", post_link: str = "") -> pd.DataFrame:

    out = df.copy()
    if retailer:
        if "retailer" not in out.columns:
            out["retailer"] = retailer
        else:
            out["retailer"] = out["retailer"].fillna("").astype(str)
            out.loc[out["retailer"].str.strip().eq(""), "retailer"] = retailer
    if source_system:
        out["source_system"] = source_system
    if post_link:
        if "post_link" not in out.columns:
            out["post_link"] = post_link
        else:
            out["post_link"] = out["post_link"].fillna("").astype(str)
            out.loc[out["post_link"].str.strip().eq(""), "post_link"] = post_link
    return out


def _build_bv_dataset(raw_reviews: List[Dict[str, Any]], *, product_url: str, product_id: str,
                      total: int, page_size: int, requests_needed: int,
                      source_label: str, retailer: str = "", source_system: str = "Bazaarvoice"):
    df = _finalize_df(pd.DataFrame([_flatten_review(r) for r in raw_reviews]))
    if not df.empty:
        df["review_id"] = df["review_id"].astype(str)
        df["product_or_sku"] = df.get("product_or_sku", pd.Series(index=df.index, dtype="object")).fillna(product_id)
        df["base_sku"] = df.get("base_sku", pd.Series(index=df.index, dtype="object")).fillna(product_id)
        df["product_id"] = df["product_id"].fillna(product_id)
        df = _apply_source_metadata(df, retailer=retailer, source_system=source_system, post_link=product_url)
    summary = ReviewBatchSummary(
        product_url=product_url,
        product_id=product_id,
        total_reviews=total,
        page_size=page_size,
        requests_needed=requests_needed,
        reviews_downloaded=len(df),
    )
    return dict(summary=summary, reviews_df=df, source_type="bazaarvoice", source_label=source_label or product_url)


def _powerreviews_bool_from_bottom_line(value):
    t = _safe_text(value).lower()
    if t in {"yes", "recommended", "true"}:
        return True
    if t in {"no", "false", "not recommended"}:
        return False
    return pd.NA


def _powerreviews_media_urls(media_items):
    urls = []
    for item in media_items or []:
        if not isinstance(item, dict):
            continue
        for key in [
            "large_url", "normal_url", "thumbnail_url", "url",
            "fullsize_url", "media_url", "src", "link",
        ]:
            val = item.get(key)
            if val:
                urls.append(str(val))
                break
    return urls


def _powerreviews_submission_iso(value):
    ts = pd.to_datetime(value, unit="ms", utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


def _build_powerreviews_dataset(reviews: List[Dict[str, Any]], *, product_url: str, product_id: str,
                                total: int, page_size: int, requests_needed: int,
                                source_label: str, product_name: str = "", retailer: str = "",
                                source_system: str = "PowerReviews"):
    rows = [_flatten_powerreviews_review(r, page_id=product_id, product_name=product_name, retailer=retailer, product_url=product_url) for r in reviews]
    df = _finalize_df(pd.DataFrame(rows))
    if not df.empty:
        df = _apply_source_metadata(df, retailer=retailer, source_system=source_system, post_link=product_url)
    summary = ReviewBatchSummary(
        product_url=product_url,
        product_id=product_id,
        total_reviews=total,
        page_size=page_size,
        requests_needed=requests_needed,
        reviews_downloaded=len(df),
    )
    return dict(summary=summary, reviews_df=df, source_type="powerreviews", source_label=source_label or product_url)


def _load_bazaarvoice_api_url(api_url: str, *, product_url_hint: str = "", retailer_hint: str = ""):
    session = _get_session()
    parsed = urlparse(api_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    params = parse_qs(parsed.query, keep_blank_values=True)
    page_size = int(_query_first_ci(params, ["limit", "Limit"], default=DEFAULT_PAGE_SIZE) or DEFAULT_PAGE_SIZE)
    product_id = _extract_bazaarvoice_product_id_from_params(params) or "UNKNOWN_PRODUCT"
    first = _fetch_bazaarvoice_raw_page(session, api_url=base_url, params=params)
    total = int(first.get("TotalResults", 0) or 0)
    raw_reviews = list(first.get("Results") or [])
    if total > len(raw_reviews):
        offsets = list(range(len(raw_reviews), total, page_size))
        progress = st.progress(0.0, text="Downloading…")
        for i, offset in enumerate(offsets, 1):
            q = _clone_params(params)
            _set_ci_param(q, ["offset", "Offset"], int(offset))
            _set_ci_param(q, ["limit", "Limit"], int(page_size))
            payload = _fetch_bazaarvoice_raw_page(session, api_url=base_url, params=q)
            raw_reviews.extend(payload.get("Results") or [])
            progress.progress(i / max(len(offsets), 1))
    source_label = product_url_hint or api_url
    return _build_bv_dataset(
        raw_reviews,
        product_url=product_url_hint or api_url,
        product_id=product_id,
        total=total,
        page_size=page_size,
        requests_needed=max(1, math.ceil(total / max(page_size, 1))) if total else 1,
        source_label=source_label,
        retailer=retailer_hint,
        source_system="Bazaarvoice API",
    )


def _probe_powerreviews_candidates(session, *, product_url: str, candidates: Sequence[str], cfg: Dict[str, Any]):
    tried = []
    zero_match = None
    for candidate in _dedupe_keep_order(candidates):
        try:
            payload = _fetch_powerreviews_page(
                session,
                merchant_id=cfg["merchant_id"],
                locale=cfg.get("locale", "en_US"),
                product_id=candidate,
                apikey=cfg["apikey"],
                paging_from=0,
                page_size=5,
                sort=cfg.get("sort", "Newest"),
            )
            paging = payload.get("paging") or {}
            total = int(paging.get("total_results", 0) or 0)
            results = payload.get("results") or []
            if total > 0 or results:
                return candidate, payload
            if zero_match is None:
                zero_match = (candidate, payload)
        except Exception as exc:
            tried.append(f"{candidate}: {exc}")
    if zero_match is not None:
        return zero_match
    raise ReviewDownloaderError("Could not match a PowerReviews product ID. Tried: " + "; ".join(tried[:8]))


def _load_bazaarvoice_product_page(session, *, product_url: str, product_html: str, cfg: Dict[str, Any], extra_candidates: Optional[Sequence[str]] = None):
    embedded_urls = [u for u in _extract_embedded_review_api_urls(product_html) if _is_bazaarvoice_api_url(u)]
    for api_url in embedded_urls:
        try:
            return _load_bazaarvoice_api_url(api_url, product_url_hint=product_url, retailer_hint=cfg.get("retailer", ""))
        except Exception:
            continue

    candidates = []
    candidates.extend(extra_candidates or [])
    candidates.extend(_extract_candidate_tokens_from_url(product_url))
    candidates.extend(_extract_candidate_tokens_from_html(product_html))
    fallback_pid = _extract_generic_bv_product_id(product_url, product_html)
    if fallback_pid:
        candidates.insert(0, fallback_pid)
    product_id, _ = _probe_bazaarvoice_candidates(session, product_url=product_url, candidates=candidates, cfg=cfg)
    return _fetch_all_bazaarvoice_for_candidate(session, product_url=product_url, product_id=product_id, cfg=cfg)


def _load_powerreviews_product_page(session, *, product_url: str, product_html: str, cfg: Dict[str, Any], extra_candidates: Optional[Sequence[str]] = None):
    embedded_urls = [u for u in _extract_embedded_review_api_urls(product_html) if _is_powerreviews_api_url(u)]
    for api_url in embedded_urls:
        try:
            return _load_powerreviews_api_url(api_url, product_url_hint=product_url, retailer_hint=cfg.get("retailer", ""))
        except Exception:
            continue

    embeds = _extract_powerreviews_embeds(product_html)
    for embed in embeds:
        try:
            cfg2 = dict(cfg)
            cfg2.update({k: v for k, v in embed.items() if v})
            return _fetch_all_powerreviews_for_candidate(
                session,
                product_url=product_url,
                product_id=cfg2["product_id"],
                cfg=cfg2,
            )
        except Exception:
            continue

    candidates = []
    candidates.extend(extra_candidates or [])
    candidates.extend(_extract_candidate_tokens_from_html(product_html))
    candidates.extend(_extract_candidate_tokens_from_url(product_url))
    product_id, _ = _probe_powerreviews_candidates(session, product_url=product_url, candidates=candidates, cfg=cfg)
    return _fetch_all_powerreviews_for_candidate(session, product_url=product_url, product_id=product_id, cfg=cfg)


def _parse_bulk_product_urls(raw_text: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for line in re.split(r"[\r\n]+", str(raw_text or "")):
        candidate = re.sub(r"^[\s\-\*\u2022\d\.)]+", "", str(line or "")).strip()
        if not candidate:
            continue
        normalized = _normalize_input_url(candidate)
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(normalized)
    return urls


def _format_multi_source_label(urls: Sequence[str]) -> str:
    hosts = []
    seen = set()
    for url in urls:
        host = _strip_www(urlparse(url).netloc) or _safe_text(url)
        if host and host not in seen:
            seen.add(host)
            hosts.append(host)
    if not hosts:
        return f"{len(urls)} links"
    if len(hosts) <= 3:
        return f"{len(urls)} links · " + ", ".join(hosts)
    return f"{len(urls)} links · " + ", ".join(hosts[:3]) + f" +{len(hosts) - 3}"


def _dedupe_combined_reviews(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "review_id" in out.columns:
        exact_key = (
            out["review_id"].fillna("").astype(str).str.strip() + "||" +
            out.get("product_id", pd.Series("", index=out.index)).fillna("").astype(str).str.strip() + "||" +
            out.get("review_text", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
        )
        out = out.loc[~exact_key.duplicated(keep="first")].copy()
        counts = Counter()
        unique_ids = []
        for rid in out["review_id"].fillna("").astype(str).str.strip().tolist():
            base = rid or f"review_{len(unique_ids) + 1}"
            counts[base] += 1
            unique_ids.append(base if counts[base] == 1 else f"{base} ({counts[base]})")
        out["review_id"] = unique_ids
    return out.reset_index(drop=True)


def _use_package_connectors() -> bool:
    """Prefer review_analyst.connectors when available (default: True)."""
    flag = str(os.getenv("STARWALK_USE_PACKAGE_CONNECTORS", "")).strip().lower()
    if flag in {"0", "false", "no", "n", "off"}: return False
    return callable(_package_load_product_reviews)


def _load_multiple_product_reviews_dispatch(urls):
    if _use_package_connectors() and callable(_package_load_multiple_product_reviews):
        return _package_load_multiple_product_reviews(urls)
    return _load_multiple_product_reviews(urls)


def _load_uploaded_files_dispatch(files, *, include_local_symptomization=False):
    if _use_package_connectors() and callable(_package_load_uploaded_files):
        return _package_load_uploaded_files(files, include_local_symptomization=include_local_symptomization)
    return _load_uploaded_files(files, include_local_symptomization=include_local_symptomization)


# ═══════════════════════════════════════════════════════════════════════════════
#  ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════
def _df_cache_key(df):
    cols = [c for c in [
        "review_id", "rating", "incentivized_review", "is_recommended",
        "is_syndicated", "photos_count", "has_photos", "submission_time",
        "title_and_text", "review_length_words", "content_locale", "product_or_sku",
    ] if c in df.columns]
    return df[cols].to_json(orient="split", date_format="iso")


def _compute_metrics_cached(df_json):
    df = pd.read_json(io.StringIO(df_json), orient="split")
    return _compute_metrics_direct(df)


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
        return _rating_dist_cached(_df_cache_key(df))
    except Exception:
        return pd.DataFrame({"rating": [1, 2, 3, 4, 5], "review_count": [0] * 5, "share": [0.0] * 5})


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


def _cohort_by_incentivized(df):
    if df.empty:
        return pd.DataFrame()
    w = df.copy()
    w["cohort"] = w["incentivized_review"].fillna(False).map({True: "Incentivized", False: "Organic"})
    w["rating_int"] = pd.to_numeric(w["rating"], errors="coerce")
    w = w.dropna(subset=["rating_int"])
    w["rating_int"] = w["rating_int"].astype(int)
    out = []
    for cohort, grp in w.groupby("cohort"):
        total = max(len(grp), 1)
        for star in [1, 2, 3, 4, 5]:
            cnt = int((grp["rating_int"] == star).sum())
            out.append(dict(cohort=cohort, star=star, count=cnt, pct=cnt / total * 100))
    return pd.DataFrame(out)


def _locale_breakdown(df, top_n=None):
    if df.empty or "content_locale" not in df.columns:
        return pd.DataFrame()
    grp = (
        df.dropna(subset=["content_locale"])
        .groupby("content_locale", as_index=False)
        .agg(count=("review_id", "count"), avg_rating=("rating", "mean"))
        .sort_values("count", ascending=False)
    )
    if top_n not in (None, "All"):
        grp = grp.head(int(top_n))
    grp["pct"] = grp["count"] / max(grp["count"].sum(), 1) * 100
    return grp


def _review_length_cohort(df):
    if df.empty or "review_length_words" not in df.columns:
        return pd.DataFrame()
    w = df.dropna(subset=["rating", "review_length_words"]).copy()
    w["review_length_words"] = pd.to_numeric(w["review_length_words"], errors="coerce")
    w = w.dropna(subset=["review_length_words"])
    if len(w) < 8:
        return pd.DataFrame()
    try:
        w["length_bin"] = pd.qcut(
            w["review_length_words"],
            q=4,
            labels=["Short (Q1)", "Medium (Q2)", "Long (Q3)", "Very Long (Q4)"],
            duplicates="drop",
        )
    except Exception:
        return pd.DataFrame()
    return (
        w.groupby("length_bin", as_index=False, observed=True)
        .agg(avg_rating=("rating", "mean"), count=("review_id", "count"), median_words=("review_length_words", "median"))
        .rename(columns={"length_bin": "Length Quartile"})
    )


def _top_locations(df, top_n=None):
    if df.empty or "user_location" not in df.columns:
        return pd.DataFrame()
    grp = (
        df.dropna(subset=["user_location"])
        .groupby("user_location", as_index=False)
        .agg(count=("review_id", "count"), avg_rating=("rating", "mean"))
        .sort_values("count", ascending=False)
    )
    if top_n not in (None, "All"):
        grp = grp.head(int(top_n))
    return grp


def _star_band_trend(df):
    if df.empty:
        return pd.DataFrame()
    md = _monthly_trend(df)
    if md.empty:
        return pd.DataFrame()
    w = df.dropna(subset=["submission_time", "rating"]).copy()
    w["month_start"] = w["submission_time"].dt.to_period("M").dt.to_timestamp()
    w["low"] = w["rating"].isin([1, 2])
    w["high"] = w["rating"].isin([4, 5])
    grp = w.groupby("month_start", as_index=False).agg(total=("review_id", "count"), low_ct=("low", "sum"), high_ct=("high", "sum"))
    grp["pct_low"] = grp["low_ct"] / grp["total"].clip(lower=1) * 100
    grp["pct_high"] = grp["high_ct"] / grp["total"].clip(lower=1) * 100
    return grp.sort_values("month_start")


def _sw_style_fig(fig):
    GRID = "rgba(148,163,184,0.18)"
    trace_count = len(getattr(fig, "data", []) or [])
    if trace_count > 3:
        legend_cfg = dict(
            orientation="v",
            y=1.0,
            x=1.01,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.86)",
            bordercolor="rgba(148,163,184,0.22)",
            borderwidth=1,
            font=dict(size=11),
        )
        margin = dict(l=26, r=108, t=56, b=44)
    else:
        legend_cfg = dict(
            orientation="h",
            y=1.12,
            x=0,
            xanchor="left",
            yanchor="bottom",
            bgcolor="rgba(255,255,255,0.84)",
            bordercolor="rgba(148,163,184,0.18)",
            borderwidth=1,
            font=dict(size=11),
        )
        margin = dict(l=26, r=18, t=64, b=44)
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, system-ui, sans-serif", size=12),
        margin=margin,
        title=dict(x=0, xanchor="left", font=dict(size=15)),
        legend=legend_cfg,
        hoverlabel=dict(font=dict(family="Inter, system-ui, sans-serif", size=12)),
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID, automargin=True, title_standoff=10)
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID, automargin=True, title_standoff=10)
    return fig


def _show_plotly(fig):
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
            "displayModeBar": False,
            "responsive": True,
            "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"],
        },
    )


def _render_chart_header(title: str, subtitle: str = ""):
    st.markdown(
        f"<div style='padding:2px 2px 10px 2px;'>"
        f"<div style='font-size:13px;font-weight:800;color:var(--navy);line-height:1.25;'>{_esc(title)}</div>"
        + (f"<div style='font-size:11.5px;color:var(--slate-500);margin-top:3px;line-height:1.35;'>{_esc(subtitle)}</div>" if subtitle else "")
        + "</div>",
        unsafe_allow_html=True,
    )


REGION_NAME_MAP = {
    "US": "USA",
    "USA": "USA",
    "GB": "UK",
    "UK": "UK",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "ES": "Spain",
    "IT": "Italy",
    "JP": "Japan",
    "MX": "Mexico",
    "BR": "Brazil",
    "NL": "Netherlands",
}


def _locale_to_region_label(locale):
    raw = _safe_text(locale).replace("-", "_").strip()
    if not raw:
        return "Unknown"
    parts = [p for p in raw.split("_") if p]
    country = (parts[-1] if parts else raw).upper()
    country = re.sub(r"[^A-Z]", "", country)
    if not country:
        return "Unknown"
    return REGION_NAME_MAP.get(country, country)


def _parse_smoothing_window(label):
    txt = _safe_text(label).lower()
    if txt.startswith("none"):
        return 1
    m = re.search(r"(\d+)", txt)
    return int(m.group(1)) if m else 1


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

    smoothing_window = _parse_smoothing_window(smoothing_label)
    if smoothing_window > 1:
        for col in [c for c in trend.columns if c.endswith("_cum_avg")]:
            trend[col] = trend[col].rolling(smoothing_window, min_periods=1).mean()

    return trend.sort_values("day").reset_index(drop=True), regions


def _build_volume_bar_series(trend, volume_mode):
    if trend is None or trend.empty:
        return pd.DataFrame(columns=["x", "volume", "width_ms", "label"]), "Reviews/day"

    w = trend[["day", "daily_volume"]].copy()
    w["day"] = pd.to_datetime(w["day"], errors="coerce")
    w["daily_volume"] = pd.to_numeric(w["daily_volume"], errors="coerce").fillna(0)
    w = w.dropna(subset=["day"])
    if w.empty:
        return pd.DataFrame(columns=["x", "volume", "width_ms", "label"]), "Reviews/day"

    mode = _safe_text(volume_mode) or "Reviews/day"
    if mode == "Reviews/week":
        w["bucket_start"] = w["day"].dt.to_period("W-SUN").dt.start_time
        grouped = w.groupby("bucket_start", as_index=False).agg(volume=("daily_volume", "sum"))
        grouped["bucket_days"] = 7.0
        grouped["label"] = grouped["bucket_start"].dt.strftime("Week of %Y-%m-%d")
        axis_title = "Reviews/week"
    elif mode == "Reviews/month":
        w["bucket_start"] = w["day"].dt.to_period("M").dt.start_time
        grouped = w.groupby("bucket_start", as_index=False).agg(volume=("daily_volume", "sum"))
        grouped["bucket_days"] = grouped["bucket_start"].dt.days_in_month.astype(float)
        grouped["label"] = grouped["bucket_start"].dt.strftime("%Y-%m")
        axis_title = "Reviews/month"
    else:
        grouped = w.rename(columns={"day": "bucket_start", "daily_volume": "volume"}).copy()
        grouped["bucket_days"] = 1.0
        grouped["label"] = grouped["bucket_start"].dt.strftime("%Y-%m-%d")
        axis_title = "Reviews/day"

    grouped["x"] = grouped["bucket_start"] + pd.to_timedelta(grouped["bucket_days"] / 2.0, unit="D")
    grouped["width_ms"] = grouped["bucket_days"].map(lambda d: int(pd.Timedelta(days=max(float(d) - 0.15, 0.35)).total_seconds() * 1000))
    grouped["volume"] = pd.to_numeric(grouped["volume"], errors="coerce").fillna(0).astype(int)
    return grouped[["x", "volume", "width_ms", "label"]], axis_title


def _add_axis_break_indicator(fig, *, side="right"):
    if side == "right":
        x0, x1 = 0.988, 0.998
    else:
        x0, x1 = 0.002, 0.012
    y0 = 0.08
    fig.add_shape(type="line", xref="paper", yref="paper", x0=x0, y0=y0, x1=x1, y1=y0 + 0.02, line=dict(color="rgba(71,85,105,0.92)", width=2), layer="above")
    fig.add_shape(type="line", xref="paper", yref="paper", x0=x0, y0=y0 + 0.028, x1=x1, y1=y0 + 0.048, line=dict(color="rgba(71,85,105,0.92)", width=2), layer="above")


def _symptom_col_lists_from_columns(columns):
    cleaned = [str(c).strip() for c in columns]
    man_det = [c for c in cleaned if c.lower() in {f"symptom {i}" for i in range(1, 11)}]
    man_del = [c for c in cleaned if c.lower() in {f"symptom {i}" for i in range(11, 21)}]
    ai_det = [c for c in cleaned if c.lower().startswith("ai symptom detractor")]
    ai_del = [c for c in cleaned if c.lower().startswith("ai symptom delighter")]
    return _dedupe_keep_order(man_det + ai_det), _dedupe_keep_order(man_del + ai_del)


def _get_symptom_col_lists(df):
    return _symptom_col_lists_from_columns(df.columns)


def _detect_symptom_state(df):
    det_cols, del_cols = _get_symptom_col_lists(df)

    def _has(cols):
        for c in cols:
            if c not in df.columns:
                continue
            s = df[c].astype("string").fillna("").str.strip()
            valid = (s != "") & (~s.str.upper().isin(SYMPTOM_NON_VALUES)) & (~s.str.startswith("<"))
            if valid.any():
                return True
        return False

    h_det = _has(det_cols)
    h_del = _has(del_cols)
    if h_det and h_del:
        return "full"
    if h_det or h_del:
        return "partial"
    return "none"


def _empty_symptom_table():
    return pd.DataFrame(columns=["Item", "Avg Star", "Mentions", "% Tagged Reviews", "Avg Tags/Review"])


def _analytics_excluded_symptom_labels() -> set[str]:
    return {
        _safe_text(_UNIVERSAL_NEUTRAL_DETRACTORS[0]),
        _safe_text(_UNIVERSAL_NEUTRAL_DELIGHTERS[0]),
    }


def analyze_symptoms_fast(df_in, symptom_cols):
    long, symptomized_reviews = _prepare_symptom_long(df_in, symptom_cols)
    if long.empty:
        return _empty_symptom_table()

    grouped = long.groupby("symptom", dropna=False)
    mention_reviews = grouped["__row"].nunique().astype(int)
    avg_tags = grouped["symptom_count"].mean()
    avg_stars = grouped["star"].mean() if "star" in long.columns else pd.Series(index=mention_reviews.index, dtype=float)
    weighted_mentions = grouped["review_weight"].sum()

    out = pd.DataFrame({
        "Item": [str(item).title() for item in mention_reviews.index.tolist()],
        "Mentions": mention_reviews.values.astype(int),
        "% Tagged Reviews": (mention_reviews.values / max(symptomized_reviews, 1) * 100).round(1).astype(str) + "%",
        "Avg Star": [round(float(avg_stars[item]), 1) if item in avg_stars and not pd.isna(avg_stars[item]) else None for item in mention_reviews.index],
        "Avg Tags/Review": np.round(avg_tags.values.astype(float), 2),
        "__Weighted Mentions": weighted_mentions.values.astype(float),
        "__Mention Reviews": mention_reviews.values.astype(int),
        "__Symptomized Reviews": symptomized_reviews,
        "__All Reviews": int(len(df_in)),
    }).sort_values(["Mentions", "__Weighted Mentions", "Item"], ascending=[False, False, True], ignore_index=True)
    out.attrs["symptomized_review_count"] = symptomized_reviews
    out.attrs["all_review_count"] = int(len(df_in))
    return out


def _infer_symptom_total_reviews(tbl):
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


def _symptom_table_for_display(df_in):
    if df_in is None or getattr(df_in, "empty", True):
        return pd.DataFrame(columns=list(_SYMPTOM_TABLE_BASE_COLUMNS))
    out = df_in.copy()
    keep = [c for c in _SYMPTOM_TABLE_BASE_COLUMNS if c in out.columns]
    if "Item" in out.columns:
        out["Item"] = out["Item"].astype(str)
    if "% Tagged Reviews" in out.columns:
        out["% Tagged Reviews"] = pd.to_numeric(out["% Tagged Reviews"].astype(str).str.replace("%", "", regex=False), errors="coerce")
    for col in ["Mentions", "Avg Star", "Avg Tags/Review", "Confidence %", "Net Hit", "Forecast Δ★", "Impact Score", "Severity Wt"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    impact_source = None
    if "Impact Score" in out.columns:
        impact_source = pd.to_numeric(out["Impact Score"], errors="coerce")
    elif "Forecast Δ★" in out.columns:
        impact_source = pd.to_numeric(out["Forecast Δ★"], errors="coerce")
    elif "Net Hit" in out.columns:
        impact_source = pd.to_numeric(out["Net Hit"], errors="coerce")
    if impact_source is not None:
        out["Impact |Abs|"] = impact_source.abs().round(3)
    return out[[c for c in (keep + _SYMPTOM_TABLE_AUX_COLUMNS) if c in out.columns]]


def _symptom_table_column_config(df_in):
    try:
        cfg = {}
        if "Item" in df_in.columns:
            cfg["Item"] = st.column_config.TextColumn("Item")
        if "L1 Theme" in df_in.columns:
            cfg["L1 Theme"] = st.column_config.TextColumn("L1 Theme")
        if "Mentions" in df_in.columns:
            cfg["Mentions"] = st.column_config.NumberColumn("Mentions", format="%d")
        if "% Tagged Reviews" in df_in.columns:
            cfg["% Tagged Reviews"] = st.column_config.NumberColumn("% Tagged Reviews", format="%.1f")
        if "Avg Star" in df_in.columns:
            cfg["Avg Star"] = st.column_config.NumberColumn("Avg Star", format="%.1f")
        if "Avg Tags/Review" in df_in.columns:
            cfg["Avg Tags/Review"] = st.column_config.NumberColumn("Avg Tags/Review", format="%.2f")
        if "Confidence %" in df_in.columns:
            cfg["Confidence %"] = st.column_config.NumberColumn("Confidence %", format="%.1f")
        if "Net Hit" in df_in.columns:
            cfg["Net Hit"] = st.column_config.NumberColumn("Net Hit", format="%.3f")
        if "Forecast Δ★" in df_in.columns:
            cfg["Forecast Δ★"] = st.column_config.NumberColumn("Forecast Δ★", format="%.3f")
        if "Impact Score" in df_in.columns:
            cfg["Impact Score"] = st.column_config.NumberColumn("Impact Score", format="%.3f")
        if "Severity Wt" in df_in.columns:
            cfg["Severity Wt"] = st.column_config.NumberColumn("Severity Wt", format="%.2f")
        if "Impact |Abs|" in df_in.columns:
            cfg["Impact |Abs|"] = st.column_config.NumberColumn("Impact |Abs|", format="%.3f")
        return cfg
    except Exception:
        return {}


def _label_severity_weight(label, *, kind):
    norm = str(label or "").strip().lower()
    if not norm:
        return 1.0
    if str(kind).lower().startswith("del"):
        if any(token in norm for token in _HIGH_VALUE_DELIGHTS):
            return 1.08
        if any(token in norm for token in _LOW_VALUE_DELIGHTS):
            return 0.96
        return 1.0
    if any(token in norm for token in _HIGH_SEVERITY_HINTS):
        return 1.35
    if any(token in norm for token in _MEDIUM_SEVERITY_HINTS):
        return 1.18
    if any(token in norm for token in _LOW_SEVERITY_HINTS):
        return 0.96
    return 1.05


def _alignment_confidence(avg_star, baseline, *, kind):
    stars = pd.to_numeric(avg_star, errors="coerce")
    if str(kind).lower().startswith("del"):
        gap = (stars - float(baseline)).clip(lower=0)
    else:
        gap = (float(baseline) - stars).clip(lower=0)
    gap = gap.fillna(0.0)
    scaled = (gap / 1.25).clip(lower=0.0, upper=1.0)
    return 0.45 + 0.55 * scaled


def _collect_row_symptom_tags(row, cols):
    tags = []
    seen = set()
    for col in cols:
        raw = row.get(col)
        txt = _safe_text(raw).strip()
        if not txt or txt.upper() in SYMPTOM_NON_VALUES or txt.startswith("<"):
            continue
        label = txt.title()
        if label in seen:
            continue
        seen.add(label)
        tags.append(label)
    return tags


def _render_symptom_bar_chart(tbl, title, color, denom, show_pct):
    if tbl is None or tbl.empty:
        st.info(f"No {title.lower()} data.")
        return
    t = tbl.copy()
    t["Mentions"] = pd.to_numeric(t["Mentions"], errors="coerce").fillna(0)
    symptomized_reviews = 0
    if "__Symptomized Reviews" in t.columns:
        symptomized_reviews = int(pd.to_numeric(t["__Symptomized Reviews"], errors="coerce").fillna(0).max() or 0)
    if symptomized_reviews <= 0:
        symptomized_reviews = int(getattr(tbl, "attrs", {}).get("symptomized_review_count") or 0)
    if symptomized_reviews <= 0:
        symptomized_reviews = int(max(denom, 1))
    t["Pct"] = t["Mentions"] / max(symptomized_reviews, 1) * 100
    x_vals = t["Pct"][::-1] if show_pct else t["Mentions"][::-1]
    x_label = "% of symptomized reviews" if show_pct else "Mentions"
    hover = "%{customdata}<br>% of symptomized reviews: %{x:.1f}%<extra></extra>" if show_pct else "%{customdata}<br>Mentions: %{x:.0f}<extra></extra>"
    fig = go.Figure(go.Bar(x=x_vals, y=t["Item"][::-1], orientation="h", marker_color=color, opacity=0.80, customdata=t["Item"][::-1].astype(str).tolist(), hovertemplate=hover))
    fig.update_layout(title=title, height=max(300, 28 * len(t) + 80), xaxis_title=x_label, yaxis_title="", margin=dict(l=160, r=20, t=46, b=30))
    _sw_style_fig(fig)
    _show_plotly(fig)


def _series_matches_any(series: pd.Series, values: Sequence[str]) -> pd.Series:
    lookup = {str(v).strip().lower() for v in values if str(v).strip()}
    s = series.astype("string").fillna("").str.strip().str.lower()
    return s.isin(lookup)


def _sanitize_multiselect(key: str, options: Sequence[Any], default: Optional[Sequence[Any]] = None):
    opts = list(options or [])
    default_vals = list(default or ["ALL"])
    cur = st.session_state.get(key, default_vals)
    if not isinstance(cur, list):
        cur = [cur]
    cur = [x for x in cur if x in opts]
    if not cur:
        cur = default_vals
    if "ALL" in cur and len(cur) > 1:
        cur = [x for x in cur if x != "ALL"]
    st.session_state[key] = cur


def _sanitize_multiselect_sym(key: str, options: Sequence[str], default: Optional[Sequence[str]] = None):
    opts = list(options or [])
    default_vals = list(default or ["All"])
    cur = st.session_state.get(key, default_vals)
    if not isinstance(cur, list):
        cur = [cur]
    cur = [x for x in cur if x in opts]
    if not cur:
        cur = default_vals
    if "All" in cur and len(cur) > 1:
        cur = [x for x in cur if x != "All"]
    st.session_state[key] = cur


def _filter_series_for_spec(df: pd.DataFrame, spec: Dict[str, Any]) -> pd.Series:
    key = spec["key"]
    if spec["kind"] == "column":
        s = df[spec["column"]].astype("string").fillna("Unknown").str.strip()
        return s.replace("", "Unknown")
    if key == "review_type":
        raw = df.get("incentivized_review", pd.Series(False, index=df.index)).astype("boolean")
        return raw.map({True: "Incentivized", False: "Organic"}).fillna("Unknown")
    if key == "recommendation":
        raw = df.get("is_recommended", pd.Series(pd.NA, index=df.index)).astype("boolean")
        return raw.map({True: "Recommended", False: "Not Recommended"}).fillna("Unknown")
    if key == "syndication":
        raw = df.get("is_syndicated", pd.Series(pd.NA, index=df.index)).astype("boolean")
        return raw.map({True: "Syndicated", False: "Not Syndicated"}).fillna("Unknown")
    if key == "media":
        raw = df.get("has_photos", pd.Series(False, index=df.index)).astype("boolean")
        return raw.map({True: "With Photos", False: "No Photos"}).fillna("Unknown")
    return pd.Series("Unknown", index=df.index)


def _core_filter_specs_for_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    for spec in CORE_REVIEW_FILTER_SPECS:
        if spec["kind"] == "column" and spec.get("column") not in df.columns:
            continue
        s = _filter_series_for_spec(df, spec)
        opts = [x for x in sorted({str(v).strip() for v in s.dropna().astype(str) if str(v).strip()}, key=lambda x: x.lower()) if x]
        if not opts:
            continue
        if len(opts) == 1 and opts[0] == "Unknown":
            continue
        specs.append({**spec, "options": ["ALL"] + opts})
    return specs


def _col_options(df: pd.DataFrame, col: str, max_vals: Optional[int] = 250) -> List[str]:
    if col not in df.columns:
        return ["ALL"]
    s = df[col]
    vals = s.astype("string").fillna("Unknown").str.strip().replace("", "Unknown").tolist()
    uniq = list(dict.fromkeys(v for v in vals if str(v).strip()))
    uniq = sorted(uniq, key=lambda x: str(x).lower())
    if max_vals is not None:
        uniq = uniq[: int(max_vals)]
    return ["ALL"] + uniq


def _infer_extra_filter_kind(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns:
        return "categorical"
    s = df[col]
    name = str(col).lower()
    try:
        if pd.api.types.is_datetime64_any_dtype(s):
            return "date"
    except Exception:
        pass
    looks_datey = any(tok in name for tok in ["date", "time", "day", "month", "year"])
    if looks_datey and not pd.api.types.is_numeric_dtype(s):
        try:
            as_dt = pd.to_datetime(s, errors="coerce")
            if as_dt.notna().mean() >= 0.75 and as_dt.nunique(dropna=True) > 2:
                return "date"
        except Exception:
            pass
    try:
        num = pd.to_numeric(s, errors="coerce")
        if num.notna().mean() >= 0.9 and num.nunique(dropna=True) > 6:
            return "numeric"
    except Exception:
        pass
    return "categorical"


def _extra_filter_candidates(df: pd.DataFrame) -> List[str]:
    det_cols, del_cols = _get_symptom_col_lists(df)
    excluded = {
        "review_id", "title", "review_text", "title_and_text", "rating", "rating_label",
        "submission_time", "submission_date", "submission_month", "year_month_sort",
        "incentivized_review", "is_recommended", "is_syndicated", "has_photos", "has_media",
        "photo_urls", "raw_json", "context_data_json", "review_length_chars", "review_length_words",
        "AI Safety", "AI Reliability", "AI # of Sessions",
    }
    excluded.update({spec["column"] for spec in CORE_REVIEW_FILTER_SPECS if spec.get("kind") == "column"})
    excluded.update(set(det_cols + del_cols))
    excluded.update({c for c in df.columns if str(c).startswith("AI Symptom ") or str(c).startswith("Symptom ")})
    return sorted([str(c) for c in df.columns if str(c) not in excluded], key=lambda x: x.lower())


def _symptom_filter_options(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str], List[str]]:
    det_cols, del_cols = _get_symptom_col_lists(df)
    def _values(cols: Sequence[str]) -> List[str]:
        vals: List[str] = []
        for col in cols:
            if col not in df.columns:
                continue
            s = df[col].astype("string").fillna("").str.strip()
            good = s[(s != "") & (~s.str.upper().isin(SYMPTOM_NON_VALUES)) & (~s.str.startswith("<"))].str.title()
            vals.extend(good.tolist())
        return sorted(list(dict.fromkeys(vals)), key=lambda x: x.lower())
    return _values(det_cols), _values(del_cols), list(det_cols), list(del_cols)


def _filter_description_from_items(items: Sequence[Tuple[str, str]]) -> str:
    return "; ".join(f"{k}={v}" for k, v in items) if items else "No active filters"


def _render_active_filter_summary(filter_state: Dict[str, Any], overall_df: pd.DataFrame):
    active_items = filter_state.get("active_items", [])
    pills = []
    for k, v in active_items[:12]:
        pills.append(f"<div class='pill'><span class='muted'>{_esc(k)}:</span> {_esc(v)}</div>")
    st.markdown(
        f"""
<div class="soft-panel">
  <div><b>Active filters</b> • Showing <b>{len(filter_state.get('filtered_df', [])):,}</b> of <b>{len(overall_df):,}</b> reviews
  <span class="small-muted"> (filter time: {float(filter_state.get('filter_seconds', 0.0)):.3f}s)</span>
  </div>
  <div class="pill-row">{''.join(pills) if pills else '<span class="small-muted">None (All data)</span>'}</div>
</div>
""",
        unsafe_allow_html=True,
    )

def _render_workspace_nav() -> str:
    current = st.session_state.get("workspace_active_tab", TAB_DASHBOARD)
    with st.container(border=True):
        st.markdown("<div class='nav-tabs-label'>Workspace</div>", unsafe_allow_html=True)
        st.markdown("<div class='workspace-nav-sub'>Start with the Dashboard for the executive summary, then move into Explorer, AI, Prompting, Symptomizer, or the new Social Listening Beta demo for mocked Meltwater-style VOC before reviews even exist.</div>", unsafe_allow_html=True)
        dash_kwargs = {"use_container_width": True, "key": "workspace_nav_dashboard"}
        if current == TAB_DASHBOARD:
            dash_kwargs["type"] = "primary"
        if st.button(TAB_DASHBOARD, **dash_kwargs):
            current = TAB_DASHBOARD
            st.session_state["workspace_active_tab"] = TAB_DASHBOARD
        st.markdown("<div style='height:.45rem'></div>", unsafe_allow_html=True)
        rows = [
            [TAB_REVIEW_EXPLORER, TAB_AI_ANALYST],
            [TAB_REVIEW_PROMPT, TAB_SYMPTOMIZER],
            [TAB_SOCIAL_LISTENING],
        ]
        for ridx, row in enumerate(rows):
            cols = st.columns(len(row))
            for cidx, (col, label) in enumerate(zip(cols, row)):
                kwargs = {"use_container_width": True, "key": f"workspace_nav_{ridx}_{cidx}_{_slugify(label, fallback='tab')}"}
                if current == label:
                    kwargs["type"] = "primary"
                if col.button(label, **kwargs):
                    current = label
                    st.session_state["workspace_active_tab"] = label
    return current


def _default_prompt_df():
    return pd.DataFrame(REVIEW_PROMPT_STARTER_ROWS)


def _normalize_prompt_defs(prompt_df, existing_columns):
    if prompt_df is None or prompt_df.empty:
        return []
    normalized = []
    seen = set()
    existing_set = {str(c) for c in existing_columns}
    for _, row in prompt_df.fillna("").iterrows():
        rp = _safe_text(row.get("prompt"))
        rl = _safe_text(row.get("labels"))
        rc = _safe_text(row.get("column_name"))
        if not rp and not rl and not rc:
            continue
        if not rp:
            raise ReviewDownloaderError("Each prompt row needs a prompt.")
        if not rl:
            raise ReviewDownloaderError("Each prompt row needs labels.")
        labels = [l.strip() for l in rl.split(",") if l.strip()]
        deduped = list(dict.fromkeys(labels))
        if "Not Mentioned" not in deduped and len(deduped) <= 7:
            deduped.append("Not Mentioned")
        if len(deduped) < 2:
            raise ReviewDownloaderError("Each prompt needs at least two labels.")
        col = _slugify(rc or rp)
        if col in existing_set and col not in {"review_id"}:
            col = f"{col}_ai"
        base = col
        suffix = 2
        while col in seen:
            col = f"{base}_{suffix}"
            suffix += 1
        seen.add(col)
        normalized.append(dict(column_name=col, display_name=col.replace("_", " ").title(), prompt=rp, labels=deduped, labels_csv=", ".join(deduped)))
    return normalized


def _build_tagging_schema(prompt_defs):
    item_props = {"review_id": {"type": "string"}}
    required = ["review_id"]
    for p in prompt_defs:
        item_props[p["column_name"]] = {"type": "string", "enum": list(p["labels"])}
        required.append(p["column_name"])
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": item_props,
                    "required": required,
                },
            },
        },
        "required": ["results"],
    }


def _classify_chunk(*, client, chunk_df, prompt_defs):
    pc = max(len(prompt_defs), 1)
    max_out = int(max(1600, min(9000, 450 + len(chunk_df) * (18 + 10 * pc))))
    reviews_payload = [
        dict(
            review_id=_safe_text(row.get("review_id")),
            rating=_safe_int(row.get("rating"), 0) if pd.notna(row.get("rating")) else None,
            title=_trunc(row.get("title", ""), 200),
            review_text=_trunc(row.get("review_text", ""), 900),
            incentivized_review=_safe_bool(row.get("incentivized_review"), False),
        )
        for _, row in chunk_df.iterrows()
    ]
    prompt_payload = [dict(column_name=p["column_name"], prompt=p["prompt"], labels=p["labels"]) for p in prompt_defs]
    instructions = "You are a deterministic review-tagging engine. For each review and prompt, return exactly one allowed label. If not mentioned, use Not Mentioned."
    user_content = json.dumps({"prompt_definitions": prompt_payload, "reviews": reviews_payload})
    msgs = [{"role": "system", "content": instructions}, {"role": "user", "content": user_content}]
    structured_rf = {"type": "json_schema", "json_schema": {"name": "review_prompt_tagging", "schema": _build_tagging_schema(prompt_defs), "strict": True}}
    result_text = ""
    try:
        result_text = _chat_complete_with_fallback_models(
            client,
            model=_shared_model(),
            structured=True,
            messages=msgs,
            temperature=0.0,
            response_format=structured_rf,
            max_tokens=max_out,
            reasoning_effort=_shared_reasoning(),
        )
    except Exception as exc:
        col_hints = ", ".join(f'{p["column_name"]}: one of [{", ".join(p["labels"])}]' for p in prompt_defs)
        fallback_instructions = (
            "You are a deterministic review-tagging engine. Return ONLY a JSON object with key 'results' containing an array. "
            "Each element must have: review_id (string), " + col_hints + ". Include every review_id from the input. Use 'Not Mentioned' if not applicable."
        )
        result_text = _chat_complete_with_fallback_models(
            client,
            model=_shared_model(),
            structured=True,
            messages=[{"role": "system", "content": fallback_instructions}, {"role": "user", "content": user_content}],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=max_out,
            reasoning_effort=_shared_reasoning(),
        )
    if not result_text:
        raise ReviewDownloaderError("OpenAI returned an empty response. Check your API key and model selection.")
    data = _safe_json_load(result_text)
    output_rows = data.get("results") or []
    out_df = pd.DataFrame(output_rows)
    if out_df.empty:
        raise ReviewDownloaderError(f"OpenAI returned no tagged rows. Raw response snippet: {result_text[:300]}")
    out_df["review_id"] = out_df["review_id"].astype(str)
    expected = set(chunk_df["review_id"].astype(str))
    returned = set(out_df["review_id"].astype(str))
    if expected != returned:
        miss = sorted(expected - returned)
        if miss:
            import warnings
            warnings.warn(f"Batch partial: missing {miss[:5]}")
        out_df = out_df[out_df["review_id"].isin(expected)]
    return out_df


def _run_review_prompt_tagging(*, client, source_df, prompt_defs, chunk_size):
    if source_df.empty:
        raise ReviewDownloaderError("No reviews in scope.")
    chunks = list(range(0, len(source_df), chunk_size))
    prog = st.progress(0.0, text="Preparing…")
    status = st.empty()
    outputs = []
    errors = []
    for i, start in enumerate(chunks, 1):
        chunk_df = source_df.iloc[start:start + chunk_size].copy()
        status.info(f"Classifying {start + 1}–{min(start + chunk_size, len(source_df))} of {len(source_df)}")
        try:
            outputs.append(_classify_chunk(client=client, chunk_df=chunk_df, prompt_defs=prompt_defs))
        except Exception as exc:
            errors.append(f"Batch {i}: {exc}")
            status.warning(f"Batch {i} failed — {exc}")
        prog.progress(i / len(chunks))
        gc.collect()
    if not outputs:
        err_detail = "; ".join(errors[:3]) if errors else "unknown error"
        raise ReviewDownloaderError(f"All batches failed. First error: {err_detail}")
    if errors:
        status.warning(f"{len(errors)} of {len(chunks)} batch(es) failed — partial results saved.")
    else:
        status.success(f"Finished tagging {len(source_df):,} reviews.")
    return pd.concat(outputs, ignore_index=True).drop_duplicates(subset=["review_id"], keep="last")


def _merge_prompt_results(overall_df, prompt_results_df, prompt_defs):
    updated = overall_df.copy()
    rids = updated["review_id"].astype(str)
    lk = prompt_results_df.set_index("review_id")
    for p in prompt_defs:
        col = p["column_name"]
        if col not in updated.columns:
            updated[col] = pd.NA
        mapping = lk[col].to_dict()
        nv = rids.map(mapping)
        updated[col] = nv.where(nv.notna(), updated[col])
    return updated


def _summarize_prompt_results(prompt_results_df, prompt_defs, source_df=None):
    merged = prompt_results_df.copy()
    merged["review_id"] = merged["review_id"].astype(str)
    if source_df is not None and not source_df.empty and "review_id" in source_df.columns:
        lk = source_df[[c for c in ["review_id", "rating"] if c in source_df.columns]].copy()
        lk["review_id"] = lk["review_id"].astype(str)
        merged = merged.merge(lk, on="review_id", how="left")
    rows = []
    total = max(len(prompt_results_df), 1)
    for p in prompt_defs:
        col = p["column_name"]
        for label in p["labels"]:
            sub = merged[merged[col] == label]
            rows.append(dict(column_name=col, display_name=p["display_name"], label=str(label), review_count=len(sub), share=_safe_pct(len(sub), total), avg_rating=_safe_mean(sub["rating"]) if "rating" in sub.columns else None))
    return pd.DataFrame(rows)

# ═══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ═══════════════════════════════════════════════════════════════════════════════


def _filled_mask(df, cols):
    if not cols:
        return pd.Series(False, index=df.index)
    mask = pd.Series(False, index=df.index)
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c].astype("string").fillna("").str.strip()
        mask |= (s != "") & (~s.str.upper().isin(SYMPTOM_NON_VALUES)) & (~s.str.startswith("<"))
    return mask


def _match_label(raw, allowed, aliases=None, cutoff=0.76):
    if _HAS_SYMPTOMIZER_V3:
        return _v3_match_label(raw, allowed, aliases=aliases, cutoff=0.72)
    if not raw or not allowed:
        return None
    raw_s = raw.strip()
    exact = {_canon_simple(x): x for x in allowed}
    lbl = exact.get(_canon_simple(raw_s))
    if lbl:
        return lbl
    if aliases:
        for canonical, als in (aliases or {}).items():
            if canonical not in allowed:
                continue
            for a in (als or []):
                if _canon_simple(raw_s) == _canon_simple(a):
                    return canonical
    m = difflib.get_close_matches(raw_s, allowed, n=1, cutoff=cutoff)
    if m:
        return m[0]
    raw_lower = raw_s.lower()
    for label in allowed:
        if raw_lower in label.lower() or label.lower() in raw_lower:
            return label
    return None


def _validate_evidence(evidence_list, review_text, max_ev_chars=120):
    if _HAS_SYMPTOMIZER_V3:
        return _v3_validate_evidence(evidence_list, review_text, max_ev_chars)
    if not evidence_list or not review_text:
        return []
    rv = re.sub(r"\s+", " ", review_text.lower())
    _neg_patt = re.compile(r"\b(not|no|never|don't|doesn't|won't|can't|isn't|wasn't|without|hardly|barely)\s+", re.I)
    out = []
    for e in evidence_list:
        e = str(e).strip()[:max_ev_chars]
        # Minimum quality: 4+ chars (matches v3)
        if len(e) < 4:
            continue
        e_norm = re.sub(r"\s+", " ", e.lower())
        if e_norm not in rv:
            continue
        # Negation check: reject short evidence preceded by negation
        idx = rv.find(e_norm)
        if idx > 0 and len(e_norm.split()) <= 4:
            preceding = rv[max(0, idx - 30):idx]
            if _neg_patt.search(preceding):
                continue
        out.append(e)
    return out[:2]


def _build_symptom_baseline_map(processed_rows):
    baseline = {}
    for rec in processed_rows or []:
        idx = str(rec.get("idx", "")).strip()
        if not idx:
            continue
        baseline[idx] = {
            "detractors": _normalize_tag_list(rec.get("wrote_dets", [])),
            "delighters": _normalize_tag_list(rec.get("wrote_dels", [])),
        }
    return baseline


def _collect_ai_tag_map(df, row_ids=None):
    if df is None or df.empty:
        return {}
    out = {}
    ids = [str(rid) for rid in row_ids] if row_ids is not None else [str(idx) for idx in df.index]
    for rid in ids:
        try:
            row = df.loc[int(rid)]
        except Exception:
            continue
        out[rid] = {
            "detractors": _normalize_tag_list(_collect_row_symptom_tags(row, AI_DET_HEADERS)),
            "delighters": _normalize_tag_list(_collect_row_symptom_tags(row, AI_DEL_HEADERS)),
        }
    return out


def _write_ai_symptom_row(df, idx, *, dets=None, dels=None, safety=None, reliability=None, sessions=None):
    df = _ensure_ai_cols(df)
    if dets is None:
        det_values = _normalize_tag_list(_collect_row_symptom_tags(df.loc[idx], AI_DET_HEADERS))
    else:
        det_values = _normalize_tag_list(dets or [])[:10]
    if dels is None:
        del_values = _normalize_tag_list(_collect_row_symptom_tags(df.loc[idx], AI_DEL_HEADERS))
    else:
        del_values = _normalize_tag_list(dels or [])[:10]

    for header in AI_DET_HEADERS:
        df.loc[idx, header] = None
    for header in AI_DEL_HEADERS:
        df.loc[idx, header] = None
    for j, label in enumerate(det_values, start=1):
        df.loc[idx, f"AI Symptom Detractor {j}"] = label
    for j, label in enumerate(del_values, start=1):
        df.loc[idx, f"AI Symptom Delighter {j}"] = label
    if safety is not None:
        df.loc[idx, "AI Safety"] = safety
    if reliability is not None:
        df.loc[idx, "AI Reliability"] = reliability
    if sessions is not None:
        df.loc[idx, "AI # of Sessions"] = sessions
    return df


def _qa_accuracy_metrics(reviews_df):
    baseline = st.session_state.get("sym_qa_baseline_map") or {}
    if not baseline:
        return {
            "baseline_total_tags": 0,
            "added_tags": 0,
            "removed_tags": 0,
            "total_changes": 0,
            "changed_reviews": 0,
            "accuracy_pct": 100.0,
        }
    current = _collect_ai_tag_map(reviews_df, row_ids=list(baseline.keys()))
    metrics = _compute_tag_edit_accuracy(baseline, current)
    st.session_state["sym_qa_accuracy"] = metrics
    return metrics


def _parse_manual_tag_entries(raw_text):
    return _normalize_tag_list([part.strip() for part in re.split(r"[\n,;|]+", str(raw_text or "")) if part.strip()])


def _standardize_symptom_lists(delighters=None, detractors=None):
    try:
        return _canonicalize_taxonomy_catalog(delighters or [], detractors or [])
    except Exception:
        return _normalize_tag_list(delighters or []), _normalize_tag_list(detractors or []), {}


def _universal_neutral_catalog():
    custom_dels, custom_dets = _custom_universal_catalog()
    return _normalize_tag_list(list(_UNIVERSAL_NEUTRAL_DELIGHTERS) + list(custom_dels)), _normalize_tag_list(list(_UNIVERSAL_NEUTRAL_DETRACTORS) + list(custom_dets))


def _all_universal_neutral_labels():
    neutral_dels, neutral_dets = _universal_neutral_catalog()
    return set(neutral_dels + neutral_dets)


def _save_custom_universal_catalog(delighters=None, detractors=None, *, merge=False):
    current_dels, current_dets = _custom_universal_catalog() if merge else ([], [])
    raw_dels = list(current_dels) + list(delighters or [])
    raw_dets = list(current_dets) + list(detractors or [])
    saved_dels, saved_dets, extra_aliases = _standardize_symptom_lists(raw_dels, raw_dets)
    built_in_dels = set(_UNIVERSAL_NEUTRAL_DELIGHTERS)
    built_in_dets = set(_UNIVERSAL_NEUTRAL_DETRACTORS)
    saved_dels = [label for label in saved_dels if label not in built_in_dels]
    saved_dets = [label for label in saved_dets if label not in built_in_dets]
    st.session_state["sym_custom_universal_delighters"] = saved_dels
    st.session_state["sym_custom_universal_detractors"] = saved_dets
    active_dels, active_dets = _canonical_symptom_catalog(st.session_state.get("sym_delighters") or [], st.session_state.get("sym_detractors") or [])
    st.session_state["sym_delighters"] = active_dels
    st.session_state["sym_detractors"] = active_dets
    st.session_state["sym_aliases"] = _alias_map_for_catalog(
        active_dels,
        active_dets,
        extra_aliases=extra_aliases,
        existing_aliases=st.session_state.get("sym_aliases", {}),
    )
    return saved_dels, saved_dets


def _promote_labels_to_custom_universal(labels, *, side):
    cleaned = _normalize_tag_list(labels or [])
    if not cleaned:
        return [], []
    if str(side).lower().startswith("del"):
        return _save_custom_universal_catalog(delighters=cleaned, detractors=[], merge=True)
    return _save_custom_universal_catalog(delighters=[], detractors=cleaned, merge=True)


def _canonical_symptom_catalog(delighters=None, detractors=None, include_universal_neutral=None):
    if include_universal_neutral is None:
        include_universal_neutral = bool(st.session_state.get("sym_include_universal_neutral", True))
    neutral_dels, neutral_dets = _universal_neutral_catalog()
    all_universal = set(neutral_dels + neutral_dets)
    del_seed, det_seed, _ = _standardize_symptom_lists(delighters or [], detractors or [])
    del_seed = [label for label in del_seed if label not in all_universal]
    det_seed = [label for label in det_seed if label not in all_universal]
    if include_universal_neutral:
        del_seed = list(neutral_dels) + del_seed
        det_seed = list(neutral_dets) + det_seed
    dels, dets, _ = _standardize_symptom_lists(del_seed, det_seed)
    return list(dels), list(dets)


def _scrub_universal_neutral_from_processed_rows(processed_rows):
    universal_labels = _all_universal_neutral_labels()
    cleaned = []
    for rec in processed_rows or []:
        rec2 = dict(rec)
        rec2["wrote_dets"] = [tag for tag in _normalize_tag_list(rec2.get("wrote_dets") or []) if tag not in universal_labels][:10]
        rec2["wrote_dels"] = [tag for tag in _normalize_tag_list(rec2.get("wrote_dels") or []) if tag not in universal_labels][:10]
        rec2["ev_det"] = {k: list(v or [])[:2] for k, v in (rec2.get("ev_det") or {}).items() if k in rec2["wrote_dets"]}
        rec2["ev_del"] = {k: list(v or [])[:2] for k, v in (rec2.get("ev_del") or {}).items() if k in rec2["wrote_dels"]}
        cleaned.append(rec2)
    return cleaned


def _scrub_universal_neutral_from_reviews_df(reviews_df):
    if reviews_df is None or getattr(reviews_df, "empty", True):
        return reviews_df, 0
    universal_labels = _all_universal_neutral_labels()
    edited = reviews_df.copy()
    changed_rows = 0
    for idx, row in reviews_df.iterrows():
        current_dets = _normalize_tag_list(_collect_row_symptom_tags(row, AI_DET_HEADERS))
        current_dels = _normalize_tag_list(_collect_row_symptom_tags(row, AI_DEL_HEADERS))
        next_dets = [tag for tag in current_dets if tag not in universal_labels]
        next_dels = [tag for tag in current_dels if tag not in universal_labels]
        if next_dets == current_dets and next_dels == current_dels:
            continue
        edited = _write_ai_symptom_row(
            edited,
            int(idx),
            dets=next_dets,
            dels=next_dels,
            safety=row.get("AI Safety"),
            reliability=row.get("AI Reliability"),
            sessions=row.get("AI # of Sessions"),
        )
        changed_rows += 1
    return edited, changed_rows


def _apply_universal_neutral_toggle(include_enabled):
    current_dels = list(st.session_state.get("sym_delighters") or [])
    current_dets = list(st.session_state.get("sym_detractors") or [])
    new_dels, new_dets = _canonical_symptom_catalog(current_dels, current_dets, include_universal_neutral=include_enabled)
    st.session_state["sym_delighters"] = new_dels
    st.session_state["sym_detractors"] = new_dets
    st.session_state["sym_aliases"] = _alias_map_for_catalog(new_dels, new_dets, existing_aliases=st.session_state.get("sym_aliases", {}))
    if include_enabled:
        st.session_state["sym_run_notice"] = "Universal Neutral Symptoms added back into the catalog for future tagging and inline edits."
        return

    dataset_edit = dict(st.session_state.get("analysis_dataset") or {})
    reviews_df = dataset_edit.get("reviews_df")
    reviews_df, changed_rows = _scrub_universal_neutral_from_reviews_df(reviews_df)
    if reviews_df is not None:
        dataset_edit["reviews_df"] = reviews_df
        st.session_state["analysis_dataset"] = dataset_edit
    processed_rows = st.session_state.get("sym_processed_rows") or []
    if processed_rows:
        st.session_state["sym_processed_rows"] = _scrub_universal_neutral_from_processed_rows(processed_rows)
    if reviews_df is not None:
        try:
            original_bytes = st.session_state.get("_uploaded_raw_bytes")
            summary_obj = dataset_edit.get("summary") or (st.session_state.get("analysis_dataset") or {}).get("summary")
            if original_bytes:
                st.session_state["sym_export_bytes"] = _gen_symptomized_workbook(original_bytes, reviews_df)
            elif summary_obj is not None:
                st.session_state["sym_export_bytes"] = _build_master_excel(summary_obj, reviews_df)
            else:
                st.session_state["sym_export_bytes"] = None
        except Exception:
            st.session_state["sym_export_bytes"] = None
        _qa_accuracy_metrics(reviews_df)
    st.session_state["sym_run_notice"] = f"Universal Neutral Symptoms removed from the catalog and stripped from {changed_rows:,} currently tagged review(s)."


def _sync_symptom_catalog_session(*, default_source=None):
    current_dels = list(st.session_state.get("sym_delighters") or [])
    current_dets = list(st.session_state.get("sym_detractors") or [])
    dels, dets = _canonical_symptom_catalog(current_dels, current_dets)
    changed = dels != current_dels or dets != current_dets
    if changed:
        st.session_state["sym_delighters"] = dels
        st.session_state["sym_detractors"] = dets
    if default_source and st.session_state.get("sym_symptoms_source", "none") in {"", "none"} and (dels or dets):
        st.session_state["sym_symptoms_source"] = default_source
    return dels, dets, changed


def _apply_pending_symptomizer_ui_state():
    pending_desc = st.session_state.pop("_sym_pdesc_pending", None)
    if pending_desc is not None:
        value = _safe_text(pending_desc)
        st.session_state["sym_pdesc"] = value
        st.session_state["sym_product_profile"] = value
    pending_inline = st.session_state.pop("_sym_inline_defaults_pending", None) or {}
    for row_id, payload in pending_inline.items():
        rid = str(row_id).strip()
        if not rid:
            continue
        st.session_state[f"sym_inline_det_select_{rid}"] = _normalize_tag_list((payload or {}).get("dets") or [])
        st.session_state[f"sym_inline_del_select_{rid}"] = _normalize_tag_list((payload or {}).get("dels") or [])
        st.session_state[f"sym_inline_det_new_{rid}"] = _safe_text((payload or {}).get("new_det"))
        st.session_state[f"sym_inline_del_new_{rid}"] = _safe_text((payload or {}).get("new_del"))


def _queue_inline_editor_defaults(row_id, *, dets=None, dels=None, new_det="", new_del=""):
    rid = str(row_id).strip()
    if not rid:
        return
    pending = dict(st.session_state.get("_sym_inline_defaults_pending") or {})
    pending[rid] = {
        "dets": _normalize_tag_list(dets or []),
        "dels": _normalize_tag_list(dels or []),
        "new_det": _safe_text(new_det),
        "new_del": _safe_text(new_del),
    }
    st.session_state["_sym_inline_defaults_pending"] = pending


def _coerce_product_knowledge_list(values, *, max_items=8):
    out = []
    seen = set()
    for raw in values or []:
        item = re.sub(r"\s+", " ", str(raw or "").strip().strip("•-"))
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= max_items:
            break
    return out


def _titleize_theme_label(value, *, default=""):
    text = re.sub(r"\s+", " ", str(value or "").strip().strip("•-"))
    if not text:
        return default
    words = [w for w in re.split(r"[^A-Za-z0-9']+", text) if w]
    if not words:
        return default
    return " ".join(word.capitalize() if not word.isupper() else word for word in words[:6])


def _dedup_candidates(raw):
    def _norm(s):
        s = s.strip().lower()
        s = re.sub(r"^(not\s+too\s+|not\s+very\s+|not\s+overly\s+|not\s+)", "", s)
        s = re.sub(r"[^a-z0-9 ]", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    labels = sorted(raw.keys(), key=lambda l: -int(raw[l].get("count", 0)))
    merged = {}
    used = set()
    for a in labels:
        if a in used:
            continue
        merged[a] = dict(raw[a])
        used.add(a)
        na = _norm(a)
        for b in labels:
            if b in used or b == a:
                continue
            nb = _norm(b)
            if difflib.SequenceMatcher(None, na, nb).ratio() >= 0.72 or na in nb or nb in na:
                merged[a]["count"] = int(merged[a].get("count", 0)) + int(raw[b].get("count", 0))
                refs = list(merged[a].get("refs", []))
                for r in raw[b].get("refs", []):
                    if r not in refs and len(refs) < 50:
                        refs.append(r)
                merged[a]["refs"] = refs
                merged[a].setdefault("_merged_from", []).append(b)
                used.add(b)
    return merged


def _record_new_symptom_candidate(label, *, idx, side):
    lab = _safe_text(label).strip()
    if not lab:
        return
    bucket = st.session_state.setdefault("sym_new_candidates", {}).setdefault(
        lab,
        {"count": 0, "refs": [], "delighter_count": 0, "detractor_count": 0, "delighter_refs": [], "detractor_refs": []},
    )
    bucket["count"] = int(bucket.get("count", 0)) + 1
    if idx not in bucket.get("refs", []) and len(bucket.get("refs", [])) < 50:
        bucket.setdefault("refs", []).append(idx)
    side_key = "delighter" if str(side).lower().startswith("del") else "detractor"
    count_key = f"{side_key}_count"
    refs_key = f"{side_key}_refs"
    bucket[count_key] = int(bucket.get(count_key, 0)) + 1
    if idx not in bucket.get(refs_key, []) and len(bucket.get(refs_key, [])) < 50:
        bucket.setdefault(refs_key, []).append(idx)


def _try_load_symptoms_from_file():
    raw = st.session_state.get("_uploaded_raw_bytes")
    if not raw:
        return False
    d, t, a = _get_symptom_whitelists(raw)
    if d or t:
        loaded_dels, loaded_dets = _canonical_symptom_catalog(d, t)
        st.session_state.update(sym_delighters=loaded_dels, sym_detractors=loaded_dets, sym_aliases=_alias_map_for_catalog(loaded_dels, loaded_dets, extra_aliases=a, existing_aliases=st.session_state.get("sym_aliases", {})), sym_symptoms_source="file", sym_taxonomy_preview_items=[], sym_taxonomy_category="general")
        return True
    return False


def _local_symptom_catalog(df):
    if df is None or df.empty:
        return [], []
    det_vals, del_vals, _, _ = _symptom_filter_options(df)
    if not del_vals and not det_vals:
        return [], []
    del_vals, det_vals = _canonical_symptom_catalog(list(del_vals), list(det_vals))
    return list(del_vals), list(det_vals)

# ═══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════


def _render_metric_card(label, value, subtext, accent=False):
    cls = "metric-card accent" if accent else "metric-card"
    st.markdown(f"""<div class="{cls}">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value}</div>
      <div class="metric-sub">{subtext}</div>
    </div>""", unsafe_allow_html=True)


def _render_workspace_header(summary, overall_df, filtered_df, prompt_artifacts, *, source_type, source_label, filter_description, active_items):
    export_df = filtered_df.copy()
    bundle, bundle_error = _safe_get_master_bundle(
        summary,
        export_df,
        prompt_artifacts,
        active_items=active_items,
        filter_description=filter_description,
        export_scope_label="Filtered current view" if active_items else "Current view (all reviews)",
        total_loaded_reviews=len(overall_df),
    )
    product_name = _product_name(summary, overall_df)
    organic = int((~overall_df["incentivized_review"].fillna(False)).sum()) if not overall_df.empty else 0
    n = len(overall_df)
    view_count = len(filtered_df)
    if source_type == "uploaded":
        src_chip = f"Uploaded · {source_label}"
    elif source_type == "multi-url":
        src_chip = f"Multi-link batch · {source_label}"
    elif source_type == "powerreviews":
        src_chip = f"{(source_label or 'PowerReviews')} · {summary.product_id}"
    elif source_type == "bazaarvoice":
        src_chip = f"{(source_label or 'Bazaarvoice')} · {summary.product_id}"
    else:
        src_chip = f"{(source_label or str(source_type).title())} · {summary.product_id}"
    st.markdown(f"""<div class="hero-card">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;">
        <div>
          <div class="hero-kicker">Review workspace · Beta</div>
          <div class="hero-title">{_esc(product_name)}</div>
        </div>
        <div class="badge-row">
          <span class="chip gray">{_esc(src_chip)}</span>
          <span class="chip yellow">Beta</span>
          <span class="chip blue">{'Filtered view' if active_items else 'All reviews'}</span>
          <span class="chip indigo">{n:,} reviews</span>
          <span class="chip green">{organic:,} organic</span>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)
    a0, a1, a2 = st.columns([1.35, 1.15, 4])
    if bundle is not None:
        a0.download_button("⬇️ Download current view", data=bundle["excel_bytes"], file_name=bundle["excel_name"], mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    else:
        a0.button("⬇️ Download current view", use_container_width=True, disabled=True, key="ws_dl_disabled")
    if a1.button("🔄 Reset workspace", use_container_width=True):
        _reset_workspace_state(reset_source=True)
        st.rerun()
    if bundle_error is not None:
        a2.caption(f"Download export is temporarily unavailable: {bundle_error}")
    else:
        a2.caption(f"Download includes {view_count:,} row(s) from the current view, a FilterCriteria sheet, Rating Distribution, Volume trend, and any AI prompt or Symptomizer columns.")


def _render_top_metrics(overall_df, filtered_df):
    m = _get_metrics(filtered_df)
    recommend_value = _fmt_pct(m.get("recommend_rate")) if m.get("recommend_rate") is not None else "n/a"
    recommend_sub = "Share of reviews with recommendation data" if m.get("recommend_rate") is not None else "No recommendation field available"
    organic_share = max(0.0, 1.0 - float(m.get("pct_incentivized") or 0.0))
    cards = [
        ("Reviews in view", f"{m['review_count']:,}", f"of {len(overall_df):,} loaded", False),
        ("Avg rating", _fmt_num(m["avg_rating"]), f"Organic avg {_fmt_num(m['avg_rating_non_incentivized'])}", False),
        ("Recommend rate", recommend_value, recommend_sub, False),
        ("% 1-2 star", _fmt_pct(m["pct_low_star"]), f"{m['low_star_count']:,} low-star reviews", True),
        ("% organic", _fmt_pct(organic_share), f"{m['non_incentivized_count']:,} organic reviews", False),
    ]
    cols = st.columns(len(cards))
    for col, (label, value, sub, acc) in zip(cols, cards):
        with col:
            _render_metric_card(label, value, sub, accent=acc)


_REVIEW_REF_PATTERN = re.compile(r"\(review_ids?\s*:\s*([^)]+)\)", flags=re.IGNORECASE)


def _reference_preview_rows(review_ids: Sequence[str], df: pd.DataFrame, max_items: int = 4) -> List[Dict[str, str]]:
    if df is None or df.empty:
        return []

    def _clean_rid(value: Any) -> str:
        rid = str(value or "").strip()
        rid = re.sub(r'^[`\'"\s]+', '', rid)
        rid = re.sub(r'[`\'"\s.,;:()\[\]{}]+$', '', rid)
        return rid.strip()

    lookup = df.copy()
    lookup["review_id"] = lookup["review_id"].astype(str)
    lookup["__rid_norm"] = lookup["review_id"].astype(str).str.strip().str.lower()
    lookup["__rid_simple"] = lookup["__rid_norm"].str.replace(r"[^a-z0-9]+", "", regex=True)
    out = []
    used = set()
    for rid in review_ids:
        cleaned = _clean_rid(rid)
        if not cleaned:
            continue
        rid_norm = cleaned.lower()
        if rid_norm in used:
            continue
        used.add(rid_norm)
        hit = lookup[lookup["__rid_norm"] == rid_norm]
        if hit.empty:
            rid_simple = re.sub(r"[^a-z0-9]+", "", rid_norm)
            if rid_simple:
                hit = lookup[lookup["__rid_simple"] == rid_simple]
        if hit.empty:
            hit = lookup[lookup["review_id"].astype(str).str.contains(re.escape(cleaned), case=False, na=False)].head(1)
        if hit.empty:
            continue
        row = hit.iloc[0]
        title = _safe_text(row.get("title"), "Untitled review") or "Untitled review"
        snippet = _trunc(_safe_text(row.get("review_text")) or _safe_text(row.get("title_and_text")), 220)
        meta = []
        if pd.notna(row.get("rating")):
            meta.append(f"★{_safe_int(row.get('rating'), 0)}")
        if _safe_text(row.get("submission_date")):
            meta.append(_safe_text(row.get("submission_date")))
        if _safe_text(row.get("content_locale")):
            meta.append(_safe_text(row.get("content_locale")))
        out.append({"meta": " · ".join(meta), "title": title, "snippet": snippet})
        if len(out) >= max_items:
            break
    return out


def _reference_tile_html_from_ids(review_ids: Sequence[str], df: pd.DataFrame, *, label: str = "Reference") -> str:
    ids = [str(x).strip() for x in review_ids if str(x).strip()]
    previews = _reference_preview_rows(ids, df)
    if not previews:
        raw_ids = ", ".join(ids[:4]) + ("…" if len(ids) > 4 else "")
        tip = f"<div class='ref-empty'>Referenced review preview not available in the loaded dataset. {_esc(raw_ids) if raw_ids else ''}</div>"
    else:
        bits = []
        for item in previews:
            bits.append(
                "<div class='ref-item'>"
                + (f"<div class='ref-meta'>{_esc(item['meta'])}</div>" if item.get("meta") else "")
                + f"<div class='ref-title'>{_esc(item['title'])}</div>"
                + f"<div class='ref-snippet'>{_esc(item['snippet'])}</div>"
                + "</div>"
            )
        extra = max(0, len(ids) - len(previews))
        if extra:
            bits.append(f"<div class='ref-item'><div class='ref-empty'>+{extra} more referenced review(s)</div></div>")
        tip = "".join(bits)
    return f"<span class='ref-wrap' tabindex='0' role='button' aria-label='{_esc(label)} review reference'><span class='ref-tile'>{_esc(label)}</span><span class='ref-tip'><span class='ref-tip-inner'>{tip}</span></span></span>"


def _normalize_ai_answer_display(text: str) -> str:
    raw = str(text or "")
    if not raw:
        return ""
    lines = []
    for line in raw.splitlines():
        if re.match(r"^\s{0,3}#{1,6}\s+", line):
            section = re.sub(r"^\s{0,3}#{1,6}\s+", "", line).strip()
            section = section.rstrip(":")
            lines.append(f"**{section}**" if section else "")
        else:
            lines.append(line)
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _replace_review_citations_with_reference_tiles(text: str, df: pd.DataFrame) -> str:
    normalized = _normalize_ai_answer_display(text)
    safe = html.escape(normalized, quote=False)
    def repl(match):
        raw = match.group(1)
        ids = [p.strip() for p in re.split(r"[,;\n|]+", raw) if p.strip()]
        if len(ids) <= 1:
            token_ids = [
                tok for tok in re.findall(r"[A-Za-z0-9_-]{3,}", raw)
                if tok.lower() not in {"review", "reviews", "reviewid", "reviewids", "review_id", "review_ids", "id", "ids"}
            ]
            if token_ids:
                ids = token_ids
        return _reference_tile_html_from_ids(ids, df, label="Reference")
    return _REVIEW_REF_PATTERN.sub(repl, safe)


def _render_markdown_with_reference_tiles(text: str, df: pd.DataFrame):
    processed = _replace_review_citations_with_reference_tiles(text, df)
    st.markdown(processed, unsafe_allow_html=True)


def _render_ai_response(text: str, df: pd.DataFrame, *, include_references: bool):
    if include_references:
        _render_markdown_with_reference_tiles(text, df)
    else:
        st.markdown(_strip_review_citations(text))


def _build_evidence_lookup(processed_rows):
    lookup = {}
    for rec in processed_rows:
        idx = str(rec.get("idx", ""))
        if not idx:
            continue
        entries = []
        for lab, evs in (rec.get("ev_det", {}) or {}).items():
            for e in (evs or []):
                if e and e.strip():
                    entries.append((e.strip(), lab))
        for lab, evs in (rec.get("ev_del", {}) or {}).items():
            for e in (evs or []):
                if e and e.strip():
                    entries.append((e.strip(), lab))
        if entries:
            lookup[idx] = entries
    return lookup


def _render_dashboard(filtered_df, overall_df=None):
    od = overall_df if overall_df is not None else filtered_df
    st.markdown("<div class='section-title'>Insights</div>", unsafe_allow_html=True)
    if not dataset:
        st.markdown("""<div class='hero-card' style='padding:20px;text-align:center;'>
        <div style='font-size:16px;font-weight:600;color:var(--navy);margin-bottom:8px;'>Welcome to StarWalk Review Analyst</div>
        <div style='font-size:13px;color:var(--slate-500);line-height:1.6;'>
        Paste a product page URL below to get started. The app will automatically discover the product profile and generate insights.<br>
        <strong>Supported:</strong> SharkNinja US/UK, Costco, Sephora, Ulta, Hoka, CurrentBody, BazaarVoice, PowerReviews, Okendo
        </div></div>""", unsafe_allow_html=True)
        return
    # ── Auto-generated key insights (no AI needed) ───────────────────────
    try:
        n_reviews = len(filtered_df)
        if n_reviews >= 5:
            avg_rating = pd.to_numeric(filtered_df.get("rating"), errors="coerce").mean()
            organic_mask = ~filtered_df.get("incentivized_review", pd.Series(False, index=filtered_df.index)).fillna(False)
            organic_avg = pd.to_numeric(filtered_df.loc[organic_mask, "rating"], errors="coerce").mean() if organic_mask.any() else avg_rating
            recent_30 = filtered_df[pd.to_datetime(filtered_df.get("submission_time"), errors="coerce", utc=True) >= (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30))]
            recent_avg = pd.to_numeric(recent_30.get("rating"), errors="coerce").mean() if len(recent_30) >= 3 else None
            low_pct = (pd.to_numeric(filtered_df.get("rating"), errors="coerce") <= 2).mean()
            insights = []
            if pd.notna(avg_rating):
                insights.append(f"**{avg_rating:.2f}★** avg across {n_reviews:,} reviews")
            if pd.notna(organic_avg) and organic_mask.sum() >= 3:
                delta = organic_avg - (avg_rating or 0)
                if abs(delta) >= 0.1:
                    direction = "higher" if delta > 0 else "lower"
                    insights.append(f"Organic reviews avg **{organic_avg:.2f}★** ({direction} than overall)")
            if recent_avg is not None and pd.notna(recent_avg) and pd.notna(avg_rating):
                delta = recent_avg - avg_rating
                if abs(delta) >= 0.15:
                    trend = "📈 trending up" if delta > 0 else "📉 trending down"
                    insights.append(f"Last 30 days: **{recent_avg:.2f}★** ({trend})")
            if low_pct >= 0.15:
                insights.append(f"⚠️ **{low_pct:.0%}** of reviews are 1-2★")
            if insights:
                st.markdown("<div class='hero-card' style='padding:12px 16px;'>" + " · ".join(insights) + "</div>", unsafe_allow_html=True)
    except Exception:
        pass
    # ── Symptom Insights (auto-populated from symptomizer results) ────
    try:
        det_cols = [c for c in filtered_df.columns if c.startswith("AI Symptom Det")]
        del_cols = [c for c in filtered_df.columns if c.startswith("AI Symptom Del")]
        if det_cols or del_cols:
            from collections import Counter
            det_counts = Counter()
            del_counts = Counter()
            for _, row in filtered_df.iterrows():
                for c in det_cols:
                    v = _safe_text(row.get(c))
                    if v and v.upper() not in NON_VALUES: det_counts[v] += 1
                for c in del_cols:
                    v = _safe_text(row.get(c))
                    if v and v.upper() not in NON_VALUES: del_counts[v] += 1
            if det_counts or del_counts:
                st.markdown("<div style='margin:12px 0 4px;font-size:13px;font-weight:600;color:var(--navy);'>Symptom snapshot</div>", unsafe_allow_html=True)
                ic1, ic2 = st.columns(2)
                with ic1:
                    if det_counts:
                        top3_det = det_counts.most_common(3)
                        det_html = "".join(
                            f"<div style='display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--border);'>"
                            f"<span style='font-size:12.5px;'>🔴 {_esc(label)}</span>"
                            f"<span style='font-size:12px;color:var(--slate-400);'>{count} ({count/max(len(filtered_df),1)*100:.0f}%)</span></div>"
                            for label, count in top3_det
                        )
                        st.markdown(f"<div style='font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--danger);font-weight:700;margin-bottom:4px;'>Top issues</div>{det_html}", unsafe_allow_html=True)
                with ic2:
                    if del_counts:
                        top3_del = del_counts.most_common(3)
                        del_html = "".join(
                            f"<div style='display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--border);'>"
                            f"<span style='font-size:12.5px;'>🟢 {_esc(label)}</span>"
                            f"<span style='font-size:12px;color:var(--slate-400);'>{count} ({count/max(len(filtered_df),1)*100:.0f}%)</span></div>"
                            for label, count in top3_del
                        )
                        st.markdown(f"<div style='font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--success);font-weight:700;margin-bottom:4px;'>Top strengths</div>{del_html}", unsafe_allow_html=True)
    except Exception:
        pass
    # Quick action: symptomize remaining reviews
    try:
        det_cols = [c for c in filtered_df.columns if c.startswith("AI Symptom Det")]
        if det_cols:
            tagged_count = sum(1 for _, row in filtered_df.iterrows()
                             if any(_safe_text(row.get(c)) and _safe_text(row.get(c)).upper() not in NON_VALUES for c in det_cols[:3]))
            untagged = len(filtered_df) - tagged_count
            if untagged > 0 and tagged_count > 0:
                ic1, ic2 = st.columns([3, 1])
                ic1.caption(f"{tagged_count:,} of {len(filtered_df):,} reviews tagged ({tagged_count/max(len(filtered_df),1)*100:.0f}%)")
                if ic2.button(f"Tag {untagged:,} more →", key="dash_sym_more", use_container_width=True):
                    st.session_state["workspace_active_tab"] = TAB_SYMPTOMIZER
                    st.session_state["workspace_tab_request"] = TAB_SYMPTOMIZER
                    st.rerun()
    except Exception:
        pass
    st.markdown("<div class='section-sub'>Start with the overall time trend and symptom tables. Additional charts are tucked into a cleaner secondary section below.</div>", unsafe_allow_html=True)
    sym_state = _detect_symptom_state(od)
    if sym_state == "none":
        st.markdown("""<div class="sym-state-banner" style="padding:1.5rem 1.8rem;text-align:left;display:flex;align-items:center;gap:16px;margin-bottom:.5rem;">
          <div style="font-size:2.2rem;flex-shrink:0;">💊</div>
          <div style="flex:1;">
            <div class="title" style="margin-bottom:4px;font-size:15px;">No symptoms tagged yet</div>
            <div class="sub" style="max-width:none;font-size:12.5px;">Run the Symptomizer to AI-tag delighters &amp; detractors — they'll surface here once complete.</div>
          </div>
        </div>""", unsafe_allow_html=True)
        if st.button("💊 Go to Symptomizer →", type="primary", key="dash_go_sym", use_container_width=False):
            st.session_state["workspace_tab_request"] = TAB_SYMPTOMIZER
            st.rerun()
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    scope = st.radio("Scope", ["All matching reviews", "Organic only"], horizontal=True, key="dashboard_scope")
    chart_df = filtered_df.copy()
    if scope == "Organic only":
        chart_df = chart_df[~chart_df["incentivized_review"].fillna(False)].reset_index(drop=True)
    if chart_df.empty:
        st.info("No reviews match the current scope.")
        return

    st.markdown("<div style='height:.3rem'></div>", unsafe_allow_html=True)
    _render_dashboard_snapshot(chart_df, od)
    _render_reviews_over_time_chart(chart_df)

    st.markdown("<div style='height:.75rem'></div>", unsafe_allow_html=True)
    _render_symptom_dashboard(chart_df, od)

    with st.expander("📊 Additional dashboard views", expanded=False):
        dash_tabs = st.tabs(["Rating mix", "Cohorts", "Sentiment", "Markets", "Review depth"])

        with dash_tabs[0]:
            rating_df = _rating_dist(chart_df)
            rating_df["rating_label"] = rating_df["rating"].map(lambda v: f"{int(v)}★")
            rating_df["count_pct_label"] = rating_df.apply(lambda r: f"{int(r['review_count']):,} · {_fmt_pct(r['share'])}", axis=1)
            with st.container(border=True):
                _render_chart_header("Rating distribution", "Volume and share by star rating for the current view.")
                fig = px.bar(
                    rating_df,
                    x="rating_label",
                    y="review_count",
                    text="count_pct_label",
                    category_orders={"rating_label": ["1★", "2★", "3★", "4★", "5★"]},
                    color="rating",
                    color_discrete_map={"1": "#ef4444", "2": "#f97316", "3": "#eab308", "4": "#84cc16", "5": "#22c55e"},
                    hover_data={"share": ":.1%", "review_count": True},
                )
                fig.update_traces(textposition="outside", cliponaxis=False, showlegend=False)
                fig.update_layout(title=None, margin=dict(l=20, r=18, t=18, b=20), xaxis_title="", yaxis_title="Reviews", height=330, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter")
                fig = _sw_style_fig(fig)
                fig.update_layout(title=None, margin=dict(l=24, r=18, t=18, b=32))
                _show_plotly(fig)

        with dash_tabs[1]:
            cohort_df = _cohort_by_incentivized(chart_df)
            with st.container(border=True):
                _render_chart_header("Rating split: Organic vs Incentivized", "Compare cohort distributions without crowding the plot area.")
                if cohort_df.empty:
                    st.info("No cohort data.")
                else:
                    fig_c = px.bar(cohort_df, x="star", y="pct", color="cohort", barmode="group", labels={"star": "Star", "pct": "% of cohort", "cohort": "Cohort"}, color_discrete_map={"Organic": "#6366f1", "Incentivized": "#f59e0b"})
                    fig_c.update_layout(xaxis=dict(tickmode="array", tickvals=[1, 2, 3, 4, 5], ticktext=["1★", "2★", "3★", "4★", "5★"]), title=None, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter", margin=dict(l=20, r=18, t=18, b=48), height=330)
                    fig_c.update_yaxes(ticksuffix="%")
                    fig_c = _sw_style_fig(fig_c)
                    fig_c.update_layout(title=None, legend=dict(orientation="h", y=-0.22, x=0, xanchor="left", yanchor="top", font=dict(size=11)), margin=dict(l=24, r=18, t=18, b=78))
                    _show_plotly(fig_c)

        with dash_tabs[2]:
            sb_df = _star_band_trend(chart_df)
            with st.container(border=True):
                _render_chart_header("Sentiment drift over time", "Track how low-star and high-star mix moves by month.")
                if sb_df.empty:
                    st.info("Insufficient date data for sentiment trend.")
                else:
                    fig_sb = go.Figure()
                    fig_sb.add_trace(go.Scatter(x=sb_df["month_start"], y=sb_df["pct_low"], name="% 1-2★", mode="lines+markers", line=dict(color="#ef4444", width=2), marker=dict(size=4), fill="tozeroy", fillcolor="rgba(239,68,68,0.08)"))
                    fig_sb.add_trace(go.Scatter(x=sb_df["month_start"], y=sb_df["pct_high"], name="% 4-5★", mode="lines+markers", line=dict(color="#22c55e", width=2), marker=dict(size=4)))
                    fig_sb.update_layout(title=None, hovermode="x unified", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter", margin=dict(l=20, r=18, t=18, b=48), height=330)
                    fig_sb.update_yaxes(ticksuffix="%", title="% of monthly reviews")
                    fig_sb = _sw_style_fig(fig_sb)
                    fig_sb.update_layout(title=None, legend=dict(orientation="h", y=-0.22, x=0, xanchor="left", yanchor="top", font=dict(size=11)), margin=dict(l=24, r=18, t=18, b=78))
                    _show_plotly(fig_sb)

        with dash_tabs[3]:
            with st.container(border=True):
                _render_chart_header("Top markets by review volume", "Review count by locale, with average rating shown as scaled markers.")
                locale_df = _locale_breakdown(chart_df, top_n=None)
                if locale_df.empty:
                    st.info("No locale data.")
                else:
                    fig_loc = go.Figure()
                    fig_loc.add_trace(go.Bar(x=locale_df["count"], y=locale_df["content_locale"], orientation="h", name="Reviews", marker_color="#6366f1", opacity=0.82, hovertemplate="%{y}<br>%{x:,} reviews<extra></extra>"))
                    fig_loc.add_trace(go.Scatter(x=locale_df["avg_rating"] * locale_df["count"].max() / 5, y=locale_df["content_locale"], mode="markers", name="Avg ★ (scaled)", marker=dict(color=locale_df["avg_rating"], colorscale="RdYlGn", cmin=1, cmax=5, size=9, showscale=True, colorbar=dict(title="Avg ★", len=0.42, x=1.01)), hovertemplate="%{y}<br>Avg ★: %{text}<extra></extra>", text=[f"{v:.2f}" for v in locale_df["avg_rating"]]))
                    fig_loc.update_layout(title=None, height=max(320, 26 * len(locale_df) + 100), margin=dict(l=70, r=40, t=18, b=48), barmode="overlay", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter", xaxis_title="Reviews", yaxis_title="")
                    fig_loc = _sw_style_fig(fig_loc)
                    fig_loc.update_layout(title=None, legend=dict(orientation="h", y=-0.18, x=0, xanchor="left", yanchor="top", font=dict(size=11)), margin=dict(l=74, r=58, t=18, b=84))
                    _show_plotly(fig_loc)
                st.markdown("<div style='height:.35rem'></div>", unsafe_allow_html=True)
                locs = _top_locations(chart_df, top_n=None)
                if not locs.empty:
                    st.markdown("**Top reviewer locations**")
                    locs_display = locs.copy()
                    locs_display["avg_rating"] = locs_display["avg_rating"].map(lambda v: f"{v:.2f}★" if pd.notna(v) else "—")
                    locs_display = locs_display.rename(columns={"user_location": "Location", "count": "Reviews", "avg_rating": "Avg ★"})
                    table_height = min(600, max(280, 35 * len(locs_display) + 40))
                    st.dataframe(locs_display[["Location", "Reviews", "Avg ★"]], use_container_width=True, hide_index=True, height=table_height)

        with dash_tabs[4]:
            with st.container(border=True):
                _render_chart_header("Review depth vs satisfaction", "Longer reviews often signal different satisfaction patterns and complexity.")
                len_df = _review_length_cohort(chart_df)
                if len_df.empty:
                    st.info("Insufficient data for review-length analysis.")
                else:
                    fig_len = go.Figure()
                    fig_len.add_trace(go.Bar(x=len_df["Length Quartile"], y=len_df["avg_rating"], text=[f"{v:.2f}★" for v in len_df["avg_rating"]], textposition="outside", marker_color=["#ef4444" if v < 3.5 else "#eab308" if v < 4.2 else "#22c55e" for v in len_df["avg_rating"]], hovertemplate="%{x}<br>Avg ★: %{y:.2f}<br>n=%{customdata}<extra></extra>", customdata=len_df["count"]))
                    fig_len.update_layout(title=None, yaxis_range=[1, 5.2], yaxis_title="Avg ★", xaxis_title="", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_family="Inter", margin=dict(l=20, r=18, t=18, b=24), height=330)
                    fig_len = _sw_style_fig(fig_len)
                    fig_len.update_layout(title=None, margin=dict(l=24, r=18, t=18, b=34))
                    _show_plotly(fig_len)

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB: REVIEW EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
def _set_review_explorer_page(page: int, page_count: int):
    target = max(1, min(int(page), max(1, int(page_count))))
    st.session_state["review_explorer_page"] = target
    st.session_state["re_page_input"] = target


def _render_review_explorer(*, summary, overall_df, filtered_df, prompt_artifacts, filter_description, active_items):
    n_filtered = len(filtered_df) if filtered_df is not None else 0
    n_total = len(overall_df) if overall_df is not None else 0
    filter_note = f" · {n_filtered:,} of {n_total:,}" if n_filtered < n_total else f" · {n_total:,} reviews"
    st.markdown(f"<div class='section-title'>Reviews<span style='font-size:13px;font-weight:400;color:var(--slate-400);'>{filter_note}</span></div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-sub'>Showing <strong>{len(filtered_df):,}</strong> reviews · Use sidebar filters to narrow the stream.</div>", unsafe_allow_html=True)
    bundle, bundle_error = _safe_get_master_bundle(
        summary,
        filtered_df,
        prompt_artifacts,
        active_items=active_items,
        filter_description=filter_description,
        export_scope_label="Filtered current view" if active_items else "Current view (all reviews)",
        total_loaded_reviews=len(overall_df),
    )
    tc = st.columns([1.45, 1.3, 0.9, 1.15, 0.85])
    if bundle is not None:
        tc[0].download_button("⬇️ Download current view", data=bundle["excel_bytes"], file_name=bundle["excel_name"], mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="explorer_dl")
    else:
        tc[0].button("⬇️ Download current view", use_container_width=True, disabled=True, key="explorer_dl_disabled")
    sort_mode = tc[1].selectbox("Sort", ["Newest", "Oldest", "Highest rating", "Lowest rating", "Longest", "Most tagged", "Least tagged"], key="review_explorer_sort")
    per_page = int(tc[2].selectbox("Per page", [10, 20, 30, 50], key="review_explorer_per_page"))
    tc[3].markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    show_ev = tc[3].toggle("Evidence highlights", value=True, key="re_show_highlights", help="Highlight Symptomizer evidence in yellow — hover to see the AI tag")
    if bundle_error is not None:
        st.warning(f"Current-view export is temporarily unavailable: {bundle_error}")
    ordered_df = _sort_reviews(filtered_df, sort_mode).reset_index(drop=True)
    if ordered_df.empty:
        st.info("No reviews match the current filters.")
        return
    page_count = max(1, math.ceil(len(ordered_df) / max(per_page, 1)))
    current_page = max(1, min(int(st.session_state.get("review_explorer_page", 1)), page_count))
    if "re_page_input" not in st.session_state:
        st.session_state["re_page_input"] = current_page
    start = (current_page - 1) * per_page
    page_df = ordered_df.iloc[start:start + per_page]
    ev_lookup = _build_evidence_lookup(st.session_state.get("sym_processed_rows") or [])
    for orig_idx, row in page_df.iterrows():
        ev_items = (ev_lookup.get(str(orig_idx)) or ev_lookup.get(str(row.get("review_id", "")))) if show_ev else None
        _render_review_card(row, evidence_items=ev_items)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        pc = st.columns([0.8, 0.8, 2.6, 0.9, 0.8, 0.8])
        pc[0].button("⏮", use_container_width=True, disabled=current_page <= 1, key="re_first", on_click=_set_review_explorer_page, args=(1, page_count))
        pc[1].button("‹", use_container_width=True, disabled=current_page <= 1, key="re_prev", on_click=_set_review_explorer_page, args=(current_page - 1, page_count))
        pc[2].markdown(f"<div class='compact-pager-status'>Page {current_page} of {page_count:,}<span class='compact-pager-sub'>{start + 1:,}–{min(start + per_page, len(ordered_df)):,} of {len(ordered_df):,} reviews</span></div>", unsafe_allow_html=True)
        go_page = int(pc[3].number_input("Go", min_value=1, max_value=page_count, value=int(st.session_state.get("re_page_input", current_page)), step=1, key="re_page_input", label_visibility="collapsed"))
        pc[4].button("›", use_container_width=True, disabled=current_page >= page_count, key="re_next", on_click=_set_review_explorer_page, args=(current_page + 1, page_count))
        pc[5].button("⏭", use_container_width=True, disabled=current_page >= page_count, key="re_last", on_click=_set_review_explorer_page, args=(page_count, page_count))
    if go_page != current_page:
        _set_review_explorer_page(go_page, page_count)
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB: AI ANALYST
# ═══════════════════════════════════════════════════════════════════════════════
def _render_ai_tab(*, settings, overall_df, filtered_df, summary, filter_description):
    st.markdown("<div class='section-title'>AI Analyst</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Use this for executive summaries, root-cause synthesis, and evidence-backed action plans grounded in the current filtered review set.</div>", unsafe_allow_html=True)
    if filtered_df.empty:
        st.info("Adjust filters — no reviews in scope.")
        return
    scope_sig = json.dumps(dict(pid=summary.product_id, fd=filter_description, n=len(filtered_df), st=(st.session_state.get("analysis_dataset") or {}).get("source_type", "bv")), sort_keys=True)
    if st.session_state.get("chat_scope_signature") != scope_sig:
        if st.session_state.get("chat_messages"):
            st.session_state["chat_messages"] = []
            st.session_state["chat_scope_notice"] = "Chat cleared — scope changed."
        st.session_state["chat_scope_signature"] = scope_sig
    notice = st.session_state.pop("chat_scope_notice", None)
    if notice:
        st.info(notice)
    st.session_state["workspace_active_tab"] = TAB_AI_ANALYST
    with st.container(border=True):
        sc = st.columns([1, 1, 1, 2])
        sc[0].metric("In scope", f"{len(filtered_df):,}")
        sc[1].metric("Organic", f"{int((~filtered_df['incentivized_review'].fillna(False)).sum()):,}")
        sc[2].metric("Model", _shared_model())
        sc[3].caption(f"Scope: {filter_description}")
        st.markdown("<div class='section-note'>Best first click for ELT: <strong>Executive summary</strong>. Then raise <strong>Reasoning effort</strong> if you want a higher-confidence long-form answer.</div>", unsafe_allow_html=True)
    api_key = settings.get("api_key")
    if not api_key:
        st.warning("Add OPENAI_API_KEY to Streamlit secrets.")
        st.code('OPENAI_API_KEY = "sk-..."', language="toml")
        return
    include_references = bool(st.session_state.get("ai_include_references", False))
    archive_msgs, live_msgs = _split_chat_messages(st.session_state.get("chat_messages") or [], keep_last=AI_VISIBLE_CHAT_MESSAGES)
    with st.container(border=True):
        st.markdown("**Current exchange**")
        if not live_msgs:
            st.info("Start with a quick report below, or type a question.")
        else:
            for msg in live_msgs:
                with st.chat_message(msg["role"]):
                    if msg["role"] == "assistant":
                        _render_ai_response(msg["content"], overall_df, include_references=include_references)
                    else:
                        st.markdown(msg["content"])
    if archive_msgs:
        msg_label = "message" if len(archive_msgs) == 1 else "messages"
        with st.expander(f"🗂️ Chat archive ({len(archive_msgs)} earlier {msg_label})", expanded=False):
            for msg in archive_msgs:
                with st.chat_message(msg["role"]):
                    if msg["role"] == "assistant":
                        _render_ai_response(msg["content"], overall_df, include_references=include_references)
                    else:
                        st.markdown(msg["content"])
    quick_actions = {
        "Executive summary": dict(prompt="Create an executive summary. Lead with biggest strengths, biggest risks, key consumer insight, and top 3 actions.", help="Leadership readout.", persona=None),
        "Product Development": dict(prompt=PERSONAS["Product Development"]["prompt"], help=PERSONAS["Product Development"]["blurb"], persona="Product Development"),
        "Quality Engineer": dict(prompt=PERSONAS["Quality Engineer"]["prompt"], help=PERSONAS["Quality Engineer"]["blurb"], persona="Quality Engineer"),
        "Consumer Insights": dict(prompt=PERSONAS["Consumer Insights"]["prompt"], help=PERSONAS["Consumer Insights"]["blurb"], persona="Consumer Insights"),
    }
    quick_trigger = None
    with st.container(border=True):
        st.markdown("**Quick reports & answer settings**")
        st.caption("Use the presets below to get a polished first draft quickly, then refine with your own follow-up question.")
        action_rows = [list(quick_actions.items())[:2], list(quick_actions.items())[2:]]
        for ridx, row in enumerate(action_rows):
            acols = st.columns(len(row))
            for cidx, (col, (label, config)) in enumerate(zip(acols, row)):
                if col.button(label, use_container_width=True, help=config["help"], key=f"ai_q_{ridx}_{cidx}_{_slugify(label)}"):
                    quick_trigger = (config["persona"], label, config["prompt"])
        size_cols = st.columns([1.18, 1.0, 1.05, 1.45])
        preset_options = ["Large (1200 words)", "Deep dive (1600 words)", "Custom"]
        cur_preset = st.session_state.get("ai_response_preset", "Large (1200 words)")
        if cur_preset not in preset_options:
            cur_preset = "Large (1200 words)"
            st.session_state["ai_response_preset"] = cur_preset
        size_cols[0].selectbox("Response size", preset_options, index=preset_options.index(cur_preset), key="ai_response_preset", help="Large is the default because shorter outputs were not detailed enough.")
        if st.session_state.get("ai_response_preset") == "Deep dive (1600 words)":
            st.session_state["ai_response_words"] = 1600
        elif st.session_state.get("ai_response_preset") == "Large (1200 words)":
            st.session_state["ai_response_words"] = 1200
        size_cols[1].number_input("Target words", min_value=250, max_value=2400, step=100, value=_current_ai_target_words(), key="ai_response_words", help="You can type your own target word count. Around 1200 words is the default large report.", disabled=st.session_state.get("ai_response_preset") != "Custom")
        size_cols[2].toggle("Include references · Beta", value=bool(st.session_state.get("ai_include_references", False)), key="ai_include_references", help="AI Analyst only. Beta feature. When on, the assistant includes hoverable Reference previews in the answer. Off by default for a cleaner reading view.")
        size_cols[3].caption(f"Target: {_current_ai_target_words():,} words · approx. {int(round(_current_ai_target_words() * 6.2)):,} characters. References are {'on' if st.session_state.get('ai_include_references') else 'off'}.")
        if st.session_state.get("ai_include_references"):
            st.caption("**Beta:** hoverable review references are enabled for AI Analyst answers.")
        reasoning_now = _shared_reasoning()
        tone = "warning" if reasoning_now in {"none", "minimal", "low"} else "info"
        getattr(st, tone)("Accuracy tip: raise **Reasoning effort** in the sidebar AI settings for higher-confidence synthesis, especially on long reports, root-cause questions, and mixed sentiment datasets.")
    helper_cols = st.columns([2, 1, 1])
    helper_cols[0].caption(f"Scope: {filter_description}")
    if helper_cols[1].button("Clear chat", use_container_width=True, key="ai_clear_chat"):
        st.session_state["chat_messages"] = []
        st.session_state["workspace_active_tab"] = TAB_AI_ANALYST
        st.rerun()
    user_message = st.chat_input("Ask about drivers, risks, opportunities, or voice-of-customer themes…", key="ai_chat_input")
    prompt_to_send = visible_user_message = persona_name = None
    if quick_trigger:
        persona_name, visible_user_message, prompt_to_send = quick_trigger
    elif user_message:
        prompt_to_send = visible_user_message = user_message
    if prompt_to_send and visible_user_message:
        prior = list(st.session_state["chat_messages"])
        st.session_state["chat_messages"].append({"role": "user", "content": visible_user_message})
        overlay = _show_thinking("Reviewing the filtered review text…")
        try:
            answer = _call_analyst(question=prompt_to_send, overall_df=overall_df, filtered_df=filtered_df, summary=summary, filter_description=filter_description, chat_history=prior, persona_name=persona_name, target_words=_current_ai_target_words(), include_references=bool(st.session_state.get('ai_include_references', False)))
            if persona_name:
                answer = f"**{persona_name} report**\n\n{answer}"
        except Exception as exc:
            answer = f"OpenAI request failed: {exc}"
        finally:
            overlay.empty()
        st.session_state["chat_messages"].append({"role": "assistant", "content": answer})
        st.session_state["workspace_active_tab"] = TAB_AI_ANALYST
        st.rerun()
# ═══════════════════════════════════════════════════════════════════════════════
#  TAB: REVIEW PROMPT
# ═══════════════════════════════════════════════════════════════════════════════
def _render_review_prompt_tab(*, settings, overall_df, filtered_df, summary, filter_description):
    st.markdown("<div class='section-title'>Review Prompt</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Create row-level AI tags that become new review columns. Start with the default starter pack, then tailor the prompts to the questions ELT, Product, Quality, or Insights need answered repeatedly.</div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown("**Prompt library**")
        st.markdown("<div class='helper-chip-row'><span class='helper-chip'>Loudness</span><span class='helper-chip'>Reliability</span><span class='helper-chip'>Usage count</span><span class='helper-chip'>Safety</span><span class='helper-chip'>Ownership period</span></div>", unsafe_allow_html=True)
        sc = st.columns([1.2, 1.2, 1])
        if sc[0].button("Add starter pack", use_container_width=True, key="prompt_add"):
            new_rows = pd.DataFrame(REVIEW_PROMPT_STARTER_ROWS)
            existing = set(st.session_state["prompt_definitions_df"]["column_name"].astype(str))
            to_add = new_rows[~new_rows["column_name"].isin(existing)]
            if not to_add.empty:
                st.session_state["prompt_definitions_df"] = pd.concat([st.session_state["prompt_definitions_df"], to_add], ignore_index=True)
            st.rerun()
        if sc[1].button("Reset to starter", use_container_width=True, key="prompt_reset"):
            st.session_state["prompt_definitions_df"] = pd.DataFrame(REVIEW_PROMPT_STARTER_ROWS)
            st.rerun()
        if sc[2].button("Clear all", use_container_width=True, key="prompt_clear"):
            st.session_state["prompt_definitions_df"] = pd.DataFrame(columns=["column_name", "prompt", "labels"])
            st.rerun()
    st.markdown("#### Prompt definitions")
    edited_df = st.data_editor(
        st.session_state["prompt_definitions_df"],
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="prompt_def_editor",
        height=320,
        column_config={
            "column_name": st.column_config.TextColumn("Column name", width="medium"),
            "prompt": st.column_config.TextColumn("Prompt", width="large"),
            "labels": st.column_config.TextColumn("Labels (comma-separated)", width="large"),
        },
    )
    st.session_state["prompt_definitions_df"] = edited_df
    _pd_sig = edited_df.to_json() if not edited_df.empty else ""
    _cached_defs = st.session_state.get("_prompt_defs_cache", {})
    if _cached_defs.get("sig") == _pd_sig and _cached_defs.get("cols") == list(overall_df.columns):
        prompt_defs = _cached_defs["defs"]
        prompt_defs_error = _cached_defs.get("error")
    else:
        prompt_defs_error = None
        try:
            prompt_defs = _normalize_prompt_defs(edited_df, overall_df.columns)
        except ReviewDownloaderError as exc:
            prompt_defs = []
            prompt_defs_error = str(exc)
        st.session_state["_prompt_defs_cache"] = dict(sig=_pd_sig, cols=list(overall_df.columns), defs=prompt_defs, error=prompt_defs_error)
    if prompt_defs_error:
        st.error(prompt_defs_error)
    api_key = settings.get("api_key")
    client = _get_client()
    with st.container(border=True):
        sc = st.columns([1.25, 1, 1, 2.45])
        tagging_scope = sc[0].selectbox("Scope", ["Current filtered reviews", "All loaded reviews"], index=0, key="prompt_tagging_scope")
        scope_df = filtered_df if tagging_scope == "Current filtered reviews" else overall_df
        batch_size = int(st.session_state.get("sym_batch_size", 5))
        est = math.ceil(len(scope_df) / max(1, batch_size)) if len(scope_df) else 0
        sc[1].metric("Reviews", f"{len(scope_df):,}")
        sc[2].metric("Requests", f"{est:,}")
        sc[3].caption(f"Scope: {tagging_scope.lower()} · {filter_description}")
        run_disabled = (not api_key) or (not prompt_defs) or len(scope_df) == 0
        if st.button("▶️ Run Review Prompt", type="primary", use_container_width=True, disabled=run_disabled, key="prompt_run_btn"):
            overlay = _show_thinking("Classifying each review…")
            try:
                prd = _run_review_prompt_tagging(client=client, source_df=scope_df.reset_index(drop=True), prompt_defs=prompt_defs, chunk_size=batch_size)
                updated = _merge_prompt_results(overall_df, prd, prompt_defs)
                dataset = dict(st.session_state["analysis_dataset"])
                dataset["reviews_df"] = updated
                st.session_state["analysis_dataset"] = dataset
                summary_df = _summarize_prompt_results(prd, prompt_defs, source_df=scope_df)
                defsig = json.dumps([dict(col=p["column_name"], prompt=p["prompt"], labels=p["labels"]) for p in prompt_defs], sort_keys=True)
                st.session_state["prompt_run_artifacts"] = dict(definitions=prompt_defs, summary_df=summary_df, scope_label=tagging_scope, scope_filter_description=filter_description, scope_review_ids=list(prd["review_id"].astype(str)), definition_signature=defsig, review_count=len(prd), generated_utc=pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
                st.session_state["master_export_bundle"] = None
                st.session_state["_prompt_bundle_ready"] = False
                st.session_state["prompt_run_notice"] = f"Finished tagging {len(prd):,} reviews."
            except Exception as exc:
                st.error(f"Review Prompt run failed: {exc}")
            finally:
                overlay.empty()
            st.rerun()
    notice = st.session_state.pop("prompt_run_notice", None)
    if notice:
        st.success(notice)
    pa = st.session_state.get("prompt_run_artifacts")
    if not pa:
        st.info("Run Review Prompt to generate AI columns.")
        return
    cur_sig = json.dumps([dict(col=p["column_name"], prompt=p["prompt"], labels=p["labels"]) for p in prompt_defs], sort_keys=True) if prompt_defs else ""
    if cur_sig != pa.get("definition_signature"):
        st.info("Prompt definitions changed — re-run to refresh.")
    updated_overall = st.session_state["analysis_dataset"]["reviews_df"]
    hc = st.columns([1.4, 1.4, 4])
    hc[2].caption(f"Run: {pa.get('generated_utc')} · Scope: {pa.get('scope_label')} · Reviews: {pa.get('review_count'):,}")
    if hc[0].button("🔄 Prepare download", use_container_width=True, key="prompt_prep_dl"):
        try:
            with st.spinner("Building export…"):
                _get_master_bundle(summary, updated_overall, pa)
            st.session_state["_prompt_bundle_ready"] = True
        except Exception as exc:
            st.session_state["_prompt_bundle_ready"] = False
            st.error(f"Could not build export: {exc}")
        st.rerun()
    bundle = st.session_state.get("master_export_bundle")
    dl_ready = bundle is not None
    hc[1].download_button("⬇️ Download tagged file", data=bundle["excel_bytes"] if dl_ready else b"", file_name=bundle["excel_name"] if dl_ready else "tagged.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", disabled=not dl_ready, key="prompt_dl_btn")
    if not dl_ready:
        st.caption("Click **Prepare download** first to build the export file.")
    plookup = {p["display_name"]: p for p in pa["definitions"]}
    pnames = list(plookup.keys())
    if not pnames:
        st.info("No prompt results yet.")
        return
    if st.session_state.get("prompt_result_view") not in pnames:
        st.session_state["prompt_result_view"] = pnames[0]
    sel = st.radio("Prompt result view", options=pnames, horizontal=True, key="prompt_result_view", label_visibility="collapsed")
    prompt = plookup[sel]
    pc_col = prompt["column_name"]
    rids = set(str(x) for x in pa.get("scope_review_ids", []))
    result_scope = updated_overall.loc[updated_overall["review_id"].astype(str).isin(rids)] if rids else updated_overall.iloc[0:0]
    lopts = [str(l) for l in pa["summary_df"][pa["summary_df"]["column_name"] == pc_col]["label"].tolist()]
    sel_labels = st.multiselect("Labels", options=lopts, default=lopts, key=f"plabels_{pc_col}")
    if pc_col in result_scope.columns and not result_scope.empty:
        _slab = result_scope[pc_col]
        _view = result_scope[_slab.isin(sel_labels)] if sel_labels else result_scope.iloc[0:0]
        _vc = _slab.value_counts() if not result_scope.empty else pd.Series(dtype=int)
        _ar = result_scope.groupby(pc_col)["rating"].mean() if "rating" in result_scope.columns and not result_scope.empty else pd.Series(dtype=float)
    else:
        _view = result_scope.iloc[0:0]
        _vc = pd.Series(dtype=int)
        _ar = pd.Series(dtype=float)
    total = max(len(result_scope), 1)
    ps_rows = [dict(label=l, review_count=int(_vc.get(l, 0)), share=_safe_pct(int(_vc.get(l, 0)), total), avg_rating=float(_ar[l]) if l in _ar.index and pd.notna(_ar[l]) else None) for l in prompt["labels"]]
    ps = pd.DataFrame(ps_rows)
    cc, tc_col = st.columns([1.45, 1.05])
    with cc:
        with st.container(border=True):
            if ps.empty or ps["review_count"].sum() == 0:
                st.info("No tagged reviews match current filters.")
            else:
                fig = px.pie(ps, names="label", values="review_count", hole=0.44, color_discrete_sequence=["#6366f1", "#10b981", "#f59e0b", "#ef4444", "#3b82f6", "#8b5cf6"])
                fig.update_layout(margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor="rgba(0,0,0,0)", font_family="Inter")
                _show_plotly(fig)
    with tc_col:
        with st.container(border=True):
            st.markdown(f"**Column** `{pc_col}`")
            st.write(prompt["prompt"])
            if not ps.empty:
                ds = ps.copy()
                ds["avg_rating"] = ds["avg_rating"].map(lambda x: f"{x:.2f}★" if pd.notna(x) and x is not None else "—")
                ds["share"] = ds["share"].map(_fmt_pct)
                st.dataframe(ds[["label", "review_count", "avg_rating", "share"]], use_container_width=True, hide_index=True, height=240)
    prevcols = [c for c in ["review_id", "rating", "incentivized_review", "submission_time", "content_locale", "title", "review_text", pc_col] if c in _view.columns]
    st.markdown("**Tagged review preview**")
    st.dataframe(_view[prevcols].head(50), use_container_width=True, hide_index=True, height=300)

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB: SYMPTOMIZER
# ═══════════════════════════════════════════════════════════════════════════════
def _render_symptomizer_tab(*, settings, overall_df, filtered_df, summary, filter_description):
    _apply_pending_symptomizer_ui_state()
    st.markdown("<div class='section-title'>Symptomizer</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Row-level AI tagging of delighters and detractors. Use this when you want a more structured theme layer for the Dashboard, Review Explorer, and downstream exports.</div>", unsafe_allow_html=True)
    client = _get_client()
    api_key = settings.get("api_key")
    sym_source = st.session_state.get("sym_symptoms_source", "none")
    if sym_source == "none":
        _try_load_symptoms_from_file()
    delighters, detractors, _ = _sync_symptom_catalog_session(default_source="built-in")
    include_universal_prev = bool(st.session_state.get("sym_include_universal_neutral", True))
    sym_source = st.session_state.get("sym_symptoms_source", "none")
    st.markdown("### 1 · Symptoms catalog")
    with st.expander("🧩 Universal Neutral Symptoms", expanded=False):
        include_universal = st.checkbox(
            "Include Universal Neutral Symptoms in the active taxonomy",
            value=include_universal_prev,
            key="sym_include_universal_neutral",
            help="Checked by default. These cross-product tags stay available for AI tagging, inline editing, and exports.",
        )
        st.caption("Turn this off to remove the universal neutral pack from the catalog, current AI tags, inline suggestions, and the export file. You can also extend the pack with your own custom cross-product symptoms below.")
        neutral_dels, neutral_dets = _universal_neutral_catalog()
        custom_neutral_dels, custom_neutral_dets = _custom_universal_catalog()
        u1, u2 = st.columns(2)
        with u1:
            st.markdown("**🟢 Delighters**")
            st.markdown(_chip_html([(lab, "green") for lab in neutral_dels]), unsafe_allow_html=True)
        with u2:
            st.markdown("**🔴 Detractors**")
            st.markdown(_chip_html([(lab, "red") for lab in neutral_dets]), unsafe_allow_html=True)
        st.markdown(_chip_html([
            (f"Built-in: {len(_UNIVERSAL_NEUTRAL_DELIGHTERS) + len(_UNIVERSAL_NEUTRAL_DETRACTORS)}", "gray"),
            (f"Custom: {len(custom_neutral_dels) + len(custom_neutral_dets)}", "indigo"),
        ]), unsafe_allow_html=True)
        with st.form("sym_custom_universal_form"):
            cu1, cu2 = st.columns(2)
            with cu1:
                st.markdown("**Custom universal delighters**")
                custom_del_text = st.text_area(
                    "Add custom universal delighters",
                    value="\n".join(custom_neutral_dels),
                    height=120,
                    key="sym_custom_universal_del_text",
                    placeholder="One per line or comma-separated, e.g. Comfortable, Attractive Design",
                    label_visibility="collapsed",
                )
            with cu2:
                st.markdown("**Custom universal detractors**")
                custom_det_text = st.text_area(
                    "Add custom universal detractors",
                    value="\n".join(custom_neutral_dets),
                    height=120,
                    key="sym_custom_universal_det_text",
                    placeholder="One per line or comma-separated, e.g. Uncomfortable, Poor Design",
                    label_visibility="collapsed",
                )
            cf1, cf2 = st.columns(2)
            save_custom_universal = cf1.form_submit_button("Save custom universal symptoms", use_container_width=True)
            clear_custom_universal = cf2.form_submit_button("Clear custom universal symptoms", use_container_width=True)
        if save_custom_universal:
            saved_custom_dels, saved_custom_dets = _save_custom_universal_catalog(
                delighters=_parse_manual_tag_entries(custom_del_text),
                detractors=_parse_manual_tag_entries(custom_det_text),
                merge=False,
            )
            st.session_state["sym_run_notice"] = f"Saved {len(saved_custom_dels)} custom universal delighters and {len(saved_custom_dets)} custom universal detractors."
            st.rerun()
        if clear_custom_universal:
            _save_custom_universal_catalog(delighters=[], detractors=[], merge=False)
            st.session_state["sym_run_notice"] = "Cleared custom universal symptoms for this workspace."
            st.rerun()
    include_universal = bool(st.session_state.get("sym_include_universal_neutral", True))
    if include_universal != include_universal_prev:
        _apply_universal_neutral_toggle(include_universal)
        st.rerun()
    st.markdown(_chip_html([
        (f"✓ {len(delighters)} delighters", "green"),
        (f"✓ {len(detractors)} detractors", "red"),
        ("Universal neutral: On" if include_universal else "Universal neutral: Off", "blue" if include_universal else "gray"),
        (f"Source: {sym_source}", "indigo"),
    ]), unsafe_allow_html=True)
    universal_del_set, universal_det_set = map(set, _universal_neutral_catalog())
    product_specific_dels = [label for label in delighters if label not in universal_del_set]
    product_specific_dets = [label for label in detractors if label not in universal_det_set]
    if not product_specific_dels and not product_specific_dets:
        if include_universal:
            st.info("Using only the Universal Neutral Symptoms pack right now. Add product-specific symptoms below for better recall and cleaner reporting.")
        else:
            st.warning("No active symptoms are configured. Add product-specific symptoms below or re-enable Universal Neutral Symptoms.")
    # ── Phase 2: One-click auto-build ────────────────────────────────────
    has_knowledge = bool(st.session_state.get("sym_product_knowledge"))
    has_profile = bool(st.session_state.get("sym_product_profile", "").strip())
    has_taxonomy = bool(st.session_state.get("sym_delighters") or st.session_state.get("sym_detractors"))
    if has_profile and api_key and not has_taxonomy:
        with st.container(border=True):
            st.markdown("**🚀 Auto-build taxonomy**")
            st.caption("Product profile detected. One click to generate a symptom taxonomy from product knowledge, calibrate it, and activate it.")
            ab_cols = st.columns([2, 1])
            if ab_cols[0].button("🚀 Auto-build & calibrate", type="primary", use_container_width=True, key="sym_auto_build_btn"):
                client_ab = _get_client()
                if client_ab:
                    with st.spinner("Building taxonomy from product knowledge…"):
                        try:
                            profile_ab = st.session_state.get("sym_product_profile", "")
                            knowledge_ab = st.session_state.get("sym_product_knowledge") or {}
                            sample_ab = _sample_reviews_for_symptomizer(overall_df, 40)
                            result = _ai_build_symptom_list(
                                client=client_ab,
                                product_description=profile_ab,
                                sample_reviews=sample_ab,
                                product_knowledge=knowledge_ab,
                            )
                            if result:
                                ai_dels = _normalize_tag_list([item.get("label") for item in result.get("delighters", [])])
                                ai_dets = _normalize_tag_list([item.get("label") for item in result.get("detractors", [])])
                                ai_aliases = {}
                                for item in result.get("delighters", []) + result.get("detractors", []):
                                    if item.get("aliases"):
                                        ai_aliases[item["label"]] = item["aliases"]
                                # Activate taxonomy
                                include_neutral = bool(st.session_state.get("sym_include_universal_neutral", True))
                                final_dets, final_dels = _ensure_universal_taxonomy(ai_dets, ai_dels, include_universal_neutral=include_neutral)
                                st.session_state["sym_detractors"] = final_dets
                                st.session_state["sym_delighters"] = final_dels
                                st.session_state["sym_aliases"] = _alias_map_for_catalog(final_dels, final_dets, extra_aliases=ai_aliases)
                                st.session_state["sym_symptoms_source"] = "ai-auto-build"
                                st.session_state["sym_taxonomy_category"] = result.get("category", "general")
                                st.session_state["sym_taxonomy_preview_items"] = _taxonomy_preview_items_with_side(result)
                                st.session_state["sym_ai_build_result"] = result
                                # Run calibration
                                if _HAS_SYMPTOMIZER_V3:
                                    calib = _v3_calibration_preflight(
                                        client=client_ab,
                                        sample_reviews=_sample_reviews_for_symptomizer(overall_df, 8),
                                        allowed_detractors=final_dets,
                                        allowed_delighters=final_dels,
                                        product_profile=profile_ab,
                                        chat_complete_fn=_chat_complete_with_fallback_models,
                                        safe_json_load_fn=_safe_json_load,
                                        model_fn=_shared_model,
                                        reasoning_fn=_shared_reasoning,
                                    )
                                    st.session_state["sym_calibration_result"] = calib
                                    rec = calib.get("recommendation", "")
                                    if rec == "ready":
                                        st.success(f"✅ Taxonomy built & calibrated — {len(final_dets)} detractors, {len(final_dels)} delighters, {calib.get('hit_rate', 0):.0%} hit rate. Ready to run!")
                                    elif rec == "needs_tuning":
                                        st.warning(f"⚠️ Taxonomy built but needs tuning — {calib.get('hit_rate', 0):.0%} hit rate. Review the catalog below before running.")
                                    else:
                                        st.error(f"🔴 Low calibration — {calib.get('hit_rate', 0):.0%} hit rate. Consider editing the taxonomy.")
                                else:
                                    st.success(f"✅ Taxonomy built — {len(final_dets)} detractors, {len(final_dels)} delighters. Ready to run!")
                                st.rerun()
                        except Exception as exc:
                            st.error(f"Auto-build failed: {exc}")
            if has_knowledge:
                ab_cols[1].markdown(f"<span style='color:var(--slate-400);font-size:12px;'>Knowledge: {sum(len(v) for v in (st.session_state.get('sym_product_knowledge') or {}).values() if isinstance(v, list))} items</span>", unsafe_allow_html=True)
    elif has_profile and has_taxonomy:
        st.markdown(f"<div class='helper-chip-row'><span class='helper-chip' style='background:rgba(5,150,105,.10);color:#059669;border-color:rgba(5,150,105,.25);'>✅ Taxonomy active — {len(st.session_state.get('sym_detractors', []))} det · {len(st.session_state.get('sym_delighters', []))} del</span></div>", unsafe_allow_html=True)
    sym_tabs = st.tabs(["🤖  AI builder", "✏️  Manual entry", "📄  Upload workbook"])
    with sym_tabs[0]:
        if not api_key:
            st.warning("OpenAI API key required.")
        else:
            pdesc = st.text_area("Product description", value=st.session_state.get("sym_product_profile", ""), placeholder="e.g. SharkNinja Ninja Air Fryer XL — 6-in-1 countertop air fryer with 6 qt basket", height=100, key="sym_pdesc")
            if not overall_df.empty and "review_text" in overall_df.columns:
                max_samples = min(250, max(5, len(overall_df)))
                sample_n = st.slider("Sample reviews", min_value=5, max_value=max_samples, value=min(50, max_samples), step=5, key="sym_sample_n")
                st.caption(f"Using {sample_n} of {len(overall_df):,} reviews.")
            else:
                sample_n = 50
            ai_note = st.session_state.get("sym_product_profile_ai_note", "")
            if ai_note:
                st.caption(ai_note)

            product_knowledge = _normalize_product_knowledge(st.session_state.get("sym_product_knowledge") or {})
            sample_reviews = _sample_reviews_for_symptomizer(overall_df, sample_n)

            desc_col, sym_col = st.columns([1.2, 1.4])
            if desc_col.button("✨ AI draft product description", use_container_width=True, key="sym_ai_desc"):
                overlay = _show_thinking("Drafting product description from reviews…")
                try:
                    result = _ai_generate_product_description(client=client, sample_reviews=sample_reviews, existing_description=pdesc)
                    if result.get("description"):
                        st.session_state["sym_product_profile"] = result["description"]
                        st.session_state["sym_product_knowledge"] = _normalize_product_knowledge(result.get("product_knowledge") or {})
                        st.session_state["_sym_pdesc_pending"] = result["description"]
                        st.session_state["sym_product_profile_ai_note"] = result.get("confidence_note", "Generated from the current review sample.")
                    else:
                        st.session_state["sym_product_profile_ai_note"] = "No product description could be drafted from the current sample."
                except Exception as exc:
                    st.error(f"AI product description failed: {exc}")
                finally:
                    overlay.empty()
                st.rerun()

            if sym_col.button("🤖 Generate symptom list", type="primary", use_container_width=True, key="sym_ai_build"):
                overlay = _show_thinking("Generating symptom catalog…")
                try:
                    result = _ai_build_symptom_list(client=client, product_description=pdesc, sample_reviews=sample_reviews, product_knowledge=product_knowledge)
                    st.session_state["sym_ai_build_result"] = result
                    st.session_state["sym_product_profile"] = pdesc
                    st.session_state["sym_product_knowledge"] = _normalize_product_knowledge(result.get("product_knowledge") or product_knowledge)
                except Exception as exc:
                    st.error(f"AI builder failed: {exc}")
                finally:
                    overlay.empty()
                st.rerun()
            _render_product_knowledge_panel(product_knowledge)
            ai_result = st.session_state.get("sym_ai_build_result")
            if ai_result:
                preview_dels = list(ai_result.get("preview_delighters") or [])
                preview_dets = list(ai_result.get("preview_detractors") or [])
                del_bucket_counts = Counter(str(item.get("bucket") or "Product Specific") for item in preview_dels)
                det_bucket_counts = Counter(str(item.get("bucket") or "Product Specific") for item in preview_dets)
                st.markdown("**Review and accept:**")
                st.markdown(_chip_html([
                    (f"Category: {str(ai_result.get('category') or 'general').replace('_', ' ').title()}", "blue"),
                    (f"Delighters: {len(ai_result.get('delighters', []))}", "green"),
                    (f"Detractors: {len(ai_result.get('detractors', []))}", "red"),
                    (f"Category drivers: {del_bucket_counts.get('Category Driver', 0) + det_bucket_counts.get('Category Driver', 0)}", "indigo"),
                    (f"Product specific: {del_bucket_counts.get('Product Specific', 0) + det_bucket_counts.get('Product Specific', 0)}", "gray"),
                ]), unsafe_allow_html=True)
                if ai_result.get("taxonomy_note"):
                    st.caption(str(ai_result.get("taxonomy_note")))
                preview_tabs = st.tabs(["🟢 Delighters preview", "🔴 Detractors preview"])
                with preview_tabs[0]:
                    _render_ai_taxonomy_preview_table(preview_dels, key_prefix="sym_ai_preview_del", side_label="Delighters")
                with preview_tabs[1]:
                    _render_ai_taxonomy_preview_table(preview_dets, key_prefix="sym_ai_preview_det", side_label="Detractors")
                with st.expander("Optional manual edit before accepting", expanded=False):
                    r1, r2 = st.columns(2)
                    with r1:
                        st.markdown("🟢 Delighters")
                        st.text_area("Edit", value="\n".join(ai_result.get("delighters", [])), height=220, key="sym_ai_del_edit")
                    with r2:
                        st.markdown("🔴 Detractors")
                        st.text_area("Edit", value="\n".join(ai_result.get("detractors", [])), height=220, key="sym_ai_det_edit")
                if st.button("✅ Accept generated taxonomy", type="primary", use_container_width=True, key="sym_accept_ai"):
                    accepted_dels, accepted_dets = _canonical_symptom_catalog(
                        _parse_manual_tag_entries(st.session_state.get("sym_ai_del_edit", "")),
                        _parse_manual_tag_entries(st.session_state.get("sym_ai_det_edit", "")),
                    )
                    st.session_state.update(
                        sym_delighters=accepted_dels,
                        sym_detractors=accepted_dets,
                        sym_aliases=_alias_map_for_catalog(
                            accepted_dels,
                            accepted_dets,
                            extra_aliases=ai_result.get("aliases") or {},
                            existing_aliases=st.session_state.get("sym_aliases", {}),
                        ),
                        sym_symptoms_source="ai",
                        sym_taxonomy_preview_items=_taxonomy_preview_items_with_side(ai_result),
                        sym_taxonomy_category=str(ai_result.get("category") or "general"),
                        sym_product_profile_ai_note=(ai_result.get("taxonomy_note") or st.session_state.get("sym_product_profile_ai_note", "")),
                    )
                    st.session_state.pop("sym_ai_build_result", None)
                    st.success("Accepted.")
                    st.rerun()
    with sym_tabs[1]:
        if include_universal:
            st.caption("Universal Neutral Symptoms are managed by the toggle above, so this editor is just for product-specific symptoms.")
        with st.form("sym_manual_catalog_form"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("🟢 **Delighters**")
                del_text = st.text_area("One per line or comma-separated", value="\n".join(product_specific_dels), height=200, key="sym_del_manual")
            with c2:
                st.markdown("🔴 **Detractors**")
                det_text = st.text_area("One per line or comma-separated", value="\n".join(product_specific_dets), height=200, key="sym_det_manual")
            save_manual = st.form_submit_button("💾 Save symptoms", use_container_width=True)
        if save_manual:
            saved_dels, saved_dets = _canonical_symptom_catalog(_parse_manual_tag_entries(del_text), _parse_manual_tag_entries(det_text))
            st.session_state.update(sym_delighters=saved_dels, sym_detractors=saved_dets, sym_aliases=_alias_map_for_catalog(saved_dels, saved_dets, existing_aliases=st.session_state.get("sym_aliases", {})), sym_symptoms_source="manual", sym_taxonomy_preview_items=[])
            st.success("Saved.")
            st.rerun()
    with sym_tabs[2]:
        st.markdown("Upload an Excel workbook with a **Symptoms** sheet: columns Symptom, Type (Delighter/Detractor), optional Aliases. If there is no Symptoms sheet but the review tab already contains populated **Symptom 1–20** or **AI Symptom** columns, the local catalog will be inferred automatically.")
        sym_upload = st.file_uploader("Upload workbook", type=["xlsx", "xlsm"], key="sym_file_uploader")
        if sym_upload:
            raw = sym_upload.getvalue()
            st.session_state["_uploaded_raw_bytes"] = raw
            d, t, a = _get_symptom_whitelists(raw)
            if d or t:
                loaded_dels, loaded_dets = _canonical_symptom_catalog(d, t)
                st.session_state.update(sym_delighters=loaded_dels, sym_detractors=loaded_dets, sym_aliases=_alias_map_for_catalog(loaded_dels, loaded_dets, extra_aliases=a, existing_aliases=st.session_state.get("sym_aliases", {})), sym_symptoms_source="file", sym_taxonomy_preview_items=[], sym_taxonomy_category="general")
                st.success(f"Loaded {len(loaded_dels)} delighters and {len(loaded_dets)} detractors.")
                st.rerun()
            else:
                st.error("Could not find a usable Symptoms sheet or any populated Symptom / AI Symptom columns.")
    current_catalog_rows = _build_structured_taxonomy_rows(
        delighters,
        detractors,
        aliases=st.session_state.get("sym_aliases", {}),
        category=str(st.session_state.get("sym_taxonomy_category") or (st.session_state.get("sym_ai_build_result") or {}).get("category") or "general"),
        preview_items=list(st.session_state.get("sym_taxonomy_preview_items") or _taxonomy_preview_items_with_side(st.session_state.get("sym_ai_build_result") or {})),
    )
    with st.expander(f"🗂 Current L1/L2 taxonomy ({len(current_catalog_rows)})", expanded=False):
        st.caption("This is the active symptom list the Symptomizer will use right now, organized as L1 themes and L2 symptoms.")
        _render_structured_taxonomy_table(current_catalog_rows, key_prefix="sym_current_catalog")
    st.divider()
    st.markdown("### 2 · Configure and run")
    delighters = list(st.session_state.get("sym_delighters") or [])
    detractors = list(st.session_state.get("sym_detractors") or [])
    colmap = _detect_sym_cols(overall_df)
    work = _detect_missing(overall_df, colmap)
    need_both = int(work["Needs_Symptomization"].sum())
    need_del = int(work["Needs_Delighters"].sum())
    need_det = int(work["Needs_Detractors"].sum())
    st.markdown(f"""<div class="hero-grid" style="grid-template-columns:repeat(4,minmax(0,1fr));margin-top:0;margin-bottom:.8rem;">
      <div class="hero-stat"><div class="label">Total reviews</div><div class="value">{len(overall_df):,}</div></div>
      <div class="hero-stat"><div class="label">Need delighters</div><div class="value">{need_del:,}</div></div>
      <div class="hero-stat"><div class="label">Need detractors</div><div class="value">{need_det:,}</div></div>
      <div class="hero-stat accent"><div class="label">Missing both</div><div class="value">{need_both:,}</div></div>
    </div>""", unsafe_allow_html=True)
    scope_choice = st.selectbox("Scope", ["Missing both", "Any missing", "Current filtered reviews", "All loaded reviews"], key="sym_scope_choice")
    if scope_choice == "Missing both":
        target_df = work[(work["Needs_Delighters"]) & (work["Needs_Detractors"])]
    elif scope_choice == "Any missing":
        target_df = work[(work["Needs_Delighters"]) | (work["Needs_Detractors"])]
    elif scope_choice == "Current filtered reviews":
        fids = set(filtered_df["review_id"].astype(str))
        target_df = work[work["review_id"].astype(str).isin(fids)]
    else:
        target_df = work
    rc = st.columns([1.5, 1, 1, 1, 1])
    n_to_process = rc[0].number_input("Reviews to process", min_value=1, max_value=max(1, len(target_df)), step=1, key="sym_n_to_process")
    batch_size = int(rc[1].number_input("Batch size", min_value=1, max_value=20, value=int(st.session_state.get("sym_batch_size", 5)), step=1, key="sym_batch_size_run"))
    est_batches = max(1, math.ceil(int(n_to_process) / batch_size)) if n_to_process else 0
    rc[2].metric("In scope", f"{len(target_df):,}")
    rc[3].metric("Est. batches", f"{est_batches:,}")
    rc[4].caption(f"Scope: {scope_choice}\nModel: {_shared_model()}")
    active_taxonomy_count = len(delighters) + len(detractors)
    run_disabled = (not api_key) or (len(target_df) == 0) or (active_taxonomy_count == 0)
    # ── Taxonomy overlap check ────────────────────────────────────────────
    if active_taxonomy_count >= 6 and (detractors and delighters):
        overlap_warnings = []
        det_stems = {label: _tokenize(label) for label in detractors}
        del_stems = {label: _tokenize(label) for label in delighters}
        for d_label, d_tokens in det_stems.items():
            for l_label, l_tokens in del_stems.items():
                if d_tokens and l_tokens:
                    shared = set(d_tokens) & set(l_tokens)
                    if len(shared) >= 2 and len(shared) / max(len(d_tokens), len(l_tokens)) > 0.5:
                        overlap_warnings.append(f"'{d_label}' ↔ '{l_label}' ({', '.join(shared)})")
        if overlap_warnings:
            with st.expander(f"⚠️ {len(overlap_warnings)} potential taxonomy overlaps", expanded=False):
                st.caption("These detractor/delighter pairs share significant keyword overlap and may cause the AI to confuse them:")
                for w in overlap_warnings[:8]:
                    st.markdown(f"- {w}")
    if run_disabled and not api_key:
        st.warning("Add OPENAI_API_KEY to Streamlit secrets.")
    elif len(target_df) == 0:
        st.info("All reviews in this scope already have tags. Try 'All loaded reviews' to re-tag, or load more reviews.")
    elif active_taxonomy_count == 0:
        st.info("No taxonomy active yet. Use the **🚀 Auto-build** button above, or the **🤖 AI builder** tab to generate one from your reviews.")
    # Pre-resolve variables needed by both calibration and run
    profile = st.session_state.get("sym_product_profile", "")
    product_knowledge = _normalize_product_knowledge(st.session_state.get("sym_product_knowledge") or {})
    client = _get_client()
    # ── Pre-run taxonomy health check ─────────────────────────────────────
    if _HAS_SYMPTOMIZER_V3 and active_taxonomy_count >= 4:
        try:
            health = _v3_score_taxonomy_health(detractors, delighters)
            score = health.get("health_score", 100)
            issues = health.get("issues", [])
            high_issues = [i for i in issues if i["severity"] == "high"]
            if score < 60:
                st.warning(f"⚠️ Taxonomy health: **{score}/100** — {len(high_issues)} high-priority issues. Review before running.")
            elif score < 85 and high_issues:
                st.info(f"Taxonomy health: **{score}/100** — {len(high_issues)} issue(s) to consider.")
            if high_issues:
                for iss in high_issues[:3]:
                    st.caption(f"  → {iss['label']}: {iss['fix']}")
        except Exception:
            pass
    run_btn = st.button(f"▶️ Symptomize {min(int(n_to_process), len(target_df)):,} review(s)", type="primary", use_container_width=True, disabled=run_disabled, key="sym_run_btn")
    if _HAS_SYMPTOMIZER_V3 and api_key and active_taxonomy_count > 0 and len(target_df) > 0:
        cc1, cc2 = st.columns([1, 3])
        if cc1.button("🧪 Validate taxonomy", use_container_width=True, key="sym_calibrate_btn", help="8-review calibration to check taxonomy fit."):
            with st.spinner("Calibrating…"):
                calib = _v3_calibration_preflight(client=client, sample_reviews=_sample_reviews_for_symptomizer(filtered_df, 8), allowed_detractors=list(detractors or []), allowed_delighters=list(delighters or []), product_profile=profile, chat_complete_fn=_chat_complete_with_fallback_models, safe_json_load_fn=_safe_json_load, model_fn=_shared_model, reasoning_fn=_shared_reasoning)
                st.session_state["sym_calibration_result"] = calib
        cr = st.session_state.get("sym_calibration_result")
        if cr:
            rec, hit, avg, unu = cr.get("recommendation",""), cr.get("hit_rate",0), cr.get("avg_tags_per_review",0), len(cr.get("unused_detractors",[]))+len(cr.get("unused_delighters",[]))
            if rec == "ready": cc2.success(f"✅ Taxonomy looks good — {hit:.0%} hit rate, {avg:.1f} avg tags. {unu} unused labels.")
            elif rec == "needs_tuning": cc2.warning(f"⚠️ Needs tuning — {hit:.0%} hit rate. Consider removing {unu} unused labels.")
            else: cc2.error(f"🔴 Low coverage — {hit:.0%} hit rate. Review taxonomy before full batch.")
            with st.expander("📊 Calibration details", expanded=False): st.json(cr)
    notice = st.session_state.pop("sym_run_notice", None)
    if notice:
        st.success(notice)
    if run_btn:
      try:
        prioritized = _prioritize_for_symptomization(target_df).head(int(n_to_process))
        rows_to_process = prioritized.copy()
        prog = st.progress(0.0, text="Starting…")
        status = st.empty()
        eta_box = st.empty()
        stats_box = st.empty()
        stop_container = st.empty()
        _user_stopped = False
        processed_local = []
        t0 = time.perf_counter()
        total_n = max(1, len(rows_to_process))
        done = 0
        failed_count = 0
        total_labels_written = 0
        updated_df = _ensure_ai_cols(overall_df.copy())
        profile = st.session_state.get("sym_product_profile", "")
        product_knowledge = _normalize_product_knowledge(st.session_state.get("sym_product_knowledge") or {})
        aliases = st.session_state.get("sym_aliases", {})
        rows_list = list(rows_to_process.iterrows())
        bidxs = list(range(0, len(rows_list), batch_size))
        empty_out = dict(dels=[], dets=[], ev_del={}, ev_det={}, unl_dels=[], unl_dets=[], safety="Not Mentioned", reliability="Not Mentioned", sessions="Unknown")
        _active_dets = list(detractors or [])
        _active_dels = list(delighters or [])
        _include_universal = bool(st.session_state.get("sym_include_universal_neutral", True))
        _ev_chars = int(st.session_state.get("sym_max_ev_chars", 120))
        # Phase 3: Initialize adaptive label tracker
        _label_tracker = _v3_LabelTracker(_active_dets, _active_dels) if _HAS_SYMPTOMIZER_V3 else None
        # Speed optimization: use larger batches for short reviews
        if _HAS_SYMPTOMIZER_V3:
            avg_words = rows_to_process.get("review_length_words", pd.Series(50)).fillna(50).mean()
            if avg_words < 80 and batch_size < 8:
                batch_size = min(10, batch_size + 3)  # Short reviews can pack tighter
                bidxs = list(range(0, len(rows_list), batch_size))
                status.info(f"Short reviews detected (avg {avg_words:.0f} words) — auto-increased batch to {batch_size}")
        for bi, start in enumerate(bidxs, 1):
            # Stop button — user can cancel without losing progress
            if stop_container.button("⏹ Stop after this batch", key=f"sym_stop_{bi}", use_container_width=True):
                _user_stopped = True
            if _user_stopped:
                status.warning(f"Stopped by user after {done} reviews. Partial results saved.")
                break
            batch = rows_list[start:start + batch_size]
            items = [
                dict(
                    idx=int(idx),
                    review=_symptomizer_review_text(row),
                    rating=row.get("rating"),
                    needs_del=bool(row.get("Needs_Delighters", True)),
                    needs_det=bool(row.get("Needs_Detractors", True)),
                )
                for idx, row in batch
            ]
            _log.info("Batch %d/%d — reviews %d–%d", bi, len(bidxs), start + 1, min(start + batch_size, len(rows_list)))
            status.info(f"Batch {bi}/{len(bidxs)} — reviews {start + 1}–{min(start + batch_size, len(rows_list))}")
            outs = {}
            if client:
                try:
                    outs = _call_symptomizer_batch(client=client, items=items, allowed_delighters=_active_dels, allowed_detractors=_active_dets, product_profile=profile, product_knowledge=product_knowledge, max_ev_chars=_ev_chars, aliases=aliases, include_universal_neutral=_include_universal)
                except Exception as exc:
                    _log.warning("Batch %d failed: %s — retrying individually", bi, exc)
                    status.warning(f"Batch {bi} failed ({type(exc).__name__}) — retrying individually…")
                    failed_count += len(items)
                    for it in items:
                        try:
                            single = _call_symptomizer_batch(client=client, items=[it], allowed_delighters=_active_dels, allowed_detractors=_active_dets, product_profile=profile, product_knowledge=product_knowledge, max_ev_chars=_ev_chars, aliases=aliases, include_universal_neutral=_include_universal)
                            outs.update(single)
                            failed_count -= 1
                        except Exception:
                            pass
            for it in items:
                idx = int(it["idx"])
                out = outs.get(idx, empty_out)
                write_det = bool(it.get("needs_det", True))
                write_del = bool(it.get("needs_del", True))
                dets_out = list(out.get("dets", []))[:10] if write_det else None
                dels_out = list(out.get("dels", []))[:10] if write_del else None
                updated_df = _write_ai_symptom_row(
                    updated_df,
                    idx,
                    dets=dets_out,
                    dels=dels_out,
                    safety=out.get("safety", "Not Mentioned"),
                    reliability=out.get("reliability", "Not Mentioned"),
                    sessions=out.get("sessions", "Unknown"),
                )
                actual_row = updated_df.loc[idx]
                final_dets = _normalize_tag_list(_collect_row_symptom_tags(actual_row, AI_DET_HEADERS))
                final_dels = _normalize_tag_list(_collect_row_symptom_tags(actual_row, AI_DEL_HEADERS))
                total_labels_written += len(dets_out or []) + len(dels_out or [])
                for lab in (out.get("unl_dels", []) or []):
                    _record_new_symptom_candidate(lab, idx=idx, side="delighter")
                for lab in (out.get("unl_dets", []) or []):
                    _record_new_symptom_candidate(lab, idx=idx, side="detractor")
                processed_local.append(dict(idx=idx, wrote_dets=final_dets, wrote_dels=final_dels, safety=out.get("safety", ""), reliability=out.get("reliability", ""), sessions=out.get("sessions", ""), ev_det=out.get("ev_det", {}), ev_del=out.get("ev_del", {}), unl_dels=out.get("unl_dels", []), unl_dets=out.get("unl_dets", [])))
                done += 1
            dataset_ck = dict(st.session_state["analysis_dataset"])
            dataset_ck["reviews_df"] = updated_df.copy()
            st.session_state["analysis_dataset"] = dataset_ck
            st.session_state["sym_processed_rows"] = list(processed_local)
            elapsed = time.perf_counter() - t0
            rate = done / elapsed if elapsed > 0 else 0
            rem = (total_n - done) / rate if rate > 0 else 0
            prog.progress(done / total_n, text=f"{done}/{total_n} processed")
            # Checkpoint: save partial results so page refresh doesn't lose everything
            if done % max(batch_size * 2, 10) == 0:
                st.session_state["sym_processed_rows"] = list(processed_local)
                st.session_state["_sym_checkpoint_done"] = done
            if not outs:
                eta_box.markdown(f"**Speed:** {rate * 60:.1f} rev/min · **ETA:** ~{_fmt_secs(rem)}")
            avg_labels = total_labels_written / max(done, 1)
            stats_box.markdown(f"<span style='font-size:12px;color:var(--slate-500);'>Labels written: **{total_labels_written}** · Avg per review: **{avg_labels:.1f}**" + (f" · ⚠️ {failed_count} failed" if failed_count > 0 else "") + "</span>", unsafe_allow_html=True)
            # Live preview: show last tagged review snippet
            if outs:
                last_idx = list(outs.keys())[-1]
                last_out = outs[last_idx]
                last_dets = ", ".join(last_out.get("dets", [])[:3]) or "—"
                last_dels = ", ".join(last_out.get("dels", [])[:3]) or "—"
                eta_box.markdown(f"**Speed:** {rate * 60:.1f} rev/min · **ETA:** ~{_fmt_secs(rem)}<br><span style='font-size:11px;color:var(--slate-400);'>Last: 🔴 {_esc(last_dets)} · 🟢 {_esc(last_dels)}</span>", unsafe_allow_html=True)
            # Phase 3: Track label performance mid-run
            if _label_tracker:
                _label_tracker.record_batch(outs)
                alerts = _label_tracker.check_alerts(min_reviews=batch_size * 3)
                for alert in alerts[:2]:
                    if alert["issue"] == "too_broad":
                        stats_box.markdown(f"<span style='font-size:11px;color:var(--warning);'>⚠️ '{alert['label']}' hitting {alert['pct']:.0f}% of reviews — may be too broad</span>", unsafe_allow_html=True)
            gc.collect()
        # ── v3 auto-retry for zero-tag reviews ───────────────────────────
        if _HAS_SYMPTOMIZER_V3 and client and processed_local:
            try:
                all_items = {}
                for bs in bidxs:
                    for ri, rw in rows_list[bs:bs + batch_size]:
                        all_items[int(ri)] = dict(idx=int(ri), review=_symptomizer_review_text(rw), rating=rw.get("rating"))
                batch_res = {r["idx"]: r for r in processed_local}
                retry_res = _v3_retry_zero_tags(
                    client=client, results=batch_res, items=list(all_items.values()),
                    allowed_detractors=_active_dets, allowed_delighters=_active_dels,
                    aliases=aliases, max_ev_chars=_ev_chars,
                    chat_complete_fn=_chat_complete_with_fallback_models,
                    safe_json_load_fn=_safe_json_load, model_fn=_shared_model, reasoning_fn=_shared_reasoning)
                rc = sum(1 for k, v in retry_res.items() if v.get("dets") != batch_res.get(k, {}).get("wrote_dets") or v.get("dels") != batch_res.get(k, {}).get("wrote_dels"))
                for ri, nr in retry_res.items():
                    if ri in updated_df.index and nr != batch_res.get(ri):
                        updated_df = _write_ai_symptom_row(updated_df, ri, dets=nr.get("dets"), dels=nr.get("dels"), safety=nr.get("safety"), reliability=nr.get("reliability"), sessions=nr.get("sessions"))
                if rc:
                    _log.info("Auto-retry recovered tags for %d review(s)", rc)
                    status.info(f"Auto-retry recovered tags for {rc} review(s)")
            except Exception: pass
        dataset = dict(st.session_state["analysis_dataset"])
        dataset["reviews_df"] = updated_df
        qa_baseline_map = _build_symptom_baseline_map(processed_local)
        qa_metrics = _compute_tag_edit_accuracy(qa_baseline_map, qa_baseline_map)
        st.session_state.update(
            analysis_dataset=dataset,
            sym_processed_rows=processed_local,
            master_export_bundle=None,
            sym_qa_baseline_map=qa_baseline_map,
            sym_qa_row_ids=list(qa_baseline_map.keys()),
            sym_qa_selected_row=(next(iter(qa_baseline_map.keys()), None)),
            sym_qa_accuracy=qa_metrics,
            sym_qa_notice=None,
            sym_export_bytes=None,
        )
        status.success(f"✅ Symptomized {done:,} reviews — {total_labels_written} labels written ({total_labels_written / max(done, 1):.1f} avg/review).")
        # Always save partial results (whether completed, stopped, or errored)
        stop_container.empty()  # Remove stop button
        _log.info("Symptomized %d reviews — %d labels written (%.1f avg)", done, total_labels_written, total_labels_written / max(done, 1))
        st.session_state["sym_run_notice"] = f"Symptomized {done:,} reviews · {total_labels_written} labels. Tags visible in Review Explorer."
        st.session_state["sym_run_just_completed"] = True
        st.session_state["sym_validation_sample"] = None  # Reset validation for new run
        # Save working taxonomy to memory for reuse
        try:
            _save_taxonomy_to_memory(
                st.session_state.get("sym_taxonomy_category", "general"),
                st.session_state.get("sym_detractors", []),
                st.session_state.get("sym_delighters", []),
                st.session_state.get("sym_aliases", {}))
        except Exception:
            pass
        st.session_state["sym_last_run_stats"] = {
            "reviews": done, "labels": total_labels_written, "failed": failed_count,
            "avg_per_review": round(total_labels_written / max(done, 1), 1),
            "elapsed_sec": round(time.perf_counter() - t0, 1),
            "model": _shared_model(), "reasoning": _shared_reasoning(),
            "staged": bool(st.session_state.get("sym_staged_pipeline")),
        }
      except Exception as run_exc:
        st.error(f"Symptomizer run failed: {run_exc}")
        _log.error("Symptomizer run failed: %s", run_exc)
        # Save whatever partial results we have
        if processed_local:
            st.session_state["sym_processed_rows"] = list(processed_local)
            st.session_state["sym_run_notice"] = f"Partial results: {len(processed_local)} reviews processed before error."
            st.warning(f"Saved partial results for {len(processed_local)} reviews. You can re-run to process the rest.")
        st.rerun()
    st.divider()
    processed = st.session_state.get("sym_processed_rows") or []
    if not processed:
        st.info("Run the Symptomizer above to see results here.")
        return
    # ── Quick Tag Validation ──────────────────────────────────────────────
    if processed and len(processed) >= 5:
        with st.expander("🎯 Quick tag check — validate accuracy", expanded=False):
            st.caption("Review 10 random tagged reviews. Click ✅ or ❌ for each tag to estimate accuracy.")
            import random
            tagged_reviews = [p for p in processed if p.get("wrote_dets") or p.get("wrote_dels")]
            sample_size = min(10, len(tagged_reviews))
            if tagged_reviews and sample_size > 0:
                if "sym_validation_sample" not in st.session_state or st.button("🔄 New sample", key="sym_resample"):
                    st.session_state["sym_validation_sample"] = random.sample(tagged_reviews, sample_size)
                    st.session_state["sym_validation_votes"] = {}
                sample = st.session_state.get("sym_validation_sample", [])
                votes = st.session_state.get("sym_validation_votes", {})
                for si, pr in enumerate(sample):
                    rid = pr.get("rid", pr.get("review_id", f"#{si}"))
                    review_text = str(pr.get("review_text", pr.get("snippet", "")))[:200]
                    rating = pr.get("rating", "?")
                    all_tags = list(pr.get("wrote_dets", [])) + list(pr.get("wrote_dels", []))
                    if not all_tags:
                        continue
                    st.markdown(f"<div style='font-size:12px;color:var(--slate-400);'>Review {rid} · {rating}★</div><div style='font-size:13px;margin:2px 0 6px;'>{_esc(review_text)}{'…' if len(review_text) >= 200 else ''}</div>", unsafe_allow_html=True)
                    tag_cols = st.columns(min(len(all_tags), 4))
                    for ti, tag in enumerate(all_tags[:4]):
                        col = tag_cols[ti % len(tag_cols)]
                        vote_key = f"{rid}_{tag}"
                        is_det = tag in (pr.get("wrote_dets") or [])
                        color = "🔴" if is_det else "🟢"
                        current_vote = votes.get(vote_key)
                        if current_vote is None:
                            c1, c2 = col.columns(2)
                            if c1.button(f"✅ {tag[:15]}", key=f"sv_y_{si}_{ti}", use_container_width=True):
                                votes[vote_key] = True
                                st.session_state["sym_validation_votes"] = votes
                                st.rerun()
                            if c2.button(f"❌", key=f"sv_n_{si}_{ti}", use_container_width=True):
                                votes[vote_key] = False
                                st.session_state["sym_validation_votes"] = votes
                                st.rerun()
                        else:
                            col.markdown(f"{color} {_esc(tag[:20])} {'✅' if current_vote else '❌'}")
                    st.divider()
                # Show accuracy score
                if votes:
                    correct = sum(1 for v in votes.values() if v)
                    total_votes = len(votes)
                    accuracy = correct / total_votes * 100
                    color = "green" if accuracy >= 80 else ("orange" if accuracy >= 60 else "red")
                    st.markdown(f"<div style='font-size:18px;font-weight:700;color:{color};'>Accuracy: {accuracy:.0f}% ({correct}/{total_votes} tags correct)</div>", unsafe_allow_html=True)
                    if accuracy < 70:
                        wrong_tags = [k.split("_", 1)[1] for k, v in votes.items() if not v]
                        if wrong_tags:
                            st.caption(f"Problematic tags: {', '.join(wrong_tags[:5])}")
            else:
                st.info("No tagged reviews to validate yet.")
    # ── Last run summary ─────────────────────────────────────────────────
    last_stats = st.session_state.get("sym_last_run_stats")
    if last_stats:
        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("Reviews", f"{last_stats.get('reviews', 0):,}")
        sc2.metric("Labels", f"{last_stats.get('labels', 0):,}")
        sc3.metric("Avg/review", f"{last_stats.get('avg_per_review', 0):.1f}")
        sc4.metric("Speed", f"{last_stats.get('reviews', 0) / max(last_stats.get('elapsed_sec', 1), 0.1) * 60:.0f}/min")
        sc5.metric("Pipeline", "Staged" if last_stats.get("staged") else "Single-pass")
    st.markdown("### 3 · Results")
    total_tags = sum(len(r.get("wrote_dets", [])) + len(r.get("wrote_dels", [])) for r in processed)
    st.markdown(_chip_html([(f"{len(processed)} reviews tagged", "green"), (f"{total_tags} labels written", "indigo")]), unsafe_allow_html=True)
    if _HAS_SYMPTOMIZER_V3 and len(processed) >= 3:
        try:
            _aud = _v3_audit_distribution({r["idx"]: {"dets":r.get("wrote_dets",[]),"dels":r.get("wrote_dels",[]),"ev_det":r.get("ev_det",{}),"ev_del":r.get("ev_del",{})} for r in processed})
            _sing = _aud.get("singleton_detractors",[])+_aud.get("singleton_delighters",[])
            _dom = _aud.get("dominant_detractors",[])+_aud.get("dominant_delighters",[])
            _noev = _aud.get("zero_evidence_tags",[])
            if _sing or _dom or _noev:
                with st.expander(f"🔍 Tag quality audit — {len(_sing)} singletons · {len(_dom)} dominant · {len(_noev)} no-evidence", expanded=False):
                    if _sing: st.caption("**Singletons** (appear once — possible hallucinations):"); st.markdown(", ".join(f"`{s}`" for s in _sing[:20]))
                    if _dom: st.caption("**Dominant** (>50% of reviews — may be too broad):"); st.markdown(", ".join(f"`{d}`" for d in _dom[:10]))
                    if _noev: st.caption("**No evidence** (tagged without supporting text):"); st.markdown(", ".join(f"`{e}`" for e in _noev[:15]))
        except Exception: pass
    # ── Phase 4: Taxonomy recommendations ────────────────────────────────
    if _HAS_SYMPTOMIZER_V3 and len(processed) >= 5:
        try:
            rec_data = {r["idx"]: {"dets": r.get("wrote_dets",[]), "dels": r.get("wrote_dels",[]),
                                    "ev_det": r.get("ev_det",{}), "ev_del": r.get("ev_del",{}),
                                    "unl_dets": r.get("unl_dets",[]), "unl_dels": r.get("unl_dels",[])} for r in processed}
            recs = _v3_generate_recommendations(rec_data,
                allowed_detractors=list(detractors or []), allowed_delighters=list(delighters or []))
            if recs:
                high_recs = [r for r in recs if r["priority"] == "high"]
                med_recs = [r for r in recs if r["priority"] == "medium"]
                with st.expander(f"💡 Taxonomy recommendations — {len(high_recs)} high priority · {len(med_recs)} medium", expanded=bool(high_recs)):
                    for rec in recs[:12]:
                        icon = {"promote": "⬆️", "split": "✂️", "merge": "🔗", "remove": "🗑️"}.get(rec["action"], "•")
                        badge_color = {"high": "rgba(220,38,38,.12)", "medium": "rgba(217,119,6,.12)"}.get(rec["priority"], "rgba(100,116,139,.1)")
                        badge_text = {"high": "#dc2626", "medium": "#d97706"}.get(rec["priority"], "#64748b")
                        st.markdown(
                            f"<div style='display:flex;align-items:flex-start;gap:8px;padding:6px 0;border-bottom:1px solid var(--border,#dde1e8);'>"
                            f"<span style='font-size:16px;flex-shrink:0;'>{icon}</span>"
                            f"<div style='flex:1;'>"
                            f"<span style='font-size:12.5px;font-weight:700;color:var(--navy);'>{rec['action'].title()}: {', '.join(rec['labels'][:3])}</span>"
                            f"<div style='font-size:11.5px;color:var(--slate-500);margin-top:2px;'>{rec['reason']}</div>"
                            f"</div>"
                            f"<span style='font-size:10px;padding:2px 6px;border-radius:99px;background:{badge_color};color:{badge_text};font-weight:700;text-transform:uppercase;'>{rec['priority']}</span>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                    # One-click apply for promote recommendations
                    promote_recs = [r for r in recs if r["action"] == "promote"]
                    if promote_recs:
                        if st.button(f"⬆️ Promote {len(promote_recs)} suggested labels to catalog", key="sym_apply_promotions"):
                            for r in promote_recs:
                                for label in r["labels"]:
                                    side = r.get("side", "detractor")
                                    target_key = "sym_delighters" if side == "delighter" else "sym_detractors"
                                    current = list(st.session_state.get(target_key) or [])
                                    if label not in current:
                                        current.append(label)
                                        st.session_state[target_key] = current
                            st.session_state["sym_aliases"] = _alias_map_for_catalog(
                                st.session_state.get("sym_delighters", []),
                                st.session_state.get("sym_detractors", []))
                            st.toast(f"Promoted {len(promote_recs)} labels to catalog", icon="⬆️")
                            st.rerun()
        except Exception:
            pass
    raw_cands = {k: v for k, v in (st.session_state.get("sym_new_candidates") or {}).items() if k.strip() and k.strip() not in (delighters + detractors)}
    det_candidate_rows = _candidate_rows_for_side(raw_cands, side="detractor") if raw_cands else []
    del_candidate_rows = _candidate_rows_for_side(raw_cands, side="delighter") if raw_cands else []
    total_candidate_labels = len({row["Label"] for row in det_candidate_rows + del_candidate_rows})
    if det_candidate_rows or del_candidate_rows:
        with st.expander(f"🟡 New symptom candidates ({total_candidate_labels})", expanded=False):
            st.caption("Separated by AI-suggested side so it is faster to add the right labels. You can also promote broad cross-product themes into the Universal Neutral pack right from here.")
            cand_tabs = st.tabs([f"🔴 Detractor candidates ({len(det_candidate_rows)})", f"🟢 Delighter candidates ({len(del_candidate_rows)})"])
            with cand_tabs[0]:
                if not det_candidate_rows:
                    st.info("No new detractor candidates in this run.")
                else:
                    det_df = pd.DataFrame(det_candidate_rows)
                    st.dataframe(det_df, use_container_width=True, hide_index=True, height=min(360, 48 + 35 * len(det_df)), column_config={"Count": st.column_config.NumberColumn(format="%d")})
                    det_lookup = {row["Label"]: row for row in det_candidate_rows}
                    with st.form("sym_new_det_form"):
                        det_pick = st.multiselect(
                            "Choose detractor candidates to add",
                            options=list(det_lookup.keys()),
                            format_func=lambda lab: f"{lab} · {det_lookup[lab].get('L1 Theme', 'Product Specific')} · {int(det_lookup[lab]['Count'])} mentions",
                            key="sym_new_det_pick",
                        )
                        dc1, dc2 = st.columns(2)
                        add_det = dc1.form_submit_button("Add selected → Detractors", use_container_width=True)
                        promote_det = dc2.form_submit_button("Promote selected → Universal Neutral Detractors", use_container_width=True)
                    if add_det and det_pick:
                        _, new_dets = _canonical_symptom_catalog(delighters, detractors + list(det_pick))
                        st.session_state["sym_detractors"] = new_dets
                        st.session_state["sym_aliases"] = _alias_map_for_catalog(st.session_state.get("sym_delighters") or delighters, new_dets, existing_aliases=st.session_state.get("sym_aliases", {}))
                        st.success(f"Added {len(det_pick)} detractor candidate(s).")
                        st.rerun()
                    if promote_det and det_pick:
                        _promote_labels_to_custom_universal(det_pick, side="detractor")
                        st.success(f"Promoted {len(det_pick)} detractor candidate(s) to Universal Neutral Symptoms.")
                        st.rerun()
            with cand_tabs[1]:
                if not del_candidate_rows:
                    st.info("No new delighter candidates in this run.")
                else:
                    del_df = pd.DataFrame(del_candidate_rows)
                    st.dataframe(del_df, use_container_width=True, hide_index=True, height=min(360, 48 + 35 * len(del_df)), column_config={"Count": st.column_config.NumberColumn(format="%d")})
                    del_lookup = {row["Label"]: row for row in del_candidate_rows}
                    with st.form("sym_new_del_form"):
                        del_pick = st.multiselect(
                            "Choose delighter candidates to add",
                            options=list(del_lookup.keys()),
                            format_func=lambda lab: f"{lab} · {del_lookup[lab].get('L1 Theme', 'Product Specific')} · {int(del_lookup[lab]['Count'])} mentions",
                            key="sym_new_del_pick",
                        )
                        dc1, dc2 = st.columns(2)
                        add_del = dc1.form_submit_button("Add selected → Delighters", use_container_width=True)
                        promote_del = dc2.form_submit_button("Promote selected → Universal Neutral Delighters", use_container_width=True)
                    if add_del and del_pick:
                        new_dels, _ = _canonical_symptom_catalog(delighters + list(del_pick), detractors)
                        st.session_state["sym_delighters"] = new_dels
                        st.session_state["sym_aliases"] = _alias_map_for_catalog(new_dels, st.session_state.get("sym_detractors") or detractors, existing_aliases=st.session_state.get("sym_aliases", {}))
                        st.success(f"Added {len(del_pick)} delighter candidate(s).")
                        st.rerun()
                    if promote_del and del_pick:
                        _promote_labels_to_custom_universal(del_pick, side="delighter")
                        st.success(f"Promoted {len(del_pick)} delighter candidate(s) to Universal Neutral Symptoms.")
                        st.rerun()
    updated_reviews = (st.session_state.get("analysis_dataset") or {}).get("reviews_df", overall_df)
    qa_row_ids = [str(rid) for rid in (st.session_state.get("sym_qa_row_ids") or []) if str(rid).strip()]
    if not qa_row_ids:
        qa_row_ids = [str(rec.get("idx")) for rec in processed if str(rec.get("idx", "")).strip()]
    if qa_row_ids and not st.session_state.get("sym_qa_baseline_map"):
        st.session_state["sym_qa_baseline_map"] = _build_symptom_baseline_map(processed)
    qa_row_ids = [rid for rid in qa_row_ids if rid in _collect_ai_tag_map(updated_reviews, row_ids=qa_row_ids)]
    if qa_row_ids:
        st.session_state["sym_qa_row_ids"] = qa_row_ids
    qa_metrics = _qa_accuracy_metrics(updated_reviews) if qa_row_ids else {}
    qa_notice = st.session_state.pop("sym_qa_notice", None)
    if qa_notice:
        st.success(qa_notice)
    if qa_metrics:
        qa_cols = st.columns(5)
        qa_cols[0].metric("Editable reviews", f"{len(qa_row_ids):,}")
        qa_cols[1].metric("Baseline tags", f"{qa_metrics.get('baseline_total_tags', 0):,}")
        qa_cols[2].metric("Added", f"{qa_metrics.get('added_tags', 0):,}")
        qa_cols[3].metric("Removed", f"{qa_metrics.get('removed_tags', 0):,}")
        qa_cols[4].metric("Accuracy", f"{qa_metrics.get('accuracy_pct', 100.0):.1f}%")
        goal_chip = "green" if float(qa_metrics.get("accuracy_pct", 100.0) or 0) >= 80 else "yellow"
        st.markdown(_chip_html([
            ("Goal: >80%", goal_chip),
            (f"Changed reviews: {qa_metrics.get('changed_reviews', 0)}", "indigo"),
            ("Inline edits save into export", "gray"),
        ]), unsafe_allow_html=True)
        with st.expander("How the accuracy % works", expanded=False):
            st.markdown(
                "Accuracy is calculated against the original AI baseline for this run: **Accuracy = 100 × (1 - (adds + removes) / baseline tags)**. "
                "Example: 30 original tags, 2 additions, and 2 removals = 86.7% accuracy. The goal is to keep this above 80% while still correcting misses."
            )

    current_preview_items = list(st.session_state.get("sym_taxonomy_preview_items") or _taxonomy_preview_items_with_side(st.session_state.get("sym_ai_build_result") or {}))
    current_category = str(st.session_state.get("sym_taxonomy_category") or (st.session_state.get("sym_ai_build_result") or {}).get("category") or "general")
    processed_indices = []
    for rec in processed:
        try:
            processed_indices.append(int(rec.get("idx")))
        except Exception:
            continue
    processed_indices = [idx for idx in processed_indices if idx in getattr(updated_reviews, "index", [])]
    processed_df = updated_reviews.loc[processed_indices].copy() if processed_indices else pd.DataFrame(columns=updated_reviews.columns)
    if not processed_df.empty:
        _render_symptomizer_taxonomy_workbench(
            processed_df=processed_df,
            delighters=delighters,
            detractors=detractors,
            aliases=st.session_state.get("sym_aliases", {}),
            category=current_category,
            preview_items=current_preview_items,
        )
        st.divider()
    evidence_lookup = _build_evidence_lookup(processed)
    log_options = [20, 50, 100, 250, "All"]
    if st.session_state.get("sym_review_log_limit") not in log_options:
        st.session_state["sym_review_log_limit"] = 50
    log_choice = st.selectbox("Review log size", options=log_options, key="sym_review_log_limit", help="Expand the result log beyond the old 20-review cap.")
    processed_pool = list(processed) if str(log_choice) == "All" else list(processed[-int(log_choice):])
    ordered_pool = list(reversed(processed_pool))
    page_size_options = [10, 20, 25, 50]
    if st.session_state.get("sym_review_page_size") not in page_size_options:
        st.session_state["sym_review_page_size"] = 20
    total_pages = max(1, int(math.ceil(len(ordered_pool) / float(max(int(st.session_state.get("sym_review_page_size") or 20), 1)))))
    current_page = int(st.session_state.get("sym_review_page") or 1)
    current_page = min(max(current_page, 1), total_pages)
    pg1, pg2, pg3 = st.columns([1.1, 1.0, 2.4])
    page_size = int(pg1.selectbox("Rows per page", options=page_size_options, index=page_size_options.index(int(st.session_state.get("sym_review_page_size") or 20)), key="sym_review_page_size"))
    total_pages = max(1, int(math.ceil(len(ordered_pool) / float(max(page_size, 1)))))
    current_page = min(max(current_page, 1), total_pages)
    st.session_state["sym_review_page"] = current_page
    page_num = int(pg2.number_input("Page", min_value=1, max_value=total_pages, value=current_page, step=1, key="sym_review_page"))
    pg3.caption(f"Rendering {min(page_size, len(ordered_pool)):,} review cards at a time keeps inline editing and taxonomy updates smoother on large runs.")
    page_start = max(0, (page_num - 1) * page_size)
    page_end = page_start + page_size
    processed_view = ordered_pool[page_start:page_end]
    with st.expander(f"📋 Review log — showing {len(processed_view)} of {len(processed_pool)} loaded from {len(processed)} processed", expanded=True):
        for rec in processed_view:
            idx = rec.get("idx", "?")
            row_key = str(idx).strip()
            head = f"Row {idx} — {len(rec.get('wrote_dets', []))} issues · {len(rec.get('wrote_dels', []))} strengths"
            with st.expander(head):
                row = None
                try:
                    row = updated_reviews.loc[int(idx)]
                    _render_review_card(row, evidence_items=evidence_lookup.get(row_key) or None)
                except Exception:
                    try:
                        vb = str(overall_df.loc[int(idx), "review_text"])[:800]
                        st.markdown(f"<div class='review-body'>{html.escape(vb)}</div>", unsafe_allow_html=True)
                    except Exception:
                        st.info("Could not load that review row.")
                st.markdown("<div class='chip-wrap' style='margin-top:8px;margin-bottom:4px;'>" + f"<span class='chip yellow'>Safety: {_esc(rec.get('safety', ''))}</span>" + f"<span class='chip indigo'>Reliability: {_esc(rec.get('reliability', ''))}</span>" + f"<span class='chip gray'>Sessions: {_esc(rec.get('sessions', ''))}</span>" + "</div>", unsafe_allow_html=True)

                if row is None:
                    continue

                baseline_map = st.session_state.get("sym_qa_baseline_map") or {}
                baseline_payload = baseline_map.get(row_key, {"detractors": [], "delighters": []})
                current_dets = _normalize_tag_list(_collect_row_symptom_tags(row, AI_DET_HEADERS))
                current_dels = _normalize_tag_list(_collect_row_symptom_tags(row, AI_DEL_HEADERS))
                suggestions = _build_inline_tag_suggestions(row, current_dets, current_dels, detractors, delighters)
                missing_dets = suggestions.get("missing_detractors", [])
                missing_dels = suggestions.get("missing_delighters", [])
                det_options = _normalize_tag_list(current_dets + detractors + missing_dets)
                del_options = _normalize_tag_list(current_dels + delighters + missing_dels)

                suggestion_chips = []
                if missing_dets:
                    suggestion_chips.extend((f"Missing detractor: {lab}", "red") for lab in missing_dets[:3])
                if missing_dels:
                    suggestion_chips.extend((f"Missing delighter: {lab}", "green") for lab in missing_dels[:3])

                current_issue_chips = [(label, "red") for label in current_dets[:8]]
                current_strength_chips = [(label, "green") for label in current_dels[:8]]
                baseline_issue_chips = [(label, "gray") for label in (baseline_payload.get("detractors", []) or [])[:8]]
                baseline_strength_chips = [(label, "gray") for label in (baseline_payload.get("delighters", []) or [])[:8]]

                with st.expander("Edit Taxonomy", expanded=False):
                    st.markdown(
                        "<div class='soft-panel' style='margin-top:.2rem;'><b>Fix tags right here</b> · Deselect a current tag to remove it, pick a missing symptom from the catalog, or type a brand-new symptom. This review updates the export file immediately after you save it.</div>",
                        unsafe_allow_html=True,
                    )
                    st.caption("Universal Neutral Symptoms are active here." if bool(st.session_state.get("sym_include_universal_neutral", True)) else "Universal Neutral Symptoms are turned off in this workspace, so this editor is only using product-specific tags.")

                    summary_cols = st.columns(2)
                    with summary_cols[0]:
                        st.markdown("**🔴 Issues**")
                        st.caption("Current saved tags")
                        if current_issue_chips:
                            st.markdown(_chip_html(current_issue_chips), unsafe_allow_html=True)
                        else:
                            st.caption("None")
                        st.caption("Original AI tags")
                        if baseline_issue_chips:
                            st.markdown(_chip_html(baseline_issue_chips), unsafe_allow_html=True)
                        else:
                            st.caption("None")
                    with summary_cols[1]:
                        st.markdown("**🟢 Strengths**")
                        st.caption("Current saved tags")
                        if current_strength_chips:
                            st.markdown(_chip_html(current_strength_chips), unsafe_allow_html=True)
                        else:
                            st.caption("None")
                        st.caption("Original AI tags")
                        if baseline_strength_chips:
                            st.markdown(_chip_html(baseline_strength_chips), unsafe_allow_html=True)
                        else:
                            st.caption("None")

                    if suggestion_chips:
                        st.markdown(_chip_html(suggestion_chips), unsafe_allow_html=True)
                    else:
                        st.caption("No obvious missing tags detected, but you can still add one manually below.")

                    suggested_det_text = ", ".join(missing_dets[:5]) if missing_dets else "No missing detractor suggestion"
                    suggested_del_text = ", ".join(missing_dels[:5]) if missing_dels else "No missing delighter suggestion"

                    with st.form(f"sym_inline_form_{row_key}"):
                        edit_cols = st.columns(2)
                        with edit_cols[0]:
                            st.caption(f"Suggested missing detractors: {suggested_det_text}")
                            chosen_dets = st.multiselect(
                                "Choose detractor symptoms",
                                options=det_options,
                                default=current_dets,
                                key=f"sym_inline_det_select_{row_key}",
                                help="Deselect a tag to remove it, or select a missing symptom to add it.",
                            )
                            new_det_text = st.text_input(
                                "Add a new detractor symptom",
                                placeholder="Comma-separated, e.g. Hard To Clean, Cheap Handle",
                                key=f"sym_inline_det_new_{row_key}",
                            )
                        with edit_cols[1]:
                            st.caption(f"Suggested missing delighters: {suggested_del_text}")
                            chosen_dels = st.multiselect(
                                "Choose delighter symptoms",
                                options=del_options,
                                default=current_dels,
                                key=f"sym_inline_del_select_{row_key}",
                                help="Deselect a tag to remove it, or select a missing symptom to add it.",
                            )
                            new_del_text = st.text_input(
                                "Add a new delighter symptom",
                                placeholder="Comma-separated, e.g. Quiet, Easy Cleanup",
                                key=f"sym_inline_del_new_{row_key}",
                            )

                        st.caption("Cleanest workflow: scan the review, make the edit here, then click Update tags + refresh accuracy.")
                        action_cols = st.columns([2.1, 1.25, 1.65])
                        apply_changes = action_cols[0].form_submit_button("✅ Update tags + refresh accuracy", type="primary", use_container_width=True)
                        reset_changes = action_cols[1].form_submit_button("↺ Reset to AI baseline", use_container_width=True)
                        action_cols[2].caption("Only the first 10 detractors and first 10 delighters are written to the export file for each review.")

                    if apply_changes:
                        new_dets = _parse_manual_tag_entries(new_det_text)
                        new_dels = _parse_manual_tag_entries(new_del_text)
                        ok, message = _save_inline_symptom_edit(
                            row_key,
                            row,
                            _normalize_tag_list(list(chosen_dets) + new_dets),
                            _normalize_tag_list(list(chosen_dels) + new_dels),
                            updated_reviews=updated_reviews,
                            processed_rows=processed,
                            detractors=detractors,
                            delighters=delighters,
                            notice_prefix="Updated",
                        )
                        if ok:
                            st.rerun()
                        else:
                            st.error(message)
                    if reset_changes:
                        ok, message = _save_inline_symptom_edit(
                            row_key,
                            row,
                            baseline_payload.get("detractors", []),
                            baseline_payload.get("delighters", []),
                            updated_reviews=updated_reviews,
                            processed_rows=processed,
                            detractors=detractors,
                            delighters=delighters,
                            notice_prefix="Reset",
                        )
                        if ok:
                            st.rerun()
                        else:
                            st.error(message)

    ec1, ec2 = st.columns([1.5, 3])
    if ec1.button("🧾 Prepare export", use_container_width=True, key="sym_prep_export"):
        upd = st.session_state["analysis_dataset"]["reviews_df"]
        orig = st.session_state.get("_uploaded_raw_bytes")
        sym_bytes = _gen_symptomized_workbook(orig, upd) if orig else _build_master_excel(summary, upd)
        st.session_state["sym_export_bytes"] = sym_bytes
        st.success("Export prepared.")
    sym_bytes = st.session_state.get("sym_export_bytes")
    ec1.download_button("⬇️ Download symptomized file", data=sym_bytes or b"", file_name=f"{summary.product_id}_Symptomized.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", disabled=(sym_bytes is None), key="sym_dl")
    qa_export_metrics = st.session_state.get("sym_qa_accuracy") or {}
    if qa_export_metrics:
        ec2.caption(f"Writes the final AI symptom tags (including QA edits) plus AI Safety · Reliability · Sessions into existing AI columns when present, otherwise it appends safe new columns without overwriting local symptomization. Current QA accuracy: {float(qa_export_metrics.get('accuracy_pct', 100.0) or 0):.1f}%.")
    else:
        ec2.caption("Writes AI Symptom Detractor / Delighter columns plus AI Safety · Reliability · Sessions into existing AI columns when present, otherwise it appends safe new columns without overwriting local symptomization.")

# ── Social Listening (extracted to review_analyst/social_listening.py) ────
if not _HAS_SOCIAL_PKG:
    # Fallback stubs when package not available
    def _build_social_beta_query(raw_query): return raw_query
    def _social_demo_payload(): return {"posts":pd.DataFrame(),"detractors":[],"delighters":[],"viral":[],"top_comments":[],"compare":{},"metrics":{}}
    def _social_demo_query(p): return p
    def _social_demo_trend(s,e): return pd.DataFrame()
    def _render_social_metric_card(l,v,s,accent="indigo"): pass
    def _render_social_post_card(r,*,highlight=""): pass
    def _social_demo_answer(q): return "Social listening module not loaded."
    def _render_social_listening_tab():
        st.info("Social Listening module not available.")
else:
    from review_analyst.social_listening import (
        _build_social_beta_query, _social_demo_payload, _social_demo_query,
        _social_demo_trend, _render_social_metric_card, _render_social_post_card,
        _social_demo_answer, _render_social_listening_tab,
    )


def _render_lava_lamp_background():
    st.markdown("""<div class='lava-lamp' aria-hidden='true'>
      <span class='lava-blob a'></span>
      <span class='lava-blob b'></span>
      <span class='lava-blob c'></span>
      <span class='lava-blob d'></span>
    </div>""", unsafe_allow_html=True)


def main():
    _render_lava_lamp_background()
    st.markdown("""<div style="display:flex;align-items:center;gap:12px;justify-content:space-between;flex-wrap:wrap;margin-bottom:.2rem;">
      <div style="display:flex;align-items:center;gap:12px;">
        <div style="width:36px;height:36px;background:#0f172a;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:18px;box-shadow:0 8px 24px rgba(15,23,42,.18);">✨</div>
        <div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <div style="font-size:20px;font-weight:800;letter-spacing:-.03em;color:#0f172a;">StarWalk Review Analyst</div>
            <span class='beta-chip'>Beta</span>
          </div>
          <div style="font-size:12px;color:#64748b;margin-top:1px;">Single-file Streamlit workspace for executive review, deep-dive exploration, Review Prompt, Symptomizer, and a mocked Social Listening Beta route that works even before reviews exist.</div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    dataset = st.session_state.get("analysis_dataset")
    if dataset:
        bc = st.columns([2.8, 1.4, 0.6, 0.6])
        # Workspace name (editable)
        ws_name = st.session_state.get("workspace_name", dataset.get("source_label", "Untitled"))
        bc[0].markdown(f"<div style='font-size:13px;color:var(--slate-500);'><strong style='color:var(--navy);'>{_esc(ws_name)}</strong> · {dataset.get('source_type', '').title()} · {len(dataset.get('reviews_df', pd.DataFrame())):,} reviews</div>", unsafe_allow_html=True)
        # Save button
        if _HAS_WORKSPACE_STORE:
            if bc[1].button("💾 Save workspace", use_container_width=True, key="ws_save_btn"):
                try:
                    summary = dataset.get("summary")
                    reviews_df = dataset.get("reviews_df", pd.DataFrame())
                    sym_state = {k: v for k, v in st.session_state.items() if k.startswith("sym_") and not k.startswith("_")}
                    ws_id = _ws_save(
                        workspace_name=ws_name,
                        source_type=dataset.get("source_type", "unknown"),
                        source_label=dataset.get("source_label", ""),
                        reviews_df=reviews_df,
                        dataset_payload={k: v for k, v in dataset.items() if k not in ("reviews_df",)},
                        state_payload=sym_state,
                        product_id=getattr(summary, "product_id", "") if summary else "",
                        product_url=getattr(summary, "product_url", "") if summary else "",
                        symptomized=bool(st.session_state.get("sym_processed_rows")),
                        workspace_id=st.session_state.get("workspace_id"),
                        allow_source_upsert=True,
                    )
                    st.session_state["workspace_id"] = ws_id
                    st.toast(f"Workspace saved: {ws_name}", icon="💾")
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
        if bc[2].button("✏️", use_container_width=True, key="ws_rename_btn", help="Rename workspace"):
            st.session_state["_ws_show_rename"] = True
        if bc[3].button("Clear", use_container_width=True, key="ws_clear"):
            _reset_workspace_state(reset_source=True)
            st.rerun()
        # Rename dialog
        if st.session_state.get("_ws_show_rename"):
            rc1, rc2 = st.columns([3, 1])
            new_name = rc1.text_input("New workspace name", value=ws_name, key="_ws_rename_input")
            if rc2.button("Save name", key="_ws_rename_confirm"):
                st.session_state["workspace_name"] = new_name
                if _HAS_WORKSPACE_STORE and st.session_state.get("workspace_id"):
                    try:
                        _ws_rename(st.session_state["workspace_id"], new_name)
                    except Exception:
                        pass
                st.session_state["_ws_show_rename"] = False
                st.rerun()
        failures = dataset.get("source_failures") or []
        if failures:
            st.warning("Partial multi-link load: " + " | ".join(f"{u} → {err}" for u, err in failures[:3]))

    if st.session_state.get("workspace_source_mode") not in {SOURCE_MODE_URL, SOURCE_MODE_FILE}:
        st.session_state["workspace_source_mode"] = SOURCE_MODE_URL

    with st.expander("🧰 Build or switch workspace", expanded=(dataset is None and st.session_state.get("workspace_active_tab") != TAB_SOCIAL_LISTENING)):
        # ── Saved Workspaces ──────────────────────────────────────────────
        if _HAS_WORKSPACE_STORE:
            try:
                saved_count = _ws_count()
            except Exception:
                saved_count = 0
            if saved_count > 0:
                with st.container(border=True):
                    st.markdown(f"**💾 Saved Workspaces** ({saved_count})")
                    try:
                        saved_list = _ws_list(limit=20)
                    except Exception:
                        saved_list = []
                    if saved_list:
                        for ws in saved_list:
                            wc1, wc2, wc3 = st.columns([3.5, 0.8, 0.7])
                            avg_r = ws.get("avg_rating")
                            avg_str = f" · {avg_r:.1f}★" if avg_r else ""
                            sym_str = f" · {ws.get('symptomized_count', 0)} tagged" if ws.get("symptomized") else ""
                            wc1.markdown(f"**{_esc(ws.get('workspace_name', 'Untitled'))}** <span style='color:var(--slate-400);font-size:12px;'>{ws.get('review_count', 0):,} reviews{avg_str}{sym_str}</span>", unsafe_allow_html=True)
                            if wc2.button("Load", key=f"ws_load_{ws['workspace_id']}", use_container_width=True):
                                try:
                                    loaded = _ws_load(ws["workspace_id"])
                                    _ws_touch(ws["workspace_id"])
                                    _reset_review_filters()
                                    # Restore dataset
                                    restored_dataset = loaded.get("dataset_payload", {})
                                    restored_dataset["reviews_df"] = loaded.get("reviews_df", pd.DataFrame())
                                    st.session_state["analysis_dataset"] = restored_dataset
                                    st.session_state["workspace_id"] = ws["workspace_id"]
                                    st.session_state["workspace_name"] = ws.get("workspace_name", "Untitled")
                                    # Restore symptomizer state
                                    for k, v in (loaded.get("state_payload") or {}).items():
                                        if k.startswith("sym_"):
                                            st.session_state[k] = v
                                    st.session_state["workspace_active_tab"] = TAB_DASHBOARD
                                    st.toast(f"Loaded: {ws.get('workspace_name', 'Untitled')}", icon="📂")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Load failed: {exc}")
                            if wc3.button("🗑️", key=f"ws_del_{ws['workspace_id']}", use_container_width=True, help="Delete"):
                                try:
                                    _ws_delete(ws["workspace_id"])
                                    st.toast(f"Deleted: {ws.get('workspace_name', '')}", icon="🗑️")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Delete failed: {exc}")
                    else:
                        st.caption("No saved workspaces yet. Build one below and save it.")
                st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
        st.markdown("<div class='builder-card'><div class='builder-kicker'>Get started</div><div class='builder-title'>Build a review workspace</div><div class='builder-sub'>Paste a supported product page or review API URL, or upload a review export file. Start with the Dashboard for an executive summary, then move into the other tabs for deeper work.</div><div class='helper-chip-row'><span class='helper-chip'>Shark/Ninja US</span><span class='helper-chip'>Shark/Ninja UK/EU</span><span class='helper-chip'>Costco</span><span class='helper-chip'>Sephora</span><span class='helper-chip'>Ulta</span><span class='helper-chip'>Hoka</span><span class='helper-chip'>CurrentBody</span><span class='helper-chip'>Okendo API</span><span class='helper-chip'>Bazaarvoice API</span><span class='helper-chip'>PowerReviews API</span></div></div>", unsafe_allow_html=True)
        source_mode = st.radio("Workspace source", [SOURCE_MODE_URL, SOURCE_MODE_FILE], horizontal=True, key="workspace_source_mode")
        if source_mode == SOURCE_MODE_URL:
            url_mode = st.radio("Link mode", ["Single link", "Multiple links"], horizontal=True, key="workspace_url_entry_mode")
            if url_mode == "Single link":
                st.text_input("Product or review URL", key="workspace_product_url", placeholder="Paste a product page or direct review endpoint")
                st.caption("Fastest path: paste a retailer product page or a direct Bazaarvoice / PowerReviews / Okendo review endpoint.")
                if st.button("Build review workspace", type="primary", key="ws_build_url", use_container_width=True):
                    try:
                        nd = _load_product_reviews_dispatch(st.session_state.get("workspace_product_url", DEFAULT_PRODUCT_URL))
                        _reset_review_filters()
                        st.session_state.update(analysis_dataset=nd, chat_messages=[], master_export_bundle=None, prompt_run_artifacts=None, sym_processed_rows=[], sym_new_candidates={}, sym_symptoms_source="none", sym_delighters=[], sym_detractors=[], sym_custom_universal_delighters=[], sym_custom_universal_detractors=[], sym_aliases={}, sym_taxonomy_preview_items=[], sym_taxonomy_category="general", sym_qa_baseline_map={}, sym_qa_accuracy={}, sym_qa_row_ids=[], sym_qa_selected_row=None, sym_qa_notice=None, sym_product_profile_ai_note="", sym_product_knowledge={}, workspace_active_tab=TAB_DASHBOARD, workspace_tab_request=None, ai_include_references=False, ot_show_volume=False, _uploaded_raw_bytes=None, sym_export_bytes=None)
                        # Phase 1: Auto-discover product profile + knowledge
                        with st.spinner("Auto-discovering product profile…"):
                            _auto_discover_product(nd)
                        st.rerun()
                    except requests.HTTPError as exc:
                        st.error(f"HTTP error: {exc}")
                    except ReviewDownloaderError as exc:
                        st.error(str(exc))
                    except Exception as exc:
                        st.error(str(exc))
            else:
                st.text_area(
                    "Product or review URLs",
                    key="workspace_product_urls_bulk",
                    height=150,
                    placeholder="Paste one product page or review endpoint per line\nhttps://www.costco.com/...\nhttps://www.sephora.com/...\nhttps://www.hoka.com/...",
                )
                bulk_urls = _parse_bulk_product_urls(st.session_state.get("workspace_product_urls_bulk", ""))
                if bulk_urls:
                    preview = ", ".join((_strip_www(urlparse(u).netloc) or u) for u in bulk_urls[:4])
                    if len(bulk_urls) > 4:
                        preview += f" +{len(bulk_urls) - 4}"
                    st.caption(f"Ready to load {len(bulk_urls)} link(s) · {preview}")
                else:
                    st.caption("Paste one product page or direct review endpoint per line to build a combined workspace.")
                if st.button("Build combined workspace", type="primary", key="ws_build_url_multi", use_container_width=True):
                    try:
                        nd = _load_multiple_product_reviews_dispatch(bulk_urls)
                        _reset_review_filters()
                        st.session_state.update(analysis_dataset=nd, chat_messages=[], master_export_bundle=None, prompt_run_artifacts=None, sym_processed_rows=[], sym_new_candidates={}, sym_symptoms_source="none", sym_delighters=[], sym_detractors=[], sym_custom_universal_delighters=[], sym_custom_universal_detractors=[], sym_aliases={}, sym_taxonomy_preview_items=[], sym_taxonomy_category="general", sym_qa_baseline_map={}, sym_qa_accuracy={}, sym_qa_row_ids=[], sym_qa_selected_row=None, sym_qa_notice=None, sym_product_profile_ai_note="", sym_product_knowledge={}, workspace_active_tab=TAB_DASHBOARD, workspace_tab_request=None, ai_include_references=False, ot_show_volume=False, _uploaded_raw_bytes=None, sym_export_bytes=None)
                        with st.spinner("Auto-discovering product profile…"):
                            _auto_discover_product(nd)

                        st.rerun()
                    except requests.HTTPError as exc:
                        st.error(f"HTTP error: {exc}")
                    except ReviewDownloaderError as exc:
                        st.error(str(exc))
                    except Exception as exc:
                        st.error(str(exc))
        else:
            uploader_key = f"workspace_files_{int(st.session_state.get('workspace_file_uploader_nonce', 0))}"
            uploaded_files = st.file_uploader("Upload review export files", type=["csv", "xlsx", "xls"], accept_multiple_files=True, help="Supports Axion-style exports and similar CSV/XLSX review files.", key=uploader_key)
            include_local_symptomization = st.checkbox(
                "Include local symptomization if detected",
                value=bool(st.session_state.get("workspace_include_local_symptomization", True)),
                key="workspace_include_local_symptomization",
                help="Preserves populated Symptom 1–20 / AI Symptom columns from uploaded files so the Dashboard, Review Explorer, filters, and Symptomizer can use them immediately.",
            )
            st.caption("Mapped columns include Event Id, Base SKU, Review Text, Rating, Opened date, Seeded Flag, and Retailer when available. Turn on local symptomization to keep existing Symptom 1–20 / AI Symptom tags and infer the symptom catalog automatically.")
            if st.button("Build review workspace from file", type="primary", key="ws_build_file", use_container_width=True):
                try:
                    nd = _load_uploaded_files_dispatch(uploaded_files or [], include_local_symptomization=include_local_symptomization)
                    _reset_review_filters()
                    raw_bytes = None
                    if uploaded_files and len(uploaded_files) == 1:
                        fname = getattr(uploaded_files[0], "name", "")
                        if fname.lower().endswith((".xlsx", ".xlsm")):
                            raw_bytes = uploaded_files[0].getvalue()
                    local_delighters, local_detractors = ([], [])
                    if include_local_symptomization:
                        local_delighters, local_detractors = _local_symptom_catalog(nd["reviews_df"])
                    has_local_catalog = bool(local_delighters or local_detractors)
                    st.session_state.update(
                        analysis_dataset=nd,
                        chat_messages=[],
                        master_export_bundle=None,
                        prompt_run_artifacts=None,
                        sym_processed_rows=[],
                        sym_new_candidates={},
                        sym_symptoms_source=("local upload" if has_local_catalog else "none"),
                        sym_delighters=(local_delighters if has_local_catalog else []),
                        sym_detractors=(local_detractors if has_local_catalog else []),
                        sym_aliases=_alias_map_for_catalog((local_delighters if has_local_catalog else []), (local_detractors if has_local_catalog else [])),
                        sym_taxonomy_preview_items=[],
                        sym_taxonomy_category="general",
                        sym_qa_baseline_map={},
                        sym_qa_accuracy={},
                        sym_qa_row_ids=[],
                        sym_qa_selected_row=None,
                        sym_qa_notice=None,
                        sym_product_profile_ai_note="",
                        sym_product_knowledge={},
                        workspace_active_tab=TAB_DASHBOARD,
                        workspace_tab_request=None,
                        ai_include_references=False,
                        ot_show_volume=False,
                        _uploaded_raw_bytes=raw_bytes,
                        sym_export_bytes=None,
                    )
                    st.rerun()
                except ReviewDownloaderError as exc:
                    st.error(str(exc))
                except Exception as exc:
                    st.exception(exc)

    dataset = st.session_state.get("analysis_dataset")
    settings = _render_sidebar(dataset["reviews_df"] if dataset else None)
    if not dataset:
        active_tab = st.session_state.get("workspace_active_tab", TAB_DASHBOARD)
        if active_tab == TAB_SOCIAL_LISTENING:
            st.markdown("""<div class='soft-panel' style='margin-top:.55rem;'>
              <b>Social-only beta mode</b> · No uploaded review file is required here. The mocked FlexStyle experience below is designed to preview the future Meltwater-powered social listening workflow before Bazaarvoice / PowerReviews / uploads are brought in.
            </div>""", unsafe_allow_html=True)
            _render_social_listening_tab()
            return
        st.markdown("""<div style="margin-top:1.1rem;padding:2rem;background:var(--surface,#fff);border:1px solid #dde1e8;border-radius:18px;text-align:center;box-shadow:0 1px 4px rgba(15,23,42,.08);">
          <div style="font-size:2.5rem;margin-bottom:.75rem;">📊</div>
          <div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:.4rem;">No workspace loaded</div>
          <div style="font-size:13px;color:#64748b;max-width:720px;margin:0 auto;line-height:1.55;">Build a workspace above to unlock the Dashboard, Review Explorer, AI Analyst, Review Prompt, and Symptomizer. Or skip reviews entirely and open <b>Social Listening Beta</b> from the sidebar to explore the mocked FlexStyle + Meltwater experience.</div>
        </div>""", unsafe_allow_html=True)
        if st.button("📣 Open Social Listening Beta", type="primary", key="empty_state_open_social"):
            st.session_state["workspace_active_tab"] = TAB_SOCIAL_LISTENING
            st.rerun()
        return

    summary = dataset["summary"]
    overall_df = dataset["reviews_df"]
    source_type = dataset.get("source_type", "bazaarvoice")
    source_label = dataset.get("source_label", "")
    filter_state = settings["review_filters"]
    filtered_df = filter_state["filtered_df"]
    filter_description = filter_state["description"]
    new_filter_sig = json.dumps(filter_state["active_items"], default=str)
    if st.session_state.get("review_filter_signature") != new_filter_sig:
        st.session_state["review_filter_signature"] = new_filter_sig
        st.session_state["review_explorer_page"] = 1

    _render_workspace_header(summary, overall_df, filtered_df, st.session_state.get("prompt_run_artifacts"), source_type=source_type, source_label=source_label, filter_description=filter_description, active_items=filter_state["active_items"])
    _render_top_metrics(overall_df, filtered_df)
    _render_active_filter_summary(filter_state, overall_df)

    pending_tab = st.session_state.pop("workspace_tab_request", None)
    if pending_tab in WORKSPACE_TABS:
        st.session_state["workspace_active_tab"] = pending_tab
    elif st.session_state.get("workspace_active_tab") not in WORKSPACE_TABS:
        st.session_state["workspace_active_tab"] = TAB_DASHBOARD
    active_tab = _render_workspace_nav()
    common = dict(settings=settings, overall_df=overall_df, filtered_df=filtered_df, summary=summary, filter_description=filter_description)
    if active_tab == TAB_DASHBOARD:
        _render_dashboard(filtered_df, overall_df)
    elif active_tab == TAB_REVIEW_EXPLORER:
        _render_review_explorer(summary=summary, overall_df=overall_df, filtered_df=filtered_df, prompt_artifacts=st.session_state.get("prompt_run_artifacts"), filter_description=filter_description, active_items=filter_state["active_items"])
    elif active_tab == TAB_AI_ANALYST:
        _render_ai_tab(**common)
    # Review Prompt now rendered inside Configure tab
    elif active_tab == TAB_SYMPTOMIZER:
        # Configure tab includes Symptomizer + Review Prompt + Export
        st.markdown("<div style='margin-bottom:8px;'></div>", unsafe_allow_html=True)
        config_sub = st.radio("Configure section", ["🔬 Symptomizer", "📝 Review Prompt", "📥 Export"], horizontal=True, key="_configure_section", label_visibility="collapsed")
        config_sub = config_sub.split(" ", 1)[1] if " " in config_sub else config_sub  # Strip emoji for comparison
        if config_sub in ("Symptomizer", "🔬 Symptomizer"):
            _render_symptomizer_tab(**common)
        elif config_sub in ("Review Prompt", "📝 Review Prompt"):
            _render_review_prompt_tab(**common)
        else:
            # Export section
            st.markdown("### Export")
            st.caption("Download your review data with symptom tags, prompt classifications, and analysis results.")
            if dataset:
                reviews_df = dataset.get("reviews_df", pd.DataFrame())
                summary = dataset.get("summary")
                if not reviews_df.empty and summary:
                    try:
                        bundle = _safe_get_master_bundle(summary, reviews_df, st.session_state.get("prompt_run_artifacts"))
                        if bundle:
                            st.download_button("📥 Download full Excel export", data=bundle,
                                file_name=f"starwalk_export_{_safe_text(summary.product_id)[:30]}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True, type="primary")
                    except Exception as exc:
                        st.error(f"Export failed: {exc}")
                else:
                    st.info("Load reviews first to enable export.")


if __name__ == "__main__":
    main()

