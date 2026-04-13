"""Workspace management: auto-discovery, state reset, taxonomy memory.

Extracted from app.py for cleaner organization.
"""
from __future__ import annotations
import json
import os
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st
import sys

# ── Constants ──
DEFAULT_PRODUCT_URL = "https://www.sharkninja.com/ninja-air-fryer-pro-xl-6-in-1/AF181.html"
SOURCE_MODE_URL = "Product / review URL"
TAB_DASHBOARD = "📊  Insights"

def _app():
    """Lazy access to app module namespace (avoids circular import)."""
    return sys.modules.get('__main__', sys.modules.get('app'))


def _auto_discover_product(dataset, *, max_sample=20):
    """Phase 1: Auto-discover product description + knowledge from reviews.
    Runs automatically after workspace build — no user action needed."""
    client = _app()._get_client()
    if not client:
        return
    reviews_df = dataset.get("reviews_df", pd.DataFrame())
    if reviews_df.empty:
        return
    try:
        # Sample reviews stratified by rating
        sample = _app()._sample_reviews_for_symptomizer(reviews_df, max_sample)
        if not sample:
            return
        # Auto-generate product description
        existing_desc = st.session_state.get("sym_product_profile", "")
        if not existing_desc:
            result = _app()._ai_generate_product_description(
                client=client, sample_reviews=sample, existing_description=""
            )
            if result and result.get("description"):
                st.session_state["sym_product_profile"] = result["description"]
                st.session_state["sym_product_profile_ai_note"] = result.get("confidence_note", "Auto-generated on workspace build.")
                _log.info("Auto-discovered product description: %s", result["description"][:80])
        # Auto-generate product knowledge
        desc = st.session_state.get("sym_product_profile", "")
        if desc and not st.session_state.get("sym_product_knowledge"):
            category_info = _app()._infer_taxonomy_category(desc, sample[:12])
            category = category_info.get("category", "general")
            st.session_state["sym_taxonomy_category"] = category
            # Build knowledge from the description + sample
            knowledge = {
                "product_archetype": _app()._infer_generic_archetype(desc, {}),
                "confidence_note": "Auto-generated from review sample on workspace build.",
            }
            # Use the AI to fill in structured knowledge
            try:
                knowledge_result = _app()._ai_generate_product_description(
                    client=client, sample_reviews=sample, existing_description=desc
                )
                if knowledge_result:
                    for key in ["product_areas", "desired_outcomes", "likely_failure_modes",
                                "workflow_steps", "use_cases", "comparison_set", "user_contexts",
                                "csat_drivers", "watchouts", "likely_delighter_themes", "likely_detractor_themes"]:
                        if knowledge_result.get(key):
                            knowledge[key] = knowledge_result[key]
            except Exception:
                pass
            st.session_state["sym_product_knowledge"] = _app()._normalize_product_knowledge(knowledge)
            _log.info("Auto-discovered product knowledge for category: %s", category)
    except Exception as exc:
        _log.warning("Auto-discovery failed: %s", exc)



def _reset_workspace_state(*, reset_source=True):
    st.session_state["analysis_dataset"] = None
    st.session_state["chat_messages"] = []
    st.session_state["chat_scope_signature"] = None
    st.session_state["chat_scope_notice"] = None
    st.session_state["master_export_bundle"] = None
    st.session_state["prompt_run_artifacts"] = None
    st.session_state["prompt_run_notice"] = None
    st.session_state["review_explorer_page"] = 1
    st.session_state["workspace_active_tab"] = TAB_DASHBOARD
    st.session_state["workspace_tab_request"] = None
    st.session_state["ai_scroll_to_top"] = False
    st.session_state["sym_processed_rows"] = []
    st.session_state["sym_new_candidates"] = {}
    st.session_state["sym_product_profile"] = ""
    st.session_state["sym_product_knowledge"] = {}
    st.session_state["sym_pdesc"] = ""
    st.session_state["sym_include_universal_neutral"] = True
    st.session_state["sym_run_notice"] = None
    st.session_state["sym_review_log_limit"] = 50
    st.session_state["sym_symptoms_source"] = "none"
    st.session_state["sym_delighters"] = []
    st.session_state["sym_detractors"] = []
    st.session_state["sym_aliases"] = {}
    st.session_state["sym_taxonomy_preview_items"] = []
    st.session_state["sym_taxonomy_category"] = "general"
    st.session_state["sym_qa_baseline_map"] = {}
    st.session_state["sym_qa_accuracy"] = {}
    st.session_state["sym_qa_row_ids"] = []
    st.session_state["sym_qa_selected_row"] = None
    st.session_state["sym_qa_notice"] = None
    st.session_state["sym_product_profile_ai_note"] = ""
    st.session_state.pop("_sym_pdesc_pending", None)
    st.session_state.pop("_sym_inline_defaults_pending", None)
    st.session_state["_uploaded_raw_bytes"] = None
    st.session_state["sym_export_bytes"] = None
    st.session_state["_prompt_bundle_ready"] = False
    st.session_state["ai_include_references"] = False
    st.session_state["ot_show_volume"] = False
    st.session_state.pop("sym_ai_build_result", None)
    _app()._reset_review_filters()
    if reset_source:
        st.session_state["workspace_source_mode"] = SOURCE_MODE_URL
        st.session_state["workspace_product_url"] = DEFAULT_PRODUCT_URL
        st.session_state["workspace_product_urls_bulk"] = ""
        st.session_state["workspace_file_uploader_nonce"] = int(st.session_state.get("workspace_file_uploader_nonce", 0)) + 1
