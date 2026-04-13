"""Filter system: sidebar rendering, filter state management,
live review filtering, and session state initialization.

Extracted from app.py for cleaner organization.
"""
from __future__ import annotations
import time
import re
from typing import Any, Dict, List, Optional
import pandas as pd
import streamlit as st
import sys
def _app():
    return sys.modules.get('__main__', sys.modules.get('app'))

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

# ── Constants ──
DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_PRODUCT_URL = "https://www.sharkninja.com/ninja-air-fryer-pro-xl-6-in-1/AF181.html"
DEFAULT_REASONING = "none"
MODEL_OPTIONS = ['gpt-5.4-mini', 'gpt-5.4', 'gpt-5.4-pro', 'gpt-5.4-nano', 'gpt-5-chat-latest', 'gpt-5-mini', 'gpt-5', 'gpt-5-nano', 'gpt-4o-mini', 'gpt-4o', 'gpt-4.1']
SOURCE_MODE_URL = "Product / review URL"
TAB_DASHBOARD = "📊  Insights"
TAB_SOCIAL_LISTENING = "📣  Social Listening Beta"

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _safe_bool(value, default=False):
    if value is None: return default
    if isinstance(value, bool): return value
    return str(value).strip().lower() in ("true", "1", "yes")


def _init_state():
    defaults = dict(
        analysis_dataset=None,
        chat_messages=[],
        master_export_bundle=None,
        prompt_definitions_df=_app()._default_prompt_df(),
        prompt_builder_suggestion=None,
        prompt_run_artifacts=None,
        prompt_run_notice=None,
        chat_scope_signature=None,
        chat_scope_notice=None,
        review_explorer_page=1,
        review_explorer_per_page=20,
        review_explorer_sort="Newest",  # Options: Newest, Oldest, Lowest rated, Highest rated, Longest, Most tagged, Least tagged
        review_filter_signature=None,
        shared_model=DEFAULT_MODEL,
        shared_reasoning=DEFAULT_REASONING,
        ai_response_preset="Large (1200 words)",
        ai_response_words=1200,
        ai_include_references=False,
        ot_show_volume=False,
        workspace_source_mode=SOURCE_MODE_URL,
        workspace_product_url=DEFAULT_PRODUCT_URL,
        workspace_product_urls_bulk="",
        workspace_file_uploader_nonce=0,
        workspace_include_local_symptomization=True,
        workspace_active_tab=TAB_DASHBOARD,
        workspace_tab_request=None,
        ai_scroll_to_top=False,
        sym_delighters=[],
        sym_detractors=[],
        sym_custom_universal_delighters=[],
        sym_custom_universal_detractors=[],
        sym_aliases={},
        sym_taxonomy_preview_items=[],
        sym_taxonomy_category="general",
        sym_symptoms_source="none",
        sym_processed_rows=[],
        sym_new_candidates={},
        sym_product_profile="",
        sym_product_knowledge={},
        sym_include_universal_neutral=True,
        sym_scope_choice="Missing both",
        sym_n_to_process=10,
        sym_batch_size=5,
        sym_max_ev_chars=120,
        sym_review_log_limit=50,
        sym_run_notice=None,
        sym_qa_baseline_map={},
        sym_qa_accuracy={},
        sym_qa_row_ids=[],
        sym_qa_selected_row=None,
        sym_qa_notice=None,
        sym_product_profile_ai_note="",
        sym_calibration_result=None,
        sym_staged_pipeline=False,
        sym_run_just_completed=False,
        sym_last_run_stats=None,
        sym_validation_sample=None,
        sym_validation_votes={},
        sidebar_manual_api_key="",
        workspace_name="",
        workspace_id=None,
        _ws_show_rename=False,
        _sym_inline_defaults_pending={},
        _prompt_defs_cache={},
        _prompt_bundle_ready=False,
    )
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)



def _render_sidebar(df: Optional[pd.DataFrame]):
    api_key = _app()._get_api_key()
    filter_state = {"filtered_df": df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(), "active_items": [], "filter_seconds": 0.0, "description": "No active filters"}
    with st.sidebar:
        current_tab = st.session_state.get("workspace_active_tab", TAB_DASHBOARD)
        st.markdown("### 🔍 Review Filters")
        st.caption("Applies live to every workspace tab.")
        if st.button("🧹 Clear all filters", use_container_width=True, key="rf_clear_btn"):
            _reset_review_filters()
            st.rerun()
        if df is None:
            st.info("Build a workspace to unlock filters.")
        else:
            core_specs = _app()._core_filter_specs_for_df(df)
            det_opts, del_opts, _, _ = _app()._symptom_filter_options(df)
            st.markdown(
                f"<div class='sidebar-scope-card'><div class='sidebar-scope-title'>Loaded workspace</div><div class='sidebar-scope-value'>{len(df):,} reviews · {len(core_specs):,} core filters{' · symptoms ready' if (det_opts or del_opts) else ''}</div></div>",
                unsafe_allow_html=True,
            )
            with st.expander("🗓️ Timeframe", expanded=False):
                tf_opts = ["All Time", "Last Week", "Last Month", "Last Year", "Custom Range"]
                if st.session_state.get("rf_tf") not in tf_opts:
                    st.session_state["rf_tf"] = "All Time"
                st.selectbox("Select timeframe", options=tf_opts, key="rf_tf")
                if st.session_state.get("rf_tf") == "Custom Range":
                    today = date.today()
                    rng = st.session_state.get("rf_tf_range", (today - timedelta(days=30), today))
                    if not (isinstance(rng, (tuple, list)) and len(rng) == 2):
                        rng = (today - timedelta(days=30), today)
                    st.session_state["rf_tf_range"] = tuple(rng)
                    st.date_input("Start / end", value=st.session_state["rf_tf_range"], key="rf_tf_range")
            with st.expander("⭐ Star rating", expanded=False):
                sr_opts = ["All", 5, 4, 3, 2, 1]
                cur = st.session_state.get("rf_sr", ["All"])
                if not isinstance(cur, list):
                    cur = [cur]
                cur = [v for v in cur if v in sr_opts]
                if not cur:
                    cur = ["All"]
                if "All" in cur and len(cur) > 1:
                    cur = [v for v in cur if v != "All"]
                st.session_state["rf_sr"] = cur
                st.multiselect("Select stars", options=sr_opts, default=st.session_state["rf_sr"], key="rf_sr")
            with st.expander("🧭 Review filters", expanded=False):
                st.caption("")
                for spec in core_specs:
                    key = f"rf_{spec['key']}"
                    _app()._sanitize_multiselect(key, spec["options"], ["ALL"])
                    st.multiselect(spec["label"], options=spec["options"], default=st.session_state[key], key=key)
            if det_opts or del_opts:
                with st.expander("🩺 Symptom filters", expanded=False):
                    st.caption("Only shown when symptom tags are present in the workspace.")
                    if det_opts:
                        det_all = ["All"] + det_opts
                        _app()._sanitize_multiselect_sym("rf_sym_detract", det_all, ["All"])
                        st.multiselect("Detractors", options=det_all, default=st.session_state["rf_sym_detract"], key="rf_sym_detract")
                    if del_opts:
                        del_all = ["All"] + del_opts
                        _app()._sanitize_multiselect_sym("rf_sym_delight", del_all, ["All"])
                        st.multiselect("Delighters", options=del_all, default=st.session_state["rf_sym_delight"], key="rf_sym_delight")
            with st.expander("🔎 Keyword", expanded=False):
                st.text_input("Search in title + review text", value=st.session_state.get("rf_kw", ""), key="rf_kw", placeholder="e.g. loud noise, filter cleaning, cord length")
            extra_candidates = _app()._extra_filter_candidates(df)
            current_extra = [c for c in (st.session_state.get("rf_extra_filter_cols", []) or []) if c in extra_candidates]
            st.session_state["rf_extra_filter_cols"] = current_extra
            with st.expander("➕ Add Filters", expanded=False):
                st.caption("Choose additional columns to surface as filters.")
                st.multiselect("Available columns", options=extra_candidates, default=current_extra, key="rf_extra_filter_cols")
            extra_cols = st.session_state.get("rf_extra_filter_cols", []) or []
            if extra_cols:
                with st.expander("🧩 Extra filters", expanded=False):
                    for col in extra_cols:
                        if col not in df.columns:
                            continue
                        kind = _app()._infer_extra_filter_kind(df, col)
                        s = df[col]
                        if kind == "numeric":
                            num = pd.to_numeric(s, errors="coerce").dropna()
                            if num.empty:
                                continue
                            lo, hi = float(num.min()), float(num.max())
                            if lo == hi:
                                st.caption(f"{col}: {lo:g} (constant)")
                                continue
                            key = f"rf_{col}_range"
                            default = st.session_state.get(key, (lo, hi))
                            if not (isinstance(default, (tuple, list)) and len(default) == 2):
                                default = (lo, hi)
                            st.session_state[key] = (float(default[0]), float(default[1]))
                            st.slider(col, min_value=lo, max_value=hi, value=st.session_state[key], key=key)
                        elif kind == "date":
                            dt = pd.to_datetime(s, errors="coerce").dropna()
                            if dt.empty:
                                continue
                            lo, hi = dt.min().date(), dt.max().date()
                            key = f"rf_{col}_date_range"
                            default = st.session_state.get(key, (lo, hi))
                            if not (isinstance(default, (tuple, list)) and len(default) == 2):
                                default = (lo, hi)
                            st.session_state[key] = tuple(default)
                            st.date_input(col, value=st.session_state[key], min_value=lo, max_value=hi, key=key)
                        else:
                            try:
                                nunique = int(s.astype("string").replace({"": pd.NA}).nunique(dropna=True))
                            except Exception:
                                nunique = 0
                            if nunique > 600:
                                st.text_input(f"{col} contains", value=str(st.session_state.get(f"rf_{col}_contains") or ""), key=f"rf_{col}_contains", help="High-cardinality column — using a contains filter for speed.")
                            else:
                                opts = _app()._col_options(df, col, max_vals=None)
                                _app()._sanitize_multiselect(f"rf_{col}", opts, ["ALL"])
                                st.multiselect(col, options=opts, default=st.session_state[f"rf_{col}"], key=f"rf_{col}")
            filter_state = _apply_live_review_filters(df)
        with st.expander("🤖 AI Model & Symptomizer", expanded=False):
            st.caption("Use higher reasoning for ELT-ready analysis and more nuanced summaries.")
            cur_model = st.session_state.get("shared_model", DEFAULT_MODEL)
            if cur_model not in MODEL_OPTIONS:
                cur_model = DEFAULT_MODEL
                st.session_state["shared_model"] = cur_model
            st.selectbox("Model", options=MODEL_OPTIONS, index=MODEL_OPTIONS.index(cur_model), key="shared_model", help="Used by AI Analyst, Review Prompt, and Symptomizer.")
            effort_options = _app()._reasoning_options_for_model(st.session_state.get("shared_model", DEFAULT_MODEL))
            cur_reasoning = _safe_text(st.session_state.get("shared_reasoning", DEFAULT_REASONING)).lower() or DEFAULT_REASONING
            if cur_reasoning not in effort_options:
                cur_reasoning = "none" if "none" in effort_options else effort_options[0]
                st.session_state["shared_reasoning"] = cur_reasoning
            st.selectbox("Reasoning effort", options=effort_options, index=effort_options.index(cur_reasoning), key="shared_reasoning", help="Applied to GPT-5 family models. Raising this usually improves quality on nuanced and long-form analysis, but may be a bit slower.")
            key_source = _app()._api_key_source()
            if key_source in ("secrets", "env"):
                st.markdown("<div class='helper-chip-row'><span class='helper-chip' style='background:rgba(5,150,105,.10);color:#059669;border-color:rgba(5,150,105,.25);'>✅ Key loaded</span><span class='helper-chip'>Higher reasoning = higher quality</span></div>", unsafe_allow_html=True)
            elif key_source == "manual":
                st.markdown("<div class='helper-chip-row'><span class='helper-chip' style='background:rgba(217,119,6,.10);color:#d97706;border-color:rgba(217,119,6,.25);'>🔑 Using manual key</span></div>", unsafe_allow_html=True)
            else:
                st.warning("No API key detected. Paste one below or set OPENAI_API_KEY in secrets.")
            if key_source in ("missing", "manual"):
                st.text_input("OpenAI API key", type="password", placeholder="sk-proj-...", key="sidebar_manual_api_key", help="Paste your OpenAI API key. Only stored in your browser session.")
                api_key = _app()._get_api_key()
            st.markdown("<div style='height:.25rem'></div>", unsafe_allow_html=True)
            st.slider("Symptomizer batch size", 1, 12, key="sym_batch_size")
            st.slider("Symptomizer max evidence chars", 60, 200, step=10, key="sym_max_ev_chars")
            if _HAS_SYMPTOMIZER_V3:
                st.toggle("Enable staged pipeline", value=bool(st.session_state.get("sym_staged_pipeline", False)), key="sym_staged_pipeline", help="Three-stage pipeline: Extract claims → Map to taxonomy → Verify. Higher accuracy but ~2x API calls.")
        st.divider()
        st.markdown("""<div class='sidebar-scope-card' style='background:linear-gradient(145deg,rgba(255,247,237,.96),rgba(238,242,255,.98));border-color:rgba(249,115,22,.24);box-shadow:0 10px 26px rgba(15,23,42,.08);'>
          <div class='sidebar-scope-title'>Beta feature</div>
          <div class='sidebar-scope-value'>Open the Social Listening demo route when you want to preview the mocked Meltwater-style workflow.</div>
        </div>""", unsafe_allow_html=True)
        social_btn_kwargs = {"use_container_width": True, "key": "sidebar_open_social_beta"}
        if current_tab == TAB_SOCIAL_LISTENING:
            social_btn_kwargs["type"] = "primary"
        if st.button("📣 Open Social Listening Beta", **social_btn_kwargs):
            st.session_state["workspace_active_tab"] = TAB_SOCIAL_LISTENING
            st.session_state["workspace_tab_request"] = TAB_SOCIAL_LISTENING
            st.rerun()
    return {"api_key": api_key, "review_filters": filter_state}

# ═══════════════════════════════════════════════════════════════════════════════
#  RENDER HELPERS
# ═══════════════════════════════════════════════════════════════════════════════



def _apply_live_review_filters(df_base: pd.DataFrame) -> Dict[str, Any]:
    t0 = time.perf_counter()
    if df_base is None or df_base.empty:
        return {"filtered_df": df_base.copy() if isinstance(df_base, pd.DataFrame) else pd.DataFrame(), "active_items": [], "filter_seconds": 0.0, "description": "No active filters"}
    d0 = df_base
    mask = pd.Series(True, index=d0.index)
    today = date.today()
    tf = st.session_state.get("rf_tf", "All Time")
    start_date = end_date = None
    if tf == "Custom Range":
        rng = st.session_state.get("rf_tf_range", (today - timedelta(days=30), today))
        if isinstance(rng, (tuple, list)) and len(rng) == 2:
            start_date, end_date = rng
    elif tf == "Last Week":
        start_date, end_date = today - timedelta(days=7), today
    elif tf == "Last Month":
        start_date, end_date = today - timedelta(days=30), today
    elif tf == "Last Year":
        start_date, end_date = today - timedelta(days=365), today

    date_col = "submission_date" if "submission_date" in d0.columns else ("submission_time" if "submission_time" in d0.columns else None)
    if start_date and end_date and date_col:
        dt = pd.to_datetime(d0[date_col], errors="coerce")
        end_inclusive = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        mask &= (dt >= pd.Timestamp(start_date)) & (dt <= end_inclusive)

    sr_raw = st.session_state.get("rf_sr", ["All"])
    sr_list = sr_raw if isinstance(sr_raw, list) else [sr_raw]
    sr_sel = [x for x in sr_list if str(x).strip() and str(x).lower() != "all"]
    if sr_sel and "rating" in d0.columns:
        sr_nums = [int(x) for x in sr_sel if str(x).isdigit()]
        if sr_nums:
            mask &= pd.to_numeric(d0["rating"], errors="coerce").isin(sr_nums)

    core_specs = _app()._core_filter_specs_for_df(d0)
    for spec in core_specs:
        sel = st.session_state.get(f"rf_{spec['key']}", ["ALL"])
        sel_list = sel if isinstance(sel, list) else [sel]
        sel_clean = [x for x in sel_list if str(x).strip() and str(x).upper() != "ALL"]
        if sel_clean:
            mask &= _app()._series_matches_any(_app()._filter_series_for_spec(d0, spec), [str(x) for x in sel_clean])

    extra_cols = [c for c in (st.session_state.get("rf_extra_filter_cols", []) or []) if c in d0.columns]
    for col in extra_cols:
        kind = _app()._infer_extra_filter_kind(d0, col)
        s = d0[col]
        if kind == "numeric":
            rk = f"rf_{col}_range"
            if rk in st.session_state and isinstance(st.session_state.get(rk), (tuple, list)) and len(st.session_state.get(rk)) == 2:
                lo, hi = st.session_state[rk]
                mask &= pd.to_numeric(s, errors="coerce").between(float(lo), float(hi), inclusive="both")
        elif kind == "date":
            dk = f"rf_{col}_date_range"
            if dk in st.session_state and isinstance(st.session_state.get(dk), (tuple, list)) and len(st.session_state.get(dk)) == 2:
                lo, hi = st.session_state[dk]
                if lo and hi:
                    dt = pd.to_datetime(s, errors="coerce")
                    hi_end = pd.Timestamp(hi) + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
                    mask &= (dt >= pd.Timestamp(lo)) & (dt <= hi_end)
        else:
            ck = f"rf_{col}_contains"
            cv = _safe_text(st.session_state.get(ck)).strip()
            if cv:
                mask &= s.astype("string").fillna("").str.contains(cv, case=False, na=False, regex=False)
            else:
                sel = st.session_state.get(f"rf_{col}", ["ALL"])
                sel_list = sel if isinstance(sel, list) else [sel]
                sel_clean = [x for x in sel_list if str(x).strip() and str(x).upper() != "ALL"]
                if sel_clean:
                    ss = s.astype("string").fillna("")
                    sample = ss.head(200).astype(str)
                    if bool(sample.str.contains(r"\s\|\s", regex=True).any()):
                        toks = [str(x).strip() for x in sel_clean if str(x).strip()]
                        pattern = r"(^|\s*\|\s*)(" + "|".join(re.escape(t) for t in toks) + r")(\s*\|\s*|$)"
                        mask &= ss.str.contains(pattern, case=False, regex=True, na=False)
                    else:
                        mask &= ss.isin([str(x) for x in sel_clean])

    _, _, det_cols, del_cols = _app()._symptom_filter_options(d0)
    sel_det = [str(x).strip().title() for x in (st.session_state.get("rf_sym_detract", ["All"]) or []) if str(x).strip() and str(x).lower() != "all"]
    sel_del = [str(x).strip().title() for x in (st.session_state.get("rf_sym_delight", ["All"]) or []) if str(x).strip() and str(x).lower() != "all"]
    if sel_det and det_cols:
        det_block = d0[det_cols].astype("string").fillna("").apply(lambda col: col.str.strip().str.title())
        mask &= det_block.isin(sel_det).any(axis=1)
    if sel_del and del_cols:
        del_block = d0[del_cols].astype("string").fillna("").apply(lambda col: col.str.strip().str.title())
        mask &= del_block.isin(sel_del).any(axis=1)

    kw = _safe_text(st.session_state.get("rf_kw")).strip()
    search_col = "title_and_text" if "title_and_text" in d0.columns else ("review_text" if "review_text" in d0.columns else None)
    if kw and search_col:
        mask &= d0[search_col].astype("string").fillna("").str.contains(kw, case=False, na=False, regex=False)

    filtered = d0.loc[mask].copy()
    active_items = _collect_active_filter_items(d0, core_specs=core_specs, extra_cols=extra_cols, tf=tf, start_date=start_date, end_date=end_date)
    return {"filtered_df": filtered, "active_items": active_items, "filter_seconds": time.perf_counter() - t0, "description": _app()._filter_description_from_items(active_items)}



def _collect_active_filter_items(df: pd.DataFrame, *, core_specs: Sequence[Dict[str, Any]], extra_cols: Sequence[str], tf: str, start_date, end_date) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if tf != "All Time":
        if tf == "Custom Range" and start_date and end_date:
            items.append(("Timeframe", f"{start_date} → {end_date}"))
        else:
            items.append(("Timeframe", tf))

    sr_raw = st.session_state.get("rf_sr", ["All"])
    sr_list = sr_raw if isinstance(sr_raw, list) else [sr_raw]
    sr_sel = [x for x in sr_list if str(x).strip() and str(x).lower() != "all"]
    if sr_sel:
        items.append(("Stars", ", ".join(str(x) for x in sr_sel)))

    for spec in core_specs:
        sel = st.session_state.get(f"rf_{spec['key']}", ["ALL"])
        sel_list = sel if isinstance(sel, list) else [sel]
        sel_clean = [x for x in sel_list if str(x).strip() and str(x).upper() != "ALL"]
        if sel_clean:
            items.append((spec["label"], ", ".join(str(x) for x in sel_clean[:4]) + ("" if len(sel_clean) <= 4 else f" +{len(sel_clean) - 4}")))

    for col in extra_cols:
        if col not in df.columns:
            continue
        kind = _app()._infer_extra_filter_kind(df, col)
        rk = f"rf_{col}_range"
        dk = f"rf_{col}_date_range"
        ck = f"rf_{col}_contains"
        if kind == "numeric":
            num = pd.to_numeric(df[col], errors="coerce").dropna()
            if not num.empty and rk in st.session_state and isinstance(st.session_state.get(rk), (tuple, list)) and len(st.session_state.get(rk)) == 2:
                lo, hi = st.session_state[rk]
                base_lo, base_hi = float(num.min()), float(num.max())
                if round(float(lo), 10) != round(base_lo, 10) or round(float(hi), 10) != round(base_hi, 10):
                    items.append((col, f"{float(lo):g} → {float(hi):g}"))
            continue
        if kind == "date":
            dt = pd.to_datetime(df[col], errors="coerce").dropna()
            if not dt.empty and dk in st.session_state and isinstance(st.session_state.get(dk), (tuple, list)) and len(st.session_state.get(dk)) == 2:
                lo, hi = st.session_state[dk]
                base_lo, base_hi = dt.min().date(), dt.max().date()
                if lo and hi and (lo != base_lo or hi != base_hi):
                    items.append((col, f"{lo} → {hi}"))
            continue
        cv = _safe_text(st.session_state.get(ck))
        if cv:
            items.append((col, f"contains: {cv}"))
            continue
        sel = st.session_state.get(f"rf_{col}", ["ALL"])
        sel_list = sel if isinstance(sel, list) else [sel]
        sel_clean = [x for x in sel_list if str(x).strip() and str(x).upper() != "ALL"]
        if sel_clean:
            items.append((col, ", ".join(str(x) for x in sel_clean[:4]) + ("" if len(sel_clean) <= 4 else f" +{len(sel_clean) - 4}")))

    sel_det = [x for x in (st.session_state.get("rf_sym_detract", ["All"]) or []) if str(x).strip() and str(x).lower() != "all"]
    sel_del = [x for x in (st.session_state.get("rf_sym_delight", ["All"]) or []) if str(x).strip() and str(x).lower() != "all"]
    if sel_det:
        items.append(("Detractors", ", ".join(sel_det[:3]) + ("" if len(sel_det) <= 3 else f" +{len(sel_det) - 3}")))
    if sel_del:
        items.append(("Delighters", ", ".join(sel_del[:3]) + ("" if len(sel_del) <= 3 else f" +{len(sel_del) - 3}")))

    kw = _safe_text(st.session_state.get("rf_kw"))
    if kw:
        items.append(("Keyword", kw))
    return items



def _reset_review_filters():
    for key in list(st.session_state.keys()):
        if key.startswith("rf_"):
            st.session_state.pop(key, None)
    st.session_state["review_explorer_page"] = 1
    st.session_state["review_filter_signature"] = None


