import importlib
import sys
import types

import pandas as pd
import pytest


class _DummyCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, name):
        if name == "button":
            return lambda *a, **k: False
        if name in {"checkbox", "toggle"}:
            return lambda *a, **k: False
        if name in {"selectbox", "radio", "text_input", "text_area", "number_input", "slider", "multiselect"}:
            return lambda *a, **k: None
        if name == "tabs":
            return lambda labels, *a, **k: [_DummyCtx() for _ in labels]
        if name == "columns":
            return lambda spec, *a, **k: [_DummyCtx() for _ in (range(spec) if isinstance(spec, int) else spec)]
        if name in {"expander", "container", "sidebar", "empty", "spinner", "status"}:
            return lambda *a, **k: _DummyCtx()
        return lambda *a, **k: None


class _FakeStreamlit(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = {}
        self.secrets = {}
        self.sidebar = _DummyCtx()

    def cache_resource(self, *args, **kwargs):
        def deco(fn):
            return fn
        return deco

    def cache_data(self, *args, **kwargs):
        def deco(fn):
            return fn
        return deco

    def set_page_config(self, *args, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def __getattr__(self, name):
        if name == "sidebar":
            return self.sidebar
        if name == "column_config":
            return _DummyCtx()
        if name == "columns":
            return lambda spec, *a, **k: [_DummyCtx() for _ in (range(spec) if isinstance(spec, int) else spec)]
        if name == "tabs":
            return lambda labels, *a, **k: [_DummyCtx() for _ in labels]
        if name in {"expander", "container", "empty", "spinner", "status"}:
            return lambda *a, **k: _DummyCtx()
        return lambda *args, **kwargs: None


@pytest.fixture()
def app_module(monkeypatch):
    fake_streamlit = _FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)
    sys.modules.pop("app", None)
    app = importlib.import_module("app")
    fake_streamlit.session_state.clear()
    app._init_state()
    return app


def test_workspace_tabs_keep_social_listening_in_sidebar_only(app_module):
    app = app_module

    assert app.TAB_SOCIAL_LISTENING not in app.WORKSPACE_TABS
    assert app.WORKSPACE_NAV_META[app.TAB_SOCIAL_LISTENING]["hint"] == "Sidebar only"


def test_compute_metrics_direct_returns_organic_breakouts(app_module):
    app = app_module
    df = pd.DataFrame(
        {
            "review_id": ["1", "2", "3", "4"],
            "rating": [5, 2, None, 1],
            "incentivized_review": [False, True, False, False],
            "is_recommended": [True, False, pd.NA, True],
            "has_photos": [True, False, False, True],
        }
    )

    metrics = app._compute_metrics_direct(df)

    assert metrics["review_count"] == 4
    assert metrics["rated_count"] == 3
    assert metrics["organic_rated_count"] == 2
    assert metrics["incentivized_count"] == 1
    assert metrics["non_incentivized_count"] == 3
    assert metrics["low_star_count"] == 2


def test_sample_review_prompt_scope_honors_requested_sample_size(app_module, monkeypatch):
    app = app_module
    source_df = pd.DataFrame(
        {
            "review_id": ["1", "2", "3", "4"],
            "priority": [1, 4, 3, 2],
            "review_text": ["a", "b", "c", "d"],
        }
    )

    monkeypatch.setattr(app, "_prioritize_for_symptomization", lambda df: df.sort_values("priority", ascending=False).reset_index(drop=True))

    sampled = app._sample_review_prompt_scope(source_df, 2)

    assert list(sampled["review_id"]) == ["2", "3"]
    assert len(sampled) == 2


def test_workspace_source_change_confirmation_detects_swapped_sources(app_module):
    app = app_module
    dataset = {
        "source_type": "bazaarvoice",
        "source_label": "Original workspace",
        "summary": {"product_url": "https://example.com/original"},
        "reviews_df": pd.DataFrame({"review_id": ["1"], "review_text": ["Great"], "rating": [5]}),
    }
    app.st.session_state["analysis_dataset"] = dataset
    app._remember_loaded_workspace_source(
        dataset=dataset,
        source_mode=app.SOURCE_MODE_URL,
        source_signature="url::original",
        source_label="Original workspace",
    )

    assert app._workspace_source_change_needs_confirmation("url::original", source_mode=app.SOURCE_MODE_URL) is False
    assert app._workspace_source_change_needs_confirmation("url::new-source", source_mode=app.SOURCE_MODE_URL) is True
    assert app._workspace_source_change_needs_confirmation("file::same-id", source_mode=app.SOURCE_MODE_FILE) is True


def test_activate_workspace_dataset_replace_current_preserves_workspace_identity(app_module, monkeypatch):
    app = app_module
    dataset = {
        "source_type": "bazaarvoice",
        "source_label": "Replacement source",
        "summary": {"product_url": "https://example.com/replacement"},
        "reviews_df": pd.DataFrame({"review_id": ["1"], "review_text": ["Great"], "rating": [5]}),
    }
    app.st.session_state["workspace_name"] = "Current workspace"
    app.st.session_state["workspace_id"] = "ws_123"

    def _fake_apply(target_dataset, raw_bytes=None, symptom_seed=None):
        app.st.session_state["analysis_dataset"] = target_dataset
        app.st.session_state["workspace_name"] = ""
        app.st.session_state["workspace_id"] = None
        return {"has_taxonomy_seed": False}

    monkeypatch.setattr(app, "_apply_workspace_dataset", _fake_apply)

    result = app._activate_workspace_dataset(
        dataset,
        replace_current=True,
        source_mode=app.SOURCE_MODE_URL,
        source_signature="url::replacement",
        source_label="Replacement source",
    )

    assert result == {"has_taxonomy_seed": False}
    assert app.st.session_state["workspace_name"] == "Current workspace"
    assert app.st.session_state["workspace_id"] == "ws_123"
    assert app.st.session_state["analysis_dataset"] is dataset
    assert app.st.session_state["_workspace_loaded_source_signature"] == "url::replacement"
