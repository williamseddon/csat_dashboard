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


def test_apply_workspace_dataset_resets_stale_state_and_applies_seed(app_module):
    app = app_module
    app.st.session_state.update(
        workspace_name="Old workspace",
        workspace_id="ws_123",
        sym_product_profile="Old product",
        sym_product_knowledge={"product_areas": ["Blade"]},
        sym_pdesc="Old product",
        sym_custom_universal_delighters=["Comfortable"],
        sym_custom_universal_detractors=["Fragile"],
        sym_ai_build_result={"delighters": ["Legacy"]},
        _sym_taxonomy_auto_applied=True,
        sym_delighters=["Legacy Delighter"],
        sym_detractors=["Legacy Detractor"],
    )
    dataset = {
        "reviews_df": pd.DataFrame({"review_text": ["Easy to set up"], "rating": [5]}),
        "summary": {"product_id": "sku-1"},
        "source_label": "Uploaded workbook",
    }
    seed = {
        "delighters": ["Easy Setup"],
        "detractors": ["Battery Dies Fast"],
        "aliases": {"Battery Dies Fast": ["Dies Quickly"]},
        "source": "file",
    }

    result = app._apply_workspace_dataset(dataset, raw_bytes=b"xlsx-bytes", symptom_seed=seed)

    assert result == {"has_taxonomy_seed": True, "source": "file"}
    assert app.st.session_state["analysis_dataset"] is dataset
    assert app.st.session_state["_uploaded_raw_bytes"] == b"xlsx-bytes"
    assert app.st.session_state["workspace_name"] == ""
    assert app.st.session_state["workspace_id"] is None
    assert app.st.session_state["sym_product_profile"] == ""
    assert app.st.session_state["sym_product_knowledge"] == {}
    assert app.st.session_state["sym_custom_universal_delighters"] == []
    assert app.st.session_state["sym_custom_universal_detractors"] == []
    assert "sym_ai_build_result" not in app.st.session_state
    assert app.st.session_state["_sym_taxonomy_auto_applied"] is False
    assert app.st.session_state["sym_symptoms_source"] == "file"
    assert "Easy Setup" in app.st.session_state["sym_delighters"]
    assert "Battery Dies Fast" in app.st.session_state["sym_detractors"]
    assert "Dies Quickly" in app.st.session_state["sym_aliases"].get("Battery Dies Fast", [])


def test_auto_prepare_workspace_taxonomy_generates_and_activates_ai_taxonomy(app_module, monkeypatch):
    app = app_module
    dataset = {
        "reviews_df": pd.DataFrame(
            {
                "review_text": [
                    "Battery lasts forever and charges fast.",
                    "Sound is clear but the battery dies fast.",
                ],
                "rating": [5, 2],
            }
        ),
        "summary": {"product_id": "speaker-1"},
    }

    monkeypatch.setattr(app, "_get_client", lambda: object())
    monkeypatch.setattr(
        app,
        "_sample_reviews_for_symptomizer",
        lambda df, max_sample: ["Battery lasts forever", "Battery dies fast"],
    )
    monkeypatch.setattr(
        app,
        "_ai_generate_product_description",
        lambda **kwargs: {
            "description": "Cordless speaker focused on battery life and portability.",
            "confidence_note": "Auto-generated note.",
            "product_knowledge": {
                "product_areas": ["Battery"],
                "likely_failure_modes": ["Battery drains fast"],
            },
        },
    )
    monkeypatch.setattr(app, "_infer_taxonomy_category", lambda desc, reviews: {"category": "electronics"})
    monkeypatch.setattr(
        app,
        "_ai_build_symptom_list",
        lambda **kwargs: {
            "delighters": ["Long Battery Life"],
            "detractors": ["Battery Dies Fast"],
            "aliases": {"Battery Dies Fast": ["Drains Quickly"]},
            "category": "electronics",
            "taxonomy_note": "Auto taxonomy note.",
            "preview_delighters": [{"label": "Long Battery Life", "bucket": "Category Driver"}],
            "preview_detractors": [{"label": "Battery Dies Fast", "bucket": "Category Driver"}],
        },
    )

    status = app._auto_prepare_workspace_taxonomy(
        dataset,
        max_sample=20,
        preserve_existing_taxonomy=True,
        auto_activate=True,
        context_label="uploaded workspace build",
    )

    assert status["status"] == "ok"
    assert status["taxonomy_generated"] is True
    assert status["auto_applied"] is True
    assert app.st.session_state["sym_product_profile"] == "Cordless speaker focused on battery life and portability."
    assert app.st.session_state["sym_taxonomy_category"] == "electronics"
    assert app.st.session_state["sym_product_knowledge"]["product_areas"] == ["Battery"]
    assert "Long Battery Life" in app.st.session_state["sym_delighters"]
    assert "Battery Dies Fast" in app.st.session_state["sym_detractors"]
    assert app.st.session_state["sym_symptoms_source"] == "ai"
    assert app.st.session_state["_sym_taxonomy_auto_applied"] is True
    assert app.st.session_state["sym_wizard_step"] == 3
    assert app.st.session_state["sym_ai_build_result"]["category"] == "electronics"


def test_auto_prepare_workspace_taxonomy_preserves_uploaded_taxonomy_seed(app_module, monkeypatch):
    app = app_module
    dataset = {
        "reviews_df": pd.DataFrame(
            {
                "review_text": ["Easy to clean", "Hard to clean"],
                "rating": [5, 2],
            }
        ),
        "summary": {"product_id": "appliance-1"},
    }
    app._apply_workspace_dataset(
        dataset,
        raw_bytes=b"xlsx",
        symptom_seed={
            "delighters": ["Easy Cleanup"],
            "detractors": ["Hard To Clean"],
            "aliases": {},
            "source": "file",
        },
    )

    monkeypatch.setattr(app, "_get_client", lambda: object())
    monkeypatch.setattr(app, "_sample_reviews_for_symptomizer", lambda df, max_sample: ["Easy cleanup", "Hard to clean"])
    monkeypatch.setattr(
        app,
        "_ai_generate_product_description",
        lambda **kwargs: {
            "description": "Compact appliance where cleanup is a major job to be done.",
            "confidence_note": "Auto-generated note.",
            "product_knowledge": {"workflow_steps": ["Cleanup"]},
        },
    )
    monkeypatch.setattr(app, "_infer_taxonomy_category", lambda desc, reviews: {"category": "home_appliance"})

    build_calls = {"count": 0}

    def _unexpected_build(**kwargs):
        build_calls["count"] += 1
        return {"delighters": ["Should Not Run"], "detractors": ["Should Not Run"]}

    monkeypatch.setattr(app, "_ai_build_symptom_list", _unexpected_build)

    status = app._auto_prepare_workspace_taxonomy(
        dataset,
        max_sample=20,
        preserve_existing_taxonomy=True,
        auto_activate=True,
        context_label="uploaded workspace build",
    )

    assert status["status"] == "ok"
    assert status["taxonomy_preserved"] is True
    assert status["taxonomy_generated"] is False
    assert build_calls["count"] == 0
    assert app.st.session_state["sym_symptoms_source"] == "file"
    assert "Easy Cleanup" in app.st.session_state["sym_delighters"]
    assert "Hard To Clean" in app.st.session_state["sym_detractors"]
    assert app.st.session_state["sym_product_knowledge"]["workflow_steps"] == ["Cleanup"]
    assert app.st.session_state["sym_taxonomy_category"] == "home_appliance"


def test_sync_symptom_wizard_editor_state_refreshes_stale_draft(app_module):
    app = app_module
    app.st.session_state.update(
        sym_ai_del_edit="Old Delighter",
        sym_ai_det_edit="Old Detractor",
        _sym_ai_editor_sig="stale-signature",
    )

    payload = app._sync_symptom_wizard_editor_state(
        {
            "delighters": ["Easy Setup", "Versatile Cooking Modes"],
            "detractors": ["Wrong Size"],
        }
    )

    assert payload["source_dels"] == ["Easy Setup", "Versatile Cooking Modes"]
    assert payload["source_dets"] == ["Wrong Size"]
    assert app.st.session_state["sym_ai_del_edit"] == "Easy Setup\nVersatile Cooking Modes"
    assert app.st.session_state["sym_ai_det_edit"] == "Wrong Size"
    assert app.st.session_state["_sym_ai_editor_sig"] == payload["signature"]



def test_taxonomy_editor_state_flags_manual_changes(app_module):
    app = app_module

    state = app._taxonomy_editor_state(
        {
            "delighters": ["Easy Setup", "Easy Cleanup"],
            "detractors": ["Wrong Size"],
        },
        del_text="Easy Setup\nVersatile Cooking Modes",
        det_text="Wrong Size",
    )

    assert state["edited"] is True
    assert state["current_dels"] == ["Easy Setup", "Versatile Cooking Modes"]
    assert state["current_dets"] == ["Wrong Size"]
    assert state["added_count"] == 1
    assert state["removed_count"] == 1
    assert state["added_dels"] == ["Versatile Cooking Modes"]
    assert state["removed_dels"] == ["Easy To Clean"]



def test_activate_ai_taxonomy_result_clears_wizard_editor_state(app_module):
    app = app_module
    app.st.session_state.update(
        sym_ai_del_edit="Old Delighter",
        sym_ai_det_edit="Old Detractor",
        _sym_ai_editor_sig="stale-signature",
    )

    activated = app._activate_ai_taxonomy_result(
        {
            "delighters": ["Easy Setup"],
            "detractors": ["Wrong Size"],
            "aliases": {"Wrong Size": ["Too Small"]},
            "category": "kitchen",
        },
        source="ai",
        preserve_draft=True,
        auto_applied=True,
    )

    assert activated is True
    assert app.st.session_state["sym_delighters"]
    assert app.st.session_state["sym_detractors"]
    assert app.st.session_state["sym_ai_build_result"]["category"] == "kitchen"
    assert "sym_ai_del_edit" not in app.st.session_state
    assert "sym_ai_det_edit" not in app.st.session_state
    assert "_sym_ai_editor_sig" not in app.st.session_state
