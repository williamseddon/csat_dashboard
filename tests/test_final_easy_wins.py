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


def test_reset_workspace_state_clears_symptomizer_run_setup(app_module):
    app = app_module
    app.st.session_state.update(
        sym_scope_choice="Current filtered missing",
        sym_n_to_process=87,
        sym_batch_size_run=5,
        _sym_recommended_setup_sig="old-sig",
        _sym_recommended_setup_auto=False,
    )

    app._reset_workspace_state(reset_source=False)

    assert app.st.session_state["sym_scope_choice"] == "Missing both"
    assert app.st.session_state["sym_n_to_process"] == 10
    assert "sym_batch_size_run" not in app.st.session_state
    assert app.st.session_state["_sym_recommended_setup_sig"] is None
    assert app.st.session_state["_sym_recommended_setup_auto"] is True


def test_recommended_symptomizer_setup_prefers_filtered_missing_reviews(app_module):
    app = app_module
    work = pd.DataFrame(
        {
            "review_id": ["1", "2", "3", "4"],
            "review_text": [
                "Battery dies fast after one use.",
                "Works well now.",
                "Hard to clean but easy to store.",
                "Love it.",
            ],
            "review_length_words": [6, 3, 7, 2],
            "Needs_Delighters": [True, False, True, False],
            "Needs_Detractors": [True, True, False, False],
        }
    )
    filtered_df = work.loc[work["review_id"].isin(["1", "3"])].copy()

    rec = app._recommended_symptomizer_setup(work, filtered_df, work)

    assert rec["scope"] == "Current filtered missing"
    assert rec["in_scope"] == 2
    assert rec["n_to_process"] == 2
    assert rec["batch_size"] == 2
    assert "filtered view" in rec["note"].lower()


def test_recommended_symptomizer_setup_falls_back_to_any_missing(app_module):
    app = app_module
    work = pd.DataFrame(
        {
            "review_id": ["1", "2", "3"],
            "review_text": ["Too loud.", "Quiet and easy.", "Battery dies fast."],
            "review_length_words": [2, 3, 3],
            "Needs_Delighters": [False, True, False],
            "Needs_Detractors": [True, False, True],
        }
    )

    rec = app._recommended_symptomizer_setup(work, work.copy(), work)

    assert rec["scope"] == "Any missing"
    assert rec["in_scope"] == 3
    assert rec["n_to_process"] == 3
    assert rec["batch_size"] == 3


def test_maybe_apply_recommended_setup_respects_manual_override(app_module):
    app = app_module
    app.st.session_state.update(
        sym_scope_choice="All loaded reviews",
        sym_n_to_process=25,
        sym_batch_size_run=4,
        _sym_recommended_setup_auto=False,
        _sym_recommended_setup_sig="stale",
    )

    app._maybe_apply_recommended_symptomizer_setup(
        {"scope": "Current filtered missing", "n_to_process": 12, "batch_size": 8, "in_scope": 12, "note": "Auto"}
    )

    assert app.st.session_state["sym_scope_choice"] == "All loaded reviews"
    assert app.st.session_state["sym_n_to_process"] == 25
    assert app.st.session_state["sym_batch_size_run"] == 4
    assert app.st.session_state["_sym_recommended_setup_sig"] != "stale"


def test_prepare_symptom_export_bytes_prefers_uploaded_workbook(app_module, monkeypatch):
    app = app_module
    calls = {"workbook": 0, "master": 0}

    def _fake_workbook(original_bytes, updated_df):
        calls["workbook"] += 1
        assert original_bytes == b"orig"
        assert list(updated_df["review_id"]) == ["1"]
        return b"workbook-bytes"

    def _fake_master(summary_obj, updated_df):
        calls["master"] += 1
        return b"master-bytes"

    monkeypatch.setattr(app, "_gen_symptomized_workbook", _fake_workbook)
    monkeypatch.setattr(app, "_build_master_excel", _fake_master)

    out = app._prepare_symptom_export_bytes(
        pd.DataFrame({"review_id": ["1"]}),
        summary_obj={"product_id": "sku-1"},
        original_bytes=b"orig",
    )

    assert out == b"workbook-bytes"
    assert calls == {"workbook": 1, "master": 0}


def test_prepare_symptom_export_bytes_falls_back_to_master_export(app_module, monkeypatch):
    app = app_module
    calls = {"master": 0}

    def _fake_master(summary_obj, updated_df):
        calls["master"] += 1
        assert summary_obj == {"product_id": "sku-2"}
        assert list(updated_df["review_id"]) == ["2"]
        return b"master-bytes"

    monkeypatch.setattr(app, "_gen_symptomized_workbook", lambda *a, **k: pytest.fail("Workbook export should not be used"))
    monkeypatch.setattr(app, "_build_master_excel", _fake_master)

    out = app._prepare_symptom_export_bytes(
        pd.DataFrame({"review_id": ["2"]}),
        summary_obj={"product_id": "sku-2"},
        original_bytes=None,
    )

    assert out == b"master-bytes"
    assert calls["master"] == 1


def test_workspace_status_chips_show_taxonomy_coverage_and_export_state(app_module):
    app = app_module
    app.st.session_state.update(
        sym_delighters=["Power Suction"],
        sym_detractors=["Battery Dies Fast"],
        sym_symptoms_source="ai",
        sym_export_bytes=b"ready",
    )
    overall_df = pd.DataFrame(
        {
            "review_id": ["1", "2", "3"],
            "AI Symptom Delighter 1": ["Power Suction", "", ""],
            "AI Symptom Detractor 1": ["", "Battery Dies Fast", ""],
        }
    )
    filtered_df = overall_df.iloc[:2].copy()

    chips = app._workspace_status_chips(overall_df, filtered_df)
    chip_text = [label for label, _ in chips]

    assert any("Taxonomy ready" in label for label in chip_text)
    assert any("Symptom coverage" in label and "2/2" in label for label in chip_text)
    assert "Export ready" in chip_text
    assert any("Filtered view" in label for label in chip_text)
