import importlib
import sys
import types

import pytest


class _DummyCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


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


def test_merge_follow_up_result_preserves_existing_labels_when_retry_payload_uses_wrote_keys_only(app_module):
    merged = app_module._merge_follow_up_symptom_result(
        {
            "idx": 11,
            "wrote_dets": ["Battery Dies Fast"],
            "wrote_dels": ["Easy Setup"],
            "ev_det": {"Battery Dies Fast": ["battery died after one use"]},
            "ev_del": {"Easy Setup": ["set up in minutes"]},
            "safety": "Not Mentioned",
            "reliability": "Failure",
            "sessions": "Unknown",
        },
        {
            "idx": 11,
            "wrote_dets": ["Battery Dies Fast"],
            "wrote_dels": ["Easy Setup"],
            "ev_det": {"Battery Dies Fast": ["battery died after one use"]},
            "ev_del": {"Easy Setup": ["set up in minutes"]},
            "safety": "Not Mentioned",
            "reliability": "Failure",
            "sessions": "Unknown",
        },
    )

    assert merged["dets"] == ["Battery Dies Fast"]
    assert merged["dels"] == ["Easy Setup"]
    assert merged["changed"] is False
    assert merged["recovered"] is False
    assert merged["ev_det"] == {"Battery Dies Fast": ["battery died after one use"]}
    assert merged["ev_del"] == {"Easy Setup": ["set up in minutes"]}


def test_merge_follow_up_result_adds_missing_side_without_wiping_existing_labels(app_module):
    merged = app_module._merge_follow_up_symptom_result(
        {
            "idx": 27,
            "wrote_dets": [],
            "wrote_dels": ["Easy Setup"],
            "ev_del": {"Easy Setup": ["set up in minutes"]},
            "safety": "Not Mentioned",
            "reliability": "Reliable",
            "sessions": "Unknown",
        },
        {
            "idx": 27,
            "dets": ["Battery Dies Fast"],
            "ev_det": {"Battery Dies Fast": ["battery died after one use"]},
            "safety": "Not Mentioned",
            "reliability": "Failure",
            "sessions": "Unknown",
        },
    )

    assert merged["dets"] == ["Battery Dies Fast"]
    assert merged["dels"] == ["Easy Setup"]
    assert merged["changed"] is True
    assert merged["recovered"] is True
    assert merged["row_meta"]["AI Reliability"] == "Failure"
    assert merged["ev_det"] == {"Battery Dies Fast": ["battery died after one use"]}
    assert merged["ev_del"] == {"Easy Setup": ["set up in minutes"]}


def test_upsert_processed_symptom_record_handles_zero_row_id(app_module):
    updated = app_module._upsert_processed_symptom_record(
        [{"idx": 0, "wrote_dets": [], "wrote_dels": [], "safety": "Not Mentioned", "reliability": "Unknown", "sessions": "Unknown"}],
        0,
        ["Battery Dies Fast"],
        ["Easy Setup"],
        row_meta={
            "AI Safety": "Not Mentioned",
            "AI Reliability": "Failure",
            "AI # of Sessions": "3+ Sessions",
        },
        ev_det={"Battery Dies Fast": ["battery died after one use"]},
        ev_del={"Easy Setup": ["set up in minutes"]},
    )

    assert updated[0]["idx"] == 0
    assert updated[0]["wrote_dets"] == ["Battery Dies Fast"]
    assert updated[0]["wrote_dels"] == ["Easy Setup"]
    assert updated[0]["reliability"] == "Failure"
    assert updated[0]["sessions"] == "3+ Sessions"
