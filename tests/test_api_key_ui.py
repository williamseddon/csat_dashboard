from review_analyst.api_key_ui import (
    BUILDER_API_KEY_SUBTITLE,
    MISSING_API_KEY_GUIDANCE,
    describe_api_key_ui,
    normalize_key_source,
)


def test_normalize_key_source_defaults_unknown_to_missing():
    assert normalize_key_source(None) == "missing"
    assert normalize_key_source("") == "missing"
    assert normalize_key_source("something-else") == "missing"


def test_missing_key_shows_builder_only():
    state = describe_api_key_ui("missing")
    assert state.key_source == "missing"
    assert state.show_builder_entry is True
    assert state.show_sidebar_entry is False
    assert state.status_message == MISSING_API_KEY_GUIDANCE


def test_manual_key_shows_sidebar_editor_only():
    state = describe_api_key_ui("manual")
    assert state.key_source == "manual"
    assert state.show_builder_entry is False
    assert state.show_sidebar_entry is True
    assert "manual session key" in state.status_message.lower()


def test_auto_loaded_key_hides_manual_inputs():
    for source in ("env", "secrets"):
        state = describe_api_key_ui(source)
        assert state.key_source == source
        assert state.show_builder_entry is False
        assert state.show_sidebar_entry is False
        assert "loaded automatically" in state.status_message.lower()


def test_builder_copy_mentions_ai_features_and_auto_detection():
    copy = BUILDER_API_KEY_SUBTITLE.lower()
    assert "taxonomy" in copy
    assert "symptomizer" in copy
    assert "not detected automatically" in copy
