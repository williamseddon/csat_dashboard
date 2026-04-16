from __future__ import annotations

from dataclasses import dataclass

MANUAL_API_KEY_HELP = "Paste your OpenAI API key. Only stored in this browser session."
MISSING_API_KEY_GUIDANCE = (
    "No API key detected. Add one in Build or switch workspace, or set OPENAI_API_KEY "
    "in the environment / Streamlit secrets."
)
BUILDER_API_KEY_SUBTITLE = (
    "AI Analyst, Review Prompt, taxonomy generation, and Symptomizer need a key. "
    "This setup only appears when OPENAI_API_KEY is not detected automatically."
)


@dataclass(frozen=True)
class ApiKeyUiState:
    key_source: str
    show_builder_entry: bool
    show_sidebar_entry: bool
    status_message: str


_VALID_SOURCES = {"secrets", "env", "manual", "missing"}


def normalize_key_source(key_source: str | None) -> str:
    normalized = str(key_source or "").strip().lower()
    return normalized if normalized in _VALID_SOURCES else "missing"


def describe_api_key_ui(key_source: str | None) -> ApiKeyUiState:
    source = normalize_key_source(key_source)
    if source in {"secrets", "env"}:
        status_message = "OpenAI key loaded automatically."
    elif source == "manual":
        status_message = "Using a manual session key."
    else:
        status_message = MISSING_API_KEY_GUIDANCE
    return ApiKeyUiState(
        key_source=source,
        show_builder_entry=(source == "missing"),
        show_sidebar_entry=(source == "manual"),
        status_message=status_message,
    )
