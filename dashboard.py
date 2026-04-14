"""AI service: OpenAI client, model routing, chat completion

Extracted from app.py. Uses _app() for cross-module access.
"""
from __future__ import annotations
import json, math, os, re, sys, time
from typing import Any, Optional
import streamlit as st

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

# ── Constants ──
DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_REASONING = "none"

def _app():
    """Lazy access to app module namespace."""
    return sys.modules.get('__main__', sys.modules.get('app'))

def _esc(text):
    return html.escape(str(text or "")) if 'html' in dir() else str(text or "")

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _normalize_tag_list(tags):
    if not tags: return []
    seen = set()
    out = []
    for t in tags:
        s = str(t).strip()
        if not s or s.upper() in NON_VALUES: continue
        key = s.lower()
        if key not in seen: seen.add(key); out.append(s)
    return out


def _get_api_key():
    try:
        if "OPENAI_API_KEY" in st.secrets:
            return str(st.secrets["OPENAI_API_KEY"])
        if "openai" in st.secrets and st.secrets["openai"].get("api_key"):
            return str(st.secrets["openai"]["api_key"])
    except Exception:
        pass
    return os.getenv("OPENAI_API_KEY")


@st.cache_resource(show_spinner=False)



def _get_client():
    key = _get_api_key()
    if not (_HAS_OPENAI and key):
        return None
    return _app()._make_openai_client(key)



def _safe_json_load(s):
    s = (s or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        i = s.find("{")
        j = s.rfind("}")
        if i >= 0 and j > i:
            return json.loads(s[i:j + 1])
    except Exception:
        pass
    return {}



def _chat_complete(client, *, model, messages, temperature=0.0, response_format=None,
                   max_tokens=1200, reasoning_effort=None, _max_retries=3):
    if client is None:
        return ""

    effort = _app()._normalize_reasoning_effort_for_model(model, reasoning_effort)
    kwargs = dict(model=model, messages=_app()._prepare_messages_for_model(model, messages))
    kwargs.update(_app()._build_completion_token_kwargs(max_tokens))
    if response_format:
        kwargs["response_format"] = response_format
    if effort:
        kwargs["reasoning_effort"] = effort
    if temperature is not None and _app()._model_accepts_temperature(model, effort):
        kwargs["temperature"] = temperature

    last_exc = None
    reasoning_enabled = "reasoning_effort" in kwargs
    temperature_enabled = "temperature" in kwargs

    for attempt in range(max(1, _max_retries)):
        try:
            resp = client.chat.completions.create(**kwargs)
            return (resp.choices[0].message.content or "").strip()
        except Exception as exc:
            last_exc = exc
            err = str(exc).lower()

            if "max_completion_tokens" in kwargs and any(k in err for k in (
                "unexpected keyword argument 'max_completion_tokens'",
                'unsupported parameter: "max_completion_tokens"',
                "unsupported parameter: 'max_completion_tokens'",
                "unknown parameter: max_completion_tokens",
                "max_completion_tokens is not supported",
            )):
                token_limit = kwargs.pop("max_completion_tokens", None)
                if token_limit is not None:
                    kwargs["max_tokens"] = token_limit
                continue

            if "max_tokens" in kwargs and any(k in err for k in (
                "unexpected keyword argument 'max_tokens'",
                'unsupported parameter: "max_tokens"',
                "unsupported parameter: 'max_tokens'",
                "use 'max_completion_tokens' instead",
                "deprecated in favor of `max_completion_tokens`",
                "not compatible with o-series models",
                "not compatible with reasoning models",
            )):
                token_limit = kwargs.pop("max_tokens", None)
                if token_limit is not None:
                    kwargs["max_completion_tokens"] = token_limit
                continue

            if reasoning_enabled and any(k in err for k in (
                "reasoning_effort",
                "unknown parameter: reasoning_effort",
                'unsupported parameter: "reasoning_effort"',
                "unsupported parameter: 'reasoning_effort'",
                "invalid reasoning",
                "invalid value for reasoning",
                "does not support reasoning effort",
                "not support reasoning effort",
            )):
                kwargs.pop("reasoning_effort", None)
                reasoning_enabled = False
                continue

            if temperature_enabled and any(k in err for k in (
                "temperature",
                "top_p",
                "only supported when using",
                "not supported when reasoning effort",
                "include these fields will raise",
            )):
                kwargs.pop("temperature", None)
                temperature_enabled = False
                continue

            if any(k in err for k in ("rate_limit", "429", "500", "503", "timeout", "overloaded")):
                time.sleep(min((2 ** attempt) + random.uniform(0, 1), 30))
                continue
            raise

    if last_exc:
        raise last_exc
    return ""



def _chat_complete_with_fallback_models(client, *, model, messages, structured=False, **kwargs):
    last_exc = None
    for candidate in _app()._model_candidates_for_task(model, structured=structured):
        try:
            return _chat_complete(client, model=candidate, messages=messages, **kwargs)
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    return ""

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA LAYER
# ═══════════════════════════════════════════════════════════════════════════════



def _shared_model():
    return st.session_state.get("shared_model", DEFAULT_MODEL)



def _shared_reasoning():
    current_model = _shared_model()
    allowed = _app()._reasoning_options_for_model(current_model)
    cur = _safe_text(st.session_state.get("shared_reasoning", DEFAULT_REASONING)).lower() or DEFAULT_REASONING
    if cur not in allowed:
        cur = "none" if "none" in allowed else allowed[0]
        st.session_state["shared_reasoning"] = cur
    return cur



def _estimate_tokens(text):
    s = str(text or "")
    if not s:
        return 0
    if _HAS_TIKTOKEN and _TIKTOKEN_ENC is not None:
        try:
            return int(len(_TIKTOKEN_ENC.encode(s)))
        except Exception:
            pass
    return int(max(1, math.ceil(len(s) / 4)))

# ═══════════════════════════════════════════════════════════════════════════════
#  OPENAI
# ═══════════════════════════════════════════════════════════════════════════════



def _coerce_ai_target_words(value, default=1200):
    try:
        n = int(value)
    except Exception:
        n = int(default)
    return max(250, min(2400, n))



def _ai_target_token_budget(target_words: int) -> int:
    words = _coerce_ai_target_words(target_words)
    return max(900, min(7000, int(round(words * 2.35))))



def _strip_review_citations(text: str) -> str:
    raw = _app()._normalize_ai_answer_display(text)
    cleaned = _app()._REVIEW_REF_PATTERN.sub("", raw)
    cleaned = re.sub(r"\s+([,.;:])", r"\1", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


