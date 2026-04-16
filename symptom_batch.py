"""Shared helper utilities used across all pages and modules."""
from __future__ import annotations
import html
import re
from typing import Any, Optional


def esc(text: Any) -> str:
    """HTML-escape a value for safe rendering."""
    return html.escape(str(text or ""))


def safe_text(value: Any, default: str = "") -> str:
    """Coerce a value to a clean string."""
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ("true", "1", "yes")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def trunc(text: Any, max_len: int = 120) -> str:
    s = safe_text(text)
    return s[:max_len] + "…" if len(s) > max_len else s


def norm_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").lower()).strip()


def tokenize(text: str) -> set:
    return set(re.findall(r"[a-z]{2,}", str(text or "").lower()))


def fmt_secs(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"
