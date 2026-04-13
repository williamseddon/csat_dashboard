"""Structured logging for StarWalk Review Analyst.

Usage in any module:
    import logging
    logger = logging.getLogger("starwalk.module_name")
    logger.info("Processing %d reviews", count)

Call setup_logging() once at app startup to configure the handler.
"""
from __future__ import annotations

import logging
import sys
from typing import Optional


_CONFIGURED = False


def setup_logging(*, level: str = "INFO", stream=None) -> None:
    """Configure structured logging for the StarWalk app.

    Call once in app.py's main() or at module level. Safe to call multiple times.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger("starwalk")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Avoid duplicate handlers on hot-reload
    if not root.handlers:
        root.addHandler(handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Get a logger under the starwalk namespace."""
    return logging.getLogger(f"starwalk.{name}")
