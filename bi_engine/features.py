"""
Feature flags for ICIE pipeline.

Gate experimental modules cleanly via environment variables.
All flags default to False (off) unless explicitly enabled.

Usage:
    from features import feature
    if feature("RERA_MODULE"):
        from ingestion.rera import ReraIngester
"""

from __future__ import annotations

import os

_FLAGS: dict[str, bool] = {
    "RERA_MODULE": os.getenv("FEATURE_RERA", "false").lower() == "true",
    "GEMINI_ROUTING": os.getenv("FEATURE_GEMINI", "false").lower() == "true",
    "PARALLEL_AGENTS": os.getenv("FEATURE_PARALLEL", "false").lower() == "true",
    "LANGGRAPH": os.getenv("FEATURE_LANGGRAPH", "false").lower() == "true",
    "GEM_MODULE": os.getenv("FEATURE_GEM", "false").lower() == "true",
    "BSE_MODULE": os.getenv("FEATURE_BSE", "false").lower() == "true",
}


def feature(name: str) -> bool:
    """Return True if the named feature flag is enabled."""
    return _FLAGS.get(name, False)


def all_flags() -> dict[str, bool]:
    """Return all feature flags and their current state."""
    return dict(_FLAGS)
