"""Utility helpers for logging, time, and retries."""

from typing import Any, Iterable, Mapping, Optional


def get_first(mapping: Mapping[str, Any], keys: Iterable[str], default: Optional[Any] = None) -> Any:
    """Return the first non-None value for keys in mapping."""
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default
