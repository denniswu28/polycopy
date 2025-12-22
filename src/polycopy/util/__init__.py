"""Utility helpers for logging, time, and retries."""

from typing import Any, Iterable, Mapping, Optional


def get_first(mapping: Mapping[str, Any], keys: Iterable[str], default: Optional[Any] = None) -> Any:
    """
    Return the first value whose key exists in the mapping and is not None (even if falsy), in order,
    or the default if none are found.
    """
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default
