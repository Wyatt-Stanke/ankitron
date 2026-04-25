"""Shared low-level utilities used across multiple ankitron modules."""

from __future__ import annotations

from typing import Any


def get_pk_value(row: dict[str, Any], pk_attr: str) -> str:
    """Extract the primary-key value from a row dict.

    Tries the canonical ``_pk_<attr>`` key first (set by sources that
    normalise the PK), then falls back to the raw attribute value.
    """
    return str(row.get(f"_pk_{pk_attr}", row.get(pk_attr, "")))
