"""
LinkStrategy — strategies for matching rows between sources.

Defines how a linked source finds corresponding rows in the primary source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from ankitron.deck import Field


@dataclass
class LinkStrategy:
    """A strategy for matching rows between sources."""

    _kind: str
    _params: dict[str, Any]

    @staticmethod
    def sitelinks() -> LinkStrategy:
        """Link via Wikidata sitelinks (Wikipedia article titles from Wikidata items)."""
        return LinkStrategy(_kind="sitelinks", _params={})

    @staticmethod
    def field(name: str) -> LinkStrategy:
        """Link by matching a field value between sources.

        Note: Matching is case-insensitive and strips whitespace.
        """
        return LinkStrategy(_kind="field", _params={"field_name": name})

    @staticmethod
    def geocode(coords_field: Field) -> LinkStrategy:
        """Link by geographic proximity to a coordinates field."""
        return LinkStrategy(_kind="geocode", _params={"coords_field": coords_field})

    @staticmethod
    def custom(fn: Callable[[dict, dict], bool]) -> LinkStrategy:
        """Link using an arbitrary matching function.

        The function receives two row dicts (primary, candidate) and returns
        True if they match.
        """
        return LinkStrategy(_kind="custom", _params={"fn": fn})

    def match(self, primary_row: dict[str, Any], candidate_row: dict[str, Any]) -> bool:
        """Test whether two rows match according to this strategy."""
        if self._kind == "field":
            field_name = self._params["field_name"]
            a = str(primary_row.get(field_name, "")).strip().lower()
            b = str(candidate_row.get(field_name, "")).strip().lower()
            return a == b and a != ""
        if self._kind == "custom":
            return self._params["fn"](primary_row, candidate_row)
        # sitelinks and geocode need specific source integration
        return False
