from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class PropertyValueType(Enum):
    ENTITY = "entity"
    LITERAL = "literal"
    SPECIAL = "special"  # For LABEL, DESCRIPTION


@dataclass(frozen=True)
class WikidataProperty:
    """Represents a Wikidata property identifier."""

    id: str
    value_type: PropertyValueType = PropertyValueType.LITERAL

    @property
    def value(self) -> str:
        return self.id


# Known properties with their value types
_KNOWN_PROPERTIES: dict[str, tuple[str, PropertyValueType]] = {
    "LABEL": ("label", PropertyValueType.SPECIAL),
    "DESCRIPTION": ("description", PropertyValueType.SPECIAL),
    "CAPITAL": ("P36", PropertyValueType.ENTITY),
    "FLAG_IMAGE": ("P41", PropertyValueType.LITERAL),
    "POPULATION": ("P1082", PropertyValueType.LITERAL),
    "INCEPTION": ("P571", PropertyValueType.LITERAL),
    "HEAD_OF_STATE": ("P6", PropertyValueType.ENTITY),
    "AREA": ("P2046", PropertyValueType.LITERAL),
    "MOTTO": ("P1451", PropertyValueType.LITERAL),
    "ANTHEM": ("P85", PropertyValueType.ENTITY),
    "TIMEZONE": ("P421", PropertyValueType.ENTITY),
}


class _PropertyAccessor:
    """
    Access Wikidata properties via named constants or escape hatch.

    Usage:
        P.LABEL         # Named constant
        P("P999")       # Escape hatch
        P("999")        # Escape hatch (P prefix added)
    """

    def __getattr__(self, name: str) -> WikidataProperty:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in _KNOWN_PROPERTIES:
            raise AttributeError(
                f"Unknown Wikidata property: {name!r}. "
                f"Available: {', '.join(_KNOWN_PROPERTIES.keys())}. "
                f"Use P('P<number>') for unlisted properties."
            )
        pid, vtype = _KNOWN_PROPERTIES[name]
        return WikidataProperty(id=pid, value_type=vtype)

    def __call__(self, raw: str) -> WikidataProperty:
        if not isinstance(raw, str):
            raise TypeError(f"Expected a string, got {type(raw).__name__}")
        # Normalize: add P prefix if missing
        normalized = raw if raw.startswith("P") else f"P{raw}"
        if not re.match(r"^P\d+$", normalized):
            raise ValueError(
                f"Invalid Wikidata property ID: {raw!r}. "
                f"Expected format: 'P<number>' (e.g., 'P36' or '36')."
            )
        return WikidataProperty(id=normalized, value_type=PropertyValueType.LITERAL)


P = _PropertyAccessor()
