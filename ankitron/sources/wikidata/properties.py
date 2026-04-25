from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ankitron.sources.wikidata.classes import _normalize_wikidata_id


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

    # Known properties — annotation-only stubs for IDE autocomplete
    LABEL: WikidataProperty
    DESCRIPTION: WikidataProperty
    CAPITAL: WikidataProperty
    FLAG_IMAGE: WikidataProperty
    POPULATION: WikidataProperty
    INCEPTION: WikidataProperty
    HEAD_OF_STATE: WikidataProperty
    AREA: WikidataProperty
    MOTTO: WikidataProperty
    ANTHEM: WikidataProperty
    TIMEZONE: WikidataProperty

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
        normalized = _normalize_wikidata_id(raw, "P", "property")
        return WikidataProperty(id=normalized, value_type=PropertyValueType.LITERAL)


P = _PropertyAccessor()
