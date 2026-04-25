from __future__ import annotations

import re
from dataclasses import dataclass


def _normalize_wikidata_id(raw: str, prefix: str, id_type: str) -> str:
    """Normalize and validate a Wikidata Q/P identifier.

    Args:
        raw: The raw string provided by the user (e.g. "Q35657", "35657").
        prefix: The expected prefix letter ("Q" or "P").
        id_type: Human-readable type name for error messages ("class" or "property").

    Returns:
        The normalized ID with prefix (e.g. "Q35657").

    Raises:
        TypeError: If raw is not a string.
        ValueError: If the normalized ID doesn't match ``^<prefix>\\d+$``.
    """
    if not isinstance(raw, str):
        raise TypeError(f"Expected a string, got {type(raw).__name__}")
    normalized = raw if raw.startswith(prefix) else f"{prefix}{raw}"
    if not re.match(rf"^{prefix}\d+$", normalized):
        raise ValueError(
            f"Invalid Wikidata {id_type} ID: {raw!r}. "
            f"Expected format: '{prefix}<number>' (e.g., '{prefix}35657' or '35657')."
        )
    return normalized


@dataclass(frozen=True)
class WikidataClass:
    """Represents a Wikidata class (Q-item) identifier."""

    id: str

    @property
    def value(self) -> str:
        return self.id


_KNOWN_CLASSES: dict[str, str] = {
    "US_STATE": "Q35657",
    "COUNTRY": "Q6256",
    "CITY": "Q515",
    "LANGUAGE": "Q34770",
    "CHEMICAL_ELEMENT": "Q11344",
    "PLANET": "Q634",
}


class _ClassAccessor:
    """
    Access Wikidata classes via named constants or escape hatch.

    Usage:
        Q.US_STATE      # Named constant
        Q("Q12345")     # Escape hatch
        Q("12345")      # Escape hatch (Q prefix added)
    """

    # Known classes — annotation-only stubs for IDE autocomplete
    US_STATE: WikidataClass
    COUNTRY: WikidataClass
    CITY: WikidataClass
    LANGUAGE: WikidataClass
    CHEMICAL_ELEMENT: WikidataClass
    PLANET: WikidataClass

    def __getattr__(self, name: str) -> WikidataClass:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in _KNOWN_CLASSES:
            raise AttributeError(
                f"Unknown Wikidata class: {name!r}. "
                f"Available: {', '.join(_KNOWN_CLASSES.keys())}. "
                f"Use Q('Q<number>') for unlisted classes."
            )
        return WikidataClass(id=_KNOWN_CLASSES[name])

    def __call__(self, raw: str) -> WikidataClass:
        normalized = _normalize_wikidata_id(raw, "Q", "class")
        return WikidataClass(id=normalized)


Q = _ClassAccessor()
