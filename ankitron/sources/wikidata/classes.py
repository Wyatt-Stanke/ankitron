from __future__ import annotations

import re
from dataclasses import dataclass


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
        if not isinstance(raw, str):
            raise TypeError(f"Expected a string, got {type(raw).__name__}")
        normalized = raw if raw.startswith("Q") else f"Q{raw}"
        if not re.match(r"^Q\d+$", normalized):
            raise ValueError(
                f"Invalid Wikidata class ID: {raw!r}. "
                f"Expected format: 'Q<number>' (e.g., 'Q35657' or '35657')."
            )
        return WikidataClass(id=normalized)


Q = _ClassAccessor()
