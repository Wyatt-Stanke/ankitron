"""
ankitron — A declarative Python SDK for programmatically generating Anki flashcard decks.
"""

from ankitron.deck import Card, Deck, Field, FieldKind, FieldRule, PKStrategy, Tag
from ankitron.deck_family import DeckFamily
from ankitron.media import ChartConfig, GeneratedMedia, MapConfig, MediaFormat, MediaType
from ankitron.provenance import ProvenanceConfig, ProvenancePosition, ProvenanceStyle
from ankitron.sources import CSVSource, LinkStrategy, WikidataSource
from ankitron.transform import Transform
from ankitron.validation import OnMismatch, Severity, Validate, VerifyStrategy

__all__ = [
    "CSVSource",
    "Card",
    "ChartConfig",
    "Deck",
    "DeckFamily",
    "Field",
    "FieldKind",
    "FieldRule",
    "GeneratedMedia",
    "LinkStrategy",
    "MapConfig",
    "MediaFormat",
    "MediaType",
    "OnMismatch",
    "PKStrategy",
    "ProvenanceConfig",
    "ProvenancePosition",
    "ProvenanceStyle",
    "Severity",
    "Tag",
    "Transform",
    "Validate",
    "VerifyStrategy",
    "WikidataSource",
]
