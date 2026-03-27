"""
ankitron — A declarative Python SDK for programmatically generating Anki flashcard decks.
"""

from ankitron.deck import Card, Deck, Field, Tag
from ankitron.enums import (
    FieldKind,
    FieldRule,
    MediaFormat,
    MediaType,
    PKStrategy,
    Severity,
)
from ankitron.media import ChartConfig, GeneratedMedia, MapConfig
from ankitron.provenance import ProvenanceConfig, ProvenancePosition, ProvenanceStyle
from ankitron.transform import Transform
from ankitron.validation import OnMismatch, Validate, VerifyStrategy

__all__ = [
    "Card",
    "ChartConfig",
    "Deck",
    "Field",
    "FieldKind",
    "FieldRule",
    "GeneratedMedia",
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
]
