"""
DeckFamily — produce multiple concrete decks from a single class.

A ``DeckFamily`` subclass describes a *template* deck whose sources
contain ``{param}`` placeholders.  At build time the SDK discovers all
parameter combinations and instantiates one ``Deck`` per variant.
"""

from __future__ import annotations

import copy
from typing import Any, ClassVar

from ankitron.deck import (
    Card,
    Deck,
    Field,
    Tag,
    _collect_deck_members,
)


class DeckFamily:
    """Base class for parameterised deck families.

    Subclasses define the deck template exactly like ``Deck``, but the
    ``deck_name`` and source paths contain ``{param}`` placeholders that
    are resolved per variant.

    Parameter discovery (in priority order):

    1. Override the ``discover()`` classmethod to yield param dicts.
    2. Set ``params`` to an explicit list of dicts.
    3. Attach ``discover=SomeSource.glob()`` on a source to auto-discover
       from the filesystem.
    """

    # User-configurable class variables
    deck_name: ClassVar[str]
    tags: ClassVar[list[str | Tag]]
    validators: ClassVar[list[Any]]
    overrides: ClassVar[dict[str, dict[str, Any]]]
    params: ClassVar[list[dict[str, Any]]]

    # Set by __init_subclass__
    _family_fields: ClassVar[list[tuple[str, Field]]]
    _family_cards: ClassVar[list[type[Card]]]
    _family_sources: ClassVar[list[tuple[str, Any]]]
    _is_abstract: ClassVar[bool]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        cls._is_abstract = getattr(cls, "__abstract__", False)
        if cls._is_abstract:
            return

        fields, cards, sources = _collect_deck_members(cls)
        cls._family_fields = fields
        cls._family_cards = cards
        cls._family_sources = sources

    # -- parameter discovery --------------------------------------------------

    @classmethod
    def discover(cls) -> list[dict[str, Any]]:
        """Discover parameter combinations.

        Override this method for custom discovery logic.  The default
        implementation checks ``cls.params`` first, then tries
        auto-discovery from source glob patterns.
        """
        # Explicit params list
        if hasattr(cls, "params") and cls.params:
            return list(cls.params)

        # Auto-discover from source glob patterns
        for _name, source in cls._family_sources:
            discover_obj = getattr(source, "_discover", None)
            if discover_obj is not None and hasattr(discover_obj, "discover"):
                path_template = getattr(source, "_path", None)
                if path_template:
                    return discover_obj.discover(path_template)

        return []

    # -- variant expansion ----------------------------------------------------

    @classmethod
    def expand(cls) -> list[type[Deck]]:
        """Expand the family into concrete ``Deck`` subclasses."""
        param_sets = cls.discover()
        if not param_sets:
            return []

        variants: list[type[Deck]] = []
        for params in param_sets:
            variant = cls._make_variant(params)
            variants.append(variant)
        return variants

    @classmethod
    def _make_variant(cls, params: dict[str, Any]) -> type[Deck]:
        """Create a single ``Deck`` subclass for a specific parameter set."""
        # Build the qualname
        param_str = ",".join(f"{k}={v}" for k, v in sorted(params.items()))
        qualname = f"{cls.__name__}[{param_str}]"

        # Resolve deck name
        deck_name_raw = getattr(cls, "deck_name", cls.__name__)
        deck_name_fn = getattr(cls, "deck_name_fn", None)
        resolved_name = deck_name_raw.format(**params)

        # Deep-copy and resolve sources
        ns: dict[str, Any] = {"deck_name": resolved_name}

        # Copy tags, validators, overrides
        if hasattr(cls, "tags"):
            ns["tags"] = cls.tags
        if hasattr(cls, "validators"):
            ns["validators"] = cls.validators
        if hasattr(cls, "overrides"):
            ns["overrides"] = cls.overrides

        # Re-create sources with resolved paths
        for src_name, source in cls._family_sources:
            new_src = _resolve_source(source, params)
            ns[src_name] = new_src

        # Re-create fields bound to the new sources
        source_id_map: dict[int, Any] = {}
        for (_src_name, orig_src), new_src_name in zip(
            cls._family_sources,
            [n for n, _ in cls._family_sources],
            strict=True,
        ):
            source_id_map[id(orig_src)] = ns.get(new_src_name, orig_src)

        for attr_name, fld in cls._family_fields:
            new_fld = _copy_field(fld, source_id_map)
            ns[attr_name] = new_fld

        # Copy card classes
        for card_cls in cls._family_cards:
            ns[card_cls.__name__] = card_cls

        # Store the family qualname for note ID isolation
        ns["_family_qualname"] = qualname
        ns["_family_params"] = params
        ns["_deck_name_fn"] = deck_name_fn

        # Create the Deck subclass (triggers __init_subclass__ validation)
        return type(qualname, (Deck,), ns)

    @classmethod
    def build_all(cls, **kwargs: Any) -> list[Deck]:
        """Expand and fetch all variants.  Returns list of Deck instances."""
        variants = cls.expand()
        instances = []
        for variant_cls in variants:
            instance = variant_cls()
            instance.fetch(**kwargs)
            instances.append(instance)
        return instances


def _resolve_source(source: Any, params: dict[str, Any]) -> Any:
    """Create a new source instance with ``{param}`` placeholders resolved."""
    new_src = copy.copy(source)
    # Resolve path-like attributes
    for attr in ("_path", "_query", "_url"):
        val = getattr(new_src, attr, None)
        if isinstance(val, str) and "{" in val:
            setattr(new_src, attr, val.format(**params))
    return new_src


def _copy_field(fld: Field, source_id_map: dict[int, Any]) -> Field:
    """Shallow-copy a field, rebinding its source to the resolved variant."""
    new_fld = copy.copy(fld)
    if fld._source is not None:
        new_src = source_id_map.get(id(fld._source))
        if new_src is not None:
            new_fld._source = new_src
    return new_fld
