from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field as dc_field
from typing import Any

from rich.table import Table
from rich import box

from ankitron.enums import AnkiTemplate, FieldKind, PKStrategy
from ankitron.wikidata.properties import PropertyValueType
from ankitron.cache import Cache
from ankitron.logging import (
    section_header,
    log_info,
    log_success,
    log_warn,
    console,
    make_progress,
    print_error_panel,
    warning_count,
)


@dataclass
class Field:
    """Represents a single piece of data on every row/record."""

    kind: FieldKind = FieldKind.TEXT
    fmt: str | None = None
    pk: PKStrategy | None = None
    internal: bool = False
    unused_ok: bool = False

    # Set automatically by __set_name__
    name: str | None = dc_field(default=None, repr=False)

    # Internal — set by source.Field(), not by the user
    _source: Any = dc_field(default=None, repr=False)
    _source_key: str | None = dc_field(default=None, repr=False)
    _source_value_type: PropertyValueType = dc_field(
        default=PropertyValueType.LITERAL, repr=False
    )

    # Internal — set by .derive(), not by the user
    _parent: Field | None = dc_field(default=None, repr=False)
    _transform: Callable | None = dc_field(default=None, repr=False)

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    @property
    def is_derived(self) -> bool:
        return self._parent is not None

    @property
    def is_internal(self) -> bool:
        return self.internal

    def derive(
        self,
        transform: Callable | None = None,
        fmt: str | None = None,
        kind: FieldKind = FieldKind.TEXT,
        internal: bool = False,
        unused_ok: bool = False,
    ) -> Field:
        """Create a derived field whose value is computed from this field's value."""
        return Field(
            kind=kind,
            fmt=fmt,
            internal=internal,
            unused_ok=unused_ok,
            _parent=self,
            _transform=transform,
        )


class Card:
    """
    Base class for card templates. Subclass inside a Deck subclass.

    Each Card subclass must define `front` and `back` class attributes
    with {{field_name}} references.

    WARNING: The declaration order of Card subclasses determines the Anki
    card template ordinal. Changing the order will misidentify cards and
    cause review history to be misassociated. Always add new Card types at
    the end of the class body.
    """

    front: str
    back: str


# Regex to find {{field_name}} references, excluding Anki special syntax
# like {{FrontSide}}, {{#field}}, {{/field}}, {{^field}}, {{type:field}}
_FIELD_REF_PATTERN = re.compile(r"\{\{(?!FrontSide|#|/|\^|type:)(\w+)\}\}")


def _resolve_derivation_order(
    fields: list[tuple[str, Field]],
) -> list[tuple[str, Field]]:
    """
    Return derived fields in dependency order (topological sort).
    Raises TypeError on circular derivation.
    """
    derived = [(name, fld) for name, fld in fields if fld.is_derived]

    resolved: list[tuple[str, Field]] = []
    resolved_ids: set[int] = set()

    # All non-derived fields are already resolved
    for _, fld in fields:
        if not fld.is_derived:
            resolved_ids.add(id(fld))

    remaining = list(derived)
    max_iterations = len(remaining) + 1
    for _ in range(max_iterations):
        if not remaining:
            break
        next_remaining = []
        for name, fld in remaining:
            if id(fld._parent) in resolved_ids:
                resolved.append((name, fld))
                resolved_ids.add(id(fld))
            else:
                next_remaining.append((name, fld))
        if len(next_remaining) == len(remaining):
            cycle_names = [n for n, _ in next_remaining]
            raise TypeError(
                f"Circular derivation detected among fields: {', '.join(cycle_names)}"
            )
        remaining = next_remaining

    return resolved


class Deck:
    """
    Base class for deck definitions. Subclass to define a deck.

    Uses __init_subclass__ to introspect the class body at definition time.
    """

    template: AnkiTemplate = AnkiTemplate.BASIC

    # These are set by __init_subclass__ on subclasses
    _deck_fields: list[Field]
    _deck_cards: list[type[Card]]
    _deck_sources: list[Any]
    _deck_name: str
    _field_attrs: list[str]
    _pk_field_attr: str
    _visible_fields: list[tuple[str, Field]]
    _derived_order: list[tuple[str, Field]]
    _all_fields: list[tuple[str, Field]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Collect fields, cards, and sources from the class body
        # Fields starting with _ are collected (they can be internal source fields)
        fields: list[tuple[str, Field]] = []
        cards: list[type[Card]] = []
        sources: list[tuple[str, Any]] = []

        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, Field):
                fields.append((attr_name, attr_value))
            elif (
                isinstance(attr_value, type)
                and issubclass(attr_value, Card)
                and attr_value is not Card
            ):
                cards.append(attr_value)
            elif (
                hasattr(attr_value, "Field")
                and hasattr(attr_value, "fetch")
                and not isinstance(attr_value, type)
                and not attr_name.startswith("__")
            ):
                sources.append((attr_name, attr_value))

        # Validation: at least one Field
        if not fields:
            raise TypeError(f"{cls.__name__}: no fields declared.")

        # Validation: at least one Card
        if not cards:
            raise TypeError(f"{cls.__name__}: no card types declared.")

        # Validation: Card subclasses have front and back
        for card_cls in cards:
            if not hasattr(card_cls, "front"):
                raise TypeError(
                    f"{cls.__name__}.{card_cls.__name__}: Card subclass must define both 'front' and 'back'."
                )
            if not hasattr(card_cls, "back"):
                raise TypeError(
                    f"{cls.__name__}.{card_cls.__name__}: Card subclass must define both 'front' and 'back'."
                )

        # Build field name sets
        field_names = {name for name, _ in fields}
        internal_field_names = {name for name, fld in fields if fld.internal}
        non_internal_field_names = field_names - internal_field_names

        # Collect all referenced fields across all card templates
        all_referenced_fields: set[str] = set()

        # Validation: Card {{field}} references exist and are not internal
        for card_cls in cards:
            for side_name in ("front", "back"):
                template_str = getattr(card_cls, side_name)
                refs = _FIELD_REF_PATTERN.findall(template_str)
                for ref in refs:
                    all_referenced_fields.add(ref)
                    if ref not in field_names:
                        raise TypeError(
                            f"{cls.__name__}.{card_cls.__name__}: {side_name} template references "
                            f"'{{{{{ref}}}}}', but no field named '{ref}' exists. "
                            f"Available fields: {', '.join(sorted(field_names))}"
                        )
                    if ref in internal_field_names:
                        # Suggest similarly named non-internal fields
                        suggestions = [
                            n for n in non_internal_field_names if ref in n or n in ref
                        ]
                        hint = (
                            f" Did you mean '{suggestions[0]}'?" if suggestions else ""
                        )
                        raise TypeError(
                            f"{cls.__name__}.{card_cls.__name__}: template references "
                            f"'{{{{{ref}}}}}', but that field is marked as internal.{hint}"
                        )

        for attr_name, fld in fields:
            if fld.internal or fld.unused_ok:
                continue
            if attr_name not in all_referenced_fields:
                log_warn(
                    f"{cls.__name__}: field '{attr_name}' is not referenced in any card template and is not marked as internal.",
                )
        # Collect source instances
        source_instances = {id(src): src for _, src in sources}

        # Validation: every source Field refers to a deck source
        for attr_name, fld in fields:
            if fld._source is not None and id(fld._source) not in source_instances:
                raise TypeError(
                    f"{cls.__name__}: field '{attr_name}' is bound to a source not "
                    f"assigned to any attribute on this deck."
                )

        # Validation: derived fields' parents are declared on this deck
        field_ids = {id(fld) for _, fld in fields}
        for attr_name, fld in fields:
            if fld.is_derived and id(fld._parent) not in field_ids:
                raise TypeError(
                    f"{cls.__name__}: field '{attr_name}' is derived from a field "
                    f"that is not declared on this deck."
                )

        # Validation: no circular derivation + compute order
        derived_order = _resolve_derivation_order(fields)

        # Validation: exactly one PK field
        pk_fields = [(name, fld) for name, fld in fields if fld.pk is not None]
        if len(pk_fields) == 0:
            raise TypeError(
                f"{cls.__name__}: exactly one field must have pk= set. Found 0."
            )
        if len(pk_fields) > 1:
            pk_names = ", ".join(f"'{n}'" for n, _ in pk_fields)
            raise TypeError(
                f"{cls.__name__}: exactly one field must have pk= set. "
                f"Found {len(pk_fields)} ({pk_names})."
            )

        # Store collected metadata on the class
        cls._deck_fields = [fld for _, fld in fields]
        cls._deck_cards = cards
        cls._deck_sources = [src for _, src in sources]
        cls._deck_name = cls.deck_name if hasattr(cls, "deck_name") else cls.__name__
        cls._field_attrs = [name for name, _ in fields]
        cls._pk_field_attr = pk_fields[0][0]
        cls._derived_order = derived_order
        cls._visible_fields = [(name, fld) for name, fld in fields if not fld.internal]
        cls._all_fields = fields
        cls._fields_by_source = {}

        # Group source (non-derived) fields by source
        for attr_name, fld in fields:
            if fld.is_derived:
                continue
            src_id = id(fld._source) if fld._source else None
            if src_id not in cls._fields_by_source:
                cls._fields_by_source[src_id] = []
            cls._fields_by_source[src_id].append((attr_name, fld))

    def __init__(self) -> None:
        self._data: list[dict[str, str]] | None = None
        self._cache = Cache()

    def fetch(self, refresh: bool = False) -> None:
        """Fetch data from all sources."""
        cls = self.__class__
        section_header(f"Fetch: {cls.__name__}")

        source_fields = [(n, f) for n, f in cls._all_fields if not f.is_derived]
        internal_count = sum(1 for _, f in cls._all_fields if f.is_internal)
        derived_count = len(cls._derived_order)
        pk_fld = next(f for n, f in cls._all_fields if n == cls._pk_field_attr)

        total_fields = len(cls._all_fields)
        log_success(
            f"Deck validated: {total_fields} fields, {len(cls._deck_cards)} card type{'s' if len(cls._deck_cards) != 1 else ''}, pk={cls._pk_field_attr} ({pk_fld.pk.name})"
        )
        if internal_count:
            log_info(
                f"  {internal_count} internal field{'s' if internal_count != 1 else ''}"
            )
        if derived_count:
            log_info(
                f"  {derived_count} derived field{'s' if derived_count != 1 else ''}"
            )

        all_rows: list[dict[str, Any]] = []

        for source_attr, source in [
            (name, src)
            for name, src in cls.__dict__.items()
            if not name.startswith("__")
            and hasattr(src, "fetch")
            and hasattr(src, "Field")
            and not isinstance(src, type)
        ]:
            src_id = id(source)
            bound_fields = cls._fields_by_source.get(src_id, [])
            if not bound_fields:
                continue

            log_info(f"Source '{source_attr}': fetching {len(bound_fields)} fields")
            rows = source.fetch(bound_fields, self._cache, refresh)
            all_rows = rows  # For MVP, single source

        # Apply derivation chain
        if cls._derived_order:
            field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
            transforms_applied = []

            for attr_name, fld in cls._derived_order:
                parent_attr = field_id_to_attr[id(fld._parent)]
                transforms_applied.append(f"{attr_name} ← {parent_attr}")

                for row in all_rows:
                    parent_val = row.get(parent_attr, "")

                    # Try to convert to numeric for transform
                    val: Any = parent_val
                    if isinstance(val, str) and val:
                        try:
                            val = float(val)
                            if val == int(val):
                                val = int(val)
                        except (ValueError, TypeError):
                            pass

                    if fld._transform is not None:
                        try:
                            val = fld._transform(val)
                        except Exception as exc:
                            pk_val = row.get(
                                f"_pk_{cls._pk_field_attr}",
                                row.get(cls._pk_field_attr, "?"),
                            )
                            raise RuntimeError(
                                f"{cls.__name__}: transform failed for field '{attr_name}' "
                                f"on row '{pk_val}': {type(exc).__name__}: {exc}. "
                                f"The source value was {parent_val!r}."
                            ) from exc

                    # Apply fmt if present
                    if fld.fmt and val is not None and val != "":
                        try:
                            numeric = (
                                float(val) if not isinstance(val, (int, float)) else val
                            )
                            if isinstance(numeric, float) and numeric == int(numeric):
                                numeric = int(numeric)
                            val = fld.fmt.format(numeric)
                        except (ValueError, TypeError):
                            val = str(val)

                    row[attr_name] = str(val) if val is not None else ""

            log_success(
                f"{len(transforms_applied)} transform{'s' if len(transforms_applied) != 1 else ''} "
                f"applied: {', '.join(transforms_applied)}"
            )

        # Apply fmt to non-derived source fields
        for attr_name, fld in cls._all_fields:
            if fld.is_derived:
                continue  # Already handled in derivation chain
            if not fld.fmt:
                continue
            for row in all_rows:
                val = row.get(attr_name, "")
                if val:
                    try:
                        numeric = float(val)
                        if numeric == int(numeric):
                            numeric = int(numeric)
                        row[attr_name] = fld.fmt.format(numeric)
                    except (ValueError, TypeError):
                        pass

        self._data = all_rows
        log_success(f"Loaded {len(self._data)} rows")

    def preview(self, max_rows: int = 10) -> None:
        """Pretty-print the data as a rich table. Only shows labeled fields."""
        cls = self.__class__
        section_header(f"Preview: {cls.__name__}")

        if self._data is None:
            log_warn("No data loaded. Call fetch() first.")
            return

        table = Table(
            title=cls.__name__,
            box=box.ROUNDED,
            show_lines=True,
            header_style="bold cyan",
        )

        visible = cls._visible_fields
        for attr_name, fld in visible:
            table.add_column(
                attr_name.replace("_", " ").title(), overflow="ellipsis", max_width=40
            )

        for row in self._data[:max_rows]:
            values = [row.get(attr, "") for attr in [n for n, _ in visible]]
            table.add_row(*values)

        console.print(table)
        total = len(self._data)
        shown = min(max_rows, total)
        if shown < total:
            console.print(f"  [dim]{total} rows total (showing {shown})[/dim]")
        else:
            console.print(f"  [dim]{total} rows total[/dim]")

    def export(self, path: str) -> None:
        """Export the deck to an .apkg file."""
        from ankitron.export import export_deck

        export_deck(self, path)

        if (n := warning_count()) > 0:
            log_warn(
                f"[yellow]{n} warning{'s' if n != 1 else ''} emitted during this run.[/yellow]"
            )
