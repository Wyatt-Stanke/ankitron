from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from rich import box
from rich.table import Table

from ankitron.cache import Cache
from ankitron.enums import (
    AnkiTemplate,
    FieldKind,
    FieldRule,
    MediaFormat,
    MediaType,
    PKStrategy,
    Severity,
)
from ankitron.logging import (
    console,
    log_info,
    log_success,
    log_warn,
    section_header,
    warning_count,
)
from ankitron.sources.wikidata.properties import PropertyValueType
from ankitron.transform import Transform, apply_transform_chain

_FIELD_REF_PATTERN = re.compile(
    r"\{\{(?!FrontSide|Tags|Type|Deck|Subdeck|CardFlag|Card|#|/|\^|type:|hint:|text:|cloze:|type:cloze:|type:nc:|c\d+::)(\w+)\}\}"
)


def _coerce_numeric(value: Any) -> Any:
    """Try to convert a string value to int or float, returning original if not possible."""
    if isinstance(value, str) and value:
        try:
            n = float(value)
            return int(n) if n == int(n) else n
        except (ValueError, TypeError):
            pass
    return value


@dataclass
class Field:
    """Represents a single piece of data on every row/record."""

    kind: FieldKind = FieldKind.TEXT
    fmt: str | None = None
    pk: PKStrategy | None = None
    internal: bool = False
    unused_ok: bool = False
    rule: FieldRule = FieldRule.OPTIONAL
    default: Any = None
    media: MediaType | None = None
    format: MediaFormat | None = None
    width: int | None = None
    height: int | None = None

    # Set automatically by __set_name__
    name: str | None = dc_field(default=None, repr=False)

    # Internal — set by source.Field(), not by the user
    _source: Any = dc_field(default=None, repr=False)
    _source_key: str | None = dc_field(default=None, repr=False)
    _source_value_type: PropertyValueType = dc_field(default=PropertyValueType.LITERAL, repr=False)

    # Internal — set by .derive(), not by the user
    _parent: Field | None = dc_field(default=None, repr=False)
    _transform: Transform | Callable | None = dc_field(default=None, repr=False)

    # Internal — set by .verify()
    _verify_config: Any | None = dc_field(default=None, repr=False)

    # Internal — set by Field.computed()
    _computed_fn: Callable | None = dc_field(default=None, repr=False)
    _computed_inputs: list[Field] | None = dc_field(default=None, repr=False)

    # Internal — set by Field.cascade()
    _cascade_sources: list[Field] | None = dc_field(default=None, repr=False)

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    @property
    def is_derived(self) -> bool:
        return self._parent is not None

    @property
    def is_computed(self) -> bool:
        return self._computed_fn is not None

    @property
    def is_cascade(self) -> bool:
        return self._cascade_sources is not None

    @property
    def is_internal(self) -> bool:
        return self.internal

    def derive(
        self,
        transform: Transform | Callable | None = None,
        fmt: str | None = None,
        kind: FieldKind = FieldKind.TEXT,
        internal: bool = False,
        unused_ok: bool = False,
        rule: FieldRule = FieldRule.OPTIONAL,
    ) -> Field:
        """Create a derived field whose value is computed from this field's value."""
        # Wrap bare callables in Transform.custom()
        xform = transform
        if callable(transform) and not isinstance(transform, Transform):
            xform = Transform.custom(transform, name="custom", description="custom transform")
        return Field(
            kind=kind,
            fmt=fmt,
            internal=internal,
            unused_ok=unused_ok,
            rule=rule,
            _parent=self,
            _transform=xform,
        )

    def verify(
        self,
        against: Any,
        strategy: Any = None,
        on_mismatch: Any = None,
    ) -> Field:
        """Configure cross-source verification for this field. Returns self for chaining."""
        from ankitron.validation.verification import OnMismatch, VerifyConfig, VerifyStrategy

        if strategy is None:
            strategy = VerifyStrategy.EXACT
        if on_mismatch is None:
            on_mismatch = OnMismatch.WARN
        self._verify_config = {
            "against": against,
            "config": VerifyConfig(strategy=strategy, on_mismatch=on_mismatch),
        }
        return self

    @staticmethod
    def computed(
        fn: Callable[..., Any],
        inputs: list[Field],
        fmt: str | None = None,
        kind: FieldKind = FieldKind.TEXT,
        internal: bool = False,
        unused_ok: bool = False,
        rule: FieldRule = FieldRule.OPTIONAL,
    ) -> Field:
        """Create a computed field derived from multiple input fields."""
        return Field(
            kind=kind,
            fmt=fmt,
            internal=internal,
            unused_ok=unused_ok,
            rule=rule,
            _computed_fn=fn,
            _computed_inputs=inputs,
        )

    @staticmethod
    def cascade(
        *sources: Field,
        fmt: str | None = None,
        kind: FieldKind = FieldKind.TEXT,
        internal: bool = False,
        unused_ok: bool = False,
        rule: FieldRule = FieldRule.OPTIONAL,
    ) -> Field:
        """Create a field that tries multiple sources in priority order."""
        return Field(
            kind=kind,
            fmt=fmt,
            internal=internal,
            unused_ok=unused_ok,
            rule=rule,
            _cascade_sources=list(sources),
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


@dataclass
class Tag:
    """A dynamic tag that resolves per-row at export time."""

    _resolve_fn: Callable[[dict], str]  # type: ignore[type-arg]
    prefix: str | None = None
    _field_name: str | None = None  # Set for from_field tags to track the field reference

    @staticmethod
    def from_field(field: Field, prefix: str | None = None) -> Tag:
        """
        Create a tag from a field's value. The field's resolved value
        for each row becomes the tag string.
        """
        return Tag(
            _resolve_fn=lambda row: row[field.name],
            prefix=prefix,
            _field_name=field.name,
        )

    @staticmethod
    def computed(transform: Callable[[dict], str], prefix: str | None = None) -> Tag:
        """
        Create a tag from a function. The function receives the full
        row dict and must return a string.
        """
        return Tag(_resolve_fn=transform, prefix=prefix)

    def resolve(self, row: dict) -> str:
        """Resolve this tag for a given row of data."""
        value = self._resolve_fn(row)
        if self.prefix:
            return f"{self.prefix}::{value}"
        return value


def _resolve_derivation_order(
    fields: list[tuple[str, Field]],
) -> list[tuple[str, Field]]:
    """
    Return derived and computed fields in dependency order (topological sort).
    Raises TypeError on circular derivation.
    """
    dependent = [(name, fld) for name, fld in fields if fld.is_derived or fld.is_computed]

    resolved: list[tuple[str, Field]] = []
    resolved_ids: set[int] = set()

    # All non-derived, non-computed fields are already resolved
    for _, fld in fields:
        if not fld.is_derived and not fld.is_computed:
            resolved_ids.add(id(fld))

    remaining = list(dependent)
    max_iterations = len(remaining) + 1
    for _ in range(max_iterations):
        if not remaining:
            break
        next_remaining = []
        for name, fld in remaining:
            if fld.is_derived:
                # Derived: depends on its parent field
                if id(fld._parent) in resolved_ids:
                    resolved.append((name, fld))
                    resolved_ids.add(id(fld))
                else:
                    next_remaining.append((name, fld))
            elif fld.is_computed:
                # Computed: depends on all input fields
                if all(id(inp) in resolved_ids for inp in fld._computed_inputs):
                    resolved.append((name, fld))
                    resolved_ids.add(id(fld))
                else:
                    next_remaining.append((name, fld))
        if len(next_remaining) == len(remaining):
            cycle_names = [n for n, _ in next_remaining]
            raise TypeError(f"Circular derivation detected among fields: {', '.join(cycle_names)}")
        remaining = next_remaining

    return resolved


def _collect_deck_members(
    cls: type,
) -> tuple[list[tuple[str, Field]], list[type[Card]], list[tuple[str, Any]]]:
    """Collect fields, cards, and source instances from a Deck subclass body."""
    fields: list[tuple[str, Field]] = []
    cards: list[type[Card]] = []
    sources: list[tuple[str, Any]] = []

    for attr_name, attr_value in cls.__dict__.items():
        if isinstance(attr_value, Field):
            fields.append((attr_name, attr_value))
        elif (
            isinstance(attr_value, type) and issubclass(attr_value, Card) and attr_value is not Card
        ):
            cards.append(attr_value)
        elif (
            hasattr(attr_value, "Field")
            and hasattr(attr_value, "fetch")
            and not isinstance(attr_value, type)
            and not attr_name.startswith("__")
        ):
            sources.append((attr_name, attr_value))

    return fields, cards, sources


def _validate_deck_structure(
    cls: type,
    fields: list[tuple[str, Field]],
    cards: list[type[Card]],
    sources: list[tuple[str, Any]],
) -> None:
    """Validate that a Deck subclass has correct structure and references."""
    if not fields:
        raise TypeError(f"{cls.__name__}: no fields declared.")
    if not cards:
        raise TypeError(f"{cls.__name__}: no card types declared.")

    for card_cls in cards:
        if not (hasattr(card_cls, "front") and hasattr(card_cls, "back")):
            raise TypeError(
                f"{cls.__name__}.{card_cls.__name__}:"
                " Card subclass must define both 'front' and 'back'."
            )

    field_names = {name for name, _ in fields}
    internal_field_names = {name for name, fld in fields if fld.internal}
    non_internal_field_names = field_names - internal_field_names

    # Validate card template references
    all_referenced_fields: set[str] = set()
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
                    suggestions = [n for n in non_internal_field_names if ref in n or n in ref]
                    hint = f" Did you mean '{suggestions[0]}'?" if suggestions else ""
                    raise TypeError(
                        f"{cls.__name__}.{card_cls.__name__}: template references "
                        f"'{{{{{ref}}}}}', but that field is marked as internal.{hint}"
                    )

    for attr_name, fld in fields:
        if fld.internal or fld.unused_ok:
            continue
        if attr_name not in all_referenced_fields:
            log_warn(
                f"{cls.__name__}: field '{attr_name}' is not referenced "
                "in any card template and is not marked as internal.",
            )

    # Validate source bindings
    source_instances = {id(src): src for _, src in sources}
    for attr_name, fld in fields:
        if fld._source is not None and id(fld._source) not in source_instances:
            raise TypeError(
                f"{cls.__name__}: field '{attr_name}' is bound to a source not "
                f"assigned to any attribute on this deck."
            )

    # Validate derivation dependencies
    field_ids = {id(fld) for _, fld in fields}
    for attr_name, fld in fields:
        if fld.is_derived and id(fld._parent) not in field_ids:
            raise TypeError(
                f"{cls.__name__}: field '{attr_name}' is derived from a field "
                f"that is not declared on this deck."
            )
        if fld.is_computed:
            for inp in fld._computed_inputs:
                if id(inp) not in field_ids:
                    raise TypeError(
                        f"{cls.__name__}: field '{attr_name}' has a computed input "
                        f"that is not declared on this deck."
                    )


def _validate_pk(cls: type, pk_fields: list[tuple[str, Field]]) -> None:
    """Validate exactly one PK field is declared."""
    if len(pk_fields) == 0:
        raise TypeError(f"{cls.__name__}: exactly one field must have pk= set. Found 0.")
    if len(pk_fields) > 1:
        pk_names = ", ".join(f"'{n}'" for n, _ in pk_fields)
        raise TypeError(
            f"{cls.__name__}: exactly one field must have pk= set. "
            f"Found {len(pk_fields)} ({pk_names})."
        )


def _validate_tags(cls: type, fields: list[tuple[str, Field]]) -> None:
    """Validate the tags list on a Deck subclass."""
    field_names = {name for name, _ in fields}
    tag_list = getattr(cls, "tags", [])
    for tag_entry in tag_list:
        if not isinstance(tag_entry, (str, Tag)):
            raise TypeError(
                f"{cls.__name__}: tags list contains invalid entry"
                f" of type {type(tag_entry).__name__}."
                " Expected str or Tag."
            )
        if (
            isinstance(tag_entry, Tag)
            and tag_entry._field_name
            and tag_entry._field_name not in field_names
        ):
            raise TypeError(
                f"{cls.__name__}: tag references field"
                f" '{tag_entry._field_name}',"
                " which is not declared on this deck."
            )


def _store_deck_metadata(
    cls: type,
    fields: list[tuple[str, Field]],
    cards: list[type[Card]],
    sources: list[tuple[str, Any]],
    derived_order: list[tuple[str, Field]],
    pk_fields: list[tuple[str, Field]],
) -> None:
    """Store collected metadata on the Deck subclass."""
    cls._deck_fields = [fld for _, fld in fields]
    cls._deck_cards = cards
    cls._deck_sources = [src for _, src in sources]
    cls._deck_name = getattr(cls, "deck_name", cls.__name__)
    cls._field_attrs = [name for name, _ in fields]
    cls._pk_field_attr = pk_fields[0][0]
    cls._derived_order = derived_order
    cls._visible_fields = [(name, fld) for name, fld in fields if not fld.internal]
    cls._all_fields = fields
    cls._deck_tags = getattr(cls, "tags", [])
    cls._deck_validators = getattr(cls, "validators", [])
    cls._deck_overrides = getattr(cls, "overrides", {})
    cls._fields_by_source = {}

    for attr_name, fld in fields:
        if fld.is_derived:
            continue
        src_id = id(fld._source) if fld._source else None
        if src_id not in cls._fields_by_source:
            cls._fields_by_source[src_id] = []
        cls._fields_by_source[src_id].append((attr_name, fld))


def _toposort_sources(
    sources: list[tuple[str, Any]],
) -> list[tuple[str, Any]]:
    """Sort sources by dependency (linked_to) order.

    Sources without dependencies come first. Sources that depend on others
    come after their dependencies.
    """
    if len(sources) <= 1:
        return sources

    id_to_entry = {id(src): (name, src) for name, src in sources}
    in_degree: dict[int, int] = {id(src): 0 for _, src in sources}
    dependents: dict[int, list[int]] = {id(src): [] for _, src in sources}

    for _, src in sources:
        linked = getattr(src, "_linked_to", None)
        if linked is not None and id(linked) in id_to_entry:
            in_degree[id(src)] += 1
            dependents[id(linked)].append(id(src))

    # Kahn's algorithm
    queue = [sid for sid, deg in in_degree.items() if deg == 0]
    result: list[tuple[str, Any]] = []

    while queue:
        sid = queue.pop(0)
        result.append(id_to_entry[sid])
        for dep_id in dependents[sid]:
            in_degree[dep_id] -= 1
            if in_degree[dep_id] == 0:
                queue.append(dep_id)

    if len(result) != len(sources):
        raise TypeError("Circular source dependency detected")

    return result


def _merge_linked_rows(
    all_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    bound_fields: list[tuple[str, Any]],
    _source: Any,
    pk_field_attr: str,
) -> None:
    """Merge data from a linked source into existing rows.

    For each bound field, copies the value from new_rows into all_rows
    by matching on position (if same length) or PK.
    """
    field_attrs = [attr for attr, _ in bound_fields]

    if len(new_rows) == len(all_rows):
        # Same-length — positional merge
        for i, row in enumerate(all_rows):
            for attr in field_attrs:
                if attr in new_rows[i]:
                    row[attr] = new_rows[i][attr]
    else:
        # Build index on new rows by PK
        pk_key = f"_pk_{pk_field_attr}"
        new_by_pk: dict[str, dict] = {}
        for nr in new_rows:
            pk = nr.get(pk_key, nr.get(pk_field_attr, ""))
            if pk:
                new_by_pk[pk] = nr

        for row in all_rows:
            pk = row.get(pk_key, row.get(pk_field_attr, ""))
            matched = new_by_pk.get(pk)
            if matched:
                for attr in field_attrs:
                    if attr in matched:
                        row[attr] = matched[attr]


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
    _deck_tags: list[str | Tag]
    _deck_validators: list[Any]
    _deck_overrides: dict[str, dict[str, Any]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        fields, cards, sources = _collect_deck_members(cls)
        _validate_deck_structure(cls, fields, cards, sources)
        derived_order = _resolve_derivation_order(fields)
        pk_fields = [(name, fld) for name, fld in fields if fld.pk is not None]
        _validate_pk(cls, pk_fields)
        _validate_tags(cls, fields)
        _store_deck_metadata(cls, fields, cards, sources, derived_order, pk_fields)

    def __init__(self) -> None:
        self._data: list[dict[str, str]] | None = None
        self._cache = Cache()

    def fetch(self, refresh: bool = False, skip_validation: bool = False) -> None:
        """Fetch data from all sources.

        Execution order:
        1. Source fetching
        2. FieldRule checks (REQUIRED → error, EXPECTED → warn)
        3. Default values applied
        4. Overrides applied
        5. Cascade resolution
        6. Computed and derived fields (with Transform support)
        7. Formatting
        8. Validators
        """
        cls = self.__class__
        section_header(f"Fetch: {cls.__name__}")

        # (source_fields used for logging only)
        internal_count = sum(1 for _, f in cls._all_fields if f.is_internal)
        derived_count = len(cls._derived_order)
        pk_fld = next(f for n, f in cls._all_fields if n == cls._pk_field_attr)

        total_fields = len(cls._all_fields)
        card_count = len(cls._deck_cards)
        card_label = "card types" if card_count != 1 else "card type"
        log_success(
            f"Deck validated: {total_fields} fields, {card_count} {card_label},"
            f" pk={cls._pk_field_attr} ({pk_fld.pk.name})"
        )
        if internal_count:
            log_info(f"  {internal_count} internal field{'s' if internal_count != 1 else ''}")
        if derived_count:
            log_info(f"  {derived_count} derived field{'s' if derived_count != 1 else ''}")

        all_rows: list[dict[str, Any]] = []

        # Provenance tracking
        from ankitron.provenance import ProvenanceConfig, ProvenancePosition, ProvenanceRecord

        prov_config: ProvenanceConfig | None = getattr(cls, "provenance", None)
        prov_enabled = (
            prov_config is not None
            and prov_config.enabled
            and prov_config.position != ProvenancePosition.NONE
        )
        all_provenance: list[dict[str, ProvenanceRecord]] = []

        # ── 1. Source fetching ──
        # Collect all source instances with their attribute names
        source_entries = [
            (name, src)
            for name, src in cls.__dict__.items()
            if not name.startswith("__")
            and hasattr(src, "fetch")
            and hasattr(src, "Field")
            and not isinstance(src, type)
        ]

        # Topological sort based on linked_to dependencies
        sorted_sources = _toposort_sources(source_entries)

        for source_attr, source in sorted_sources:
            src_id = id(source)
            bound_fields = cls._fields_by_source.get(src_id, [])
            if not bound_fields:
                continue

            log_info(f"Source '{source_attr}': fetching {len(bound_fields)} fields")

            # For linked sources, pass existing rows so they can join
            if hasattr(source, "_linked_to") and source._linked_to is not None and all_rows:
                rows = source.fetch(bound_fields, self._cache, refresh)
                # Merge linked source data into existing rows
                _merge_linked_rows(all_rows, rows, bound_fields, source, cls._pk_field_attr)
            else:
                rows = source.fetch(bound_fields, self._cache, refresh)
                if not all_rows:
                    all_rows = rows
                else:
                    # Merge additional source data into existing rows
                    _merge_linked_rows(all_rows, rows, bound_fields, source, cls._pk_field_attr)

        # Initialise provenance records after source fetching
        if prov_enabled:
            source_attr_by_id = {id(src): name for name, src in source_entries}
            for _row_idx, row in enumerate(all_rows):
                prov_row: dict[str, ProvenanceRecord] = {}
                for attr_name, fld in cls._all_fields:
                    if fld.is_derived or fld.is_computed:
                        continue
                    src = fld._source
                    prov_row[attr_name] = ProvenanceRecord(
                        source_type=type(src).__name__ if src else "unknown",
                        source_name=source_attr_by_id.get(id(src), "") if src else "",
                        source_key=fld._source_key,
                        raw_value=row.get(attr_name),
                        raw_type=type(row.get(attr_name)).__name__,
                    )
                all_provenance.append(prov_row)

        # ── 2. FieldRule checks ──
        # FieldRule checks
        for attr_name, fld in cls._all_fields:
            if fld.is_derived or fld.is_computed:
                continue  # Checked after derivation
            if fld.rule == FieldRule.OPTIONAL:
                continue
            missing_pks = []
            for row in all_rows:
                val = row.get(attr_name)
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    pk_val = row.get(f"_pk_{cls._pk_field_attr}", row.get(cls._pk_field_attr, "?"))
                    missing_pks.append(str(pk_val))
            if missing_pks:
                msg = (
                    f"Field '{attr_name}' ({fld.rule.value}): "
                    f"{len(missing_pks)} row(s) missing values"
                )
                if fld.rule == FieldRule.REQUIRED:
                    raise RuntimeError(f"{cls.__name__}: {msg}. Aborting.")
                if fld.rule == FieldRule.EXPECTED:
                    log_warn(f"{cls.__name__}: {msg}")

        # ── 3. Default values applied ──
        for attr_name, fld in cls._all_fields:
            if fld.default is None:
                continue
            if fld.is_derived or fld.is_computed:
                continue
            for row in all_rows:
                val = row.get(attr_name)
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    row[attr_name] = (
                        str(fld.default) if not isinstance(fld.default, str) else fld.default
                    )

        # ── 4. Overrides applied ──
        if cls._deck_overrides:
            pk_attr = cls._pk_field_attr
            overrides_applied = 0
            for row_idx, row in enumerate(all_rows):
                pk_val = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
                if pk_val in cls._deck_overrides:
                    for override_field, override_val in cls._deck_overrides[pk_val].items():
                        # Track provenance for override
                        if prov_enabled and row_idx < len(all_provenance):
                            prov_rec = all_provenance[row_idx].get(override_field)
                            if prov_rec:
                                prov_rec.overridden = True
                                prov_rec.original_value = row.get(override_field)
                        row[override_field] = (
                            str(override_val) if not isinstance(override_val, str) else override_val
                        )
                        overrides_applied += 1
            if overrides_applied:
                log_info(f"  {overrides_applied} override(s) applied")

        # ── 5. Cascade resolution ──
        cascade_fields = [(n, f) for n, f in cls._all_fields if f.is_cascade]
        if cascade_fields:
            field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
            for attr_name, fld in cascade_fields:
                source_attrs = [field_id_to_attr[id(src)] for src in fld._cascade_sources]
                for row in all_rows:
                    for src_attr in source_attrs:
                        val = row.get(src_attr)
                        if val is not None and (not isinstance(val, str) or val.strip()):
                            row[attr_name] = val
                            break
                    else:
                        row[attr_name] = ""

        # ── 6. Derivation chain (derived + computed fields) ──
        if cls._derived_order:
            field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
            transforms_applied = []

            for attr_name, fld in cls._derived_order:
                if fld.is_computed:
                    # Computed field: apply function to multiple inputs
                    input_attrs = [field_id_to_attr[id(inp)] for inp in fld._computed_inputs]
                    transforms_applied.append(f"{attr_name} ← computed({', '.join(input_attrs)})")

                    for row in all_rows:
                        input_vals = []
                        for inp_attr in input_attrs:
                            v = row.get(inp_attr, "")
                            input_vals.append(_coerce_numeric(v))

                        try:
                            val = fld._computed_fn(*input_vals)
                        except Exception as exc:
                            pk_val = row.get(
                                f"_pk_{cls._pk_field_attr}",
                                row.get(cls._pk_field_attr, "?"),
                            )
                            raise RuntimeError(
                                f"{cls.__name__}: computed field '{attr_name}' failed "
                                f"on row '{pk_val}': {type(exc).__name__}: {exc}."
                            ) from exc

                        row[attr_name] = str(val) if val is not None else ""

                elif fld.is_derived:
                    parent_attr = field_id_to_attr[id(fld._parent)]
                    transforms_applied.append(f"{attr_name} ← {parent_attr}")

                    if isinstance(fld._transform, Transform):
                        # Use the Transform API
                        if fld._transform.is_dataset_aware:
                            # Dataset-aware: batch apply
                            parent_vals = [row.get(parent_attr, "") for row in all_rows]
                            converted = [
                                _coerce_numeric(v) if v != "" else None for v in parent_vals
                            ]
                            results = apply_transform_chain(fld._transform, converted)
                            for row, val in zip(all_rows, results, strict=False):
                                row[attr_name] = str(val) if val is not None else ""
                        else:
                            # Per-row apply
                            for row in all_rows:
                                parent_val = row.get(parent_attr, "")
                                val: Any = _coerce_numeric(parent_val)
                                try:
                                    val = fld._transform.apply(val)
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
                                row[attr_name] = str(val) if val is not None else ""
                    else:
                        # Legacy callable transform
                        for row in all_rows:
                            parent_val = row.get(parent_attr, "")
                            val = _coerce_numeric(parent_val)

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

                            row[attr_name] = str(val) if val is not None else ""

                # Apply fmt per-row for this derived/computed field
                if fld.fmt:
                    for row in all_rows:
                        val = row.get(attr_name, "")
                        if val:
                            numeric = _coerce_numeric(val)
                            if isinstance(numeric, (int, float)):
                                with contextlib.suppress(ValueError, TypeError):
                                    row[attr_name] = fld.fmt.format(numeric)

            log_success(
                f"{len(transforms_applied)} transform{'s' if len(transforms_applied) != 1 else ''} "
                f"applied: {', '.join(transforms_applied)}"
            )

        # ── 6. Format non-derived source fields ──
        for attr_name, fld in cls._all_fields:
            if fld.is_derived or fld.is_computed:
                continue
            if not fld.fmt:
                continue
            for row_idx, row in enumerate(all_rows):
                val = row.get(attr_name, "")
                if val:
                    numeric = _coerce_numeric(val)
                    if isinstance(numeric, (int, float)):
                        with contextlib.suppress(ValueError, TypeError):
                            formatted = fld.fmt.format(numeric)
                            row[attr_name] = formatted
                            if prov_enabled and row_idx < len(all_provenance):
                                prov_rec = all_provenance[row_idx].get(attr_name)
                                if prov_rec:
                                    prov_rec.fmt = fld.fmt
                                    prov_rec.formatted_value = formatted

        # Populate provenance for derived/computed fields
        if prov_enabled:
            field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
            for row_idx in range(len(all_rows)):
                if row_idx >= len(all_provenance):
                    break
                prov_row = all_provenance[row_idx]
                for attr_name, fld in cls._all_fields:
                    if attr_name in prov_row:
                        continue
                    rec = ProvenanceRecord(
                        source_type="derived" if fld.is_derived else "computed",
                        source_name="",
                        raw_value=all_rows[row_idx].get(attr_name),
                        raw_type=type(all_rows[row_idx].get(attr_name)).__name__,
                    )
                    if fld.is_derived and fld._parent:
                        rec.derived_from = field_id_to_attr.get(id(fld._parent))
                    if fld.is_computed and fld._computed_inputs:
                        rec.computed_from = [
                            field_id_to_attr.get(id(inp), "?") for inp in fld._computed_inputs
                        ]
                    prov_row[attr_name] = rec

        # ── 7. Validators ──
        if cls._deck_validators and not skip_validation:
            from ankitron.validation import run_validators

            results = run_validators(cls._deck_validators, all_rows)
            for result in results:
                if result.passed:
                    log_success(f"Validator '{result.name}': passed")
                else:
                    msg = f"Validator '{result.name}': FAILED — {'; '.join(result.messages[:3])}"
                    if result.severity == Severity.ERROR:
                        raise RuntimeError(f"{cls.__name__}: {msg}")
                    log_warn(msg)

        self._data = all_rows
        if prov_enabled:
            self._provenance = all_provenance
            log_info(f"Provenance: {len(all_provenance)} rows x {len(cls._all_fields)} fields")
        log_success(f"Loaded {len(self._data)} rows")

    def preview(self, max_rows: int = 10, mode: str = "table") -> None:
        """Pretty-print the data or launch live preview server.

        Args:
            max_rows: Max rows to show in table mode.
            mode: "table" for rich table output, "live" for browser-based live preview.
        """
        cls = self.__class__

        if mode == "live":
            from ankitron.preview.server import run_preview_server

            run_preview_server(deck_instance=self)
            return

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
        for attr_name, _fld in visible:
            table.add_column(attr_name.replace("_", " ").title(), overflow="ellipsis", max_width=40)

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
