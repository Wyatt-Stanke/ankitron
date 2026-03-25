from __future__ import annotations

import genanki
import html

from ankitron.identity import generate_deck_id, generate_model_id, generate_note_id
from ankitron.logging import (
    section_header,
    log_info,
    log_success,
    make_progress,
    console,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ankitron.deck import Deck


def build_genanki_model(deck_cls: type[Deck]) -> genanki.Model:
    """Construct a genanki.Model from a Deck subclass's fields and cards."""
    model_id = generate_model_id(deck_cls.__qualname__)

    gk_fields = [{"name": name} for name, f in deck_cls._all_fields if not f.internal]

    gk_templates = []
    for card_cls in deck_cls._deck_cards:
        gk_templates.append(
            {
                "name": card_cls.__name__,
                "qfmt": card_cls.front,
                "afmt": '{{FrontSide}}<hr id="answer">' + card_cls.back,
            }
        )

    return genanki.Model(
        model_id=model_id,
        name=f"ankitron::{deck_cls._deck_name}",
        fields=gk_fields,
        templates=gk_templates,
    )


def export_deck(deck_instance: Deck, path: str) -> None:
    """Export a Deck instance to an .apkg file."""
    deck_cls = deck_instance.__class__
    section_header(f"Export: {deck_cls.__name__}")

    # Phase 3 validation
    if not hasattr(deck_instance, "_data") or deck_instance._data is None:
        raise RuntimeError(
            f"Deck '{deck_cls.__name__}' has no data loaded. "
            f"Call deck.fetch() before deck.export()."
        )

    if not deck_instance._data:
        raise RuntimeError(
            f"Deck '{deck_cls.__name__}' has no rows of data. "
            f"fetch() returned 0 results."
        )

    # Check PK uniqueness
    pk_field_attr = deck_instance._pk_field_attr
    pk_values: list[str] = []
    for row in deck_instance._data:
        pk_val = row.get(f"_pk_{pk_field_attr}", row.get(pk_field_attr, ""))
        pk_values.append(pk_val)

    seen: dict[str, int] = {}
    for v in pk_values:
        seen[v] = seen.get(v, 0) + 1
    duplicates = {k: cnt for k, cnt in seen.items() if cnt > 1}
    if duplicates:
        dup_str = ", ".join(f"{k!r} ({cnt}x)" for k, cnt in duplicates.items())
        raise RuntimeError(
            f"Deck '{deck_cls.__name__}' has duplicate primary key values: {dup_str}. "
            f"Each row must have a unique PK."
        )

    model = build_genanki_model(deck_cls)
    deck_id = generate_deck_id(deck_cls.__qualname__)
    gk_deck = genanki.Deck(deck_id=deck_id, name=deck_cls._deck_name)

    log_info(f"Model ID: {model.model_id}, Deck ID: {deck_id}")
    log_info(f"Generating {len(deck_instance._data)} notes...")

    # Build field attr name list in declaration order, excluding internal
    visible_attrs = [name for name, f in deck_cls._all_fields if not f.internal]

    with make_progress() as progress:
        task = progress.add_task("Creating notes", total=len(deck_instance._data))
        for row in deck_instance._data:
            pk_val = row.get(f"_pk_{pk_field_attr}", row.get(pk_field_attr, ""))
            note_id = generate_note_id(deck_cls.__qualname__, pk_val)

            field_values = [html.escape(row.get(attr, "")) for attr in visible_attrs]

            note = genanki.Note(
                model=model,
                fields=field_values,
                guid=note_id,
            )
            gk_deck.add_note(note)
            progress.advance(task)

    genanki.Package(gk_deck).write_to_file(path)
    log_success(f"Exported {len(deck_instance._data)} notes to [bold]{path}[/bold]")
