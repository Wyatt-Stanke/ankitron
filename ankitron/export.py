from __future__ import annotations

import html
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

import genanki

from ankitron.enums import MediaType
from ankitron.identity import generate_deck_id, generate_model_id, generate_note_id
from ankitron.logging import (
    log_info,
    log_success,
    log_warn,
    make_progress,
    section_header,
)

if TYPE_CHECKING:
    from ankitron.deck import Deck, Tag


def sanitize_tag(tag: str) -> str:
    """
    Sanitize a tag string for Anki.
    - Replace spaces with hyphens
    - Strip leading/trailing whitespace
    - Collapse consecutive hyphens
    """
    tag = tag.strip()
    tag = re.sub(r"\s+", "-", tag)
    tag = re.sub(r"-+", "-", tag)
    return tag  # noqa: RET504


def resolve_tags(tag_list: list[str | Tag], row: dict) -> list[str]:
    """
    Resolve a mixed list of static strings and Tag objects
    into a flat list of sanitized tag strings for a single row.
    """
    from ankitron.deck import Tag

    resolved = []
    for tag in tag_list:
        try:
            if isinstance(tag, str):
                resolved.append(sanitize_tag(tag))
            elif isinstance(tag, Tag):
                value = tag.resolve(row)
                resolved.append(sanitize_tag(value))
        except Exception as exc:
            # Log warning and skip tag for this note
            pk_val = row.get("_pk_", row.get("id", "?"))
            log_warn(
                f"Tag resolution failed for row '{pk_val}': "
                f"{type(exc).__name__}: {exc}. Skipping tag."
            )
            continue
    return resolved


def build_genanki_model(deck_cls: type[Deck]) -> genanki.Model:
    """Construct a genanki.Model from a Deck subclass's fields and cards."""
    from ankitron.deck import _FIELD_REF_PATTERN
    from ankitron.provenance import ProvenanceConfig, ProvenancePosition, render_provenance_html

    model_id = generate_model_id(deck_cls.__qualname__)

    gk_fields = [{"name": name} for name, f in deck_cls._all_fields if not f.internal]

    # Check provenance config
    prov_config: ProvenanceConfig | None = getattr(deck_cls, "provenance", None)
    prov_enabled = (
        prov_config is not None
        and prov_config.enabled
        and prov_config.position != ProvenancePosition.NONE
    )

    if prov_enabled:
        gk_fields.append({"name": "_ankitron_provenance"})

    gk_templates = []
    for card_cls in deck_cls._deck_cards:
        back = '{{FrontSide}}<hr id="answer">' + card_cls.back

        if prov_enabled:
            card_fields = _FIELD_REF_PATTERN.findall(card_cls.front + card_cls.back)
            back += render_provenance_html(prov_config, card_fields or None)

        gk_templates.append(
            {
                "name": card_cls.__name__,
                "qfmt": card_cls.front,
                "afmt": back,
            }
        )

    return genanki.Model(
        model_id=model_id,
        name=f"ankitron::{deck_cls._deck_name.removeprefix('ankitron::').lstrip(':')}",
        fields=gk_fields,
        templates=gk_templates,
    )


def export_deck(deck_instance: Deck, path: str) -> None:
    """Export a Deck instance to an .apkg file."""
    deck_cls = deck_instance.__class__
    section_header(f"Export: {deck_cls._deck_name}")

    # Phase 3 validation
    if not hasattr(deck_instance, "_data") or deck_instance._data is None:
        raise RuntimeError(
            f"Deck '{deck_cls._deck_name}' has no data loaded. "
            f"Call deck.fetch() before deck.export()."
        )

    if not deck_instance._data:
        raise RuntimeError(
            f"Deck '{deck_cls._deck_name}' has no rows of data. fetch() returned 0 results."
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
            f"Deck '{deck_cls._deck_name}' has duplicate primary key values: {dup_str}. "
            f"Each row must have a unique PK."
        )

    model = build_genanki_model(deck_cls)
    deck_id = generate_deck_id(deck_cls.__qualname__)
    gk_deck = genanki.Deck(deck_id=deck_id, name=deck_cls._deck_name)

    log_info(f"Model ID: {model.model_id}, Deck ID: {deck_id}")
    log_info(f"Generating {len(deck_instance._data)} notes...")

    # Build field attr name list in declaration order, excluding internal
    visible_attrs = [name for name, f in deck_cls._all_fields if not f.internal]

    # Build map of media fields for bundling
    media_fields = {
        name: fld
        for name, fld in deck_cls._all_fields
        if fld.media is not None and not fld.internal
    }
    media_files: list[str] = []  # Paths of media files to bundle

    # Check provenance
    from ankitron.provenance import ProvenanceConfig, ProvenancePosition, provenance_to_json

    prov_config: ProvenanceConfig | None = getattr(deck_cls, "provenance", None)
    prov_enabled = (
        prov_config is not None
        and prov_config.enabled
        and prov_config.position != ProvenancePosition.NONE
    )
    provenance_data = getattr(deck_instance, "_provenance", None)

    # Resolve tags
    if deck_cls._deck_tags:
        static_count = sum(1 for tag in deck_cls._deck_tags if isinstance(tag, str))
        computed_count = sum(1 for tag in deck_cls._deck_tags if not isinstance(tag, str))
        log_info("Resolving tags...")
        log_info(
            f"  {len(deck_cls._deck_tags)} tag rules"
            f" ({static_count} static, {computed_count} computed)"
        )

    with make_progress() as progress:
        task = progress.add_task("Creating notes", total=len(deck_instance._data))
        all_tags_set: set[str] = set()

        for row_idx, row in enumerate(deck_instance._data):
            pk_val = row.get(f"_pk_{pk_field_attr}", row.get(pk_field_attr, ""))
            note_id = generate_note_id(deck_cls.__qualname__, pk_val)

            # Process media fields: replace URL values with <img>/<sound> tags
            for mf_name, mf_fld in media_fields.items():
                val = row.get(mf_name, "")
                if not val:
                    continue
                # Check if value looks like a file path or URL
                if os.path.isfile(val):
                    media_files.append(val)
                    fname = os.path.basename(val)
                    if mf_fld.media == MediaType.IMAGE:
                        from ankitron.media.pipeline import make_img_tag

                        row[mf_name] = make_img_tag(fname, mf_fld.width, mf_fld.height)
                    elif mf_fld.media == MediaType.AUDIO:
                        from ankitron.media.pipeline import make_sound_tag

                        row[mf_name] = make_sound_tag(fname)
                elif val.startswith(("http://", "https://")):
                    # URL-based media — attempt to find in cache
                    cache_dir = Path.home() / ".cache" / "ankitron" / "media"
                    from ankitron.media.pipeline import generate_media_filename

                    ext = mf_fld.format.value if mf_fld.format else "png"
                    fname = generate_media_filename(deck_cls._deck_name, pk_val, mf_name, ext)
                    cached_path = cache_dir / fname
                    if cached_path.is_file():
                        media_files.append(str(cached_path))
                        if mf_fld.media == MediaType.IMAGE:
                            from ankitron.media.pipeline import make_img_tag

                            row[mf_name] = make_img_tag(fname, mf_fld.width, mf_fld.height)
                        elif mf_fld.media == MediaType.AUDIO:
                            from ankitron.media.pipeline import make_sound_tag

                            row[mf_name] = make_sound_tag(fname)

            field_values = [html.escape(row.get(attr, "")) for attr in visible_attrs]

            # Append provenance JSON if enabled
            if prov_enabled and provenance_data and row_idx < len(provenance_data):
                prov_json = provenance_to_json(
                    provenance_data[row_idx],
                    deck_cls._deck_name,
                    pk_val,
                    row.get(pk_field_attr, pk_val),
                    visible_fields=visible_attrs,
                )
                field_values.append(prov_json)
            elif prov_enabled:
                field_values.append("")

            # Resolve tags for this row
            tags_to_add = ["ankitron"]  # Always add "ankitron" tag
            if deck_cls._deck_tags:
                tags_to_add.extend(resolve_tags(deck_cls._deck_tags, row))

            all_tags_set.update(tags_to_add)

            # Deduplicate
            tags_to_add = list(dict.fromkeys(tags_to_add))

            note = genanki.Note(
                model=model,
                fields=field_values,
                guid=note_id,
                tags=tags_to_add,
            )
            gk_deck.add_note(note)
            progress.advance(task)

    # Log tag summary
    if deck_cls._deck_tags:
        log_info(f"  {len(all_tags_set)} unique tags across {len(deck_instance._data)} notes")
        log_success("Tags resolved")

    pkg = genanki.Package(gk_deck)
    if media_files:
        pkg.media_files = media_files
        log_info(f"Bundling {len(media_files)} media files")
    pkg.write_to_file(path)
    log_success(f"Exported {len(deck_instance._data)} notes to [bold]{path}[/bold]")
