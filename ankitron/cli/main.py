"""
ankitron CLI — main entry point and command dispatch.

Usage: ankitron <command> [options] [arguments]
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import argparse


def _load_deck_module(filepath: str) -> list[Any]:
    """Import a Python file and return all Deck/DeckFamily subclasses found.

    Raises ImportError or RuntimeError on failure (does not call sys.exit).
    """
    import importlib.util

    from ankitron.deck import Deck
    from ankitron.deck_family import DeckFamily

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"file not found: {filepath}")

    spec = importlib.util.spec_from_file_location("_deck_module", filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {filepath}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    decks: list[Any] = []
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue
        obj = getattr(module, attr_name)
        if not isinstance(obj, type):
            continue
        if (
            issubclass(obj, DeckFamily)
            and obj is not DeckFamily
            and not getattr(obj, "_is_abstract", False)
        ):
            decks.extend(obj.expand())
        elif issubclass(obj, Deck) and obj is not Deck:
            decks.append(obj)

    return decks


def _discover_decks(filepath: str, deck_filter: list[str] | None = None) -> list[Any]:
    """Import a Python file (or walk a directory) and discover all Deck/DeckFamily subclasses."""
    filepath = os.path.abspath(filepath)

    if os.path.isdir(filepath):
        return _discover_decks_recursive(filepath, deck_filter)

    try:
        decks = _load_deck_module(filepath)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(3)

    if deck_filter:
        decks = [d for d in decks if d.__name__ in deck_filter or d._deck_name in deck_filter]

    return decks


def _discover_decks_recursive(dirpath: str, deck_filter: list[str] | None = None) -> list[Any]:
    """Walk a directory tree and discover all Deck/DeckFamily subclasses."""
    all_decks: list[Any] = []

    for root, dirs, files in os.walk(dirpath):
        # Skip hidden/private directories
        dirs[:] = [d for d in dirs if not d.startswith(("_", "."))]

        for filename in sorted(files):
            if not filename.endswith(".py"):
                continue
            if filename.startswith(("_", ".")):
                continue
            if (
                filename == "conftest.py"
                or filename.startswith("test_")
                or filename.endswith("_test.py")
            ):
                continue

            filepath = os.path.join(root, filename)
            try:
                found = _load_deck_module(filepath)
                if deck_filter:
                    found = [
                        d for d in found if d.__name__ in deck_filter or d._deck_name in deck_filter
                    ]
                all_decks.extend(found)
            except Exception:  # noqa: S112
                continue

    return all_decks


def _cmd_build(args: argparse.Namespace) -> int:
    """Execute the build command."""
    from ankitron.logging import warning_count

    decks = _discover_decks(args.file, args.deck)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    # Filter DeckFamily variants by --params (e.g. "lesson=3")
    if args.params:
        kv_pairs = dict(kv.split("=", 1) for kv in args.params.split(","))
        filtered = []
        for d in decks:
            family_params = getattr(d, "_family_params", None)
            if family_params is None:
                filtered.append(d)  # not a variant, keep
            elif all(str(family_params.get(k)) == v for k, v in kv_pairs.items()):
                filtered.append(d)
        decks = filtered
        if not decks:
            print("Error: --params filter matched no deck variants.", file=sys.stderr)
            return 4

    if args.output_file and len(decks) > 1:
        print("Error: --output-file can only be used with single-deck files.", file=sys.stderr)
        return 1

    if args.merge and args.merge_by_directory:
        print("Error: --merge and --merge-by-directory are mutually exclusive.", file=sys.stderr)
        return 1

    if args.dry_run:
        for deck_cls in decks:
            print(f"Deck: {deck_cls._deck_name}")
            print(f"  Fields: {len(deck_cls._all_fields)}")
            print(f"  Cards: {len(deck_cls._deck_cards)}")
            print(f"  Sources: {len(deck_cls._deck_sources)}")
        return 0

    # Determine effective refresh flag: --force or --refresh or any granular --refresh-*
    do_refresh = (
        args.refresh
        or args.force
        or args.refresh_media
        or args.refresh_maps
        or args.refresh_ai
        or bool(args.refresh_source)
    )

    instances: list[Any] = []
    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch(
            refresh=do_refresh,
            skip_validation=args.skip_validation,
        )
        instances.append(instance)

    if args.format != "apkg":
        for instance in instances:
            _export_alt_format(instance, args)
    elif args.merge:
        # Merge all decks into a single .apkg
        _export_merged(instances, args)
    else:
        for instance in instances:
            output_path = _build_output_path(instance, args)
            instance.export(output_path)

    if args.fail_on_warn and warning_count() > 0:
        return 2

    return 0


def _build_output_path(instance: Any, args: argparse.Namespace) -> str:
    """Compute the output .apkg path respecting --flat, --output-file, --output-dir."""
    cls = instance.__class__

    if args.output_file:
        return os.path.join(args.output_dir, args.output_file)

    safe_name = cls.__name__.lower()
    ext = args.format if args.format != "apkg" else "apkg"

    if args.flat or not hasattr(cls, "_family_qualname"):
        # Flat: everything in output_dir directly
        return os.path.join(args.output_dir, f"{safe_name}.{ext}")

    # Preserve hierarchy from deck_name (e.g. "Spanish Vocabulary::Lesson 1" → subdir)
    deck_name = getattr(cls, "_deck_name", safe_name)
    parts = [p.strip().replace(" ", "_").lower() for p in deck_name.split("::")]
    if len(parts) > 1:
        subdir = os.path.join(args.output_dir, *parts[:-1])
        os.makedirs(subdir, exist_ok=True)
        return os.path.join(subdir, f"{safe_name}.{ext}")

    return os.path.join(args.output_dir, f"{safe_name}.{ext}")


def _export_merged(instances: list[Any], args: argparse.Namespace) -> None:
    """Merge multiple deck instances into a single .apkg file."""
    import genanki

    from ankitron.export import build_genanki_model
    from ankitron.identity import generate_deck_id
    from ankitron.logging import log_success

    package = genanki.Package([])
    media_files: list[str] = []

    for instance in instances:
        cls = instance.__class__
        model = build_genanki_model(cls)
        deck_id = generate_deck_id(cls.__qualname__)
        gk_deck = genanki.Deck(deck_id=deck_id, name=cls._deck_name)

        visible_attrs = [name for name, f in cls._all_fields if not f.internal]
        pk_attr = cls._pk_field_attr

        from ankitron.export import resolve_tags
        from ankitron.identity import generate_note_id

        for row in instance._data or []:
            import html

            pk_val = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            field_values = [html.escape(str(row.get(attr, ""))) for attr in visible_attrs]
            note_id = generate_note_id(cls.__qualname__, pk_val)
            tags = resolve_tags(cls._deck_tags, row) if cls._deck_tags else []
            note = genanki.Note(model=model, fields=field_values, guid=note_id, tags=tags)
            gk_deck.add_note(note)

        package.decks.append(gk_deck)

        # Collect media files from the media cache directory
        from ankitron.cache import CACHE_DIR

        media_cache_dir = CACHE_DIR / "media"
        if media_cache_dir.is_dir():
            for f in os.listdir(media_cache_dir):
                fpath = media_cache_dir / f
                if fpath.is_file():
                    media_files.append(str(fpath))

    if media_files:
        package.media_files = list(set(media_files))

    output_name = args.output_file or "merged.apkg"
    output_path = os.path.join(args.output_dir, output_name)
    package.write_to_file(output_path)
    log_success(f"Merged {len(instances)} deck(s) → {output_path}")


def _export_alt_format(instance: Any, args: argparse.Namespace) -> None:
    """Export deck data in CSV, JSON, or Markdown format."""
    import csv as csv_mod
    import json

    cls = instance.__class__
    data = instance._data or []
    visible = [name for name, f in cls._all_fields if not f.internal]

    safe_name = cls.__name__.lower()
    ext = args.format
    if args.output_file:
        output_path = os.path.join(args.output_dir, args.output_file)
    else:
        output_path = os.path.join(args.output_dir, f"{safe_name}.{ext}")

    if args.format == "csv":
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=visible, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)

    elif args.format == "json":
        rows = [{k: row.get(k, "") for k in visible} for row in data]
        if args.include_provenance and hasattr(instance, "_provenance"):
            from dataclasses import asdict, is_dataclass

            for i, row_prov in enumerate(instance._provenance):
                if i < len(rows):
                    prov_out = {}
                    for field_name, rec in (row_prov or {}).items():
                        prov_out[field_name] = asdict(rec) if is_dataclass(rec) else str(rec)
                    rows[i]["_provenance"] = prov_out
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)

    elif args.format == "markdown":
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("| " + " | ".join(visible) + " |\n")
            f.write("| " + " | ".join(["---"] * len(visible)) + " |\n")
            for row in data:
                vals = [str(row.get(k, "")).replace("|", "\\|") for k in visible]
                f.write("| " + " | ".join(vals) + " |\n")

    print(f"Exported to {output_path}")


def _cmd_preview(args: argparse.Namespace) -> int:
    """Execute the preview command."""
    if args.live:
        try:
            from ankitron.preview.server import run_preview_server

            run_preview_server(args.file, host=args.host, port=args.port, deck_name=args.deck)
        except ImportError:
            print("Error: live preview requires extra dependencies.", file=sys.stderr)
            print("Install with: pip install ankitron[preview]", file=sys.stderr)
            return 1
        return 0

    decks = _discover_decks(args.file, [args.deck] if args.deck else None)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch(refresh=args.refresh)
        instance.preview(max_rows=args.rows)

    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    """Execute the check command."""
    from ankitron.logging import log_success, warning_count

    decks = _discover_decks(args.file, [args.deck] if args.deck else None)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    # If we got here, class definition succeeded — fields/cards/sources validated
    for deck_cls in decks:
        log_success(f"Deck '{deck_cls._deck_name}' definition is valid")
        log_success(f"  {len(deck_cls._all_fields)} fields, {len(deck_cls._deck_cards)} cards")

        if args.with_fetch:
            instance = deck_cls()
            instance.fetch()
            log_success(f"  Fetch succeeded: {len(instance._data)} rows")

    if args.strict and warning_count() > 0:
        return 2

    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Execute the doctor command."""
    from ankitron.logging import log_info, log_success, log_warn

    log_info("ankitron doctor — checking installation...")

    # Core deps
    checks = [
        ("genanki", "genanki"),
        ("requests", "requests"),
        ("rich", "rich"),
    ]
    for name, pkg in checks:
        try:
            __import__(pkg)
            log_success(f"  {name}: installed")
        except ImportError:
            log_warn(f"  {name}: NOT INSTALLED")

    # Optional deps
    optional = [
        ("Pillow", "PIL", "media"),
        ("resvg-py", "resvg", "media"),
        ("mwparserfromhell", "mwparserfromhell", "wikipedia"),
        ("anthropic", "anthropic", "ai"),
        ("matplotlib", "matplotlib", "maps/charts"),
        ("contextily", "contextily", "maps"),
        ("geopandas", "geopandas", "maps"),
        ("shapely", "shapely", "maps"),
        ("fastapi", "fastapi", "preview"),
        ("uvicorn", "uvicorn", "preview"),
        ("websockets", "websockets", "preview"),
        ("watchdog", "watchdog", "preview"),
        ("anki", "anki", "sync"),
    ]
    log_info("Optional dependencies:")
    for name, pkg, extra in optional:
        try:
            __import__(pkg)
            log_success(f"  {name} [{extra}]: installed")
        except ImportError:
            log_info(f"  {name} [{extra}]: not installed (pip install ankitron[{extra}])")

    return 0


def _cmd_sync(args: argparse.Namespace) -> int:
    """Execute the sync command."""
    try:
        from ankitron.sync.sync import run_sync
    except ImportError:
        print("Error: sync requires the 'sync' extra.", file=sys.stderr)
        print("Install with: pip install ankitron[sync]", file=sys.stderr)
        return 1

    import getpass

    username = args.username or os.environ.get("ANKIWEB_USERNAME")
    password = args.password or os.environ.get("ANKIWEB_PASSWORD")

    if not args.dry_run:
        if not username:
            username = input("AnkiWeb username (email): ").strip()
        if not password:
            password = getpass.getpass("AnkiWeb password: ")
    else:
        username = username or ""
        password = password or ""

    if not args.dry_run and (not username or not password):
        print("Error: Username and password are required.", file=sys.stderr)
        return 1

    try:
        result = run_sync(
            apkg_files=args.files,
            username=username,
            password=password,
            collection_path=args.collection,
            dry_run=args.dry_run,
            force_full_upload=args.full_upload,
            force_full_download=args.full_download,
            allow_updates=args.allow_updates,
            full_download_import_sync=args.full_download_import_sync,
        )

        if result.imported_files:
            print(f"Imported {len(result.imported_files)} file(s)")
            print(f"  Added: {result.notes_added}, Updated: {result.notes_updated}")
        if result.sync_action:
            print(f"Sync: {result.sync_action}")

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    """Compare deck data against an existing .apkg file."""
    import sqlite3
    import zipfile

    decks = _discover_decks(args.file, [args.deck] if args.deck else None)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    if not os.path.isfile(args.apkg):
        print(f"Error: file not found: {args.apkg}", file=sys.stderr)
        return 1

    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch(refresh=args.refresh)
        new_data = instance._data or []

        # Extract notes from existing .apkg
        existing_notes: dict[str, dict[str, str]] = {}
        tmp_path = None
        try:
            with zipfile.ZipFile(args.apkg, "r") as zf:
                # Anki 2.1.50+ may use collection.anki21b instead
                db_name = "collection.anki2"
                if db_name not in zf.namelist():
                    candidates = [n for n in zf.namelist() if "collection" in n]
                    if candidates:
                        db_name = candidates[0]
                    else:
                        print(
                            "Error: .apkg does not contain a collection database.",
                            file=sys.stderr,
                        )
                        return 1

                with zf.open(db_name) as db_file:
                    import tempfile

                    with tempfile.NamedTemporaryFile(suffix=".anki2", delete=False) as tmp:
                        tmp.write(db_file.read())
                        tmp_path = tmp.name

                conn = sqlite3.connect(tmp_path)
                try:
                    cursor = conn.execute("SELECT flds FROM notes")
                    visible = [n for n, f in deck_cls._all_fields if not f.internal]
                    for row in cursor:
                        fields = row[0].split("\x1f")
                        note = {}
                        for i, name in enumerate(visible):
                            note[name] = fields[i] if i < len(fields) else ""
                        pk = note.get(deck_cls._pk_field_attr, "")
                        if pk:
                            existing_notes[pk] = note
                finally:
                    conn.close()
        except KeyError as exc:
            print(f"Error: incompatible .apkg format: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"Error reading .apkg: {exc}", file=sys.stderr)
            return 1
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        # Build diff
        pk_attr = deck_cls._pk_field_attr
        visible = [n for n, f in deck_cls._all_fields if not f.internal]
        if args.field:
            visible = [v for v in visible if v in args.field]

        new_by_pk = {}
        for row in new_data:
            pk = row.get(pk_attr, "")
            new_by_pk[pk] = row

        added = [pk for pk in new_by_pk if pk not in existing_notes]
        removed = [pk for pk in existing_notes if pk not in new_by_pk]
        changed = []
        for pk, new_row in new_by_pk.items():
            if pk in existing_notes:
                for field_name in visible:
                    old_val = existing_notes[pk].get(field_name, "")
                    new_val = new_row.get(field_name, "")
                    if old_val != new_val:
                        changed.append((pk, field_name, old_val, new_val))

        if args.format == "json":
            import json

            result = {"added": added, "removed": removed, "changed": changed}
            print(json.dumps(result, indent=2, default=str))
        else:
            if not args.only_removed and not args.only_changed:
                print(f"\nAdded ({len(added)}):")
                for pk in added[:20]:
                    print(f"  + {pk}")

            if not args.only_added and not args.only_changed:
                print(f"\nRemoved ({len(removed)}):")
                for pk in removed[:20]:
                    print(f"  - {pk}")

            if not args.only_added and not args.only_removed:
                print(f"\nChanged ({len(changed)}):")
                for pk, fname, old, new in changed[:20]:
                    print(f"  ~ {pk}.{fname}: {old!r} → {new!r}")

            total = len(added) + len(removed) + len(changed)
            print(f"\nTotal differences: {total}")

    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    """Inspect a specific row with provenance."""
    import json as json_mod

    decks = _discover_decks(args.file, [args.deck] if args.deck else None)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch()

        pk_attr = deck_cls._pk_field_attr
        target_row = None
        for row in instance._data or []:
            pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if pk == args.pk:
                target_row = row
                break

        if target_row is None:
            print(f"Error: no row with PK '{args.pk}' found.", file=sys.stderr)
            return 1

        target_provenance: dict[str, Any] = {}
        prov_rows = getattr(instance, "_provenance", None) or []
        for row_idx, row in enumerate(instance._data or []):
            pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if pk == args.pk:
                if row_idx < len(prov_rows):
                    target_provenance = prov_rows[row_idx] or {}
                break

        def _serialize_prov_record(rec: Any) -> dict[str, Any]:
            out: dict[str, Any] = {
                "source_type": rec.source_type,
                "source_name": rec.source_name,
                "source_key": rec.source_key,
                "source_url": rec.source_url,
                "source_entity_id": rec.source_entity_id,
                "raw_value": rec.raw_value,
                "raw_type": rec.raw_type,
                "formatted_value": rec.formatted_value,
                "fmt": rec.fmt,
                "derived_from": rec.derived_from,
                "computed_from": rec.computed_from,
                "overridden": rec.overridden,
                "original_value": rec.original_value,
                "ai_generated": rec.ai_generated,
                "ai_model": rec.ai_model,
                "ai_reviewed": rec.ai_reviewed,
                "fetched_at": rec.fetched_at.isoformat() if rec.fetched_at else None,
                "cached": rec.cached,
                "flagged": rec.flagged,
                "flag_note": rec.flag_note,
            }
            out["transform_chain"] = [
                {
                    "name": step.name,
                    "description": step.description,
                    "input_value": step.input_value,
                    "output_value": step.output_value,
                }
                for step in rec.transform_chain
            ]
            return out

        if args.json:
            visible = [n for n, f in deck_cls._all_fields if not f.internal]
            if args.field:
                visible = [v for v in visible if v == args.field]
            output = {
                "deck": deck_cls._deck_name,
                "pk_field": pk_attr,
                "pk": args.pk,
                "values": {k: target_row.get(k, "") for k in visible},
                "provenance": {},
            }
            for field_name in visible:
                rec = target_provenance.get(field_name)
                if rec is not None:
                    output["provenance"][field_name] = _serialize_prov_record(rec)
            print(json_mod.dumps(output, indent=2, default=str))
        else:
            fields_to_show = deck_cls._all_fields
            if args.field:
                fields_to_show = [(n, f) for n, f in fields_to_show if n == args.field]

            for name, fld in fields_to_show:
                val = target_row.get(name, "")
                kind = fld.kind.value
                flags = []
                if fld.internal:
                    flags.append("internal")
                if fld.is_derived:
                    flags.append("derived")
                if fld.is_computed:
                    flags.append("computed")
                if fld.pk:
                    flags.append(f"pk={fld.pk.name}")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                print(f"  {name} ({kind}{flag_str}): {val}")

                rec = target_provenance.get(name)
                if rec is not None:
                    src = rec.source_name or rec.source_type or "unknown"
                    src_key = f" -> {rec.source_key}" if rec.source_key else ""
                    print(f"    source: {src}{src_key}")
                    if rec.source_url:
                        print(f"    source_url: {rec.source_url}")
                    if rec.derived_from:
                        print(f"    derived_from: {rec.derived_from}")
                    if rec.computed_from:
                        print(f"    computed_from: {', '.join(rec.computed_from)}")
                    if rec.fmt:
                        print(f"    format: {rec.fmt}")
                    if rec.formatted_value is not None:
                        print(f"    formatted_value: {rec.formatted_value}")
                    if rec.raw_value is not None:
                        print(f"    raw_value ({rec.raw_type}): {rec.raw_value}")
                    if rec.fetched_at:
                        print(f"    fetched_at: {rec.fetched_at.isoformat()}")
                    print(f"    cached: {rec.cached}")
                    if rec.overridden:
                        print(f"    overridden: True (original: {rec.original_value})")
                    if rec.ai_generated:
                        print(
                            "    ai_generated: True"
                            + (f" (model: {rec.ai_model})" if rec.ai_model else "")
                        )
                    if rec.flagged:
                        print(
                            "    flagged: True" + (f" ({rec.flag_note})" if rec.flag_note else "")
                        )
                    if rec.transform_chain:
                        print("    transforms:")
                        for step in rec.transform_chain:
                            print(
                                f"      - {step.name}: {step.description}"
                                f" | in={step.input_value!r} out={step.output_value!r}"
                            )

        if args.render:
            for card_cls in deck_cls._deck_cards:
                from ankitron.preview.app import _render_card

                rendered = _render_card(card_cls, target_row)
                print(f"\n── {rendered['name']} ──")
                print(f"Front: {rendered['front']}")
                print(f"Back:  {rendered['back']}")

    return 0


def _cmd_review(args: argparse.Namespace) -> int:
    """Interactive review of flagged/AI content."""
    decks = _discover_decks(args.file, [args.deck] if args.deck else None)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch()
        data = instance._data or []

        if not data:
            print("No data to review.")
            return 0

        visible = [(n, f) for n, f in deck_cls._all_fields if not f.internal]
        if args.field:
            visible = [(n, f) for n, f in visible if n == args.field]

        pk_attr = deck_cls._pk_field_attr
        prov_rows = getattr(instance, "_provenance", None) or []
        overrides: dict[str, dict[str, str]] = {}

        # Filter rows by --flags / --ai / --tag / --all
        review_indices: list[int] = []
        for i, row in enumerate(data):
            if args.all or (not args.flags and not args.ai and not args.tag):
                review_indices.append(i)
                continue

            row_prov = prov_rows[i] if i < len(prov_rows) else {}
            if (
                args.flags
                and any(getattr(rec, "flagged", False) for rec in (row_prov or {}).values())
            ) or (
                args.ai
                and any(getattr(rec, "ai_generated", False) for rec in (row_prov or {}).values())
            ):
                review_indices.append(i)
            elif args.tag:
                tags = row.get("_tags", "")
                if args.tag in str(tags):
                    review_indices.append(i)

        if not review_indices:
            print("No rows match the review filter.")
            return 0

        print(f"Reviewing {len(review_indices)} of {len(data)} rows")

        for idx_pos, i in enumerate(review_indices):
            row = data[i]
            pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, "?"))
            print(f"\n── Row {idx_pos + 1}/{len(review_indices)}: {pk} ──")
            for name, _fld in visible:
                print(f"  {name}: {row.get(name, '')}")

            while True:
                action = input("[s]kip / [e]dit / [d]one / [q]uit > ").strip().lower()
                if action in ("s", "d"):
                    break
                if action == "q":
                    if args.export_overrides and overrides:
                        import json as json_mod

                        with open(args.export_overrides, "w", encoding="utf-8") as f:
                            json_mod.dump(overrides, f, indent=2)
                        print(f"Exported {len(overrides)} override(s) to {args.export_overrides}")
                    return 0
                if action == "e":
                    field_name = input("  Field to edit: ").strip()
                    if field_name not in {n for n, _ in visible}:
                        print(f"  Unknown field: {field_name}")
                        continue
                    new_val = input(f"  New value for {field_name}: ").strip()
                    if pk not in overrides:
                        overrides[pk] = {}
                    overrides[pk][field_name] = new_val
                    print(f"  Override recorded: {field_name} = {new_val}")
                    break

        if args.export_overrides and overrides:
            import json as json_mod

            with open(args.export_overrides, "w", encoding="utf-8") as f:
                json_mod.dump(overrides, f, indent=2)
            print(f"Exported {len(overrides)} override(s) to {args.export_overrides}")

    return 0


def _cmd_cache(args: argparse.Namespace) -> int:
    """Cache management command."""
    from ankitron.cache import CACHE_DIR

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if args.cache_command == "status":
        cache_dir = str(CACHE_DIR)
        if not os.path.isdir(cache_dir):
            print("Cache directory does not exist yet.")
            return 0

        total_size = 0
        file_count = 0
        for root, _dirs, files in os.walk(cache_dir):
            for f in files:
                fp = os.path.join(root, f)
                total_size += os.path.getsize(fp)
                file_count += 1

        print(f"Cache directory: {cache_dir}")
        print(f"  Files: {file_count}")
        print(f"  Size: {total_size / 1024 / 1024:.1f} MB")

    elif args.cache_command == "clear":
        import shutil

        cache_dir = str(CACHE_DIR)
        if not os.path.isdir(cache_dir):
            print("Nothing to clear.")
            return 0

        if not args.yes:
            confirm = input(f"Clear cache at {cache_dir}? [y/N] ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                return 0

        if args.all:
            shutil.rmtree(cache_dir)
            print("Cache cleared.")
        else:
            # Clear specific subdirectories
            targets = []
            if args.responses:
                targets.append("responses")
            if args.media:
                targets.append("media")
            if args.maps:
                targets.append("maps")
            if args.ai:
                targets.append("ai")
            if not targets:
                targets = ["responses", "media", "maps", "ai"]

            for target in targets:
                target_dir = os.path.join(cache_dir, target)
                if os.path.isdir(target_dir):
                    shutil.rmtree(target_dir)
                    print(f"  Cleared: {target}")

    elif args.cache_command == "warm":
        decks = _discover_decks(args.file, [args.deck] if args.deck else None)
        if not decks:
            print("Error: no Deck subclasses found.", file=sys.stderr)
            return 4
        for deck_cls in decks:
            instance = deck_cls()
            instance.fetch()
            print(f"Cache warmed for {deck_cls._deck_name} ({len(instance._data or [])} rows)")

    elif args.cache_command == "promote":
        from ankitron.ai.cache import AICache

        ai_cache = AICache()
        promoted = ai_cache.promote(
            deck_class=args.deck,
            field_name=args.field,
            from_version=args.from_version,
            to_version=args.to_version,
        )
        print(f"Promoted {promoted} cache entries from v{args.from_version} to v{args.to_version}")

    else:
        print("Usage: ankitron cache {status|clear|warm}", file=sys.stderr)
        return 1

    return 0


def _cmd_init(args: argparse.Namespace) -> int:
    """Create a new deck file from a template."""
    templates = {
        "wikidata": '''\
"""Example deck using WikidataSource."""

from ankitron import Card, Deck, Field, FieldKind, PKStrategy
from ankitron.sources import WikidataSource

wd = WikidataSource("Q6256")  # sovereign states


class Countries(Deck):
    deck_name = "Countries"

    name = wd.Field("P1448", pk=PKStrategy.LABEL)
    capital = wd.Field("P36")
    population = wd.Field("P1082", kind=FieldKind.NUMERIC, fmt="{:,.0f}")

    class Front(Card):
        front = "What is the capital of {{name}}?"
        back = "{{capital}}"
''',
        "csv": '''\
"""Example deck using CSVSource."""

from ankitron import Card, Deck, Field, PKStrategy
from ankitron.sources import CSVSource

data = CSVSource("data.csv")


class MyDeck(Deck):
    deck_name = "My Deck"

    term = data.Field("term", pk=PKStrategy.VALUE)
    definition = data.Field("definition")

    class Front(Card):
        front = "{{term}}"
        back = "{{definition}}"
''',
        "wikipedia": '''\
"""Example deck using WikipediaSource linked to WikidataSource."""

from ankitron import Card, Deck, Field, PKStrategy
from ankitron.sources import WikidataSource, WikipediaSource
from ankitron.sources.link_strategy import LinkStrategy

wd = WikidataSource("Q6256")
wp = WikipediaSource(linked_to=wd, via=LinkStrategy.sitelinks())


class Countries(Deck):
    deck_name = "Countries"

    name = wd.Field("P1448", pk=PKStrategy.LABEL)
    capital = wd.Field("P36")

    class Front(Card):
        front = "What is the capital of {{name}}?"
        back = "{{capital}}"
''',
    }

    if args.non_interactive and not args.template:
        print("Error: --template required with --non-interactive", file=sys.stderr)
        return 1

    if args.template:
        template_name = args.template
    else:
        print("Available templates:")
        for name in templates:
            print(f"  - {name}")
        template_name = input("Choose template: ").strip()
        if template_name not in templates:
            print(f"Unknown template: {template_name}", file=sys.stderr)
            return 1

    content = templates.get(template_name)
    if content is None:
        print(f"Unknown template: {template_name}", file=sys.stderr)
        return 1

    output = args.output or f"{template_name}_deck.py"

    if os.path.exists(output):
        confirm = input(f"{output} exists. Overwrite? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return 0

    with open(output, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Created {output}")
    return 0


def _cmd_sources(args: argparse.Namespace) -> int:
    """Source exploration commands."""
    if args.sources_command == "wikidata":
        if args.wikidata_command == "search":
            import requests

            search_type = "class" if args.type == "class" else "property"
            url = "https://www.wikidata.org/w/api.php"
            params = {
                "action": "wbsearchentities",
                "search": args.query,
                "language": "en",
                "limit": args.limit,
                "format": "json",
                "type": "item" if search_type == "class" else "property",
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("search", []):
                qid = item["id"]
                label = item.get("label", "")
                desc = item.get("description", "")
                print(f"  {qid}: {label} — {desc}")

        elif args.wikidata_command == "describe":
            import requests

            url = f"https://www.wikidata.org/wiki/Special:EntityData/{args.qid}.json"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            entity = data.get("entities", {}).get(args.qid, {})
            labels = entity.get("labels", {})
            label = labels.get("en", {}).get("value", args.qid)
            desc = entity.get("descriptions", {}).get("en", {}).get("value", "")
            print(f"{args.qid}: {label}")
            print(f"  {desc}")

            claims = entity.get("claims", {})
            print(f"\n  Properties ({len(claims)}):")
            for shown, prop_id in enumerate(sorted(claims.keys())):
                if shown >= 20:
                    print(f"  ... and {len(claims) - 20} more")
                    break
                print(f"    {prop_id}: {len(claims[prop_id])} claim(s)")

        else:
            print("Usage: ankitron sources wikidata {search|describe}", file=sys.stderr)
            return 1

    elif args.sources_command == "wikipedia":
        if args.wikipedia_command == "infobox":
            import requests

            url = "https://en.wikipedia.org/w/api.php"
            params = {
                "action": "parse",
                "page": args.title,
                "prop": "wikitext",
                "format": "json",
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
            if not wikitext:
                print(f"No wikitext found for '{args.title}'")
                return 1

            # Extract infobox parameters
            import re

            infobox_match = re.search(r"\{\{Infobox[^}]*\n(.*?)\n\}\}", wikitext, re.DOTALL)
            if not infobox_match:
                print(f"No infobox found in '{args.title}'")
                return 0

            params_text = infobox_match.group(1)
            for raw_line in params_text.split("\n"):
                stripped = raw_line.strip()
                if stripped.startswith("|"):
                    parts = stripped[1:].split("=", 1)
                    if len(parts) == 2:
                        param = parts[0].strip()
                        value = parts[1].strip()[:60]
                        print(f"  {param} = {value}")
        else:
            print("Usage: ankitron sources wikipedia {infobox}", file=sys.stderr)
            return 1
    else:
        print("Usage: ankitron sources {wikidata|wikipedia}", file=sys.stderr)
        return 1

    return 0


def _cmd_addon(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Anki add-on commands (stub)."""
    print("Anki add-on support is planned for a future release.")
    print("See https://github.com/Wyatt-Stanke/ankitron for updates.")
    return 0


def _cmd_batch(args: argparse.Namespace) -> int:
    """Manage AI batch processing."""
    if args.batch_command == "submit":
        print("Batch submit: use `ankitron build --batch --submit-only` to submit.")
        return 0

    if args.batch_command == "status":
        from ankitron.ai.batch import check_batch_status

        result = check_batch_status(args.batch_id)
        print(f"Batch: {result.batch_id}")
        print(f"  Status: {result.status}")
        print(f"  Completed: {result.completed}/{result.total_requests}")
        if result.failed:
            print(f"  Failed: {result.failed}")
        return 0

    if args.batch_command == "list":
        print("Batch list: not yet implemented (requires persistent batch tracking).")
        return 0

    if args.batch_command == "collect":
        if args.batch_id:
            from ankitron.ai.batch import collect_batch_results

            result = collect_batch_results(args.batch_id)
            print(f"Collected {result.completed} results from batch {args.batch_id}")
            if result.failed:
                print(f"  Failed: {result.failed}")
        else:
            print("Specify a batch_id or use --all")
        return 0

    if args.batch_command == "cancel":
        from ankitron.ai.batch import cancel_batch

        cancel_batch(args.batch_id)
        print(f"Cancelled batch {args.batch_id}")
        return 0

    print("Usage: ankitron batch {submit|status|list|collect|cancel}", file=sys.stderr)
    return 1


_COMMAND_MAP = {
    "build": _cmd_build,
    "preview": _cmd_preview,
    "check": _cmd_check,
    "diff": _cmd_diff,
    "inspect": _cmd_inspect,
    "review": _cmd_review,
    "cache": _cmd_cache,
    "batch": _cmd_batch,
    "init": _cmd_init,
    "sources": _cmd_sources,
    "doctor": _cmd_doctor,
    "sync": _cmd_sync,
    "addon": _cmd_addon,
}


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    from ankitron.cli.parser import build_parser

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    # Apply global options
    if args.quiet:
        import ankitron.logging as log_mod

        log_mod._quiet = True
    if args.no_color:
        os.environ["NO_COLOR"] = "1"

    # Reset warning counter for each invocation
    from ankitron.logging import reset_warning_count

    reset_warning_count()

    handler = _COMMAND_MAP.get(args.command)
    if handler:
        exit_code = handler(args)
        sys.exit(exit_code)
    else:
        # Commands not yet implemented
        print(f"Command '{args.command}' is not yet implemented.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
