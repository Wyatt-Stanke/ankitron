"""
ankitron CLI — main entry point and command dispatch.

Usage: ankitron <command> [options] [arguments]
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any


def _get_version() -> str:
    """Read version from package metadata."""
    try:
        from importlib.metadata import version

        return version("ankitron")
    except Exception:
        return "dev"


def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="ankitron",
        description="ankitron — A declarative Python SDK for generating Anki flashcard decks.",
    )

    # Global options
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (stackable: -vv, -vvv)",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Suppress all output except errors"
    )
    parser.add_argument("--no-color", action="store_true", help="Disable rich formatting")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Config file path (default: ~/.config/ankitron/config.toml)",
    )
    parser.add_argument("--cache-dir", type=str, default=None, help="Override cache directory")
    parser.add_argument("--version", action="version", version=f"ankitron {_get_version()}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ── build ──
    build_p = subparsers.add_parser("build", help="Build .apkg deck files")
    build_p.add_argument("file", help="Python file containing Deck subclass(es)")
    build_p.add_argument("-o", "--output-dir", default=".", help="Output directory (default: cwd)")
    build_p.add_argument(
        "-f",
        "--output-file",
        default=None,
        help="Explicit output filename (single-deck files only)",
    )
    build_p.add_argument(
        "-d", "--deck", action="append", default=None, help="Build only named deck(s), repeatable"
    )
    build_p.add_argument("--refresh", action="store_true", help="Bypass all caches")
    build_p.add_argument(
        "--refresh-source",
        action="append",
        default=None,
        help="Bypass cache for one source (repeatable)",
    )
    build_p.add_argument(
        "--refresh-media", action="store_true", help="Re-download/convert all media"
    )
    build_p.add_argument("--refresh-maps", action="store_true", help="Re-generate all maps")
    build_p.add_argument("--refresh-ai", action="store_true", help="Re-run all AI prompts")
    build_p.add_argument("--dry-run", action="store_true", help="Show plan without executing")
    build_p.add_argument("--skip-validation", action="store_true", help="Skip all validators")
    build_p.add_argument(
        "--fail-on-warn", action="store_true", help="Treat warnings as errors (exit code 2)"
    )
    build_p.add_argument(
        "--format",
        default="apkg",
        choices=["apkg", "csv", "json", "markdown"],
        help="Output format (default: apkg)",
    )
    build_p.add_argument(
        "--include-provenance", action="store_true", help="Include provenance in csv/json exports"
    )

    # ── preview ──
    preview_p = subparsers.add_parser("preview", help="Preview deck in terminal or browser")
    preview_p.add_argument("file", help="Python file containing Deck subclass(es)")
    preview_p.add_argument("-d", "--deck", default=None, help="Preview specific deck")
    preview_p.add_argument("--live", action="store_true", help="Launch live browser preview")
    preview_p.add_argument("--port", type=int, default=8742, help="Preview server port")
    preview_p.add_argument("--host", default="127.0.0.1", help="Preview server host")
    preview_p.add_argument("--rows", type=int, default=10, help="Number of rows to show")
    preview_p.add_argument("--row", default=None, help="Show specific row by PK")
    preview_p.add_argument("--card-type", default=None, help="Filter by card type")
    preview_p.add_argument("--refresh", action="store_true")

    # ── check ──
    check_p = subparsers.add_parser("check", help="Validate deck definition")
    check_p.add_argument("file", help="Python file containing Deck subclass(es)")
    check_p.add_argument("-d", "--deck", default=None)
    check_p.add_argument(
        "--with-fetch", action="store_true", help="Also fetch data and run validators"
    )
    check_p.add_argument("--strict", action="store_true", help="Treat warnings as errors")

    # ── diff ──
    diff_p = subparsers.add_parser("diff", help="Compare data against existing .apkg")
    diff_p.add_argument("file", help="Python file containing Deck subclass(es)")
    diff_p.add_argument("apkg", help="Existing .apkg file to compare against")
    diff_p.add_argument("-d", "--deck", default=None)
    diff_p.add_argument("--refresh", action="store_true")
    diff_p.add_argument("--format", default="table", choices=["table", "json", "csv"])
    diff_p.add_argument("--field", action="append", default=None, help="Only diff specified fields")
    diff_p.add_argument("--only-changed", action="store_true")
    diff_p.add_argument("--only-added", action="store_true")
    diff_p.add_argument("--only-removed", action="store_true")

    # ── inspect ──
    inspect_p = subparsers.add_parser("inspect", help="Inspect a specific row")
    inspect_p.add_argument("file", help="Python file containing Deck subclass(es)")
    inspect_p.add_argument("-d", "--deck", default=None)
    inspect_p.add_argument("--pk", required=True, help="Primary key of row to inspect")
    inspect_p.add_argument("--field", default=None, help="Inspect specific field")
    inspect_p.add_argument("--render", action="store_true", help="Render cards for this row")
    inspect_p.add_argument("--json", action="store_true", help="Output as JSON")

    # ── review ──
    review_p = subparsers.add_parser("review", help="Interactive review of flagged/AI content")
    review_p.add_argument("file", help="Python file containing Deck subclass(es)")
    review_p.add_argument("-d", "--deck", default=None)
    review_p.add_argument("--flags", action="store_true", help="Review flagged items")
    review_p.add_argument("--ai", action="store_true", help="Review AI-generated content")
    review_p.add_argument("--all", action="store_true", help="Review everything")
    review_p.add_argument("--field", default=None)
    review_p.add_argument("--tag", default=None)
    review_p.add_argument(
        "--export-overrides", default=None, help="Export approved overrides to file"
    )

    # ── cache ──
    cache_p = subparsers.add_parser("cache", help="Cache management")
    cache_sub = cache_p.add_subparsers(dest="cache_command")

    cache_status = cache_sub.add_parser("status", help="Show cache status")
    cache_status.add_argument("-d", "--deck", default=None)
    cache_status.add_argument("-v", "--verbose", action="store_true")

    cache_clear = cache_sub.add_parser("clear", help="Clear cache entries")
    cache_clear.add_argument("--all", action="store_true")
    cache_clear.add_argument("--responses", action="store_true")
    cache_clear.add_argument("--media", action="store_true")
    cache_clear.add_argument("--maps", action="store_true")
    cache_clear.add_argument("--ai", action="store_true")
    cache_clear.add_argument("--stale", action="store_true")
    cache_clear.add_argument("--older-than", default=None)
    cache_clear.add_argument("-d", "--deck", default=None)
    cache_clear.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")

    cache_warm = cache_sub.add_parser("warm", help="Pre-populate cache")
    cache_warm.add_argument("file", help="Python file")
    cache_warm.add_argument("-d", "--deck", default=None)
    cache_warm.add_argument("--media", action="store_true")
    cache_warm.add_argument("--maps", action="store_true")
    cache_warm.add_argument("--ai", action="store_true")

    # ── init ──
    init_p = subparsers.add_parser("init", help="Create a new deck file")
    init_p.add_argument("-o", "--output", default=None, help="Output file path")
    init_p.add_argument(
        "--template", default=None, choices=["wikidata", "csv", "wikipedia"], help="Template type"
    )
    init_p.add_argument("--non-interactive", action="store_true")

    # ── sources ──
    sources_p = subparsers.add_parser("sources", help="Explore data sources")
    sources_sub = sources_p.add_subparsers(dest="sources_command")

    wd_p = sources_sub.add_parser("wikidata", help="Wikidata exploration")
    wd_sub = wd_p.add_subparsers(dest="wikidata_command")

    wd_search = wd_sub.add_parser("search", help="Search Wikidata classes/properties")
    wd_search.add_argument("query", help="Search term")
    wd_search.add_argument("--type", default="class", choices=["class", "property"])
    wd_search.add_argument("--limit", type=int, default=10)

    wd_describe = wd_sub.add_parser("describe", help="Describe a QID")
    wd_describe.add_argument("qid", help="Wikidata QID (e.g., Q30)")
    wd_describe.add_argument("--sample", type=int, default=5)

    wp_p = sources_sub.add_parser("wikipedia", help="Wikipedia exploration")
    wp_sub = wp_p.add_subparsers(dest="wikipedia_command")

    wp_infobox = wp_sub.add_parser("infobox", help="Inspect infobox params")
    wp_infobox.add_argument("title", help="Wikipedia article title")
    wp_infobox.add_argument("--language", default="en")

    # ── doctor ──
    subparsers.add_parser("doctor", help="Diagnose installation and dependencies")

    # ── addon ──
    subparsers.add_parser("addon", help="Anki add-on management (stub)")

    # ── sync ──
    sync_p = subparsers.add_parser("sync", help="Sync .apkg files to AnkiWeb")
    sync_p.add_argument("files", nargs="+", help=".apkg files to sync")
    sync_p.add_argument("-u", "--username", default=None, help="AnkiWeb username")
    sync_p.add_argument("-p", "--password", default=None, help="AnkiWeb password")
    sync_p.add_argument("--dry-run", action="store_true")
    sync_p.add_argument(
        "--allow-updates", action="store_true", help="Update existing notes (default: add-only)"
    )
    sync_p.add_argument(
        "--collection",
        default=None,
        help="Collection path (~/.local/share/Anki2/User 1/collection.anki2)",
    )
    sync_p.add_argument("--full-upload", action="store_true")
    sync_p.add_argument("--full-download", action="store_true")

    return parser


def _discover_decks(filepath: str, deck_filter: list[str] | None = None) -> list[Any]:
    """Import a Python file and discover all Deck subclasses."""
    import importlib.util
    import os

    from ankitron.deck import Deck

    filepath = os.path.abspath(filepath)
    if not os.path.isfile(filepath):
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(3)

    spec = importlib.util.spec_from_file_location("_deck_module", filepath)
    if spec is None or spec.loader is None:
        print(f"Error: cannot import {filepath}", file=sys.stderr)
        sys.exit(3)

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        print(f"Error importing {filepath}: {exc}", file=sys.stderr)
        sys.exit(3)

    decks = []
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Deck)
            and obj is not Deck
            and not attr_name.startswith("_")
        ):
            decks.append(obj)

    if deck_filter:
        decks = [d for d in decks if d.__name__ in deck_filter or d._deck_name in deck_filter]

    return decks


def _cmd_build(args: argparse.Namespace) -> int:
    """Execute the build command."""
    import os

    from ankitron.logging import warning_count

    decks = _discover_decks(args.file, args.deck)
    if not decks:
        print("Error: no Deck subclasses found.", file=sys.stderr)
        return 4

    if args.output_file and len(decks) > 1:
        print("Error: --output-file can only be used with single-deck files.", file=sys.stderr)
        return 1

    if args.dry_run:
        for deck_cls in decks:
            print(f"Deck: {deck_cls._deck_name}")
            print(f"  Fields: {len(deck_cls._all_fields)}")
            print(f"  Cards: {len(deck_cls._deck_cards)}")
            print(f"  Sources: {len(deck_cls._deck_sources)}")
        return 0

    for deck_cls in decks:
        instance = deck_cls()
        instance.fetch(
            refresh=args.refresh,
            skip_validation=args.skip_validation,
        )

        if args.format == "apkg":
            if args.output_file:
                output_path = os.path.join(args.output_dir, args.output_file)
            else:
                safe_name = deck_cls.__name__.lower()
                output_path = os.path.join(args.output_dir, f"{safe_name}.apkg")
            instance.export(output_path)
        elif args.format in ("csv", "json", "markdown"):
            # Export to alternate formats
            _export_alt_format(instance, args)

    if args.fail_on_warn and warning_count() > 0:
        return 2

    return 0


def _export_alt_format(instance: Any, args: argparse.Namespace) -> None:
    """Export deck data in CSV, JSON, or Markdown format."""
    import csv as csv_mod
    import json
    import os

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
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)

    elif args.format == "markdown":
        with open(output_path, "w", encoding="utf-8") as f:
            # Header
            f.write("| " + " | ".join(visible) + " |\n")
            f.write("| " + " | ".join(["---"] * len(visible)) + " |\n")
            for row in data:
                vals = [row.get(k, "") for k in visible]
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
        ("cairosvg", "cairosvg", "media"),
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
    import os

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
        try:
            with zipfile.ZipFile(args.apkg, "r") as zf:
                with zf.open("collection.anki2") as db_file:
                    import tempfile

                    with tempfile.NamedTemporaryFile(suffix=".anki2", delete=False) as tmp:
                        tmp.write(db_file.read())
                        tmp_path = tmp.name

                conn = sqlite3.connect(tmp_path)
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
                conn.close()
                os.unlink(tmp_path)
        except Exception as exc:
            print(f"Error reading .apkg: {exc}", file=sys.stderr)
            return 1

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
                    new_val = new_rowield_name, "")
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

        if args.json:
            visible = [n for n, f in deck_cls._all_fields if not f.internal]
            if args.field:
                visible = [v for v in visible if v == args.field]
            output = {k: target_row.get(k, "") for k in visible}
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

        if args.render:
            for card_cls in deck_cls._deck_cards:
                from ankitron.preview.server import _render_card

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

        # Filter rows based on flags
        visible = [(n, f) for n, f in deck_cls._all_fields if not f.internal]
        if args.field:
            visible = [(n, f) for n, f in visible if n == args.field]

        pk_attr = deck_cls._pk_field_attr
        overrides: dict[str, dict[str, str]] = {}

        for i, row in enumerate(data):
            pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, "?"))
            print(f"\n── Row {i + 1}/{len(data)}: {pk} ──")
            for name, _fld in visible:
                print(f"  {name}: {row.get(name, '')}")

            while True:
                action = input("[s]kip / [e]dit / [d]one / [q]uit > ").strip().lower()
                if action == "s":
                    break
                if action == "d":
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
    from ankitron.cache import Cache

    cache = Cache()

    if args.cache_command == "status":
        cache_dir = cache.cache_dir
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

        cache_dir = cache.cache_dir
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


_COMMAND_MAP = {
    "build": _cmd_build,
    "preview": _cmd_preview,
    "check": _cmd_check,
    "diff": _cmd_diff,
    "inspect": _cmd_inspect,
    "review": _cmd_review,
    "cache": _cmd_cache,
    "init": _cmd_init,
    "sources": _cmd_sources,
    "doctor": _cmd_doctor,
    "sync": _cmd_sync,
    "addon": _cmd_addon,
}


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    # Apply global options
    if args.quiet:
        import ankitron.logging as log_mod

        log_mod._quiet = True  # type: ignore[attr-defined]
    if args.no_color:
        import os

        os.environ["NO_COLOR"] = "1"

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
