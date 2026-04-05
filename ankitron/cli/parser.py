"""Argument parser definition for the ankitron CLI."""

from __future__ import annotations

import argparse


def _get_version() -> str:
    """Read version from package metadata."""
    try:
        from importlib.metadata import version

        return version("ankitron")
    except Exception:
        return "dev"


def build_parser() -> argparse.ArgumentParser:
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
    build_p.add_argument("--force", action="store_true", help="Rebuild even if cached/fresh")
    build_p.add_argument("--flat", action="store_true", help="All .apkg files in one directory")
    build_p.add_argument("--merge", action="store_true", help="Merge all decks into a single .apkg")
    build_p.add_argument(
        "--merge-by-directory",
        action="store_true",
        help="One .apkg per directory",
    )
    build_p.add_argument(
        "--params",
        default=None,
        help="DeckFamily variant filter (e.g. lesson=3)",
    )
    build_p.add_argument(
        "--batch", action="store_true", help="Use Anthropic Batch API for AI fields"
    )
    build_p.add_argument(
        "--wait", action="store_true", help="Block until batch completes (with --batch)"
    )
    build_p.add_argument(
        "--submit-only",
        action="store_true",
        help="Submit batch and exit (with --batch)",
    )
    build_p.add_argument(
        "--batch-timeout",
        type=int,
        default=86400,
        help="Batch wait timeout in seconds (default: 86400)",
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

    cache_promote = cache_sub.add_parser("promote", help="Promote AI cache across versions")
    cache_promote.add_argument("--deck", required=True, help="Deck class name")
    cache_promote.add_argument("--field", required=True, help="Field name")
    cache_promote.add_argument("--from-version", type=int, required=True)
    cache_promote.add_argument("--to-version", type=int, required=True)
    cache_promote.add_argument("--exclude-tag", default=None, help="Exclude rows with this tag")

    # ── batch ──
    batch_p = subparsers.add_parser("batch", help="Manage AI batch processing")
    batch_sub = batch_p.add_subparsers(dest="batch_command")

    batch_submit = batch_sub.add_parser("submit", help="Submit batch for processing")
    batch_submit.add_argument("file", help="Python file or directory")
    batch_submit.add_argument("-d", "--deck", default=None)

    batch_status = batch_sub.add_parser("status", help="Check batch status")
    batch_status.add_argument("batch_id", help="Batch ID")

    batch_sub.add_parser("list", help="List pending batches")

    batch_collect = batch_sub.add_parser("collect", help="Collect batch results")
    batch_collect.add_argument("batch_id", nargs="?", default=None, help="Batch ID")
    batch_collect.add_argument("--all", action="store_true", help="Collect all completed batches")

    batch_cancel = batch_sub.add_parser("cancel", help="Cancel a batch")
    batch_cancel.add_argument("batch_id", help="Batch ID")

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
    sync_p.add_argument("--full-download-import-sync", action="store_true")

    return parser
