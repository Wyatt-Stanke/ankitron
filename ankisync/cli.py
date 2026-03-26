"""CLI entry point for ankisync — import .apkg files and sync to AnkiWeb."""

from __future__ import annotations

import argparse
import getpass
import os
import sys

from ankisync.sync import SyncResult, _is_interactive, run_sync


def _load_env_file() -> None:
    """Load variables from .env file in the current directory if it exists."""
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            if key not in os.environ:
                os.environ[key] = value


def _get_credentials(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve credentials from args, env vars, or interactive input."""
    username = args.username or os.environ.get("ANKIWEB_USERNAME")
    password = args.password or os.environ.get("ANKIWEB_PASSWORD")

    if not username:
        if _is_interactive():
            username = input("AnkiWeb username (email): ").strip()
        else:
            print("Error: No username provided.", file=sys.stderr)
            print(
                "Set ANKIWEB_USERNAME env var, use -u, or run interactively.",
                file=sys.stderr,
            )
            sys.exit(1)

    if not password:
        if _is_interactive():
            password = getpass.getpass("AnkiWeb password: ")
        else:
            print("Error: No password provided.", file=sys.stderr)
            print(
                "Set ANKIWEB_PASSWORD env var, use -p, or run interactively.",
                file=sys.stderr,
            )
            sys.exit(1)

    if not username or not password:
        print("Error: Username and password are required.", file=sys.stderr)
        sys.exit(1)

    return username, password


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ankisync",
        description=(
            "Import .apkg files and sync to AnkiWeb.\n\n"
            "By default, ankisync only adds new cards and never overwrites or\n"
            "deletes anything on AnkiWeb. Use --full-upload or --full-download\n"
            "to explicitly replace data when needed."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  ankisync deck.apkg               Import and push new cards\n"
            "  ankisync deck.apkg --dry-run      Preview without making changes\n"
            "  ankisync --full-upload             Replace remote with local collection\n"
            "  ankisync --full-download           Replace local with remote collection\n"
            "  ankisync deck.apkg --full-download-import-sync\n"
            "                                   Download remote, import deck, then sync\n"
            "\n"
            "credentials:\n"
            "  Provide via -u/-p flags, ANKIWEB_USERNAME/ANKIWEB_PASSWORD env vars,\n"
            "  a .env file in the current directory, or enter interactively."
        ),
    )

    parser.add_argument(
        "files",
        nargs="*",
        metavar="FILE",
        help=".apkg files to import before syncing",
    )

    auth_group = parser.add_argument_group("authentication")
    auth_group.add_argument("-u", "--username", help="AnkiWeb email")
    auth_group.add_argument("-p", "--password", help="AnkiWeb password")

    parser.add_argument(
        "-c",
        "--collection",
        metavar="PATH",
        help="local collection path (default: ~/.ankisync/collection.anki2)",
    )
    parser.add_argument(
        "--endpoint",
        help="custom sync server URL (default: AnkiWeb)",
    )

    sync_group = parser.add_argument_group("sync mode (mutually exclusive)")
    sync_exclusive = sync_group.add_mutually_exclusive_group()
    sync_exclusive.add_argument(
        "--full-upload",
        action="store_true",
        help="replace the entire remote collection with local data",
    )
    sync_exclusive.add_argument(
        "--full-download",
        action="store_true",
        help="replace the local collection with remote data",
    )
    sync_exclusive.add_argument(
        "--full-download-import-sync",
        action="store_true",
        help="download remote first, then import FILEs, then sync",
    )

    safety_group = parser.add_argument_group("safety")
    safety_group.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="preview what would happen without making changes",
    )
    safety_group.add_argument(
        "--allow-updates",
        action="store_true",
        help="allow updating existing notes during import (default: add-only)",
    )

    return parser


def _print_summary(result: SyncResult) -> None:
    prefix = "[DRY RUN] " if result.dry_run else ""
    print(f"\n{'=' * 50}")
    print(f"{prefix}Summary")
    print(f"{'=' * 50}")

    if result.imported_files:
        print(f"  Files imported:    {len(result.imported_files)}")
        print(f"  Notes added:       {result.notes_added}")
        if result.notes_updated:
            print(f"  Notes updated:     {result.notes_updated}")
        if result.notes_duplicate:
            print(f"  Duplicates:        {result.notes_duplicate}")

    if result.sync_action:
        print(f"  Sync:              {result.sync_action}")
    if result.server_message:
        print(f"  Server message:    {result.server_message}")


def main(argv: list[str] | None = None) -> int:
    _load_env_file()

    parser = build_parser()
    args = parser.parse_args(argv)

    if (
        not args.files
        and not args.full_upload
        and not args.full_download
        and not args.full_download_import_sync
    ):
        parser.print_help()
        return 0

    if args.full_download_import_sync and not args.files:
        print(
            "Error: --full-download-import-sync requires at least one .apkg FILE.",
            file=sys.stderr,
        )
        return 1

    for f in args.files:
        if not os.path.isfile(f):
            print(f"Error: File not found: {f}", file=sys.stderr)
            return 1
        if not f.endswith(".apkg"):
            print(f"Warning: {f} does not have .apkg extension", file=sys.stderr)

    if args.dry_run:
        username = args.username or os.environ.get("ANKIWEB_USERNAME") or ""
        password = args.password or os.environ.get("ANKIWEB_PASSWORD") or ""
    else:
        username, password = _get_credentials(args)

    try:
        result = run_sync(
            apkg_files=args.files,
            username=username,
            password=password,
            collection_path=args.collection,
            endpoint=args.endpoint,
            dry_run=args.dry_run,
            force_full_upload=args.full_upload,
            force_full_download=args.full_download,
            full_download_import_sync=args.full_download_import_sync,
            allow_updates=args.allow_updates,
        )
        _print_summary(result)
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
