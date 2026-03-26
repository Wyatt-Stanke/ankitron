"""CLI entry point for ankisync — import .apkg files and sync to AnkiWeb."""

from __future__ import annotations

import argparse
import getpass
import os
import sys

from ankisync.sync import SyncResult, _is_interactive, run_sync


def _load_env_file() -> None:
    """Load variables from .env file in the current directory if it exists.
    Does not override already-set environment variables."""
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
            # Remove surrounding quotes if present
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
            print("Set ANKIWEB_USERNAME env var, use --username, or run interactively.", file=sys.stderr)
            sys.exit(1)

    if not password:
        if _is_interactive():
            password = getpass.getpass("AnkiWeb password: ")
        else:
            print("Error: No password provided.", file=sys.stderr)
            print("Set ANKIWEB_PASSWORD env var, use --password, or run interactively.", file=sys.stderr)
            sys.exit(1)

    if not username or not password:
        print("Error: Username and password are required.", file=sys.stderr)
        sys.exit(1)

    return username, password


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ankisync",
        description="Import .apkg files into a local Anki collection and sync to AnkiWeb.",
        epilog=(
            "Credentials can be provided via --username/--password flags, "
            "ANKIWEB_USERNAME/ANKIWEB_PASSWORD environment variables, "
            "or entered interactively when running in a terminal."
        ),
    )

    parser.add_argument(
        "files",
        nargs="*",
        metavar="FILE",
        help=".apkg files to import before syncing (optional)",
    )

    # Auth
    auth_group = parser.add_argument_group("authentication")
    auth_group.add_argument(
        "-u", "--username",
        help="AnkiWeb username/email (or set ANKIWEB_USERNAME)",
    )
    auth_group.add_argument(
        "-p", "--password",
        help="AnkiWeb password (or set ANKIWEB_PASSWORD)",
    )

    # Collection
    parser.add_argument(
        "-c", "--collection",
        metavar="PATH",
        help="Path to local collection file (default: ~/.ankisync/collection.anki2)",
    )

    # Sync options
    sync_group = parser.add_argument_group("sync options")
    sync_group.add_argument(
        "--full-upload",
        action="store_true",
        help="Force full upload, replacing the entire remote collection",
    )
    sync_group.add_argument(
        "--no-media",
        action="store_true",
        help="Skip media file sync",
    )
    sync_group.add_argument(
        "--endpoint",
        help="Custom sync server URL (default: AnkiWeb)",
    )

    # Safety options
    safety_group = parser.add_argument_group("safety options")
    safety_group.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Show what would happen without making any changes",
    )
    safety_group.add_argument(
        "--allow-updates",
        action="store_true",
        help="Allow updating existing notes (default: add-only, never overwrite)",
    )

    return parser


def _print_summary(result: SyncResult) -> None:
    prefix = "[DRY RUN] " if result.dry_run else ""
    print(f"\n{'=' * 50}")
    print(f"{prefix}Summary")
    print(f"{'=' * 50}")

    if result.imported_files:
        print(f"  Files processed:   {len(result.imported_files)}")
        print(f"  Notes added:       {result.notes_added}")
        print(f"  Notes updated:     {result.notes_updated}")
        print(f"  Duplicates:        {result.notes_duplicate}")

    print(f"  Sync action:       {result.sync_action}")
    if result.media_synced:
        print(f"  Media synced:      yes")
    if result.server_message:
        print(f"  Server message:    {result.server_message}")


def main(argv: list[str] | None = None) -> int:
    _load_env_file()

    parser = build_parser()
    args = parser.parse_args(argv)

    # Validate .apkg files exist before doing anything
    for f in args.files:
        if not os.path.isfile(f):
            print(f"Error: File not found: {f}", file=sys.stderr)
            return 1
        if not f.endswith(".apkg"):
            print(f"Warning: {f} does not have .apkg extension", file=sys.stderr)

    # Credentials are not needed for dry-run
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
            sync_media=not args.no_media,
            allow_updates=args.allow_updates,
        )
        _print_summary(result)
        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
