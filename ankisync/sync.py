"""Core sync logic: import .apkg files into a local Anki collection and sync to AnkiWeb."""

from __future__ import annotations

import io
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from anki.collection import Collection
from anki.import_export_pb2 import (
    IMPORT_ANKI_PACKAGE_UPDATE_CONDITION_IF_NEWER,
    IMPORT_ANKI_PACKAGE_UPDATE_CONDITION_NEVER,
    ImportAnkiPackageOptions,
    ImportAnkiPackageRequest,
    ImportResponse,
)

from ankisync.http_client import AnkiWebClient, SyncError, SyncMeta

DEFAULT_COLLECTION_DIR = Path.home() / ".ankisync"
DEFAULT_COLLECTION_PATH = DEFAULT_COLLECTION_DIR / "collection.anki2"


@contextmanager
def _suppress_anki_debug():
    """Suppress the 'blocked main thread' debug output from the Anki backend."""
    real_stdout = sys.stdout
    buf = io.StringIO()
    sys.stdout = buf
    try:
        yield
    finally:
        sys.stdout = real_stdout
        for line in buf.getvalue().splitlines():
            if not line.strip().startswith(("blocked main thread", "File \"", "raw_bytes", "print(")):
                print(line)


@dataclass
class SyncResult:
    imported_files: list[str] = field(default_factory=list)
    notes_added: int = 0
    notes_updated: int = 0
    notes_duplicate: int = 0
    sync_action: str = ""
    server_message: str = ""
    media_synced: bool = False
    dry_run: bool = False


def _is_interactive() -> bool:
    return hasattr(sys.stdin, "isatty") and sys.stdin.isatty()


def _confirm(prompt: str) -> bool:
    """Ask yes/no confirmation. Returns True if user confirms.
    In non-interactive mode, returns False (safe default)."""
    if not _is_interactive():
        return False
    while True:
        answer = input(f"{prompt} [y/N] ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no", ""):
            return False


def open_collection(path: str | Path | None = None) -> Collection:
    """Open or create a local Anki collection at the given path."""
    col_path = Path(path) if path else DEFAULT_COLLECTION_PATH
    col_path.parent.mkdir(parents=True, exist_ok=True)
    return Collection(str(col_path))


def import_apkg(
    col: Collection,
    apkg_path: str,
    *,
    dry_run: bool = False,
    allow_updates: bool = False,
) -> ImportResponse.Log | None:
    """Import an .apkg file into the collection.

    By default, notes are never updated (add-only). If allow_updates is True,
    existing notes will be updated if the imported version is newer.

    Returns the import log, or None if dry_run.
    """
    abs_path = os.path.abspath(apkg_path)
    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"File not found: {abs_path}")

    update_condition = (
        IMPORT_ANKI_PACKAGE_UPDATE_CONDITION_IF_NEWER
        if allow_updates
        else IMPORT_ANKI_PACKAGE_UPDATE_CONDITION_NEVER
    )

    options = ImportAnkiPackageOptions(
        merge_notetypes=True,
        update_notes=update_condition,
        update_notetypes=update_condition,
        with_scheduling=True,
        with_deck_configs=True,
    )
    request = ImportAnkiPackageRequest(
        package_path=abs_path,
        options=options,
    )

    if dry_run:
        print(f"  [dry-run] Would import: {apkg_path}")
        return None

    response = col.import_anki_package(request)
    return response.log


def _get_local_meta(col: Collection) -> dict:
    """Get local sync metadata from the collection database."""
    row = col.db.first("select scm, mod, ls from col")
    return {
        "schema": row[0],
        "modified": row[1],
        "last_sync": row[2],
    }


def _determine_sync_action(
    local_meta: dict,
    remote_meta: SyncMeta,
    local_empty: bool,
) -> str:
    """Determine what sync action is needed.

    Returns one of: 'no_changes', 'normal_sync', 'full_upload', 'full_download', 'full_sync'
    """
    if remote_meta.schema != local_meta["schema"]:
        # Schemas differ — full sync needed
        if local_empty and not remote_meta.empty:
            return "full_download"
        if remote_meta.empty and not local_empty:
            return "full_upload"
        if remote_meta.empty and local_empty:
            return "no_changes"
        return "full_sync"

    if remote_meta.modified == local_meta["modified"]:
        return "no_changes"

    return "normal_sync"


def _do_upload(col: Collection, client: AnkiWebClient) -> str:
    """Close the collection, read the file, and upload it to AnkiWeb."""
    col_path = col.path

    # Close the collection so the file is flushed. Use downgrade=False to keep schema 18.
    col.close(downgrade=False)

    col_data = Path(col_path).read_bytes()
    print(f"  Uploading {len(col_data):,} bytes...")
    result = client.upload(col_data)

    return result


def sync_to_ankiweb(
    col: Collection,
    client: AnkiWebClient,
    remote_meta: SyncMeta,
    *,
    dry_run: bool = False,
    force_full_upload: bool = False,
) -> SyncResult:
    """Sync the local collection with AnkiWeb using direct HTTP.

    By default:
    - If schemas match and nothing changed → no action
    - If remote is empty → full upload (safe)
    - If local is empty and remote has data → asks for confirmation
    - If both have data and schemas differ → asks for confirmation

    With force_full_upload=True:
    - Always does a full upload, replacing server data entirely
    """
    result = SyncResult(dry_run=dry_run)

    if remote_meta.server_message:
        result.server_message = remote_meta.server_message
        print(f"  Server message: {remote_meta.server_message}")

    if force_full_upload:
        if dry_run:
            result.sync_action = "full upload (forced)"
            print("  [dry-run] Would force full upload to AnkiWeb")
            return result

        print("  Performing forced full upload...")
        _do_upload(col, client)
        result.sync_action = "full upload (forced)"
        print("  Upload complete.")
        return result

    # Determine what to do
    local_empty = col.is_empty()
    local_meta = _get_local_meta(col)
    action = _determine_sync_action(local_meta, remote_meta, local_empty)

    if action == "no_changes":
        result.sync_action = "no changes"
        print("  Already in sync — no changes needed.")

    elif action == "normal_sync":
        # For a simple upload tool, normal sync (incremental merge) is complex.
        # Since we're primarily adding cards and uploading, do a full upload
        # if local is newer, or report the status.
        if local_meta["modified"] > remote_meta.modified:
            if dry_run:
                result.sync_action = "full upload (local newer)"
                print("  [dry-run] Would upload (local collection is newer)")
                return result

            print("  Local collection is newer. Uploading...")
            _do_upload(col, client)
            result.sync_action = "full upload (local newer)"
            print("  Upload complete.")
        else:
            # Remote is newer — ask before overwriting
            if dry_run:
                result.sync_action = "upload needed (local has changes)"
                print("  [dry-run] Collections differ — would need to upload or download")
                return result

            print("  Collections have diverged (remote may be newer).")
            if _confirm("  Upload local collection to replace remote?"):
                _do_upload(col, client)
                result.sync_action = "full upload (user choice)"
                print("  Upload complete.")
            else:
                result.sync_action = "skipped (user declined)"
                print("  Skipping sync.")

    elif action == "full_upload":
        # Remote is empty — safe to upload
        if dry_run:
            result.sync_action = "full upload (remote empty)"
            print("  [dry-run] Would full upload (remote collection is empty)")
            return result

        print("  Remote collection is empty. Uploading local collection...")
        _do_upload(col, client)
        result.sync_action = "full upload (remote empty)"
        print("  Upload complete.")

    elif action == "full_download":
        if dry_run:
            result.sync_action = "full download needed (local empty)"
            print("  [dry-run] Would need full download (local is empty, remote has data)")
            return result

        print("  Local collection is empty but remote has data.")
        print("  A full download would replace your local collection with the remote one.")
        if _confirm("  Download remote collection?"):
            col_data = client.download()
            col_path = col.path
            col.close(downgrade=False)
            Path(col_path).write_bytes(col_data)
            result.sync_action = "full download"
            print("  Download complete.")
        else:
            print("  Skipping sync. Use --full-upload to upload local data instead.")
            result.sync_action = "skipped (user declined download)"

    elif action == "full_sync":
        if dry_run:
            result.sync_action = "full sync needed (conflict)"
            print("  [dry-run] Full sync needed — schemas differ between local and remote")
            return result

        print("  Full sync required — local and remote collections have diverged.")
        print("  You must choose to either upload (replace remote) or download (replace local).")
        print()
        print("  Options:")
        print("    Upload  — your local collection replaces the remote one")
        print("    Download — the remote collection replaces your local one")
        print()
        if _confirm("  Upload local collection to replace remote?"):
            _do_upload(col, client)
            result.sync_action = "full upload (user choice)"
            print("  Upload complete.")
        elif _confirm("  Download remote collection to replace local?"):
            col_data = client.download()
            col_path = col.path
            col.close(downgrade=False)
            Path(col_path).write_bytes(col_data)
            result.sync_action = "full download (user choice)"
            print("  Download complete.")
        else:
            print("  Skipping sync.")
            result.sync_action = "skipped (user declined)"

    return result


def run_sync(
    apkg_files: list[str],
    username: str,
    password: str,
    *,
    collection_path: str | None = None,
    endpoint: str | None = None,
    dry_run: bool = False,
    force_full_upload: bool = False,
    sync_media: bool = True,
    allow_updates: bool = False,
) -> SyncResult:
    """High-level entry point: import .apkg files and sync to AnkiWeb.

    This is the main orchestration function used by the CLI.
    """
    result = SyncResult(dry_run=dry_run)

    # Open collection
    col_display = collection_path or DEFAULT_COLLECTION_PATH
    print(f"Opening collection at {col_display}...")
    col = open_collection(collection_path)

    try:
        # Import .apkg files
        for apkg_path in apkg_files:
            print(f"\nImporting {apkg_path}...")
            log = import_apkg(col, apkg_path, dry_run=dry_run, allow_updates=allow_updates)
            result.imported_files.append(apkg_path)

            if log is not None:
                n_new = len(log.new)
                n_updated = len(log.updated)
                n_dup = len(log.duplicate)
                result.notes_added += n_new
                result.notes_updated += n_updated
                result.notes_duplicate += n_dup
                print(f"  Added: {n_new}, Updated: {n_updated}, Duplicates (skipped): {n_dup}")

                if n_updated > 0 and not allow_updates:
                    print(f"  Warning: {n_updated} notes were updated despite add-only mode")

        # Report collection state
        note_count = col.note_count()
        card_count = col.card_count()
        print(f"\nCollection: {note_count} notes, {card_count} cards")

        if dry_run:
            print("\n[dry-run] Would sync to AnkiWeb (skipping login)")
            result.sync_action = "dry-run (skipped)"
        else:
            # Login and sync via direct HTTP
            print("\nLogging in to AnkiWeb...")
            client = AnkiWebClient(endpoint)
            client.login(username, password)
            print("  Logged in successfully.")

            print("\nFetching sync status...")
            remote_meta = client.meta()

            sync_result = sync_to_ankiweb(
                col,
                client,
                remote_meta,
                dry_run=False,
                force_full_upload=force_full_upload,
            )
            result.sync_action = sync_result.sync_action
            result.server_message = sync_result.server_message

    finally:
        try:
            col.close()
        except Exception:
            pass  # Collection may already be closed after upload

    return result
