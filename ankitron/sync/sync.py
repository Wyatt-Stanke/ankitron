"""Core sync logic: import .apkg files into a local Anki collection and sync to AnkiWeb."""

from __future__ import annotations

import os
import sys
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

from ankitron.sync.http_client import AnkiWebClient, SyncMeta

DEFAULT_COLLECTION_DIR = Path.home() / ".ankisync"
DEFAULT_COLLECTION_PATH = DEFAULT_COLLECTION_DIR / "collection.anki2"


@dataclass
class SyncResult:
    imported_files: list[str] = field(default_factory=list)
    notes_added: int = 0
    notes_updated: int = 0
    notes_duplicate: int = 0
    sync_action: str = ""
    server_message: str = ""
    dry_run: bool = False


def _is_interactive() -> bool:
    return hasattr(sys.stdin, "isatty") and sys.stdin.isatty()


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
    col.close(downgrade=False)
    col_data = Path(col_path).read_bytes()
    print(f"  Uploading {len(col_data):,} bytes...")
    return client.upload(col_data)


def sync_to_ankiweb(
    col: Collection,
    client: AnkiWebClient,
    remote_meta: SyncMeta,
    *,
    dry_run: bool = False,
    force_full_upload: bool = False,
    force_full_download: bool = False,
) -> SyncResult:
    """Sync the local collection with AnkiWeb using direct HTTP.

    Safe-by-default: never overwrites or deletes remote data unless
    --full-upload is explicitly passed.
    """
    result = SyncResult(dry_run=dry_run)

    if remote_meta.server_message:
        result.server_message = remote_meta.server_message
        print(f"  Server message: {remote_meta.server_message}")

    if force_full_download:
        if dry_run:
            result.sync_action = "full download (forced)"
            print("  [dry-run] Would download remote collection, replacing local")
            return result
        print("  Downloading remote collection (replacing local)...")
        col_data = client.download()
        col_path = col.path
        col.close(downgrade=False)
        Path(col_path).write_bytes(col_data)
        result.sync_action = "full download (forced)"
        print("  Download complete.")
        return result

    if force_full_upload:
        if dry_run:
            result.sync_action = "full upload (forced)"
            print("  [dry-run] Would upload local collection, replacing remote")
            return result
        print("  Uploading local collection (replacing remote)...")
        _do_upload(col, client)
        result.sync_action = "full upload (forced)"
        print("  Upload complete.")
        return result

    local_empty = col.is_empty()
    local_meta = _get_local_meta(col)
    action = _determine_sync_action(local_meta, remote_meta, local_empty)

    if action == "no_changes":
        result.sync_action = "no changes"
        print("  Already in sync — no changes needed.")

    elif action == "full_upload" and remote_meta.empty:
        if dry_run:
            result.sync_action = "upload (remote empty)"
            print("  [dry-run] Would upload to empty remote collection")
            return result
        print("  Remote collection is empty — uploading...")
        _do_upload(col, client)
        result.sync_action = "upload (remote empty)"
        print("  Upload complete.")

    elif action == "normal_sync" and local_meta["modified"] > remote_meta.modified:
        if dry_run:
            result.sync_action = "upload (local newer)"
            print("  [dry-run] Would upload (local collection is newer)")
            return result
        print("  Local collection is newer — uploading...")
        _do_upload(col, client)
        result.sync_action = "upload (local newer)"
        print("  Upload complete.")

    elif action == "normal_sync":
        result.sync_action = "skipped (remote newer)"
        print("  Remote collection is newer than local.")
        print("  Use --full-download to replace local with remote,")
        print("  or --full-upload to replace remote with local.")

    elif action == "full_download":
        result.sync_action = "skipped (local empty, remote has data)"
        print("  Local collection is empty but remote has data.")
        print("  Use --full-download to pull remote data into your local collection.")

    elif action == "full_sync":
        result.sync_action = "skipped (schemas diverged)"
        print("  Local and remote collections have diverged (different schemas).")
        print("  Use --full-upload to replace remote with local,")
        print("  or --full-download to replace local with remote.")

    else:
        result.sync_action = f"skipped ({action})"
        print(f"  Unexpected sync state: {action}")
        print("  Use --full-upload or --full-download to resolve manually.")

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
    force_full_download: bool = False,
    full_download_import_sync: bool = False,
    allow_updates: bool = False,
) -> SyncResult:
    """High-level entry point: import .apkg files and sync to AnkiWeb."""
    result = SyncResult(dry_run=dry_run)

    col_display = collection_path or DEFAULT_COLLECTION_PATH
    print(f"Opening collection at {col_display}...")
    col = open_collection(collection_path)

    client: AnkiWebClient | None = None

    try:
        if full_download_import_sync:
            if dry_run:
                print("\n[dry-run] Would download remote collection before importing FILEs")
            else:
                print("\nLogging in to AnkiWeb...")
                client = AnkiWebClient(endpoint)
                client.login(username, password)
                print("  Logged in successfully.")

                print("\nChecking sync status for pre-import download...")
                remote_meta = client.meta()

                pre_result = sync_to_ankiweb(
                    col,
                    client,
                    remote_meta,
                    dry_run=False,
                    force_full_download=True,
                )
                if pre_result.server_message:
                    result.server_message = pre_result.server_message

                col = open_collection(collection_path)
                print("  Reopened local collection after full download.")

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

        note_count = col.note_count()
        card_count = col.card_count()
        print(f"\nCollection: {note_count} notes, {card_count} cards")

        if dry_run:
            print("\n[dry-run] Would sync to AnkiWeb (skipping login)")
            result.sync_action = "dry-run (skipped)"
        else:
            if client is None:
                print("\nLogging in to AnkiWeb...")
                client = AnkiWebClient(endpoint)
                client.login(username, password)
                print("  Logged in successfully.")

            print("\nChecking sync status...")
            remote_meta = client.meta()

            sync_result = sync_to_ankiweb(
                col,
                client,
                remote_meta,
                dry_run=False,
                force_full_upload=force_full_upload,
                force_full_download=force_full_download,
            )
            result.sync_action = sync_result.sync_action
            result.server_message = sync_result.server_message

    finally:
        import contextlib

        with contextlib.suppress(Exception):
            col.close()

    return result
