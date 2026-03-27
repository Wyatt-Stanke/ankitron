"""
ankitron.sync — import .apkg files into a local Anki collection and sync to AnkiWeb.

Migrated from the standalone ``ankisync`` package.
"""

from ankitron.sync.http_client import AnkiWebClient, SyncError, SyncMeta
from ankitron.sync.sync import (
    SyncResult,
    import_apkg,
    open_collection,
    run_sync,
    sync_to_ankiweb,
)

__all__ = [
    "AnkiWebClient",
    "SyncError",
    "SyncMeta",
    "SyncResult",
    "import_apkg",
    "open_collection",
    "run_sync",
    "sync_to_ankiweb",
]
