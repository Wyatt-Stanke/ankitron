"""File watcher — monitors deck files for changes and triggers reload."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


def start_watcher(filepath: str, host: str, port: int) -> Callable[[], None]:
    """Start a file watcher that sends reload signals on changes.

    Returns a stop function to shut down the watcher.
    """
    try:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError as err:
        raise ImportError(
            "File watching requires watchdog. Install with: pip install ankitron[preview]"
        ) from err

    filepath = os.path.abspath(filepath)
    watch_dir = os.path.dirname(filepath)

    class _Handler(FileSystemEventHandler):
        def __init__(self) -> None:
            self._last_reload = 0.0

        def _maybe_reload(self, event: FileSystemEvent) -> None:
            if event.is_directory:
                return

            changed_paths = [getattr(event, "src_path", ""), getattr(event, "dest_path", "")]
            if not any(str(p).lower().endswith(".py") for p in changed_paths if p):
                return

            # Debounce — at most once per second.
            now = time.time()
            if now - self._last_reload < 1.0:
                return

            self._last_reload = now
            _send_reload(host=host, port=port)

        def on_modified(self, event: FileSystemEvent) -> None:  # type: ignore[override]
            self._maybe_reload(event)

        def on_created(self, event: FileSystemEvent) -> None:  # type: ignore[override]
            self._maybe_reload(event)

        def on_moved(self, event: FileSystemEvent) -> None:  # type: ignore[override]
            self._maybe_reload(event)

    handler = _Handler()
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=True)
    observer.daemon = True
    observer.start()

    def stop():
        observer.stop()
        observer.join(timeout=2)

    return stop


def _send_reload(host: str, port: int) -> None:
    """Send a reload signal to the preview server via HTTP."""
    try:
        import requests

        requests.post(f"http://{host}:{port}/api/reload", timeout=1)
    except Exception:  # noqa: S110
        pass  # Server might not be ready yet
