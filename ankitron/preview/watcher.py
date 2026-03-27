"""
File watcher — monitors deck files for changes and triggers reload.

Uses watchdog to watch for file modifications and sends
reload signals via WebSocket.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


def start_watcher(filepath: str, port: int) -> Callable[[], None]:
    """Start a file watcher that sends reload signals on changes.

    Returns a stop function to shut down the watcher.
    """
    try:
        from watchdog.events import FileModifiedEvent, FileSystemEventHandler
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

        def on_modified(self, event: FileModifiedEvent) -> None:  # type: ignore[override]
            if event.is_directory:
                return
            if not str(event.src_path).endswith(".py"):
                return
            # Debounce — at most once per second
            now = time.time()
            if now - self._last_reload < 1.0:
                return
            self._last_reload = now
            _send_reload(port)

    handler = _Handler()
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=True)
    observer.daemon = True
    observer.start()

    def stop():
        observer.stop()
        observer.join(timeout=2)

    return stop


def _send_reload(port: int) -> None:
    """Send a reload signal to the preview server via WebSocket."""
    try:
        import websockets.sync.client as ws_client

        with ws_client.connect(f"ws://127.0.0.1:{port}/ws") as ws:
            ws.send("reload")
    except Exception:  # noqa: S110
        pass  # Server might not be ready yet
