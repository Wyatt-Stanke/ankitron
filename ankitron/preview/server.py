"""Live preview server runner for hot-reloading deck previews.

All FastAPI app construction lives in `ankitron.preview.app`.
"""

from __future__ import annotations

from os import path
from typing import Any

from ankitron.preview.app import create_preview_app


def _ensure_deps() -> None:
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError as err:
        raise ImportError(
            "Live preview requires extra dependencies. Install with: pip install ankitron[preview]"
        ) from err


def create_app(filepath: str, deck_name: str | None = None) -> Any:
    """Create preview app backed by a file and hot-reload callback."""
    _ensure_deps()
    state: dict[str, Any] = {
        "runtime": {
            "module": None,
            "deck": None,
            "instance": None,
            "version": 0,
        }
    }

    def reload_state() -> None:
        from ankitron.cli.main import _load_deck_module

        decks = _load_deck_module(filepath)
        if deck_name:
            decks = [d for d in decks if d.__name__ == deck_name or d._deck_name == deck_name]
        if not decks:
            raise RuntimeError("No Deck subclasses found")
        deck_cls = decks[0]
        instance = deck_cls()
        instance.fetch()

        previous = state["runtime"]
        state["runtime"] = {
            "module": None,
            "deck": deck_cls,
            "instance": instance,
            "version": previous["version"] + 1,
        }

    reload_state()
    return create_preview_app(_get_frontend_html(), state, reload_callback=reload_state)


def create_app_from_instance(deck_instance: Any) -> Any:
    """Create preview app backed by an existing deck instance."""
    _ensure_deps()
    state: dict[str, Any] = {
        "runtime": {
            "module": None,
            "deck": deck_instance.__class__,
            "instance": deck_instance,
            "version": 1,
        }
    }
    return create_preview_app(_get_frontend_html(), state, reload_callback=None)


def run_preview_server(
    filepath: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8742,
    deck_name: str | None = None,
    deck_instance: Any = None,
) -> None:
    """Launch the preview server with file watching.

    Either filepath or deck_instance must be provided.
    """
    _ensure_deps()
    import uvicorn

    if deck_instance is not None:
        app = create_app_from_instance(deck_instance)
        filepath_for_watcher = None
    elif filepath is not None:
        app = create_app(filepath, deck_name)
        filepath_for_watcher = filepath
    else:
        raise ValueError("Either filepath or deck_instance must be provided")

    print(f"ankitron preview: http://{host}:{port}")
    print("Press Ctrl+C to stop")

    stop_watcher = None
    if filepath_for_watcher:
        try:
            from ankitron.preview.watcher import start_watcher

            stop_watcher = start_watcher(filepath_for_watcher, host, port)
        except ImportError:
            pass

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    finally:
        if stop_watcher:
            stop_watcher()


def _get_frontend_html() -> str:
    """Lazily load the frontend HTML template."""
    global _FRONTEND_HTML  # noqa: PLW0603
    if _FRONTEND_HTML is None:
        with open(path.join(path.dirname(__file__), "index.html"), encoding="utf-8") as f:
            _FRONTEND_HTML = f.read()
    return _FRONTEND_HTML


_FRONTEND_HTML: str | None = None
