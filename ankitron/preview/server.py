"""
Live preview server — FastAPI + WebSocket for hot-reloading deck previews.

Requires the `preview` extra: ``pip install ankitron[preview]``.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import re

from ankitron.deck import _FIELD_REF_PATTERN


def _ensure_deps() -> None:
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError as err:
        raise ImportError(
            "Live preview requires extra dependencies. Install with: pip install ankitron[preview]"
        ) from err


def _load_deck_module(filepath: str) -> Any:
    """Load/reload a deck module from file."""
    filepath = os.path.abspath(filepath)
    spec = importlib.util.spec_from_file_location("_preview_deck", filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {filepath}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_deck_classes(module: Any, deck_name: str | None = None) -> list[type]:
    """Find all Deck subclasses in a module."""
    from ankitron.deck import Deck

    decks = []
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if isinstance(obj, type) and issubclass(obj, Deck) and obj is not Deck:
            if deck_name and obj.__name__ != deck_name and obj._deck_name != deck_name:
                continue
            decks.append(obj)
    return decks


def _render_card(card_cls: type, row: dict[str, str]) -> dict[str, str]:
    """Render a card template for a specific row."""

    def substitute(template: str) -> str:
        def repl(m: re.Match) -> str:
            return row.get(m.group(1), "")

        return _FIELD_REF_PATTERN.sub(repl, template)

    return {
        "name": card_cls.__name__,
        "front": substitute(card_cls.front),
        "back": substitute(card_cls.back),
    }


def create_app(filepath: str, deck_name: str | None = None) -> Any:
    """Create the FastAPI application for live preview."""
    _ensure_deps()
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse

    app = FastAPI(title="ankitron preview")
    _state: dict[str, Any] = {"module": None, "deck": None, "instance": None}

    def _reload():
        module = _load_deck_module(filepath)
        decks = _find_deck_classes(module, deck_name)
        if not decks:
            raise RuntimeError("No Deck subclasses found")
        deck_cls = decks[0]
        instance = deck_cls()
        instance.fetch()
        _state["module"] = module
        _state["deck"] = deck_cls
        _state["instance"] = instance

    # Initial load
    _reload()

    @app.get("/")
    async def index():
        return HTMLResponse(_FRONTEND_HTML)

    @app.get("/api/deck")
    async def api_deck():
        cls = _state["deck"]
        return JSONResponse(
            {
                "name": cls._deck_name,
                "fields": [
                    {"name": n, "kind": f.kind.value, "internal": f.internal}
                    for n, f in cls._all_fields
                ],
                "cards": [c.__name__ for c in cls._deck_cards],
                "row_count": len(_state["instance"]._data or []),
            }
        )

    @app.get("/api/rows")
    async def api_rows(offset: int = 0, limit: int = 50):
        data = _state["instance"]._data or []
        cls = _state["deck"]
        visible = [n for n, f in cls._all_fields if not f.internal]
        rows = [{k: row.get(k, "") for k in visible} for row in data[offset : offset + limit]]
        return JSONResponse({"rows": rows, "total": len(data)})

    @app.get("/api/row/{pk}")
    async def api_row(pk: str):
        inst = _state["instance"]
        cls = _state["deck"]
        pk_attr = cls._pk_field_attr
        for row in inst._data or []:
            row_pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if row_pk == pk:
                visible = [n for n, f in cls._all_fields if not f.internal]
                return JSONResponse({k: row.get(k, "") for k in visible})
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.get("/api/card/{card_type}/{pk}")
    async def api_card(card_type: str, pk: str):
        inst = _state["instance"]
        cls = _state["deck"]
        pk_attr = cls._pk_field_attr

        card_cls = None
        for c in cls._deck_cards:
            if c.__name__ == card_type:
                card_cls = c
                break
        if card_cls is None:
            return JSONResponse({"error": "card type not found"}, status_code=404)

        for row in inst._data or []:
            row_pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if row_pk == pk:
                rendered = _render_card(card_cls, row)
                return JSONResponse(rendered)
        return JSONResponse({"error": "row not found"}, status_code=404)

    @app.get("/api/tags")
    async def api_tags():
        cls = _state["deck"]
        from ankitron.export import resolve_tags

        inst = _state["instance"]
        tag_counts: dict[str, int] = {}
        for row in inst._data or []:
            tags = resolve_tags(cls._deck_tags, row)
            for t in tags:
                tag_counts[t] = tag_counts.get(t, 0) + 1
        return JSONResponse(tag_counts)

    @app.get("/api/validation")
    async def api_validation():
        cls = _state["deck"]
        inst = _state["instance"]
        if not cls._deck_validators:
            return JSONResponse({"validators": []})

        from ankitron.validation import run_validators

        results = run_validators(cls._deck_validators, inst._data or [])
        return JSONResponse(
            {
                "validators": [
                    {
                        "name": r.name,
                        "passed": r.passed,
                        "severity": r.severity.value,
                        "messages": r.messages[:10],
                    }
                    for r in results
                ]
            }
        )

    connected_websockets: list[WebSocket] = []

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        connected_websockets.append(ws)
        try:
            while True:
                msg = await ws.receive_text()
                if msg == "reload":
                    try:
                        _reload()
                        await ws.send_json({"type": "full_reload"})
                    except Exception as exc:
                        await ws.send_json({"type": "error", "message": str(exc)})
        except WebSocketDisconnect:
            connected_websockets.remove(ws)

    return app


def _create_app_from_instance(deck_instance: Any, _deck_name: str | None = None) -> Any:
    """Create a FastAPI app from a pre-built deck instance (no file watching)."""
    _ensure_deps()
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse

    app = FastAPI(title="ankitron preview")
    cls = deck_instance.__class__

    @app.get("/")
    async def index():
        return HTMLResponse(_FRONTEND_HTML)

    @app.get("/api/deck")
    async def api_deck():
        return JSONResponse(
            {
                "name": cls._deck_name,
                "fields": [
                    {"name": n, "kind": f.kind.value, "internal": f.internal}
                    for n, f in cls._all_fields
                ],
                "cards": [c.__name__ for c in cls._deck_cards],
                "row_count": len(deck_instance._data or []),
            }
        )

    @app.get("/api/rows")
    async def api_rows(offset: int = 0, limit: int = 50):
        data = deck_instance._data or []
        visible = [n for n, f in cls._all_fields if not f.internal]
        rows = [{k: row.get(k, "") for k in visible} for row in data[offset : offset + limit]]
        return JSONResponse({"rows": rows, "total": len(data)})

    @app.get("/api/row/{pk}")
    async def api_row(pk: str):
        pk_attr = cls._pk_field_attr
        for row in deck_instance._data or []:
            row_pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if row_pk == pk:
                visible = [n for n, f in cls._all_fields if not f.internal]
                return JSONResponse({k: row.get(k, "") for k in visible})
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.get("/api/card/{card_type}/{pk}")
    async def api_card(card_type: str, pk: str):
        pk_attr = cls._pk_field_attr
        card_cls = None
        for c in cls._deck_cards:
            if c.__name__ == card_type:
                card_cls = c
                break
        if card_cls is None:
            return JSONResponse({"error": "card type not found"}, status_code=404)
        for row in deck_instance._data or []:
            row_pk = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            if row_pk == pk:
                rendered = _render_card(card_cls, row)
                return JSONResponse(rendered)
        return JSONResponse({"error": "row not found"}, status_code=404)

    return app


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
        # Create app from an already-built deck instance
        app = _create_app_from_instance(deck_instance, deck_name)
        filepath_for_watcher = None
    elif filepath is not None:
        app = create_app(filepath, deck_name)
        filepath_for_watcher = filepath
    else:
        raise ValueError("Either filepath or deck_instance must be provided")

    print(f"ankitron preview: http://{host}:{port}")
    print("Press Ctrl+C to stop")

    # Start file watcher in background (only for file-based preview)
    stop_watcher = None
    if filepath_for_watcher:
        try:
            from ankitron.preview.watcher import start_watcher

            stop_watcher = start_watcher(filepath_for_watcher, port)
        except ImportError:
            pass

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    finally:
        if stop_watcher:
            stop_watcher()


_FRONTEND_HTML = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ankitron preview</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: system-ui, -apple-system, sans-serif; background: #f5f5f5; color: #333; }
.header { background: #1a1a2e; color: white; padding: 12px 20px; display: flex; align-items: center; gap: 12px; }
.header h1 { font-size: 18px; font-weight: 600; }
.header .badge { background: #4a90d9; padding: 2px 8px; border-radius: 12px; font-size: 12px; }
.tabs { display: flex; background: #fff; border-bottom: 1px solid #ddd; padding: 0 20px; }
.tab { padding: 10px 16px; cursor: pointer; border-bottom: 2px solid transparent; font-size: 14px; color: #666; }
.tab.active { color: #4a90d9; border-color: #4a90d9; }
.content { padding: 20px; max-width: 1200px; margin: 0 auto; }
.card-preview { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 24px; margin: 12px 0; min-height: 120px; cursor: pointer; }
.card-preview .label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
.card-preview .content-text { font-size: 18px; line-height: 1.5; }
table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
th { background: #f8f8f8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #666; padding: 10px 12px; text-align: left; }
td { padding: 10px 12px; border-top: 1px solid #f0f0f0; font-size: 14px; }
tr:hover td { background: #fafafa; }
.status { text-align: center; padding: 40px; color: #999; }
#ws-status { width: 8px; height: 8px; border-radius: 50%; background: #ccc; display: inline-block; }
#ws-status.connected { background: #4caf50; }
#ws-status.disconnected { background: #f44336; }
</style>
</head>
<body>
<div class="header">
    <h1>ankitron</h1>
    <span class="badge" id="deck-name">loading...</span>
    <span class="badge" id="row-count">...</span>
    <span id="ws-status"></span>
</div>
<div class="tabs">
    <div class="tab active" data-view="cards">Cards</div>
    <div class="tab" data-view="table">Data</div>
    <div class="tab" data-view="validation">Validation</div>
</div>
<div class="content" id="main-content">
    <div class="status">Loading...</div>
</div>

<script>
(function() {
    let state = { view: 'cards', deck: null, rows: [], cards: [], currentRow: 0, flipped: false };

    async function fetchJSON(url) {
        const r = await fetch(url);
        return r.json();
    }

    async function loadDeck() {
        state.deck = await fetchJSON('/api/deck');
        document.getElementById('deck-name').textContent = state.deck.name;
        document.getElementById('row-count').textContent = state.deck.row_count + ' rows';

        const rowData = await fetchJSON('/api/rows?limit=100');
        state.rows = rowData.rows;
        state.cards = state.deck.cards;

        render();
    }

    function render() {
        const el = document.getElementById('main-content');
        if (state.view === 'cards') renderCards(el);
        else if (state.view === 'table') renderTable(el);
        else if (state.view === 'validation') renderValidation(el);
    }

    function renderCards(el) {
        if (!state.rows.length) { el.innerHTML = '<div class="status">No data</div>'; return; }
        const row = state.rows[state.currentRow] || state.rows[0];
        const fields = Object.keys(row);
        let html = '<div style="text-align:center;margin-bottom:12px;color:#999;font-size:13px;">' +
            'Row ' + (state.currentRow + 1) + ' / ' + state.rows.length +
            ' &nbsp; ← → to navigate, Space to flip</div>';

        for (const cardName of state.cards) {
            html += '<div class="card-preview" onclick="this.classList.toggle(\'flipped\')">' +
                '<div class="label">' + cardName + '</div>' +
                '<div class="content-text">' + JSON.stringify(row) + '</div></div>';
        }
        el.innerHTML = html;
    }

    function renderTable(el) {
        if (!state.rows.length) { el.innerHTML = '<div class="status">No data</div>'; return; }
        const keys = Object.keys(state.rows[0]);
        let html = '<table><tr>';
        keys.forEach(k => html += '<th>' + k + '</th>');
        html += '</tr>';
        state.rows.forEach(row => {
            html += '<tr>';
            keys.forEach(k => html += '<td>' + (row[k] || '') + '</td>');
            html += '</tr>';
        });
        html += '</table>';
        el.innerHTML = html;
    }

    async function renderValidation(el) {
        const data = await fetchJSON('/api/validation');
        if (!data.validators || !data.validators.length) {
            el.innerHTML = '<div class="status">No validators configured</div>';
            return;
        }
        let html = '<table><tr><th>Validator</th><th>Status</th><th>Severity</th><th>Messages</th></tr>';
        data.validators.forEach(v => {
            const status = v.passed ? '✅ Passed' : '❌ Failed';
            const msgs = v.messages.join('; ');
            html += '<tr><td>' + v.name + '</td><td>' + status + '</td><td>' + v.severity + '</td><td>' + msgs + '</td></tr>';
        });
        html += '</table>';
        el.innerHTML = html;
    }

    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            state.view = tab.dataset.view;
            render();
        });
    });

    // Keyboard navigation
    document.addEventListener('keydown', e => {
        if (state.view === 'cards') {
            if (e.key === 'ArrowRight') { state.currentRow = Math.min(state.currentRow + 1, state.rows.length - 1); render(); }
            if (e.key === 'ArrowLeft') { state.currentRow = Math.max(state.currentRow - 1, 0); render(); }
        }
    });

    // WebSocket
    function connectWS() {
        const ws = new WebSocket('ws://' + location.host + '/ws');
        const indicator = document.getElementById('ws-status');
        ws.onopen = () => { indicator.className = 'connected'; };
        ws.onclose = () => { indicator.className = 'disconnected'; setTimeout(connectWS, 2000); };
        ws.onmessage = (e) => {
            const msg = JSON.parse(e.data);
            if (msg.type === 'full_reload') loadDeck();
        };
    }

    loadDeck();
    connectWS();
})();
</script>
</body>
</html>
"""
