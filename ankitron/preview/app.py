"""FastAPI app construction for ankitron preview."""

from __future__ import annotations

from typing import Any

from ankitron.deck import _FIELD_REF_PATTERN


def _render_card(card_cls: Any, row: dict[str, str]) -> dict[str, str]:
    """Render a card template for a specific row."""

    def substitute(template: str) -> str:
        def repl(match: Any) -> str:
            return row.get(match.group(1), "")

        return _FIELD_REF_PATTERN.sub(repl, template)

    rendered_front = substitute(card_cls.front)
    rendered_back = substitute(card_cls.back).replace("{{FrontSide}}", rendered_front)

    return {
        "name": card_cls.__name__,
        "front": rendered_front,
        "back": rendered_back,
    }


def _pk_matches(row: dict[str, Any], pk_attr: str, requested_pk: str) -> bool:
    """Return True if requested PK matches either canonical or display PK for a row."""
    canonical_pk = str(row.get(f"_pk_{pk_attr}", row.get(pk_attr, "")))
    display_pk = str(row.get(pk_attr, ""))
    return requested_pk in {canonical_pk, display_pk}


def create_preview_app(
    frontend_html: str,
    state: dict[str, Any],
    reload_callback: Any | None = None,
) -> Any:
    """Create a FastAPI app for previewing deck data.

    Args:
        frontend_html: HTML string served at `/`.
        state: Mutable runtime dict with keys: deck, instance, version.
        reload_callback: Optional callable invoked on websocket `reload` message.
    """
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse

    app = FastAPI(title="ankitron preview")

    def runtime() -> dict[str, Any]:
        return state["runtime"]

    @app.get("/")
    async def index() -> HTMLResponse:
        return HTMLResponse(frontend_html)

    @app.get("/api/deck")
    async def api_deck() -> JSONResponse:
        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        return JSONResponse(
            {
                "name": cls._deck_name,
                "pk_field": cls._pk_field_attr,
                "fields": [
                    {"name": name, "kind": field.kind.value, "internal": field.internal}
                    for name, field in cls._all_fields
                ],
                "cards": [card.__name__ for card in cls._deck_cards],
                "row_count": len(instance._data or []),
                "version": data["version"],
            }
        )

    @app.get("/api/rows")
    async def api_rows(offset: int = 0, limit: int = 50) -> JSONResponse:
        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        visible = [name for name, field in cls._all_fields if not field.internal]
        rows = []
        pk_attr = cls._pk_field_attr
        for row in (instance._data or [])[offset : offset + limit]:
            out = {key: row.get(key, "") for key in visible}
            out["__pk"] = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
            rows.append(out)
        return JSONResponse({"rows": rows, "total": len(instance._data or [])})

    @app.get("/api/row/{pk}")
    async def api_row(pk: str) -> JSONResponse:
        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        pk_attr = cls._pk_field_attr
        for row in instance._data or []:
            if _pk_matches(row, pk_attr, pk):
                visible = [name for name, field in cls._all_fields if not field.internal]
                out = {key: row.get(key, "") for key in visible}
                out["__pk"] = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
                return JSONResponse(out)
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.get("/api/card/{card_type}/{pk}")
    async def api_card(card_type: str, pk: str) -> JSONResponse:
        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        pk_attr = cls._pk_field_attr

        card_cls = None
        for candidate in cls._deck_cards:
            if candidate.__name__ == card_type:
                card_cls = candidate
                break
        if card_cls is None:
            return JSONResponse({"error": "card type not found"}, status_code=404)

        for row in instance._data or []:
            if _pk_matches(row, pk_attr, pk):
                return JSONResponse(_render_card(card_cls, row))
        return JSONResponse(
            {"error": "row not found"},
            status_code=404,
        )

    @app.get("/api/tags")
    async def api_tags() -> JSONResponse:
        from ankitron.export import resolve_tags

        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        tag_counts: dict[str, int] = {}
        for row in instance._data or []:
            for tag in resolve_tags(cls._deck_tags, row):
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        return JSONResponse(tag_counts)

    @app.get("/api/validation")
    async def api_validation() -> JSONResponse:
        from ankitron.validation import run_validators

        data = runtime()
        cls = data["deck"]
        instance = data["instance"]
        if not cls._deck_validators:
            return JSONResponse({"validators": []})

        results = run_validators(cls._deck_validators, instance._data or [])
        return JSONResponse(
            {
                "validators": [
                    {
                        "name": result.name,
                        "passed": result.passed,
                        "severity": result.severity.value,
                        "messages": result.messages[:10],
                    }
                    for result in results
                ]
            }
        )

    @app.post("/api/reload")
    async def api_reload() -> JSONResponse:
        if reload_callback is None:
            return JSONResponse(
                {"reloaded": False, "reason": "reload unsupported"},
                status_code=400,
            )
        try:
            reload_callback()
        except Exception as exc:
            return JSONResponse({"reloaded": False, "error": str(exc)}, status_code=500)
        return JSONResponse({"reloaded": True})

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        try:
            while True:
                msg = await ws.receive_text()
                if msg == "reload" and reload_callback is not None:
                    try:
                        reload_callback()
                        await ws.send_json({"type": "full_reload"})
                    except Exception as exc:
                        await ws.send_json({"type": "error", "message": str(exc)})
                else:
                    await ws.send_json({"type": "noop"})
        except WebSocketDisconnect:
            return

    return app
