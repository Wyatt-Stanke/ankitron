"""
SQLite-backed AI cache — version-gated, input-hash-keyed.

AI cache entries never expire by time.  They are invalidated only when the
field version changes or the resolved input values change.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _locate_db() -> Path:
    """Return the cache DB path (project-local or global)."""
    project_dir = Path(".ankitron")
    if project_dir.is_dir():
        return project_dir / "cache.db"
    global_dir = Path.home() / ".cache" / "ankitron"
    global_dir.mkdir(parents=True, exist_ok=True)
    return global_dir / "cache.db"


def ai_cache_key(field_version: int, resolved_inputs: dict[str, str]) -> str:
    """Deterministic hash of version + input values (prompt & model excluded)."""
    content = json.dumps(
        {"version": field_version, "inputs": resolved_inputs},
        sort_keys=True,
    )
    return hashlib.sha256(content.encode()).hexdigest()


def card_source_cache_key(version: int, input_hash: str) -> str:
    """Cache key for AICardSource output (entire row set)."""
    content = json.dumps(
        {"version": version, "input": input_hash},
        sort_keys=True,
    )
    return hashlib.sha256(content.encode()).hexdigest()


_FIELD_SCHEMA = """\
CREATE TABLE IF NOT EXISTS ai_cache (
    deck_class    TEXT NOT NULL,
    row_pk        TEXT NOT NULL,
    field_name    TEXT NOT NULL,
    field_version INTEGER NOT NULL,
    input_hash    TEXT NOT NULL,
    model         TEXT NOT NULL,
    prompt_template TEXT NOT NULL,
    resolved_prompt TEXT NOT NULL,
    resolved_inputs TEXT NOT NULL,
    output        TEXT NOT NULL,
    generated_at  TIMESTAMP NOT NULL,
    tokens_in     INTEGER,
    tokens_out    INTEGER,
    cost_usd      REAL,
    reviewed      BOOLEAN DEFAULT FALSE,
    reviewed_at   TIMESTAMP,
    PRIMARY KEY (deck_class, row_pk, field_name)
);
"""

_CARD_SOURCE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS ai_card_source_cache (
    deck_class    TEXT NOT NULL,
    source_name   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    input_hash    TEXT NOT NULL,
    output_rows   TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    generated_at  TIMESTAMP NOT NULL,
    tokens_in     INTEGER,
    tokens_out    INTEGER,
    cost_usd      REAL,
    PRIMARY KEY (deck_class, source_name)
);
"""


class AICache:
    """SQLite-backed AI value cache with version-gated invalidation."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or _locate_db()
        self._conn: sqlite3.Connection | None = None

    # -- connection management ------------------------------------------------

    @property
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_FIELD_SCHEMA)
            self._conn.executescript(_CARD_SOURCE_SCHEMA)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- per-field cache API --------------------------------------------------

    def get(
        self,
        deck_class: str,
        row_pk: str,
        field_name: str,
        field_version: int,
        input_hash: str,
    ) -> str | None:
        """Look up a cached value.  Returns ``None`` on miss."""
        row = self._db.execute(
            "SELECT output, field_version, input_hash FROM ai_cache "
            "WHERE deck_class=? AND row_pk=? AND field_name=?",
            (deck_class, row_pk, field_name),
        ).fetchone()
        if row is None:
            return None
        stored_output, stored_version, stored_hash = row
        if stored_version != field_version or stored_hash != input_hash:
            return None
        return stored_output

    def put(
        self,
        *,
        deck_class: str,
        row_pk: str,
        field_name: str,
        field_version: int,
        input_hash: str,
        model: str,
        prompt_template: str,
        resolved_prompt: str,
        resolved_inputs: dict[str, str],
        output: str,
        tokens_in: int | None = None,
        tokens_out: int | None = None,
        cost_usd: float | None = None,
    ) -> None:
        """Insert or replace a cached AI value."""
        self._db.execute(
            "INSERT OR REPLACE INTO ai_cache "
            "(deck_class,row_pk,field_name,field_version,input_hash,"
            "model,prompt_template,resolved_prompt,resolved_inputs,"
            "output,generated_at,tokens_in,tokens_out,cost_usd) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                deck_class,
                row_pk,
                field_name,
                field_version,
                input_hash,
                model,
                prompt_template,
                resolved_prompt,
                json.dumps(resolved_inputs, sort_keys=True),
                output,
                datetime.now(UTC).isoformat(),
                tokens_in,
                tokens_out,
                cost_usd,
            ),
        )
        self._db.commit()

    # -- cache promotion ------------------------------------------------------

    def promote(
        self,
        deck_class: str,
        field_name: str,
        from_version: int,
        to_version: int,
        exclude_pks: set[str] | None = None,
    ) -> int:
        """Promote cached values from one version to another.

        Returns the number of promoted entries.
        """
        rows = self._db.execute(
            "SELECT row_pk FROM ai_cache WHERE deck_class=? AND field_name=? AND field_version=?",
            (deck_class, field_name, from_version),
        ).fetchall()
        promoted = 0
        for (pk,) in rows:
            if exclude_pks and pk in exclude_pks:
                continue
            self._db.execute(
                "UPDATE ai_cache SET field_version=? "
                "WHERE deck_class=? AND row_pk=? AND field_name=?",
                (to_version, deck_class, pk, field_name),
            )
            promoted += 1
        self._db.commit()
        return promoted

    # -- AICardSource cache ---------------------------------------------------

    def get_card_source(
        self,
        deck_class: str,
        source_name: str,
        version: int,
        input_hash: str,
    ) -> list[dict[str, Any]] | None:
        """Look up a cached AICardSource result set."""
        row = self._db.execute(
            "SELECT output_rows, version, input_hash FROM ai_card_source_cache "
            "WHERE deck_class=? AND source_name=?",
            (deck_class, source_name),
        ).fetchone()
        if row is None:
            return None
        stored_rows, stored_ver, stored_hash = row
        if stored_ver != version or stored_hash != input_hash:
            return None
        return json.loads(stored_rows)

    def put_card_source(
        self,
        *,
        deck_class: str,
        source_name: str,
        version: int,
        input_hash: str,
        output_rows: list[dict[str, Any]],
        tokens_in: int | None = None,
        tokens_out: int | None = None,
        cost_usd: float | None = None,
    ) -> None:
        """Insert or replace a cached AICardSource result set."""
        self._db.execute(
            "INSERT OR REPLACE INTO ai_card_source_cache "
            "(deck_class,source_name,version,input_hash,output_rows,"
            "row_count,generated_at,tokens_in,tokens_out,cost_usd) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                deck_class,
                source_name,
                version,
                input_hash,
                json.dumps(output_rows),
                len(output_rows),
                datetime.now(UTC).isoformat(),
                tokens_in,
                tokens_out,
                cost_usd,
            ),
        )
        self._db.commit()

    # -- stats / maintenance --------------------------------------------------

    def stats(self, deck_class: str | None = None) -> dict[str, Any]:
        """Return aggregate stats about the cache."""
        where = "WHERE deck_class=?" if deck_class else ""
        params: tuple = (deck_class,) if deck_class else ()
        total = self._db.execute(
            f"SELECT COUNT(*) FROM ai_cache {where}",  # noqa: S608
            params,
        ).fetchone()[0]
        cost = self._db.execute(
            f"SELECT COALESCE(SUM(cost_usd),0) FROM ai_cache {where}",  # noqa: S608
            params,
        ).fetchone()[0]
        return {"total_entries": total, "total_cost_usd": cost}

    def clear(self, deck_class: str | None = None) -> int:
        """Remove cache entries.  Returns number of rows deleted."""
        if deck_class:
            cur = self._db.execute("DELETE FROM ai_cache WHERE deck_class=?", (deck_class,))
        else:
            cur = self._db.execute("DELETE FROM ai_cache")
        self._db.commit()
        return cur.rowcount
