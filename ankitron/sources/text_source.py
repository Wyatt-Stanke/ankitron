"""
TextSource — read plain-text or Markdown files as input.

Supports single files and glob-based discovery for DeckFamily.
"""

from __future__ import annotations

import glob as glob_mod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ankitron.deck import Field


class TextSource:
    """A data source backed by a text file (plain text, Markdown, etc.).

    The entire file content is provided as a single value.  Useful as input
    to ``AICardSource`` for generating flashcards from prose.

    Args:
        path: File path (may contain ``{param}`` placeholders for DeckFamily).
        encoding: File encoding (default ``utf-8``).
    """

    def __init__(
        self,
        path: str,
        *,
        encoding: str = "utf-8",
        discover: Any | None = None,
    ) -> None:
        self._path = path
        self._encoding = encoding
        self._discover = discover

    def Field(self, source_key: str | None = None, **kwargs: Any) -> Field:
        """Create a Field bound to this source's text content."""
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = source_key or "text"
        return fld

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Read the file and return a single-row result."""
        p = Path(self._path)
        if not p.exists():
            raise FileNotFoundError(f"TextSource: file not found: {self._path}")

        content = p.read_text(encoding=self._encoding)

        row: dict[str, str] = {}
        for attr_name, _fld in fields:
            row[attr_name] = content

        return [row]

    @staticmethod
    def glob() -> _TextGlob:
        """Marker for DeckFamily auto-discovery via glob patterns."""
        return _TextGlob()


class _TextGlob:
    """Sentinel returned by ``TextSource.glob()`` for DeckFamily discovery."""

    def discover(self, pattern: str) -> list[dict[str, str]]:
        """Expand ``{param}`` placeholders into glob patterns and extract params."""
        import re

        # Replace {param} with (*) for globbing, track param names
        glob_pattern = pattern

        glob_pattern = re.sub(r"\{(\w+)\}", "*", pattern)

        # Build regex to extract param values from matched paths
        regex_pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/\\\\]+)", pattern)
        regex = re.compile(regex_pattern)

        results: list[dict[str, str]] = []
        for matched_path in sorted(glob_mod.glob(glob_pattern)):
            m = regex.match(matched_path)
            if m:
                results.append(m.groupdict())

        return results
