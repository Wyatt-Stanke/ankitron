"""
CSVSource — load data from CSV/TSV files.

Supports standalone use (as the primary source for a deck) or
linked use (joined to another source via a LinkStrategy).
"""

from __future__ import annotations

import csv
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ankitron.deck import Field
    from ankitron.sources.link_strategy import LinkStrategy


class CSVSource:
    """A data source backed by a local CSV or TSV file."""

    def __init__(
        self,
        path: str,
        *,
        linked_to: Any | None = None,
        via: LinkStrategy | None = None,
        delimiter: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self._path = path
        self._linked_to = linked_to
        self._via = via
        self._delimiter = delimiter
        self._encoding = encoding

    def Field(
        self,
        column: str,
        *,
        coerce: type | None = None,
        **kwargs: Any,
    ) -> Field:
        """Create a Field bound to a column in this CSV."""
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = column
        fld._csv_coerce = coerce  # type: ignore[attr-defined]
        return fld

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Read the CSV file and return rows as dicts.

        Each dict maps deck field attribute names to string values.
        """
        if not os.path.isfile(self._path):
            raise FileNotFoundError(f"CSVSource: file not found: {self._path}")

        delimiter = self._delimiter
        if delimiter is None:
            # Auto-detect from extension
            ext = os.path.splitext(self._path)[1].lower()
            delimiter = "\t" if ext in (".tsv", ".tab") else ","

        with open(self._path, newline="", encoding=self._encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            csv_rows = list(reader)

        # Map CSV columns → deck field attrs
        column_to_attr: dict[str, tuple[str, Any]] = {}
        for attr_name, fld in fields:
            col = fld._source_key or attr_name
            column_to_attr[col] = (attr_name, getattr(fld, "_csv_coerce", None))

        result: list[dict[str, str]] = []
        for csv_row in csv_rows:
            row: dict[str, str] = {}
            for col, (attr, coerce) in column_to_attr.items():
                raw = csv_row.get(col, "")
                if coerce is not None and raw:
                    try:
                        val = coerce(raw)
                        row[attr] = str(val)
                    except (ValueError, TypeError):
                        row[attr] = raw
                else:
                    row[attr] = raw
            result.append(row)

        return result
