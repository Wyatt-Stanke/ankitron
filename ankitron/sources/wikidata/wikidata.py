from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from ankitron.cache import Cache

from ankitron.enums import FieldRule, PKStrategy
from ankitron.logging import (
    console,
    log_cache_hit,
    log_info,
    log_network,
    log_success,
    log_warn,
    make_progress,
    section_header,
)
from ankitron.sources.wikidata.properties import PropertyValueType, WikidataProperty
from ankitron.sources.wikidata.query import QueryType, WikidataQuery

_PROP_RE = re.compile(r"^P\d+$")

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"


class WikidataSource:
    """Fetches structured data from Wikidata via SPARQL."""

    def __init__(self, query: WikidataQuery) -> None:
        self.query = query
        self._last_cache_info: dict | None = None

    def Field(self, prop: WikidataProperty, **kwargs: Any) -> Any:
        # Avoid circular import at module level
        from ankitron.deck import Field

        return Field(
            **kwargs,
            _source=self,
            _source_key=prop.id,
            _source_value_type=prop.value_type,
        )

    def _build_field_var_map(self, fields: list[tuple[str, Any]]) -> dict[str, str]:
        """Build stable SPARQL variable names keyed by deck attribute name.

        Variable names are derived from source keys (not deck labels) so renaming a
        field does not invalidate cache entries tied to query text.
        """
        occurrence_by_key: dict[str, int] = {}
        var_map: dict[str, str] = {}

        for attr_name, field in fields:
            source_key = field._source_key
            if source_key in {"label", "description"}:
                continue

            occurrence_by_key[source_key] = occurrence_by_key.get(source_key, 0) + 1
            occurrence = occurrence_by_key[source_key]

            sanitized = "".join(ch.lower() if ch.isalnum() else "_" for ch in source_key).strip("_")
            if not sanitized:
                sanitized = "value"
            key_hash = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:8]

            var_name = f"v_{sanitized}_{key_hash}"
            if occurrence > 1:
                var_name = f"{var_name}_{occurrence}"

            var_map[attr_name] = var_name

        return var_map

    def _build_sparql(self, fields: list[tuple[str, Any]]) -> str:
        """Build a SPARQL query from the WikidataQuery and bound fields."""
        if self.query.query_type != QueryType.INSTANCES_OF:
            raise NotImplementedError(f"Query type {self.query.query_type} not yet supported.")

        target_qid = self.query.target.id
        select_vars = ["?item"]
        where_clauses = [f"  ?item wdt:P31 wd:{target_qid} ."]
        need_label_service = False
        var_map = self._build_field_var_map(fields)

        for attr_name, field in fields:
            source_key = field._source_key
            value_type = field._source_value_type

            if source_key == "label":
                # Handled by the label service → ?itemLabel
                select_vars.append("?itemLabel")
                need_label_service = True
            elif source_key == "description":
                select_vars.append("?itemDescription")
                need_label_service = True
            else:
                # Regular property
                var_name = var_map[attr_name]
                select_vars.append(f"?{var_name}")
                if value_type == PropertyValueType.ENTITY:
                    select_vars.append(f"?{var_name}Label")
                    need_label_service = True
                if field.rule != FieldRule.REQUIRED:
                    where_clauses.append(f"  OPTIONAL {{ ?item wdt:{source_key} ?{var_name} . }}")
                else:
                    where_clauses.append(f"  ?item wdt:{source_key} ?{var_name} .")

        # Always include label service if any entity-valued or special fields
        if need_label_service:
            where_clauses.append(
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }'
            )

        select_str = " ".join(select_vars)
        where_str = "\n".join(where_clauses)
        return f"SELECT {select_str}\nWHERE {{\n{where_str}\n}}"

    def fetch(
        self, fields: list[tuple[str, Any]], cache: Cache, refresh: bool
    ) -> list[dict[str, str]]:
        """Fetch data from Wikidata, using cache when available."""
        section_header("WikidataSource Fetch")

        sparql = self._build_sparql(fields)
        log_info(f"Generated SPARQL query ({len(fields)} fields)")
        console.print(f"  [dim]{sparql}[/dim]")

        cache_params = {"source": "wikidata", "sparql": sparql}

        if not refresh:
            cached_data, remaining = cache.get(cache_params)
            if cached_data is not None:
                log_cache_hit(remaining)
                self._last_cache_info = {"cached": True, "remaining": float(remaining or 0)}
                return self._parse_results(cached_data, fields)

        self._last_cache_info = {"cached": False}
        log_network(WIKIDATA_SPARQL_ENDPOINT)

        resp = requests.get(
            WIKIDATA_SPARQL_ENDPOINT,
            params={"query": sparql, "format": "json"},
            headers={"User-Agent": "ankitron/0.1.0 (https://github.com/Wyatt-Stanke/ankitron)"},
            timeout=60,
        )
        resp.raise_for_status()
        raw_data = resp.json()

        cache.put(cache_params, raw_data)
        result_count = len(raw_data.get("results", {}).get("bindings", []))
        log_success(f"Received {result_count} results from Wikidata")

        if result_count == 0:
            log_warn("SPARQL query returned 0 results. Verify the query class ID.")

        return self._parse_results(raw_data, fields)

    def _parse_results(self, raw_data: dict, fields: list[tuple[str, Any]]) -> list[dict[str, str]]:
        """Parse SPARQL JSON results into list of dicts keyed by field attribute names."""
        bindings = raw_data.get("results", {}).get("bindings", [])
        var_map = self._build_field_var_map(fields)

        # Deduplicate by item URI (take first occurrence)
        seen_items: dict[str, dict] = {}
        for binding in bindings:
            item_uri = binding.get("item", {}).get("value", "")
            if item_uri not in seen_items:
                seen_items[item_uri] = binding

        rows: list[dict[str, str]] = []

        with make_progress() as progress:
            task = progress.add_task("Parsing results", total=len(seen_items))

            for item_uri, binding in seen_items.items():
                row: dict[str, str] = {}
                # Always store the item URI for PK extraction
                row["_item_uri"] = item_uri

                for attr_name, field in fields:
                    source_key = field._source_key
                    value_type = field._source_value_type

                    if source_key == "label":
                        val = binding.get("itemLabel", {}).get("value", "")
                    elif source_key == "description":
                        val = binding.get("itemDescription", {}).get("value", "")
                    elif value_type == PropertyValueType.ENTITY:
                        # Use the Label variant for entity-valued properties
                        val = binding.get(f"{var_map[attr_name]}Label", {}).get("value", "")
                    else:
                        val = binding.get(var_map[attr_name], {}).get("value", "")

                    row[attr_name] = val or ""

                    # Handle PK
                    if field.pk == PKStrategy.SOURCE_ID:
                        # Extract QID from URI
                        qid = item_uri.rsplit("/", 1)[-1] if item_uri else ""
                        row[f"_pk_{attr_name}"] = qid
                    elif field.pk == PKStrategy.FIELD_VALUE:
                        row[f"_pk_{attr_name}"] = row[attr_name]

                rows.append(row)
                progress.advance(task)

        return rows

    def build_provenance_records(
        self,
        rows: list[dict],
        fields: list[tuple[str, Any]],
        source_name: str,
    ) -> list[dict[str, Any]]:
        """Build per-row provenance records for all fields belonging to this source.

        Implements the source provenance protocol so the deck pipeline does not need
        to know about Wikidata internals.  Any source that wants richer provenance
        can implement this same method signature.
        """
        from datetime import UTC
        from datetime import datetime as _dt

        from ankitron.provenance import ProvenanceRecord

        cache_info = self._last_cache_info
        cached = cache_info.get("cached", False) if cache_info else False
        fetched_at = _dt.now(UTC)

        result: list[dict[str, Any]] = []
        for row in rows:
            prov_row: dict[str, Any] = {}
            item_uri = row.get("_item_uri", "")
            qid = item_uri.rsplit("/", 1)[-1] if item_uri else None

            for attr_name, fld in fields:
                # Skip field types populated later in the deck pipeline
                if fld.is_derived or fld.is_computed or fld.is_cascade:
                    continue
                # Only process fields that belong to this source instance
                if fld._source is not self:
                    continue

                source_key = fld._source_key
                raw_val = row.get(attr_name)

                # Build entity URL; add the property fragment when the key is a
                # Wikidata property ID (e.g. P36 -> #P36)
                entity_url: str | None = None
                if qid:
                    if source_key and _PROP_RE.match(source_key):
                        entity_url = f"https://www.wikidata.org/wiki/{qid}#{source_key}"
                    else:
                        entity_url = f"https://www.wikidata.org/wiki/{qid}"

                prov_row[attr_name] = ProvenanceRecord(
                    source_type="WikidataSource",
                    source_name=source_name,
                    source_key=source_key,
                    source_url=entity_url,
                    source_entity_id=qid,
                    raw_value=raw_val,
                    raw_type=type(raw_val).__name__,
                    fetched_at=fetched_at,
                    cached=cached,
                )

            result.append(prov_row)
        return result

