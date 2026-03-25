from __future__ import annotations

from typing import Any

import requests

from ankitron.cache import Cache
from ankitron.enums import PKStrategy
from ankitron.wikidata.properties import WikidataProperty, PropertyValueType
from ankitron.wikidata.query import WikidataQuery, QueryType
from ankitron.logging import (
    section_header,
    log_info,
    log_success,
    log_warn,
    log_cache_hit,
    log_network,
    make_progress,
    console,
)

WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"


class WikidataSource:
    """Fetches structured data from Wikidata via SPARQL."""

    def __init__(self, query: WikidataQuery) -> None:
        self.query = query

    def Field(self, prop: WikidataProperty, **kwargs: Any) -> Any:
        # Avoid circular import at module level
        from ankitron.deck import Field

        return Field(
            **kwargs,
            _source=self,
            _source_key=prop.id,
            _source_value_type=prop.value_type,
        )

    def _build_sparql(self, fields: list[tuple[str, Any]]) -> str:
        """Build a SPARQL query from the WikidataQuery and bound fields."""
        if self.query.query_type != QueryType.INSTANCES_OF:
            raise NotImplementedError(
                f"Query type {self.query.query_type} not yet supported."
            )

        target_qid = self.query.target.id
        select_vars = ["?item"]
        where_clauses = [f"  ?item wdt:P31 wd:{target_qid} ."]
        need_label_service = False

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
                var_name = attr_name
                select_vars.append(f"?{var_name}")
                if value_type == PropertyValueType.ENTITY:
                    select_vars.append(f"?{var_name}Label")
                    need_label_service = True
                if field.optional:
                    where_clauses.append(
                        f"  OPTIONAL {{ ?item wdt:{source_key} ?{var_name} . }}"
                    )
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
                return self._parse_results(cached_data, fields)

        log_network(WIKIDATA_SPARQL_ENDPOINT)

        resp = requests.get(
            WIKIDATA_SPARQL_ENDPOINT,
            params={"query": sparql, "format": "json"},
            headers={"User-Agent": "ankitron/0.1.0 (https://github.com/ankitron)"},
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

    def _parse_results(
        self, raw_data: dict, fields: list[tuple[str, Any]]
    ) -> list[dict[str, str]]:
        """Parse SPARQL JSON results into list of dicts keyed by field attribute names."""
        bindings = raw_data.get("results", {}).get("bindings", [])

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
                        val = binding.get(f"{attr_name}Label", {}).get("value", "")
                    else:
                        val = binding.get(attr_name, {}).get("value", "")

                    row[attr_name] = val if val else ""

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
