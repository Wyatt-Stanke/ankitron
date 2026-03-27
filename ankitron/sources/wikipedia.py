"""
WikipediaSource — extract data from Wikipedia articles.

Supports infobox parameter extraction, first-section text, and
section-level extraction. Typically linked to a WikidataSource
via LinkStrategy.sitelinks().
"""

from __future__ import annotations

import enum
import re
from typing import TYPE_CHECKING, Any

from ankitron.sources.link_strategy import LinkStrategy

if TYPE_CHECKING:
    from ankitron.deck import Field


class ExtractionMode(enum.Enum):
    """How to extract data from a Wikipedia article."""

    INFOBOX = "infobox"
    FIRST_SECTION = "first_section"
    SECTIONS = "sections"


class InfoboxParam:
    """Named infobox parameter accessor."""

    def __init__(self, name: str, *, aliases: list[str] | None = None) -> None:
        self.name = name
        self.aliases = aliases or []

    def __repr__(self) -> str:
        return f"InfoboxParam({self.name!r})"


class WikipediaSource:
    """A data source that extracts data from Wikipedia articles.

    Requires the `wikipedia` extra: ``pip install ankitron[wikipedia]``.
    """

    def __init__(
        self,
        *,
        linked_to: Any | None = None,
        via: LinkStrategy | None = None,
        language: str = "en",
        mode: ExtractionMode = ExtractionMode.INFOBOX,
    ) -> None:
        self._linked_to = linked_to
        self._via = via or LinkStrategy.sitelinks()
        self._language = language
        self._mode = mode

    def Field(
        self,
        param: str | InfoboxParam,
        **kwargs: Any,
    ) -> Field:
        """Create a Field bound to a Wikipedia infobox parameter or section."""
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        if isinstance(param, InfoboxParam):
            fld._source_key = param.name
        else:
            fld._source_key = param
        return fld

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Fetch data from Wikipedia.

        For linked sources, expects the parent source to have provided
        sitelinks or article titles. For standalone use, requires article
        titles to be specified directly.
        """
        try:
            import mwparserfromhell  # noqa: F401
        except ImportError as err:
            raise ImportError(
                "WikipediaSource requires the 'wikipedia' extra. "
                "Install with: pip install ankitron[wikipedia]"
            ) from err

        import time

        import requests

        api_url = f"https://{self._language}.wikipedia.org/w/api.php"
        results: list[dict[str, str]] = []

        # Determine article titles to fetch
        # For linked sources, the parent source must provide sitelink data
        # For now, this works with a list of titles passed via cache context
        titles = getattr(self, "_article_titles", [])
        if not titles:
            return results

        for title in titles:
            # Rate limiting — Wikimedia courtesy
            time.sleep(0.1)

            # Check cache first
            cache_key = f"wikipedia:{self._language}:{title}"
            if cache and not refresh:
                cached = cache.get(cache_key)
                if cached is not None:
                    results.append(cached)
                    continue

            # Fetch wikitext
            params = {
                "action": "parse",
                "page": title,
                "prop": "wikitext",
                "format": "json",
            }
            resp = requests.get(api_url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
            if not wikitext:
                results.append({attr: "" for attr, _ in fields})
                continue

            row: dict[str, str] = {}
            for attr_name, fld in fields:
                param_name = fld._source_key or attr_name
                aliases = []
                value = self._extract_infobox_param(wikitext, param_name, aliases)
                row[attr_name] = value or ""

            if cache:
                cache.set(cache_key, row)

            results.append(row)

        return results

    @staticmethod
    def _extract_infobox_param(
        wikitext: str,
        param_name: str,
        aliases: list[str] | None = None,
    ) -> str | None:
        """Extract a parameter value from a wikitext infobox."""
        try:
            import mwparserfromhell
        except ImportError:
            return None

        parsed = mwparserfromhell.parse(wikitext)
        templates = parsed.filter_templates()

        for template in templates:
            tname = str(template.name).strip().lower()
            if "infobox" not in tname:
                continue

            names_to_try = [param_name] + (aliases or [])
            for name in names_to_try:
                if template.has(name):
                    val = str(template.get(name).value).strip()
                    # Strip wiki markup links
                    val = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", val)
                    # Strip HTML tags
                    val = re.sub(r"<[^>]+>", "", val)
                    # Strip ref tags
                    val = re.sub(r"<ref[^>]*>.*?</ref>", "", val, flags=re.DOTALL)
                    val = re.sub(r"<ref[^/]*/?>", "", val)
                    return val.strip() or None

        return None
