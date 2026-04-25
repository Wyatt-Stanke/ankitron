from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ankitron.sources.wikidata.classes import WikidataClass


class QueryType(Enum):
    INSTANCES_OF = "instances_of"


@dataclass(frozen=True)
class WikidataQuery:
    """Describes what to ask Wikidata for. Constructed via class methods."""

    query_type: QueryType
    target: WikidataClass | list[WikidataClass] | str | None = None

    @classmethod
    def instances_of(cls, wikidata_class: WikidataClass) -> WikidataQuery:
        """Query for all instances of a Wikidata class (wdt:P31)."""
        return cls(query_type=QueryType.INSTANCES_OF, target=wikidata_class)
