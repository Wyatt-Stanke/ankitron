from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ankitron.wikidata.classes import WikidataClass


class QueryType(Enum):
    INSTANCES_OF = "instances_of"
    SUBCLASSES_OF = "subclasses_of"
    ENTITY = "entity"
    ENTITIES = "entities"
    SPARQL = "sparql"


@dataclass(frozen=True)
class WikidataQuery:
    """Describes what to ask Wikidata for. Constructed via class methods."""

    query_type: QueryType
    target: WikidataClass | list[WikidataClass] | str | None = None

    @classmethod
    def instances_of(cls, wikidata_class: WikidataClass) -> WikidataQuery:
        """Query for all instances of a Wikidata class (wdt:P31)."""
        return cls(query_type=QueryType.INSTANCES_OF, target=wikidata_class)

    @classmethod
    def subclasses_of(cls, wikidata_class: WikidataClass) -> WikidataQuery:
        raise NotImplementedError("subclasses_of is planned but not yet implemented.")

    @classmethod
    def entity(cls, wikidata_class: WikidataClass) -> WikidataQuery:
        raise NotImplementedError("entity is planned but not yet implemented.")

    @classmethod
    def entities(cls, wikidata_classes: list[WikidataClass]) -> WikidataQuery:
        raise NotImplementedError("entities is planned but not yet implemented.")

    @classmethod
    def sparql(cls, query: str) -> WikidataQuery:
        raise NotImplementedError("sparql is planned but not yet implemented.")
