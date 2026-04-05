from ankitron import (
    Card,
    Deck,
    MediaFormat,
    MediaType,
    PKStrategy,
    ProvenanceConfig,
    WikidataSource,
)
from ankitron.sources.wikidata import P, Q, WikidataQuery


class USStates(Deck):
    deck_name = "US States"
    provenance = ProvenanceConfig(enabled=True)

    wikidata = WikidataSource(query=WikidataQuery.instances_of(Q.US_STATE))

    state = wikidata.Field(P.LABEL, pk=PKStrategy.SOURCE_ID)
    capital = wikidata.Field(P.CAPITAL)

    _population = wikidata.Field(P.POPULATION, internal=True)
    population_approx = _population.derive(
        transform=lambda x: round(x, -5) if x >= 3_000_000 else round(x, -4),
        fmt="~{:,.0f}",
    )

    flag = wikidata.Field(P.FLAG_IMAGE, media=MediaType.IMAGE, format=MediaFormat.PNG, width=300)

    class StateToCapital(Card):
        front = "What is the capital of {{state}}?"
        back = "{{capital}}"

    class CapitalToState(Card):
        front = "Which US state has {{capital}} as its capital?"
        back = "{{state}}"

    class Population(Card):
        front = "Approximately how many people live in {{state}}?"
        back = "{{population_approx}}"

    class FlagToState(Card):
        front = "{{flag}}"
        back = "{{state}}"
