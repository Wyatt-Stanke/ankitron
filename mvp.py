from ankitron import AnkiTemplate, Card, Deck, PKStrategy
from ankitron.provenance import ProvenanceConfig
from ankitron.sources import WikidataSource
from ankitron.sources.wikidata import P, Q, WikidataQuery
from ankitron.transform import Transform


class USStates(Deck):
    deck_name = "Geography::US States"
    provenance = ProvenanceConfig(enabled=True)

    template = AnkiTemplate.BASIC
    wikidata = WikidataSource(query=WikidataQuery.instances_of(Q.US_STATE))

    state = wikidata.Field(P.LABEL, pk=PKStrategy.SOURCE_ID)
    capital = wikidata.Field(P.CAPITAL)

    _population = wikidata.Field(P.POPULATION, fmt="{:,}", internal=True)
    approximate_population = _population.derive(
        transform=Transform.round_to_nearest(
            {
                3_000_000: 100_000,
                float("inf"): 500_000,
            }
        ),
        fmt="~{:,.0f}",
    )

    class capitals(Card):
        front = "What is the capital of {{state}}?"
        back = "{{FrontSide}}<br><hr>{{capital}}"

    class capitals_reverse(Card):
        front = "Which US state has {{capital}} as its capital?"
        back = "{{FrontSide}}<br><hr>{{state}}"

    class populations(Card):
        front = "Approximately how many people live in {{state}}?"
        back = "{{FrontSide}}<br><hr>{{approximate_population}}"
