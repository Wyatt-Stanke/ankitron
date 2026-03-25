from ankitron import Deck, Card, Tag, AnkiTemplate, PKStrategy
from ankitron.sources import WikidataSource
from ankitron.sources.wikidata import P, Q, WikidataQuery


class USStates(Deck):
    deck_name = "Geography::US States"

    template = AnkiTemplate.BASIC
    wikidata = WikidataSource(query=WikidataQuery.instances_of(Q.US_STATE))

    state = wikidata.Field(P.LABEL, pk=PKStrategy.SOURCE_ID)
    capital = wikidata.Field(P.CAPITAL)

    _population = wikidata.Field(P.POPULATION, fmt="{:,}", internal=True)
    approximate_population = _population.derive(
        transform=lambda x: round(x, -5) if x > 1_000_000 else round(x, -4),
        fmt="~{:,.0f}",
    )

    class capitals(Card):
        front = "What is the capital of {{state}}?"
        back = "{{capital}}"

    class capitals_reverse(Card):
        front = "Which US state has {{capital}} as its capital?"
        back = "{{state}}"

    class populations(Card):
        front = "Approximately how many people live in {{state}}?"
        back = "{{approximate_population}}"


deck = USStates()
deck.fetch()
deck.preview()
deck.export("us_states.apkg")
