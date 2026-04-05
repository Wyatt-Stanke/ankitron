from ankitron.deck import Card, Deck
from ankitron.enums import MediaFormat, MediaType, PKStrategy
from ankitron.provenance import ProvenanceConfig
from ankitron.sources import CSVSource


class SpanishVocabulary(Deck):
    deck_name = "Spanish Vocabulary"
    provenance = ProvenanceConfig(enabled=True)

    csv = CSVSource(path="cards/school/spanish/2/vocabulary/lesson1.tsv", delimiter="\t")
    spanish = csv.Field("spanish", pk=PKStrategy.FIELD_VALUE)
    english = csv.Field("english")

    class SpanishToEnglish(Card):
        front = "{{spanish}}"
        back = "{{english}}"

    class EnglishToSpanish(Card):
        front = "{{english}}"
        back = "{{spanish}}"
