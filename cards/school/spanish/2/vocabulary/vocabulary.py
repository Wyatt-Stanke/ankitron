from ankitron import Card, CSVSource, DeckFamily, PKStrategy, ProvenanceConfig


class SpanishVocabulary(DeckFamily):
    deck_name = "Spanish Vocabulary::Lesson {lesson}"
    provenance = ProvenanceConfig(enabled=True)

    csv = CSVSource(path="cards/school/spanish/2/vocabulary/lesson{lesson}.tsv", delimiter="\t")

    spanish = csv.Field("spanish", pk=PKStrategy.FIELD_VALUE)
    english = csv.Field("english")

    class SpanishToEnglish(Card):
        front = "{{spanish}}"
        back = "{{english}}"

    class EnglishToSpanish(Card):
        front = "{{english}}"
        back = "{{spanish}}"

    @classmethod
    def discover(cls):
        import glob

        lessons = []
        for path in sorted(glob.glob("cards/school/spanish/2/vocabulary/lesson*.tsv")):
            lesson = int(path.split("lesson")[1].split(".")[0])
            lessons.append({"lesson": lesson})
        return lessons
