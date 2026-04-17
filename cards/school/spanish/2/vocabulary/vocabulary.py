from ankitron import Card, CSVSource, DeckFamily, PKStrategy, ProvenanceConfig


class SpanishVocabulary(DeckFamily):
    deck_name = "School::Spanish::2::Unit {lesson}::Vocabulary"
    provenance = ProvenanceConfig(enabled=True)

    css = """\
.card {
  font-family: Georgia, serif;
  font-size: 20px;
  text-align: center;
  color: #333;
  background: #fffef5;
  padding: 20px;
}

.word {
  font-size: 32px;
  font-weight: bold;
  margin-bottom: 6px;
}

.pos {
  font-size: 14px;
  color: #888;
  font-style: italic;
  margin-bottom: 16px;
}

.gender {
  font-size: 14px;
  color: #888;
  font-weight: normal;
}

.example {
  font-size: 18px;
  color: #555;
  margin-top: 10px;
  font-style: italic;
}

.answer {
  font-size: 28px;
  font-weight: bold;
  color: #2a7b3f;
  margin-top: 10px;
}

.example-translation {
  font-size: 16px;
  color: #666;
  margin-top: 8px;
}

.notes {
  font-size: 14px;
  color: #999;
  margin-top: 14px;
  border-top: 1px dashed #ccc;
  padding-top: 10px;
}

hr {
  border: none;
  border-top: 2px solid #ddd;
  margin: 20px 0;
}
"""

    csv = CSVSource(path="cards/school/spanish/2/vocabulary/lesson{lesson}+.tsv", delimiter="\t")

    spanish = csv.Field("Spanish", pk=PKStrategy.FIELD_VALUE)
    example_sentence = csv.Field("ExampleSentence")
    english = csv.Field("English")
    example_translation = csv.Field("ExampleTranslation")
    part_of_speech = csv.Field("PartOfSpeech")
    gender = csv.Field("Gender")
    notes = csv.Field("Notes")

    class SpanishToEnglish(Card):
        front = (
            '<div class="word">{{spanish}}</div>'
            '<div class="pos">{{part_of_speech}}{{#gender}} · {{gender}}{{/gender}}</div>'
            '<div class="example">{{example_sentence}}</div>'
        )
        back = (
            "{{FrontSide}}"
            "<hr>"
            '<div class="answer">{{english}}</div>'
            '<div class="example-translation">{{example_translation}}</div>'
            '{{#notes}}<div class="notes">{{notes}}</div>{{/notes}}'
        )

    class EnglishToSpanish(Card):
        front = (
            '<div class="word">{{english}}</div>'
            '<div class="pos">{{part_of_speech}}</div>'
            '<div class="example-translation">{{example_translation}}</div>'
        )
        back = (
            "{{FrontSide}}"
            "<hr>"
            '<div class="answer">{{spanish}}{{#gender}} <span class="gender">({{gender}})</span>{{/gender}}</div>'
            '<div class="example">{{example_sentence}}</div>'
            '{{#notes}}<div class="notes">{{notes}}</div>{{/notes}}'
        )

    @classmethod
    def discover(cls):
        import glob

        lessons = []
        for path in sorted(glob.glob("cards/school/spanish/2/vocabulary/lesson*+.tsv")):
            num = path.split("lesson")[1].split("+")[0]
            lessons.append({"lesson": int(num)})
        return lessons
