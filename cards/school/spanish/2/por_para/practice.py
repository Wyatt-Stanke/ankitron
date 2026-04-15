from ankitron import Card, CSVSource, Deck, PKStrategy, ProvenanceConfig


class PorParaPractice(Deck):
    deck_name = "School::Spanish::2::Por vs Para::Practice"
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

.sentence {
    font-size: 24px;
    font-weight: bold;
    margin-bottom: 10px;
    line-height: 1.4;
}

.sentence .blank {
    border-bottom: 3px solid #c0392b;
    padding: 0 8px;
    color: #c0392b;
    font-weight: bold;
}

.translation {
    font-size: 16px;
    color: #666;
    margin-top: 8px;
    font-style: italic;
}

.answer {
    font-size: 32px;
    font-weight: bold;
    margin: 12px 0;
}

.answer.por {
    color: #c0392b;
}

.answer.para {
    color: #2980b9;
}

.completed {
    font-size: 20px;
    color: #2a7b3f;
    margin-top: 10px;
    font-style: italic;
}

.reason {
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

    csv = CSVSource(path="cards/school/spanish/2/por_para/por_para_practice.csv")

    spanish = csv.Field("Spanish", pk=PKStrategy.FIELD_VALUE)
    english = csv.Field("English")
    spanish_back = csv.Field("SpanishBack")
    por_or_para = csv.Field("PorOrPara")
    reason = csv.Field("Reason")

    class FillInTheBlank(Card):
        front = '<div class="sentence">{{spanish}}</div><div class="translation">{{english}}</div>'
        back = (
            '<div class="answer">{{por_or_para}}</div>'
            '<div class="completed">{{spanish_back}}</div>'
            '<div class="reason">{{reason}}</div>'
        )

    class EnglishToSpanish(Card):
        front = (
            '<div class="translation" style="font-size: 22px; font-style: normal;">{{english}}</div>'
            '<div class="reason" style="border-top: none; padding-top: 0;">¿Por o Para?</div>'
        )
        back = (
            '<div class="answer">{{por_or_para}}</div>'
            '<div class="completed">{{spanish_back}}</div>'
            '<div class="reason">{{reason}}</div>'
        )
