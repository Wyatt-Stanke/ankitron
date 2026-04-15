from ankitron import Card, CSVSource, Deck, PKStrategy, ProvenanceConfig


class PorParaGrammar(Deck):
    deck_name = "School::Spanish::2::Por vs Para::Grammar Rules"
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

.rule {
    font-size: 28px;
    font-weight: bold;
    margin-bottom: 10px;
}

.por-para {
    font-size: 36px;
    font-weight: bold;
    margin: 12px 0;
}

.por-para.por {
    color: #c0392b;
}

.por-para.para {
    color: #2980b9;
}

.example {
    font-size: 18px;
    color: #555;
    margin-top: 10px;
    font-style: italic;
}

.example-translation {
    font-size: 16px;
    color: #666;
    margin-top: 8px;
}

.explanation {
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

    csv = CSVSource(path="cards/school/spanish/2/por_para/por_para_grammar.csv")

    rule = csv.Field("Rule", pk=PKStrategy.FIELD_VALUE)
    por_or_para = csv.Field("PorOrPara")
    spanish_example = csv.Field("SpanishExample")
    english_example = csv.Field("EnglishExample")
    explanation = csv.Field("Explanation")

    class RuleToAnswer(Card):
        front = (
            '<div class="rule">{{rule}}</div><div class="example-translation">¿Por o Para?</div>'
        )
        back = (
            '<div class="por-para">{{por_or_para}}</div>'
            '<div class="example">{{spanish_example}}</div>'
            '<div class="example-translation">{{english_example}}</div>'
            '<div class="explanation">{{explanation}}</div>'
        )

    class ExampleToRule(Card):
        front = '<div class="example">{{spanish_example}}</div><div class="example-translation">{{english_example}}</div><div class="example-translation">What rule does this illustrate?</div>'
        back = (
            '<div class="por-para">{{por_or_para}}</div>'
            '<div class="rule">{{rule}}</div>'
            '<div class="explanation">{{explanation}}</div>'
        )
