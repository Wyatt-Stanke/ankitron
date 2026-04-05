from ankitron.sources.csv_source import CSVSource
from ankitron.sources.link_strategy import LinkStrategy
from ankitron.sources.wikidata.wikidata import WikidataSource

__all__ = [
    "AICardSource",
    "AISource",
    "CSVSource",
    "LinkStrategy",
    "TextSource",
    "WikidataSource",
    "WikipediaSource",
]


# Lazy imports for optional dependencies
def __getattr__(name: str):
    if name == "WikipediaSource":
        from ankitron.sources.wikipedia import WikipediaSource

        return WikipediaSource
    if name == "AISource":
        from ankitron.sources.ai import AISource

        return AISource
    if name == "AICardSource":
        from ankitron.sources.ai_card_source import AICardSource

        return AICardSource
    if name == "TextSource":
        from ankitron.sources.text_source import TextSource

        return TextSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
