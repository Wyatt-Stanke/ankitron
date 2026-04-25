from ankitron.sources.csv_source import CSVSource
from ankitron.sources.link_strategy import LinkStrategy
from ankitron.sources.text_source import TextSource
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


# Lazy imports for sources that require optional extras
def __getattr__(name: str):
    if name == "WikipediaSource":
        # Requires: pip install ankitron[wikipedia]
        from ankitron.sources.wikipedia import WikipediaSource

        return WikipediaSource
    if name == "AISource":
        # Requires: pip install ankitron[ai]
        from ankitron.sources.ai import AISource

        return AISource
    if name == "AICardSource":
        # Requires: pip install ankitron[ai]
        from ankitron.sources.ai_card_source import AICardSource

        return AICardSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
