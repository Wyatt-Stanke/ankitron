from ankitron.sources.csv_source import CSVSource
from ankitron.sources.link_strategy import LinkStrategy
from ankitron.sources.wikidata.wikidata import WikidataSource

__all__ = [
    "CSVSource",
    "LinkStrategy",
    "WikidataSource",
]


# Lazy imports for optional dependencies
def __getattr__(name: str):
    if name == "WikipediaSource":
        from ankitron.sources.wikipedia import WikipediaSource

        return WikipediaSource
    if name == "AISource":
        from ankitron.sources.ai import AISource

        return AISource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
