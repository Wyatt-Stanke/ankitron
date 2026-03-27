from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ankitron.cache import Cache
    from ankitron.deck import Field


class BaseSource(Protocol):
    """Protocol that all data sources must implement."""

    def Field(self, source_key: Any, **kwargs: Any) -> Field:
        """Create a Field bound to this source."""
        ...

    def fetch(
        self, fields: list[tuple[str, Field]], cache: Cache, refresh: bool
    ) -> list[dict[str, str]]:
        """
        Fetch data for the given fields.
        Returns a list of dicts where keys are field attribute names
        and values are the resolved string data.
        """
        ...
