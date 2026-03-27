"""
GeneratedMedia — factory for auto-generated media fields (maps, charts).

Provides a unified entry point for creating Field instances that
generate media content during the build process.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ankitron.enums import FieldKind

if TYPE_CHECKING:
    from ankitron.deck import Field


@dataclass
class MapConfig:
    """Configuration for generated map images."""

    zoom: int | None = None
    width: int = 400
    height: int = 300
    style: str = "default"
    highlight_color: str = "#FF4444"
    context_color: str = "#DDDDDD"
    show_labels: bool = True
    marker: bool = False


@dataclass
class ChartConfig:
    """Configuration for generated chart images."""

    chart_type: str = "bar"  # bar, donut, line, scatter, histogram
    width: int = 400
    height: int = 300
    color: str = "#4A90D9"
    highlight_color: str = "#FF4444"
    show_labels: bool = True
    show_grid: bool = True
    title: str | None = None


class GeneratedMedia:
    """Factory for creating generated media fields."""

    @staticmethod
    def map(
        coords_field: Field,
        *,
        config: MapConfig | None = None,
        internal: bool = False,
        unused_ok: bool = False,
    ) -> Field:
        """Create a field that generates a map image for each row.

        Requires the `maps` extra: ``pip install ankitron[maps]``.

        Args:
            coords_field: Field containing coordinates (lat, lon).
            config: Map rendering configuration.
        """
        from ankitron.deck import Field as DeckField

        cfg = config or MapConfig()
        fld = DeckField(
            kind=FieldKind.IMAGE,
            internal=internal,
            unused_ok=unused_ok,
        )
        fld._generated_media_type = "map"  # type: ignore[attr-defined]
        fld._generated_media_config = cfg  # type: ignore[attr-defined]
        fld._generated_media_input = coords_field  # type: ignore[attr-defined]
        return fld

    @staticmethod
    def chart(
        data_field: Field,
        *,
        config: ChartConfig | None = None,
        internal: bool = False,
        unused_ok: bool = False,
    ) -> Field:
        """Create a field that generates a chart image for each row.

        Requires the `charts` extra: ``pip install ankitron[charts]``.

        Args:
            data_field: Field containing the data to chart.
            config: Chart rendering configuration.
        """
        from ankitron.deck import Field as DeckField

        cfg = config or ChartConfig()
        fld = DeckField(
            kind=FieldKind.IMAGE,
            internal=internal,
            unused_ok=unused_ok,
        )
        fld._generated_media_type = "chart"  # type: ignore[attr-defined]
        fld._generated_media_config = cfg  # type: ignore[attr-defined]
        fld._generated_media_input = data_field  # type: ignore[attr-defined]
        return fld
