"""
GeneratedMedia — factory for auto-generated media fields (maps, charts).

Provides a unified entry point for creating Field instances that
generate media content during the build process.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ankitron.deck import Field


class MediaType(Enum):
    """Declares the type of media a field contains."""

    IMAGE = "image"
    AUDIO = "audio"


class MediaFormat(Enum):
    """Target format for media conversion."""

    PNG = "png"
    JPEG = "jpeg"
    SVG = "svg"
    WEBP = "webp"
    MP3 = "mp3"
    OGG = "ogg"


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
    def _make_generated_field(
        input_field: Field,
        media_type: str,
        config: object,
        *,
        internal: bool = False,
        unused_ok: bool = False,
    ) -> Field:
        from ankitron.deck import Field as DeckField, FieldKind

        fld = DeckField(
            kind=FieldKind.IMAGE,
            internal=internal,
            unused_ok=unused_ok,
        )
        fld._generated_media_type = media_type  # type: ignore[attr-defined]
        fld._generated_media_config = config  # type: ignore[attr-defined]
        fld._generated_media_input = input_field  # type: ignore[attr-defined]
        return fld

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
        return GeneratedMedia._make_generated_field(
            coords_field, "map", config or MapConfig(), internal=internal, unused_ok=unused_ok
        )

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
        return GeneratedMedia._make_generated_field(
            data_field, "chart", config or ChartConfig(), internal=internal, unused_ok=unused_ok
        )
