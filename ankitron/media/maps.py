"""
Map rendering — generate map images for flashcards.

Renders map tiles, highlights geometries, and composites
into images. Uses contextily, shapely, geopandas, and matplotlib.

Requires the `maps` extra: ``pip install ankitron[maps]``.
"""

from __future__ import annotations

import contextlib
import hashlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from ankitron.media.generated import MapConfig

# Half-circumference of the Earth in Web Mercator (EPSG:3857) metres.
_WEB_MERCATOR_HALF_CIRC = 20037508.34


def _ensure_deps() -> None:
    """Check that map rendering dependencies are installed."""
    missing = []
    try:
        import matplotlib as mpl  # noqa: F401
    except ImportError:
        missing.append("matplotlib")
    try:
        import contextily  # noqa: F401
    except ImportError:
        missing.append("contextily")

    if missing:
        raise ImportError(
            f"Map rendering requires: {', '.join(missing)}. "
            "Install with: pip install ankitron[maps]"
        )


def render_map(
    lat: float,
    lon: float,
    config: MapConfig,
    output_path: Path,
    geometry: Any | None = None,
) -> Path:
    """Render a map image centered on (lat, lon).

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.
        config: Map rendering configuration.
        output_path: Where to save the rendered image.
        geometry: Optional shapely geometry to highlight.

    Returns:
        Path to the rendered PNG image.
    """
    _ensure_deps()
    import matplotlib as mpl

    mpl.use("Agg")
    import contextily as cx
    import matplotlib.pyplot as plt

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(1, 1, figsize=(config.width / 100, config.height / 100), dpi=100)

    # Set map extent
    zoom = config.zoom or _auto_zoom(geometry)
    if geometry is not None:
        try:
            import geopandas as gpd
            from shapely.geometry import shape

            if isinstance(geometry, dict):
                geometry = shape(geometry)

            gdf = gpd.GeoDataFrame(geometry=[geometry], crs="EPSG:4326")
            gdf = gdf.to_crs(epsg=3857)
            gdf.plot(
                ax=ax,
                color=config.highlight_color,
                alpha=0.5,
                edgecolor=config.highlight_color,
            )
            minx, miny, maxx, maxy = gdf.total_bounds
            pad = max(maxx - minx, maxy - miny) * 0.15
            ax.set_xlim(minx - pad, maxx + pad)
            ax.set_ylim(miny - pad, maxy + pad)
        except ImportError:
            _set_extent_from_point(ax, lat, lon, zoom)
    else:
        _set_extent_from_point(ax, lat, lon, zoom)

    if config.marker:
        # Convert lat/lon to Web Mercator for marker
        import math

        mx = lon * _WEB_MERCATOR_HALF_CIRC / 180
        my = math.log(math.tan((90 + lat) * math.pi / 360)) / (math.pi / 180)
        my = my * _WEB_MERCATOR_HALF_CIRC / 180
        ax.plot(
            mx,
            my,
            "o",
            color=config.highlight_color,
            markersize=8,
            markeredgecolor="white",
            markeredgewidth=1.5,
        )

    try:
        cx.add_basemap(ax, zoom=zoom, source=cx.providers.OpenStreetMap.Mapnik)
    except Exception:
        # Fallback: try without specific zoom
        with contextlib.suppress(Exception):
            cx.add_basemap(ax, source=cx.providers.OpenStreetMap.Mapnik)

    ax.set_axis_off()
    plt.tight_layout(pad=0)
    fig.savefig(str(output_path), bbox_inches="tight", pad_inches=0, dpi=100)
    plt.close(fig)

    return output_path


def _auto_zoom(geometry: Any | None) -> int:
    """Estimate an appropriate zoom level."""
    if geometry is None:
        return 10
    try:
        bounds = geometry.bounds  # (minx, miny, maxx, maxy)
        span = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
        if span > 10:
            return 5
        if span > 2:
            return 7
        if span > 0.5:
            return 9
        return 11
    except Exception:
        return 10


def _set_extent_from_point(ax: Any, lat: float, lon: float, zoom: int) -> None:
    """Set axis extent around a lat/lon point at a given zoom level."""
    import math

    mx = lon * _WEB_MERCATOR_HALF_CIRC / 180
    my = math.log(math.tan((90 + lat) * math.pi / 360)) / (math.pi / 180)
    my = my * _WEB_MERCATOR_HALF_CIRC / 180

    # Approximate extent from zoom level
    extent = _WEB_MERCATOR_HALF_CIRC / (2 ** (zoom - 1))
    ax.set_xlim(mx - extent, mx + extent)
    ax.set_ylim(my - extent, my + extent)


def map_cache_key(lat: float, lon: float, config: MapConfig) -> str:
    """Generate a cache key for a map rendering."""
    data = (
        f"{lat:.6f},{lon:.6f},{config.zoom},{config.width},"
        f"{config.height},{config.style},{config.highlight_color}"
    )
    return hashlib.sha256(data.encode()).hexdigest()[:16]
