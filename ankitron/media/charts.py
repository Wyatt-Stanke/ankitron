"""
Chart rendering — generate chart images for flashcards.

Renders bar charts, donut charts, line charts, scatter plots,
and histograms. Uses matplotlib.

Requires the `charts` extra: ``pip install ankitron[charts]``.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from ankitron.media.generated import ChartConfig


def _ensure_deps() -> None:
    """Check that chart rendering dependencies are installed."""
    try:
        import matplotlib as mpl  # noqa: F401
    except ImportError as err:
        raise ImportError(
            "Chart rendering requires matplotlib. Install with: pip install ankitron[charts]"
        ) from err


def render_chart(
    values: list[Any],
    labels: list[str] | None,
    config: ChartConfig,
    output_path: Path,
    *,
    highlight_index: int | None = None,
) -> Path:
    """Render a chart image.

    Args:
        values: List of numeric values to chart.
        labels: Optional labels for each value.
        config: Chart rendering configuration.
        output_path: Where to save the rendered image.
        highlight_index: Index of the value to highlight (e.g., "this row").

    Returns:
        Path to the rendered PNG image.
    """
    _ensure_deps()
    import matplotlib as mpl

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(1, 1, figsize=(config.width / 100, config.height / 100), dpi=100)

    chart_type = config.chart_type.lower()

    # Build color list
    colors = [config.color] * len(values)
    if highlight_index is not None and 0 <= highlight_index < len(values):
        colors[highlight_index] = config.highlight_color

    if chart_type == "bar":
        x = list(range(len(values)))
        ax.bar(x, values, color=colors)
        if labels and config.show_labels:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)

    elif chart_type == "donut":
        ax.pie(
            values,
            colors=colors,
            startangle=90,
            wedgeprops={"width": 0.4},
        )
        if labels and config.show_labels:
            ax.legend(labels, loc="center left", bbox_to_anchor=(1, 0.5), fontsize=8)

    elif chart_type == "line":
        x = list(range(len(values)))
        ax.plot(x, values, color=config.color, linewidth=2, marker="o", markersize=4)
        if highlight_index is not None and 0 <= highlight_index < len(values):
            ax.plot(
                highlight_index,
                values[highlight_index],
                "o",
                color=config.highlight_color,
                markersize=8,
                zorder=5,
            )
        if labels and config.show_labels:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)

    elif chart_type == "scatter":
        if len(values) >= 2:
            # Assume values is pairs flattened: [x1, y1, x2, y2, ...]
            xs = values[::2]
            ys = values[1::2]
            ax.scatter(xs, ys, color=config.color, s=30)
        else:
            ax.scatter(range(len(values)), values, color=config.color, s=30)

    elif chart_type == "histogram":
        ax.hist(values, bins="auto", color=config.color, edgecolor="white")

    if config.title:
        ax.set_title(config.title, fontsize=10)
    if config.show_grid and chart_type != "donut":
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(str(output_path), bbox_inches="tight", dpi=100)
    plt.close(fig)

    return output_path


def chart_cache_key(
    values: list[Any],
    config: ChartConfig,
    highlight_index: int | None = None,
) -> str:
    """Generate a cache key for a chart rendering."""
    data = (
        f"{values},{config.chart_type},{config.width},"
        f"{config.height},{config.color},{highlight_index}"
    )
    return hashlib.sha256(data.encode()).hexdigest()[:16]
