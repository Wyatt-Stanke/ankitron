"""
Media pipeline — download, cache, convert, and resize media files.

Handles image and audio media fields: downloads from URLs, converts
formats (SVG→PNG, raster→raster), resizes, and caches results.

Requires the `media` extra: ``pip install ankitron[media]``.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from ankitron.enums import MediaFormat


def _ensure_deps(what: str = "media") -> None:
    """Check that media dependencies are installed."""
    try:
        from PIL import Image  # noqa: F401
    except ImportError as err:
        raise ImportError(
            f"ankitron[{what}] requires Pillow. Install with: pip install ankitron[{what}]"
        ) from err


def generate_media_filename(deck_name: str, pk: str, field_name: str, ext: str) -> str:
    """Generate a stable, unique filename for a media file.

    Format: ankitron_{hash}_{field}.{ext}
    """
    raw = f"{deck_name}:{pk}:{field_name}"
    h = hashlib.sha256(raw.encode()).hexdigest()[:12]
    safe_field = sanitize_filename(field_name)
    return f"ankitron_{h}_{safe_field}.{ext}"


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use as part of a filename."""
    name = re.sub(r"[^\w\-.]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")[:50]


def convert_image(
    input_path: str | Path,
    output_path: str | Path,
    target_format: MediaFormat,
    width: int | None = None,
    height: int | None = None,
) -> Path:
    """Convert and optionally resize an image.

    Supports SVG→PNG (via cairosvg) and raster→raster (via Pillow).
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    ext = input_path.suffix.lower()

    if ext == ".svg":
        return _convert_svg(input_path, output_path, target_format, width, height)
    return _convert_raster(input_path, output_path, target_format, width, height)


def _convert_svg(
    input_path: Path,
    output_path: Path,
    target_format: MediaFormat,
    width: int | None = None,
    height: int | None = None,
) -> Path:
    """Convert SVG to a raster format using cairosvg."""
    try:
        import cairosvg
    except ImportError as err:
        raise ImportError(
            "SVG conversion requires cairosvg. Install with: pip install ankitron[media]"
        ) from err

    output_path.parent.mkdir(parents=True, exist_ok=True)

    kwargs: dict[str, Any] = {"url": str(input_path)}
    if width:
        kwargs["output_width"] = width
    if height:
        kwargs["output_height"] = height

    if target_format == MediaFormat.PNG:
        cairosvg.svg2png(write_to=str(output_path), **kwargs)
    elif target_format == MediaFormat.JPEG:
        # SVG → PNG → JPEG
        png_data = cairosvg.svg2png(**kwargs)
        _ensure_deps()
        import io

        from PIL import Image

        img = Image.open(io.BytesIO(png_data))
        if img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            img = bg
        img.save(str(output_path), "JPEG", quality=90)
    else:
        cairosvg.svg2png(write_to=str(output_path), **kwargs)

    return output_path


def _convert_raster(
    input_path: Path,
    output_path: Path,
    target_format: MediaFormat,
    width: int | None = None,
    height: int | None = None,
) -> Path:
    """Convert and resize a raster image using Pillow."""
    _ensure_deps()
    from PIL import Image

    output_path.parent.mkdir(parents=True, exist_ok=True)

    img = Image.open(str(input_path))

    # Resize if dimensions specified
    if width or height:
        orig_w, orig_h = img.size
        if width and height:
            new_size = (width, height)
        elif width:
            ratio = width / orig_w
            new_size = (width, round(orig_h * ratio))
        else:
            ratio = height / orig_h
            new_size = (round(orig_w * ratio), height)
        img = img.resize(new_size, Image.LANCZOS)

    # Convert color mode for JPEG
    fmt_map = {
        MediaFormat.PNG: "PNG",
        MediaFormat.JPEG: "JPEG",
        MediaFormat.WEBP: "WEBP",
    }
    pil_format = fmt_map.get(target_format, "PNG")

    if pil_format == "JPEG" and img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        img = bg

    save_kwargs: dict[str, Any] = {}
    if pil_format == "JPEG":
        save_kwargs["quality"] = 90
    elif pil_format == "WEBP":
        save_kwargs["quality"] = 85

    img.save(str(output_path), pil_format, **save_kwargs)
    return output_path


def download_media(url: str, dest: Path, timeout: int = 30) -> Path:
    """Download a media file from a URL."""
    import requests

    dest.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "ankitron/0.1.0 (https://github.com/Wyatt-Stanke/ankitron)"}
    resp = requests.get(url, timeout=timeout, stream=True, headers=headers)
    resp.raise_for_status()

    with open(dest, "wb") as f:
        f.writelines(resp.iter_content(chunk_size=8192))

    return dest


def make_img_tag(filename: str, width: int | None = None, height: int | None = None) -> str:
    """Generate an Anki-compatible <img> tag."""
    import html

    safe = html.escape(filename, quote=True)
    attrs = f'src="{safe}"'
    if width:
        attrs += f' width="{width}"'
    if height:
        attrs += f' height="{height}"'
    return f"<img {attrs}>"


def make_sound_tag(filename: str) -> str:
    """Generate an Anki-compatible [sound:] tag."""
    return f"[sound:{filename}]"
