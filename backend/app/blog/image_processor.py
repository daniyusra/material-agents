"""
Image processing pipeline for blog media uploads.

On ingest: fix EXIF orientation, convert to RGB, cap to MAX_DIMENSION,
strip all metadata, encode as WebP (JPEG fallback if libwebp unavailable),
generate a thumbnail.
"""

import io
import os
import struct

from PIL import Image, ImageOps

_MAX_DIMENSION = int(os.getenv("MEDIA_MAX_DIMENSION", "2048"))
_THUMBNAIL_SIZE = 400


# ── Magic-byte detection ───────────────────────────────────────────────────────

def detect_image_type(data: bytes) -> str | None:
    """Return MIME type from magic bytes, or None if not a recognised image."""
    if len(data) < 12:
        return None
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


ALLOWED_MIME_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}


# ── Processing ─────────────────────────────────────────────────────────────────

def _to_rgb(img: Image.Image) -> Image.Image:
    """Convert any mode to RGB, compositing transparency onto white."""
    if img.mode in ("RGBA", "LA", "PA"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        src = img.convert("RGBA") if img.mode != "RGBA" else img
        bg.paste(src, mask=src.split()[3])
        return bg
    if img.mode == "P":
        return img.convert("RGBA").convert("RGB")
    if img.mode != "RGB":
        return img.convert("RGB")
    return img


def _encode(img: Image.Image, quality: int = 85) -> tuple[bytes, str]:
    """Encode to WebP, falling back to JPEG if the encoder is unavailable."""
    buf = io.BytesIO()
    try:
        img.save(buf, format="WEBP", quality=quality, method=6)
        return buf.getvalue(), "image/webp"
    except Exception:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue(), "image/jpeg"


def process_image(data: bytes) -> dict:
    """
    Process raw image bytes.

    Returns:
        main        – re-encoded bytes (WebP or JPEG)
        main_type   – MIME type of main
        thumb       – thumbnail bytes
        thumb_type  – MIME type of thumbnail
        width       – final width in pixels
        height      – final height in pixels
        ext         – file extension without dot ('webp' or 'jpg')
    """
    img = Image.open(io.BytesIO(data))

    # Fix EXIF orientation (strips EXIF in the process via a copy)
    img = ImageOps.exif_transpose(img)

    # Drop all metadata by reconstructing from pixel data
    clean = Image.new(img.mode, img.size)
    clean.putdata(list(img.getdata()))
    img = clean

    img = _to_rgb(img)

    # Cap longest edge
    if max(img.width, img.height) > _MAX_DIMENSION:
        img.thumbnail((_MAX_DIMENSION, _MAX_DIMENSION), Image.LANCZOS)

    width, height = img.size

    main_bytes, main_type = _encode(img, quality=85)

    thumb = img.copy()
    thumb.thumbnail((_THUMBNAIL_SIZE, _THUMBNAIL_SIZE), Image.LANCZOS)
    thumb_bytes, thumb_type = _encode(thumb, quality=80)

    ext = "webp" if main_type == "image/webp" else "jpg"
    return {
        "main": main_bytes,
        "main_type": main_type,
        "thumb": thumb_bytes,
        "thumb_type": thumb_type,
        "width": width,
        "height": height,
        "ext": ext,
    }
