"""Shared image validation for agent backends.

Moved verbatim from ``nerve/agent/engine.py`` — both backends validate
inbound images before handing them to their runtime: an unprocessable
image in a conversation history can poison every subsequent API call
(the Claude case that motivated this), and no runtime benefits from
garbage bytes.
"""

from __future__ import annotations

import os

# Anthropic API image limit; a sane general ceiling for codex too.
MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

# Magic byte signatures for supported image formats.
# Each format maps to a list of valid signatures.  A signature is a list
# of (magic_bytes, offset) pairs that must ALL match (AND logic).
IMAGE_MAGIC: dict[str, list[list[tuple[bytes, int]]]] = {
    ".png":  [[(b"\x89PNG\r\n\x1a\n", 0)]],
    ".jpg":  [[(b"\xff\xd8\xff", 0)]],
    ".jpeg": [[(b"\xff\xd8\xff", 0)]],
    ".gif":  [[(b"GIF87a", 0)], [(b"GIF89a", 0)]],
    # WebP is RIFF container: must have RIFF at 0 AND WEBP at 8
    ".webp": [[(b"RIFF", 0), (b"WEBP", 8)]],
}


def validate_image_file(file_path: str) -> str | None:
    """Validate that a file with an image extension contains actual image data.

    Returns None if valid, or an error string describing the problem.
    This prevents the runtime from base64-encoding non-image files (e.g.
    HTML redirect pages saved with a .png extension) and poisoning the
    conversation context with an unprocessable image block.
    """
    from pathlib import Path

    ext = Path(file_path).suffix.lower()
    if ext not in IMAGE_EXTENSIONS:
        return None  # Not an image — nothing to validate

    try:
        size = os.path.getsize(file_path)
    except OSError:
        return None  # Let the Read tool handle missing files

    if size == 0:
        return f"Image file is empty (0 bytes): {file_path}"

    if size > MAX_IMAGE_BYTES:
        size_mb = size / (1024 * 1024)
        return (
            f"Image file too large ({size_mb:.1f} MB > 5 MB API limit): {file_path}. "
            f"The Anthropic API rejects images larger than 5 MB."
        )

    # Check magic bytes
    magic_specs = IMAGE_MAGIC.get(ext, [])
    if not magic_specs:
        return None  # No magic spec — let it through

    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
    except OSError:
        return None  # Let the Read tool handle I/O errors

    # Each signature is a list of (bytes, offset) pairs — ALL must match.
    # Multiple signatures per format are OR'd (e.g. GIF87a vs GIF89a).
    for signature in magic_specs:
        if all(
            header[off: off + len(magic)] == magic
            for magic, off in signature
        ):
            return None  # Valid magic — good to go

    # None of the magic signatures matched
    # Check if it's actually HTML (common when auth fails on image URLs)
    is_html = header.lstrip()[:5].lower() in (b"<!doc", b"<html", b"<?xml")
    hint = (
        " The file appears to contain HTML — it may be a redirect or error page "
        "downloaded instead of the actual image."
        if is_html
        else " The file header does not match any supported image format "
        "(JPEG, PNG, GIF, WebP)."
    )
    return (
        f"File {file_path} has {ext} extension but does not contain valid image data.{hint} "
        f"Reading this file would poison the conversation with an unprocessable image block."
    )


def validate_image_data(data_b64: str, media_type: str) -> str | None:
    """Validate base64-encoded image data before sending to the API.

    Returns None if valid, or an error string describing the problem.
    Used for images entering through Nerve's own pipeline (Telegram, etc).
    """
    import base64

    try:
        raw = base64.b64decode(data_b64[:64])  # Only need first bytes
    except Exception:
        return f"Invalid base64 encoding for {media_type} image"

    if len(raw) < 4:
        return f"Image data too small ({len(raw)} bytes) for {media_type}"

    # Map media_type to extension for magic check
    type_to_ext = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/gif": ".gif",
        "image/webp": ".webp",
    }
    ext = type_to_ext.get(media_type)
    if not ext:
        return None  # Unknown type — let the API decide

    magic_specs = IMAGE_MAGIC.get(ext, [])
    for signature in magic_specs:
        if all(
            raw[off: off + len(magic)] == magic
            for magic, off in signature
        ):
            return None  # Valid

    return (
        f"Image data does not match declared type {media_type}. "
        f"The file header bytes do not contain a valid {ext.upper().strip('.')} signature."
    )
