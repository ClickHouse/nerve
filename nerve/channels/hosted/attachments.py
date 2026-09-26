"""Attachments of hosted events: read them and turn them into prompt input.

The rules follow the self-hosted Slack channel. Text files are put in the
prompt text, images and PDFs become base64 blocks, ZIP files are unpacked one
level by :func:`extract_zip`, and any other file gets a metadata line only.
Nerve reads a file only when it can use it, and never more than the limits
below, also when the event does not give the size.
"""

from __future__ import annotations

import base64
from typing import Awaitable, Callable

from nerve.channels.archives import IMAGE_EXT_TO_MIME, MAX_TEXT_SIZE, TEXT_EXTENSIONS, extract_zip
from nerve.channels.hosted.contract.model import MAX_TRANSFER_BYTES, AttachmentReference

# The self-hosted per-file limit, with room for one more byte in a transfer.
MAX_FILE_BYTES = min(20_000_000, MAX_TRANSFER_BYTES - 1)
# All files of one message together, with the content unpacked from ZIP files.
MAX_MESSAGE_BYTES = 2 * MAX_TRANSFER_BYTES
_ZIP_TYPES = ("application/zip", "application/x-zip-compressed")

# Reads up to the given number of bytes of an attachment; ``None`` on failure.
Reader = Callable[[AttachmentReference, int], Awaitable[bytes | None]]


def _size_text(size: int) -> str:
    return f"{size / 1024:.0f} KB" if size < 1_000_000 else f"{size / 1_000_000:.1f} MB"


def _kind(name: str, media_type: str) -> str:
    extension = f".{name.rsplit('.', 1)[-1].lower()}" if "." in name else ""
    if media_type.startswith("text/") or extension in TEXT_EXTENSIONS:
        return "text"
    if media_type in IMAGE_EXT_TO_MIME.values() or extension in IMAGE_EXT_TO_MIME:
        return "image"
    if media_type == "application/pdf" or extension == ".pdf":
        return "pdf"
    if extension == ".zip" or media_type in _ZIP_TYPES:
        return "zip"
    return ""


def _image_type(name: str, media_type: str) -> str:
    """The media type of an image: the given one when it names a known image type."""
    if media_type in IMAGE_EXT_TO_MIME.values():
        return media_type
    extension = f".{name.rsplit('.', 1)[-1].lower()}" if "." in name else ""
    return IMAGE_EXT_TO_MIME.get(extension, "image/png")


async def extract_attachments(
    attachments: tuple[AttachmentReference, ...], read: Reader,
) -> tuple[str, list[dict[str, str]]]:
    """Read *attachments* and return ``(context_text, blocks)``.

    A text file is read up to one byte past the inline limit, so a file of
    unknown size that is too large is refused, not cut. Other files are read
    the same way up to :data:`MAX_FILE_BYTES`. When a read fails, the file
    gets its metadata line only. The message budget counts the bytes read
    and also the content unpacked from a ZIP file.
    """
    parts: list[str] = []
    blocks: list[dict[str, str]] = []
    budget = MAX_MESSAGE_BYTES
    for attachment in attachments:
        name = attachment.name or "unnamed"
        media_type = attachment.media_type
        size = attachment.size_bytes
        meta = f"[File: {name} ({_size_text(size)}, {media_type or 'unknown type'})]"
        kind = _kind(name, media_type)
        if not kind:
            parts.append(meta)
            continue
        limit = MAX_TEXT_SIZE if kind == "text" else MAX_FILE_BYTES
        if size > limit:
            if kind == "text":
                parts.append(f"{meta}\n(Text file too large to inline: {_size_text(size)})")
            else:
                parts.append(f"{meta}\n(Too large or not downloadable)")
            continue
        # One byte more than the limit tells a file of unknown size that is
        # too large from one that fits.
        length = size or limit + 1
        if length > budget:
            parts.append(f"{meta}\n(Skipped: the message's files are too large together)")
            continue
        data = await read(attachment, length)
        if data is None:
            parts.append(meta)
            continue
        budget -= len(data)
        if len(data) > limit:
            parts.append(f"{meta}\n(Too large or not downloadable)")
            continue
        if kind == "text":
            parts.append(f"{meta}\n```\n{data.decode('utf-8', errors='replace')}\n```")
        elif kind == "zip":
            zip_blocks, zip_text = extract_zip(data, meta)
            budget -= len(zip_text.encode("utf-8")) + sum(
                len(block["data"]) * 3 // 4 for block in zip_blocks
            )
            blocks.extend(zip_blocks)
            parts.append(zip_text)
        else:
            blocks.append({
                "type": "base64",
                "media_type": "application/pdf" if kind == "pdf" else _image_type(name, media_type),
                "data": base64.b64encode(data).decode("utf-8"),
            })
            parts.append(meta)
    return "\n".join(parts), blocks


__all__ = ["MAX_FILE_BYTES", "MAX_MESSAGE_BYTES", "Reader", "extract_attachments"]
