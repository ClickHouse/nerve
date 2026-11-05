"""Bounded ZIP unpacking, shared by the chat channels.

Slack and Telegram both accept an attached archive and both cap the file
they download. That cap is on the *compressed* bytes, so it says nothing
about what the archive expands to: a few megabytes of zeros expand to
gigabytes, and reading them into base64 blocks exhausts the daemon.

Every limit here is checked against the archive's own directory before any
entry is read, and the read itself is bounded as well, because a ZIP header
can under-report an entry's size. An entry past a limit is refused with a
line saying so; nothing is silently cut short.
"""

from __future__ import annotations

import base64
import io
import logging
import zipfile

logger = logging.getLogger(__name__)

TEXT_EXTENSIONS: frozenset[str] = frozenset({
    ".txt", ".py", ".js", ".ts", ".jsx", ".tsx", ".json", ".yaml", ".yml",
    ".toml", ".xml", ".html", ".htm", ".css", ".scss", ".less",
    ".md", ".rst", ".csv", ".tsv", ".sql", ".sh", ".bash", ".zsh",
    ".rb", ".go", ".rs", ".java", ".kt", ".c", ".cpp", ".h", ".hpp",
    ".swift", ".lua", ".r", ".m", ".pl", ".php", ".env", ".ini", ".cfg",
    ".conf", ".log", ".diff", ".patch", ".vue", ".svelte",
})

IMAGE_EXT_TO_MIME: dict[str, str] = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp",
}

# Inline text budget for the whole archive.
MAX_TEXT_SIZE = 512 * 1024
# Files in one archive. A directory listing longer than this is a machine
# dump, not something a person meant to show the agent.
MAX_ENTRIES = 100
# Uncompressed bytes for one entry, and for the archive as a whole.
# Images and PDFs reach the model as base64, which is 4/3 of the bytes
# read, so the total is set from what the prompt can carry after that
# expansion. The channels cap the download at about 20 MB of compressed
# bytes, which says nothing about what the archive expands to.
MAX_ENTRY_SIZE = 5_000_000
MAX_TOTAL_SIZE = 12_000_000
# Uncompressed / compressed for one entry, checked only above RATIO_FLOOR.
# Small files reach a high ratio for ordinary reasons, such as generated
# code or a log of one repeated line, and refusing those loses real
# content. MAX_ENTRY_SIZE and MAX_TOTAL_SIZE already bound what any entry
# adds to the prompt, so the ratio only has to catch the large entries.
MAX_RATIO = 100
RATIO_FLOOR = 1_000_000


class _EntryTooLarge(Exception):
    """An entry produced more bytes than its directory record promised."""


def _read_bounded(zf: zipfile.ZipFile, info: zipfile.ZipInfo, limit: int) -> bytes:
    """Read one entry, refusing more than *limit* bytes.

    The bound is on what comes out, not only on the size the central
    directory declares, so the caller's budget holds even for an archive
    whose records do not describe its contents.
    """
    with zf.open(info) as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise _EntryTooLarge(info.filename)
    return raw


def _refusal(info: zipfile.ZipInfo, reason: str) -> str:
    return f"- {info.filename} ({info.file_size} bytes) [{reason}]"


def extract_zip(data: bytes, meta_line: str) -> tuple[list[dict[str, str]], str]:
    """Unpack a ZIP one level — text inline, images and PDFs as blocks.

    Returns ``(content_blocks, context_text)``. ``meta_line`` is the caller's
    one-line description of the archive and heads the context text.
    """
    buf = io.BytesIO(data)
    if not zipfile.is_zipfile(buf):
        return [], f"{meta_line}\n(Invalid or corrupted ZIP archive)"
    buf.seek(0)

    blocks: list[dict[str, str]] = []
    parts: list[str] = [meta_line]
    try:
        with zipfile.ZipFile(buf) as zf:
            entries = [
                i for i in zf.infolist()
                if not i.is_dir() and not i.filename.startswith("__MACOSX/")
            ]
            if len(entries) > MAX_ENTRIES:
                return [], (
                    f"{meta_line}\n(Archive holds {len(entries)} files; "
                    f"the limit is {MAX_ENTRIES})"
                )

            parts.append(f"Archive contains {len(entries)} file(s):")
            total_text = 0
            total_read = 0
            for info in entries:
                name = info.filename
                size = info.file_size
                ext = ""
                if "." in name.rsplit("/", 1)[-1]:
                    ext = "." + name.rsplit(".", 1)[-1].lower()
                wanted = ext in TEXT_EXTENSIONS or ext in IMAGE_EXT_TO_MIME or ext == ".pdf"

                if not wanted:
                    parts.append(f"- {name} ({size} bytes)")
                    continue

                # Every bound below is read off the central directory, so a
                # refusal costs nothing but the listing itself.
                if size > MAX_ENTRY_SIZE:
                    parts.append(_refusal(info, "too large to read"))
                    continue
                if (
                    size > RATIO_FLOOR
                    and info.compress_size
                    and size / info.compress_size > MAX_RATIO
                ):
                    logger.warning(
                        "Refusing ZIP entry %s: %d bytes from %d compressed",
                        name, size, info.compress_size,
                    )
                    parts.append(_refusal(info, "compression ratio too high"))
                    continue
                # The type-specific budget is checked first so a refusal names
                # the limit the entry actually hit.
                if ext in TEXT_EXTENSIONS and total_text + size > MAX_TEXT_SIZE:
                    parts.append(_refusal(info, "text, too large to inline"))
                    continue
                if total_read + size > MAX_TOTAL_SIZE:
                    parts.append(_refusal(info, "archive size budget spent"))
                    continue

                try:
                    raw = _read_bounded(zf, info, size)
                except _EntryTooLarge:
                    logger.warning(
                        "Refusing ZIP entry %s: it expands past the %d bytes "
                        "its header declares", name, size,
                    )
                    parts.append(_refusal(info, "larger than it declares"))
                    continue
                except RuntimeError:
                    # A password-protected archive fails the same way on
                    # every entry, so it is reported once, below.
                    raise
                except Exception:
                    parts.append(_refusal(info, "read error"))
                    continue
                total_read += len(raw)

                if ext in TEXT_EXTENSIONS:
                    total_text += len(raw)
                    parts.append(
                        f"--- {name} ({size} bytes) ---\n"
                        f"```\n{raw.decode('utf-8', errors='replace')}\n```"
                    )
                else:
                    is_pdf = ext == ".pdf"
                    blocks.append({
                        "type": "base64",
                        "media_type": (
                            "application/pdf" if is_pdf else IMAGE_EXT_TO_MIME[ext]
                        ),
                        "data": base64.b64encode(raw).decode("utf-8"),
                    })
                    parts.append(
                        f"- {name} ({size} bytes) [{'PDF' if is_pdf else 'image'}]"
                    )
    except zipfile.BadZipFile:
        return [], f"{meta_line}\n(Invalid or corrupted ZIP archive)"
    except RuntimeError as e:
        # Password-protected archives.
        return [], f"{meta_line}\n(Cannot extract: {e})"

    return blocks, "\n".join(parts)
