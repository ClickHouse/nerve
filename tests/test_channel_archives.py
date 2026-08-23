"""Bounded ZIP unpacking for the chat channels.

A channel caps the archive it downloads at about 20 MB of *compressed*
bytes. That says nothing about what the archive expands to, so an
authorized user could hand the daemon a few megabytes that unpack to
gigabytes of base64 blocks. Every bound is read off the archive's own
directory before an entry is opened, so refusing one costs nothing.
"""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels import archives
from nerve.channels.archives import extract_zip

# A one-pixel PNG. Real bytes, so the image branch is genuinely exercised.
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000a49444154789c6360000002000100ffff03000006000557bfabd400"
    "00000049454e44ae426082"
)


def _zip(entries: dict[str, bytes], compression=zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


class TestOrdinaryArchives:
    def test_a_text_file_is_inlined(self):
        _, text = extract_zip(_zip({"notes.md": b"hello"}), "[File: a.zip]")
        assert "notes.md" in text
        assert "hello" in text

    def test_an_image_becomes_a_content_block(self):
        blocks, text = extract_zip(_zip({"shot.png": _PNG}), "[File: a.zip]")
        assert [b["media_type"] for b in blocks] == ["image/png"]
        assert "[image]" in text

    def test_an_unknown_type_is_listed_without_being_read(self):
        _, text = extract_zip(_zip({"blob.bin": b"\x00" * 32}), "[File: a.zip]")
        assert "blob.bin" in text

    def test_a_corrupt_archive_is_reported(self):
        blocks, text = extract_zip(b"not a zip at all", "[File: a.zip]")
        assert blocks == []
        assert "Invalid or corrupted" in text

    def test_a_password_protected_archive_is_reported_once(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("a.txt", b"one")
            zf.writestr("b.txt", b"two")
            for info in zf.infolist():
                info.flag_bits |= 0x1
        blocks, text = extract_zip(buf.getvalue(), "[File: a.zip]")
        assert blocks == []
        assert "Cannot extract" in text
        assert "read error" not in text


class TestDecompressionBounds:
    def test_too_many_entries_reject_the_whole_archive(self, monkeypatch):
        monkeypatch.setattr(archives, "MAX_ENTRIES", 3)
        data = _zip({f"f{i}.txt": b"x" for i in range(4)})
        blocks, text = extract_zip(data, "[File: a.zip]")
        assert blocks == []
        assert "the limit is 3" in text

    def test_an_entry_over_the_size_cap_is_refused_not_truncated(
        self, monkeypatch,
    ):
        monkeypatch.setattr(archives, "MAX_ENTRY_SIZE", 100)
        data = _zip({"big.txt": b"a" * 500, "small.txt": b"ok"})
        _, text = extract_zip(data, "[File: a.zip]")
        assert "big.txt (500 bytes) [too large to read]" in text
        assert "a" * 500 not in text
        # A refusal is per entry, so the rest of the archive still arrives.
        assert "ok" in text

    def test_a_high_compression_ratio_is_refused(self):
        # 4 MB of zeros compresses to a few kilobytes. The outer download
        # cap never sees it; only the ratio does.
        blocks, text = extract_zip(
            _zip({"bomb.png": b"\x00" * 4_000_000}), "[File: a.zip]",
        )
        assert blocks == []
        assert "compression ratio too high" in text

    def test_the_ratio_is_checked_before_anything_is_read(self, monkeypatch):
        # The whole point of reading ZipInfo first: refusing must not cost
        # the memory the refusal exists to save.
        data = _zip({"bomb.png": b"\x00" * 4_000_000})

        def _boom(*args, **kwargs):
            raise AssertionError("the entry was opened")

        monkeypatch.setattr(zipfile.ZipFile, "open", _boom)
        blocks, text = extract_zip(data, "[File: a.zip]")
        assert blocks == []
        assert "compression ratio too high" in text

    def test_the_aggregate_budget_stops_later_entries(self, monkeypatch):
        monkeypatch.setattr(archives, "MAX_TOTAL_SIZE", 120)
        monkeypatch.setattr(archives, "MAX_RATIO", 100_000)
        data = _zip({"a.txt": b"a" * 100, "b.txt": b"b" * 100})
        _, text = extract_zip(data, "[File: a.zip]")
        assert "a" * 100 in text
        assert "b.txt (100 bytes) [archive size budget spent]" in text

    def test_the_text_budget_refuses_rather_than_cuts(self, monkeypatch):
        monkeypatch.setattr(archives, "MAX_TEXT_SIZE", 50)
        monkeypatch.setattr(archives, "MAX_RATIO", 100_000)
        _, text = extract_zip(_zip({"long.txt": b"c" * 200}), "[File: a.zip]")
        assert "long.txt (200 bytes) [text, too large to inline]" in text
        assert "c" * 200 not in text

    def test_a_refused_entry_still_appears_in_the_listing(self, monkeypatch):
        monkeypatch.setattr(archives, "MAX_ENTRY_SIZE", 10)
        _, text = extract_zip(_zip({"big.txt": b"a" * 500}), "[File: a.zip]")
        assert "Archive contains 1 file(s):" in text
        assert "big.txt" in text

    def test_a_stored_entry_at_ratio_one_is_allowed(self):
        # Ratio 1 is what an already-compressed payload looks like; the
        # guard must not turn into a size cap by another name.
        data = _zip({"plain.txt": b"d" * 5000}, compression=zipfile.ZIP_STORED)
        _, text = extract_zip(data, "[File: a.zip]")
        assert "d" * 5000 in text


@pytest.mark.asyncio
class TestTelegramUsesTheBounds:
    async def test_telegram_refuses_a_bomb_in_a_document(self):
        from nerve.channels.telegram import TelegramChannel
        from nerve.config import NerveConfig

        channel = TelegramChannel(lambda: NerveConfig(), router=MagicMock())
        tg_file = MagicMock()
        tg_file.download_as_bytearray = AsyncMock(
            return_value=bytearray(_zip({"bomb.png": b"\x00" * 4_000_000})),
        )
        doc = MagicMock()
        doc.get_file = AsyncMock(return_value=tg_file)

        blocks, text = await channel._extract_zip(
            doc, "payload.zip", "[Document: payload.zip]",
        )
        assert blocks == []
        assert "compression ratio too high" in text
