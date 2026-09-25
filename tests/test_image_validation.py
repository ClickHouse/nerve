"""Read-image validation: magic bytes, the API size cap, and the transport bound.

The transport bound is the Agent SDK's per-line ``max_buffer_size``: a Read
image result travels as ONE stream-json line carrying the base64 twice, and
a line over the bound aborts the whole turn rather than the one tool call.
"""

from nerve.agent.backends.images import (
    CLI_MAX_IMAGE_BASE64,
    IMAGE_WIRE_COPIES,
    IMAGE_WIRE_OVERHEAD,
    MAX_IMAGE_BYTES,
    estimate_image_wire_bytes,
    validate_image_file,
)

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
ONE_MIB = 1024 * 1024


def _png(tmp_path, size: int, name: str = "shot.png") -> str:
    path = tmp_path / name
    path.write_bytes(PNG_MAGIC + b"\0" * (size - len(PNG_MAGIC)))
    return str(path)


def test_wire_estimate_counts_both_copies_of_the_base64():
    # A 420,301-byte PNG is 560,404 chars of base64 — shipped twice, the
    # line is already over 1 MiB.
    assert estimate_image_wire_bytes(420_301) == (
        IMAGE_WIRE_COPIES * 560_404 + IMAGE_WIRE_OVERHEAD
    )
    assert estimate_image_wire_bytes(420_301) > ONE_MIB
    # The CLI downsizes to at most 5 MiB of base64, so the bound saturates.
    assert estimate_image_wire_bytes(MAX_IMAGE_BYTES) == (
        IMAGE_WIRE_COPIES * CLI_MAX_IMAGE_BASE64 + IMAGE_WIRE_OVERHEAD
    )


def test_image_over_transport_cap_is_refused(tmp_path):
    err = validate_image_file(_png(tmp_path, 600 * 1024), max_message_bytes=ONE_MIB)
    assert err is not None
    assert "per-message limit" in err
    assert "cli_max_message_bytes" in err


def test_same_image_passes_under_a_roomy_cap(tmp_path):
    assert validate_image_file(
        _png(tmp_path, 600 * 1024), max_message_bytes=64 * ONE_MIB,
    ) is None


def test_no_cap_means_no_transport_check(tmp_path):
    assert validate_image_file(_png(tmp_path, 600 * 1024)) is None


def test_api_limit_still_wins(tmp_path):
    err = validate_image_file(
        _png(tmp_path, MAX_IMAGE_BYTES + 1), max_message_bytes=64 * ONE_MIB,
    )
    assert err is not None
    assert "5 MB API limit" in err


def test_bad_magic_is_still_refused_as_poison(tmp_path):
    path = tmp_path / "redirect.png"
    path.write_bytes(b"<!doctype html><html>not an image</html>")
    err = validate_image_file(str(path), max_message_bytes=64 * ONE_MIB)
    assert err is not None
    assert "HTML" in err


def test_non_image_extension_is_ignored(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_bytes(b"\0" * (2 * ONE_MIB))
    assert validate_image_file(str(path), max_message_bytes=ONE_MIB) is None
