"""Generated images are bounded inline data, stored only in their workspace."""

from __future__ import annotations

import base64
import errno
import hashlib
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

from nerve.agent import generated_images
from nerve.agent.generated_images import store_generated_image


# A complete synthetic one-pixel PNG, not a private photo or runtime fixture.
PNG_DATA = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVR4nGP4DwABAQEAsTj2FAAAAABJRU5ErkJggg=="
)
PNG_URL = "data:image/png;base64," + base64.b64encode(PNG_DATA).decode()


def _destination(workspace: Path, session_id: str = "session-1") -> Path:
    session_key = hashlib.sha256(session_id.encode()).hexdigest()[:16]
    digest = hashlib.sha256(PNG_DATA).hexdigest()
    return workspace / "output" / "generated-images" / session_key / (digest + ".png")


def test_materializes_png_and_reuses_identical_bytes(tmp_path):
    first, content = store_generated_image(tmp_path, "session-1", PNG_URL)
    assert first == _destination(tmp_path)
    assert content == PNG_DATA
    assert first.read_bytes() == PNG_DATA
    assert stat.S_IMODE(first.stat().st_mode) == 0o600
    first_info = first.stat()
    again, content = store_generated_image(tmp_path, "session-1", PNG_URL)
    assert again == first
    assert content == PNG_DATA
    assert again.stat().st_ino == first_info.st_ino
    assert again.stat().st_mtime_ns == first_info.st_mtime_ns
    assert list(first.parent.iterdir()) == [first]


@pytest.mark.parametrize("image_url", [
    "https://example.com/image.png",
    "file:///etc/passwd",
    "data:image/png,not-base64",
    "data:image/svg+xml;base64,PHN2Zy8+",
    "data:image/gif;base64,R0lGODlh",
    "data:image/png;base64,",
    "data:image/png;base64,%%%",
    "data:image/png;base64," + base64.b64encode(b"not an image").decode(),
    "data:image/jpeg;base64," + base64.b64encode(PNG_DATA).decode(),
])
def test_rejects_remote_non_image_and_invalid_data(tmp_path, image_url):
    with pytest.raises(ValueError):
        store_generated_image(tmp_path, "session-1", image_url)
    assert not (tmp_path / "output").exists()


def test_data_url_size_is_bounded_before_decoding(tmp_path, monkeypatch):
    monkeypatch.setattr(generated_images, "MAX_IMAGE_URL_BYTES", len(PNG_URL) - 1)
    with pytest.raises(ValueError, match="data URL.*too large"):
        store_generated_image(tmp_path, "session-1", PNG_URL)


def test_decoded_size_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(generated_images, "MAX_GENERATED_IMAGE_BYTES", len(PNG_DATA) - 1)
    with pytest.raises(ValueError, match="size limit"):
        store_generated_image(tmp_path, "session-1", PNG_URL)


def test_generated_image_is_not_subject_to_the_inbound_five_mib_cap(tmp_path):
    # Match the existing magic-validation semantics, without decoding pixels.
    content = PNG_DATA + b"\0" * (5 * 1024 * 1024)
    image_url = "data:image/png;base64," + base64.b64encode(content).decode()
    path, _ = store_generated_image(tmp_path, "session-1", image_url)
    assert path.stat().st_size > 5 * 1024 * 1024
    assert path.read_bytes() == content


@pytest.mark.parametrize("session_id", ["../../outside", "/outside", "nested/name", ""])
def test_session_names_cannot_escape_workspace(tmp_path, session_id):
    path, _ = store_generated_image(tmp_path, session_id, PNG_URL)
    assert path == _destination(tmp_path, session_id)
    assert path.resolve().is_relative_to(tmp_path.resolve())


@pytest.mark.parametrize("component", ["output", "generated-images", "session"])
def test_rejects_parent_symlinks_outside_workspace(tmp_path, component):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = _destination(workspace)
    if component == "output":
        link = workspace / "output"
    elif component == "generated-images":
        (workspace / "output").mkdir()
        link = workspace / "output" / "generated-images"
    else:
        destination.parent.parent.mkdir(parents=True)
        link = destination.parent
    link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        store_generated_image(workspace, "session-1", PNG_URL)
    assert list(outside.iterdir()) == []


def test_rejects_destination_symlink_without_touching_outside_file(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    destination = _destination(workspace)
    destination.parent.mkdir(parents=True)
    outside = tmp_path / "private"
    outside.write_bytes(b"private bytes")
    destination.symlink_to(outside)
    with pytest.raises(ValueError):
        store_generated_image(workspace, "session-1", PNG_URL)
    assert destination.is_symlink()
    assert outside.read_bytes() == b"private bytes"


def test_replaces_corrupt_regular_output_atomically(tmp_path):
    destination = _destination(tmp_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"corrupt")
    store_generated_image(tmp_path, "session-1", PNG_URL)
    assert destination.read_bytes() == PNG_DATA
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert list(destination.parent.iterdir()) == [destination]


def test_rejects_hard_linked_destination_without_touching_original(tmp_path):
    destination = _destination(tmp_path)
    destination.parent.mkdir(parents=True)
    outside = tmp_path / "original"
    outside.write_bytes(PNG_DATA)
    os.link(outside, destination)
    before = outside.stat()
    with pytest.raises(ValueError, match="private file"):
        store_generated_image(tmp_path, "session-1", PNG_URL)
    assert outside.read_bytes() == PNG_DATA
    assert outside.stat().st_mode == before.st_mode


@pytest.fixture
def git(tmp_path):
    if not shutil.which("git"):
        pytest.skip("git not available")

    def run(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True,
        ).stdout

    return run


@pytest.mark.parametrize("init_before_generation", [True, False])
def test_generated_images_do_not_stage_with_existing_config(tmp_path, git, init_before_generation):
    root_ignore = tmp_path / ".gitignore"
    root_ignore.write_text("/config.yaml\n")
    if init_before_generation:
        git("init", "-q")
    image, _ = store_generated_image(tmp_path, "session-1", PNG_URL)
    if not init_before_generation:
        git("init", "-q")

    git("add", "-A")
    assert git("ls-files").splitlines() == [".gitignore"]
    assert root_ignore.read_text() == "/config.yaml\n"
    assert image.read_bytes() == PNG_DATA


def test_refuses_to_replace_indexed_outputs(tmp_path, git):
    git("init", "-q")
    path = _destination(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"indexed")
    git("add", str(path))
    with pytest.raises(ValueError, match="ignored and untracked"):
        store_generated_image(tmp_path, "session-1", PNG_URL)
    assert path.read_bytes() == b"indexed"


def test_refuses_to_save_when_git_metadata_cannot_be_verified(tmp_path):
    (tmp_path / ".git").write_text("invalid gitdir link")
    with pytest.raises((ValueError, OSError)):
        store_generated_image(tmp_path, "session-1", PNG_URL)
    assert not _destination(tmp_path).exists()


def test_ignore_install_recovers_after_first_write_fails(tmp_path, git, monkeypatch):
    git("init")
    generated = tmp_path / "output" / "generated-images"

    def fail_sync(_descriptor):
        raise OSError(errno.ENOSPC, "No space left")

    with monkeypatch.context() as patch:
        patch.setattr(generated_images.os, "fsync", fail_sync)
        with pytest.raises(OSError) as error:
            store_generated_image(tmp_path, "session-1", PNG_URL)
        assert error.value.errno == errno.ENOSPC

    assert list(generated.iterdir()) == []
    path, content = store_generated_image(tmp_path, "session-1", PNG_URL)
    assert (generated / ".gitignore").read_bytes() == b"*\n"
    assert path.read_bytes() == content == PNG_DATA
    git("add", "-A")
    assert git("ls-files").splitlines() == []
