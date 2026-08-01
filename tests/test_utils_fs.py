"""Tests for the shared atomic file writer."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from nerve.utils.fs import atomic_write_text


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


@pytest.fixture
def permissive_umask():
    old = os.umask(0o022)
    yield
    os.umask(old)


def test_writes_content_and_creates_parents(tmp_path):
    target = tmp_path / "deep" / "nested" / "file.yaml"
    atomic_write_text(target, "hello\n")
    assert target.read_text(encoding="utf-8") == "hello\n"


def test_mode_is_applied_regardless_of_umask(tmp_path, permissive_umask):
    target = tmp_path / "secrets.yaml"
    atomic_write_text(target, "token: x\n")
    assert _mode(target) == 0o600

    public = tmp_path / "shared.yaml"
    atomic_write_text(public, "timezone: UTC\n", mode=0o644)
    assert _mode(public) == 0o644


@pytest.mark.parametrize("umask,expected", [(0o022, 0o644), (0o077, 0o600), (0o002, 0o664)])
def test_mode_none_on_a_new_file_matches_a_plain_write(tmp_path, umask, expected):
    """``mode=None`` is for shareable files: same permissions ``open()`` would
    have given them, so a restrictive umask is not overridden."""
    old = os.umask(umask)
    try:
        atomic_write_text(tmp_path / "shared.yaml", "timezone: UTC\n", mode=None)
        reference = tmp_path / "reference.yaml"
        with open(reference, "w") as f:
            f.write("timezone: UTC\n")
    finally:
        os.umask(old)
    assert _mode(tmp_path / "shared.yaml") == expected == _mode(reference)


@pytest.mark.parametrize("existing,umask", [(0o600, 0o000), (0o644, 0o077), (0o640, 0o022)])
def test_mode_none_keeps_an_existing_files_permissions(tmp_path, existing, umask):
    """``open(path, "w")`` re-permissions nothing — only a *new* file takes its
    mode from the umask. Deriving the mode from the umask on every write would
    widen a 0o600 file to 0o666 under a lax umask, silently publishing a file
    that had been locked down, and clamp a group-readable one under a strict
    one. The rename this helper does must not change what the mode would be."""
    target = tmp_path / "shared.yaml"
    reference = tmp_path / "reference.yaml"
    for p in (target, reference):
        p.write_text("old\n", encoding="utf-8")
        os.chmod(p, existing)

    old = os.umask(umask)
    try:
        atomic_write_text(target, "new\n", mode=None)
        with open(reference, "w") as f:
            f.write("new\n")
    finally:
        os.umask(old)

    assert _mode(target) == existing == _mode(reference)
    assert target.read_text(encoding="utf-8") == "new\n"


def test_explicit_mode_beats_an_existing_files_permissions(tmp_path):
    """Preserving what is on disk is only what ``mode=None`` asks for. A file
    that has started holding a credential has to be tightened despite being
    world-readable a moment ago, and an explicit loosening has to stick too."""
    tightened = tmp_path / "config.local.yaml"
    tightened.write_text("timezone: UTC\n", encoding="utf-8")
    os.chmod(tightened, 0o644)
    atomic_write_text(tightened, "openai_api_key: sk-x\n")
    assert _mode(tightened) == 0o600

    loosened = tmp_path / "shared.yaml"
    loosened.write_text("old\n", encoding="utf-8")
    os.chmod(loosened, 0o600)
    atomic_write_text(loosened, "new\n", mode=0o644)
    assert _mode(loosened) == 0o644


def test_symlinked_destination_is_written_through(tmp_path):
    """A config file linked into a dotfiles repo must keep being that link —
    replacing it with a regular file would leave the repo copy stale and put
    the new secret outside the repo."""
    real = tmp_path / "repo" / "config.local.yaml"
    real.parent.mkdir()
    real.write_text("old\n", encoding="utf-8")
    link = tmp_path / "config.local.yaml"
    link.symlink_to(real)

    atomic_write_text(link, "new\n")

    assert link.is_symlink()
    assert real.read_text(encoding="utf-8") == "new\n"
    assert _mode(real) == 0o600


@pytest.mark.skipif(
    not hasattr(os, "fchmod"),
    reason="no os.fchmod here; the chmod fallback it forces is covered below",
)
def test_mode_is_set_before_the_content_is_written(tmp_path, permissive_umask, monkeypatch):
    """Chmod'ing after the write is a race: the secret is already on disk, and
    readable, for however far apart the two syscalls land."""
    at_chmod = []
    real_fchmod = os.fchmod

    def spy_fchmod(fd, mode):
        at_chmod.append((os.fstat(fd).st_size, mode))
        return real_fchmod(fd, mode)

    monkeypatch.setattr(os, "fchmod", spy_fchmod)
    atomic_write_text(tmp_path / "secrets.yaml", "token: hunter2\n")

    # Permissions settled while the file was still empty.
    assert at_chmod == [(0, 0o600)]


def test_mode_is_set_before_the_content_is_written_without_fchmod(
    tmp_path, permissive_umask, monkeypatch
):
    """Windows has no ``os.fchmod``, so the writer falls back to ``os.chmod`` on
    the temp file's name. Take the attribute away here rather than only on the
    platform that lacks it, or the one path that needs checking is the one path
    never exercised.

    The mode asked for is 0o644, not 0o600: ``mkstemp`` already creates at 0o600,
    so a 0o600 expectation would pass just as happily if the fallback never ran.
    """
    monkeypatch.delattr(os, "fchmod", raising=False)
    at_chmod = []
    real_chmod = os.chmod

    def spy_chmod(path, mode, **kwargs):
        at_chmod.append((os.stat(path).st_size, mode))
        return real_chmod(path, mode, **kwargs)

    monkeypatch.setattr(os, "chmod", spy_chmod)
    target = tmp_path / "shared.yaml"
    atomic_write_text(target, "timezone: UTC\n", mode=0o644)

    assert at_chmod == [(0, 0o644)]
    assert _mode(target) == 0o644
    assert target.read_text(encoding="utf-8") == "timezone: UTC\n"


def test_existing_file_survives_a_failed_write(tmp_path, monkeypatch):
    target = tmp_path / "config.local.yaml"
    target.write_text("openai_api_key: sk-only-copy\n", encoding="utf-8")

    def boom(fd):
        raise OSError("No space left on device")

    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(OSError):
        atomic_write_text(target, "clobbered\n")

    assert target.read_text(encoding="utf-8") == "openai_api_key: sk-only-copy\n"
    # ...and no debris left behind for the next reader to trip over.
    assert [p.name for p in tmp_path.iterdir()] == ["config.local.yaml"]


def test_replaces_an_existing_file(tmp_path):
    target = tmp_path / "f.txt"
    target.write_text("old", encoding="utf-8")
    atomic_write_text(target, "new")
    assert target.read_text(encoding="utf-8") == "new"
    assert list(tmp_path.iterdir()) == [target]
