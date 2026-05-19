"""Workspace filter — decides which Codex threads are in scope.

The filter runs at file-open time using only the cwd from
``session_meta``. The tests cover every mode plus malformed inputs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve.sources.codex_threads.base import WorkspaceFilter


def test_nerve_workspace_matches_only_configured_path(tmp_path: Path):
    ws = tmp_path / "ws"
    ws.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    f = WorkspaceFilter(mode="nerve_workspace", nerve_workspace_path=ws)
    assert f.matches(str(ws))
    assert f.matches(ws)
    assert not f.matches(str(other))


def test_nerve_workspace_resolves_symlink(tmp_path: Path):
    ws = tmp_path / "ws"
    ws.mkdir()
    link = tmp_path / "link-to-ws"
    link.symlink_to(ws)
    f = WorkspaceFilter(mode="nerve_workspace", nerve_workspace_path=ws)
    assert f.matches(str(link))


def test_nerve_workspace_without_configured_path_denies(tmp_path: Path):
    f = WorkspaceFilter(mode="nerve_workspace", nerve_workspace_path=None)
    assert not f.matches(str(tmp_path))


def test_explicit_matches_listed_paths(tmp_path: Path):
    a = tmp_path / "a"; a.mkdir()
    b = tmp_path / "b"; b.mkdir()
    c = tmp_path / "c"; c.mkdir()
    f = WorkspaceFilter(mode="explicit", explicit_paths=[a, b])
    assert f.matches(str(a))
    assert f.matches(str(b))
    assert not f.matches(str(c))


def test_any_mode_matches_everything(tmp_path: Path):
    f = WorkspaceFilter(mode="any")
    assert f.matches(str(tmp_path))
    assert f.matches("/nonexistent/path")
    # "any" really means any — including a missing cwd. Operators
    # opting in to this mode want everything.
    assert f.matches(None)


def test_cwd_none_denies(tmp_path: Path):
    f = WorkspaceFilter(mode="nerve_workspace", nerve_workspace_path=tmp_path)
    assert not f.matches(None)


def test_unknown_mode_fails_closed(tmp_path: Path):
    f = WorkspaceFilter(mode="bogus", nerve_workspace_path=tmp_path)  # type: ignore[arg-type]
    assert not f.matches(str(tmp_path))


def test_explicit_handles_relative_paths(tmp_path: Path):
    a = tmp_path / "a"; a.mkdir()
    f = WorkspaceFilter(mode="explicit", explicit_paths=[a])
    # Pass an absolute, but with a "." segment — Path.resolve() normalises.
    assert f.matches(str(a / "." / "."))


def test_explicit_path_with_user_expansion(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    target = tmp_path / "in-home"
    target.mkdir()
    f = WorkspaceFilter(mode="explicit", explicit_paths=[Path("~/in-home")])
    assert f.matches(str(target))
