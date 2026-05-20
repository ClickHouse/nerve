"""Tests for :class:`nerve.external_agents.writer.ConfigWriter`.

Covers the three things that must be airtight before we let either
the bootstrap wizard or the sync cron loose on user files:

- **Allowlist** — writes outside ``~/.codex``/``~/.claude``/``~/.cursor``
  must raise :class:`SecurityError`.
- **Conflict policy** — ``backup`` saves a copy, ``skip`` leaves files
  alone, ``merge`` deep-merges JSON.
- **Idempotency** — hash sidecar makes repeat writes a no-op.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nerve.external_agents.writer import ConfigWriter, SecurityError


@pytest.fixture
def writer(tmp_path: Path) -> ConfigWriter:
    """Writer pointed at a temp dir so tests don't touch real files."""
    return ConfigWriter(
        conflict_policy="backup",
        allowlist=[tmp_path.resolve()],
    )


def test_allowlist_rejects_path_outside(tmp_path: Path) -> None:
    w = ConfigWriter(conflict_policy="backup", allowlist=[tmp_path.resolve()])
    # /tmp/<random>/outside.txt — outside the allowlist's tmp_path
    bad = tmp_path.parent / "outside.txt"
    with pytest.raises(SecurityError):
        w.write(bad, "nope")


def test_allowlist_accepts_path_inside(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "ok.toml"
    writer.write(out, "hello\n")
    assert out.read_text() == "hello\n"


def test_write_creates_parent_dirs(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "nested" / "deep" / "file.md"
    writer.write(out, "content")
    assert out.exists()
    assert out.read_text() == "content"


def test_backup_policy_makes_copy(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "config.toml"
    out.write_text("original")

    backup = writer.write(out, "new content")

    assert backup is not None
    assert backup.exists()
    assert backup.read_text() == "original"
    assert out.read_text() == "new content"


def test_skip_policy_leaves_file_alone(tmp_path: Path) -> None:
    w = ConfigWriter(conflict_policy="skip", allowlist=[tmp_path.resolve()])
    out = tmp_path / "config.toml"
    out.write_text("original")

    backup = w.write(out, "new content")

    assert backup is None
    assert out.read_text() == "original"


def test_write_to_nonexistent_no_backup(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "new.toml"
    backup = writer.write(out, "hello")
    assert backup is None
    assert out.read_text() == "hello"


def test_is_up_to_date_after_write(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "memory.md"
    writer.write(out, "v1 content")
    assert writer.is_up_to_date(out, "v1 content")
    assert not writer.is_up_to_date(out, "v2 different")


def test_is_up_to_date_returns_false_when_sidecar_missing(
    writer: ConfigWriter, tmp_path: Path
) -> None:
    out = tmp_path / "external.md"
    out.write_text("written by user, no sidecar")
    assert not writer.is_up_to_date(out, "written by user, no sidecar")


def test_merge_json_creates_when_missing(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "settings.json"
    writer.merge_json(out, {"mcpServers": {"nerve": {"url": "x"}}})
    data = json.loads(out.read_text())
    assert data == {"mcpServers": {"nerve": {"url": "x"}}}


def test_merge_json_backup_overlays_partial(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "settings.json"
    out.write_text(json.dumps({"model": "opus", "mcpServers": {"other": {"url": "y"}}}))

    writer.merge_json(out, {"mcpServers": {"nerve": {"url": "x"}}})

    data = json.loads(out.read_text())
    # User key preserved
    assert data["model"] == "opus"
    # Both MCP servers present after merge (partial wins on key conflict in
    # backup mode, but no conflict here)
    assert data["mcpServers"]["other"]["url"] == "y"
    assert data["mcpServers"]["nerve"]["url"] == "x"


def test_merge_policy_user_keys_win(tmp_path: Path) -> None:
    """In ``merge`` policy, existing user keys take precedence."""
    w = ConfigWriter(conflict_policy="merge", allowlist=[tmp_path.resolve()])
    out = tmp_path / "settings.json"
    out.write_text(json.dumps({"mcpServers": {"nerve": {"url": "user-set"}}}))

    w.merge_json(out, {"mcpServers": {"nerve": {"url": "nerve-default"}}})

    data = json.loads(out.read_text())
    # User's existing url wins
    assert data["mcpServers"]["nerve"]["url"] == "user-set"


def test_atomic_write_no_tmp_left_behind(writer: ConfigWriter, tmp_path: Path) -> None:
    out = tmp_path / "atomic.md"
    writer.write(out, "content")
    tmp = out.with_suffix(out.suffix + ".nerve-tmp")
    assert not tmp.exists(), "atomic temp file should be renamed away"


def test_invalid_policy_raises() -> None:
    with pytest.raises(ValueError):
        ConfigWriter(conflict_policy="invalid")
