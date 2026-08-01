"""The daemon's opt-in background loops must follow a config reload.

A config reload replaces the process-wide config object. Every one of these
loops used to close over the object built at start-up, so an operator could
change an interval or switch a feature on, watch the reload report success, and
get nothing — with no way to tell from the outside. Each test drives the real
loop with a fake clock and swaps the real config object underneath it.

The reverse direction matters too and is asserted here: a loop that was never
created because its feature was off cannot notice the flag later, so switching
one *on* is restart-only. That is what ``docs/config.md`` promises.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import nerve.config as cfgmod
from nerve.config import BackupConfig, NerveConfig, RetentionConfig
from nerve.gateway import server as srv


class _StopLoop(Exception):
    """Breaks out of a ``while True`` loop after the planned number of ticks."""


def _fake_clock(monkeypatch, ticks: int, between=None):
    """Replace ``asyncio.sleep`` with a counter that ends the loop after *ticks*.

    ``between(n)`` runs *during* the wait before the n'th pass, which is where a
    test stands in for the operator editing config and asking for a reload — so
    a change made at ``n`` is first visible to the n'th pass, not the one before
    it. Returns the list the requested delays are recorded into.
    """
    delays: list[float] = []

    async def fake_sleep(seconds, *a, **k):
        delays.append(seconds)
        if between is not None:
            between(len(delays))
        if len(delays) > ticks:
            raise _StopLoop
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return delays


def _config(**kw) -> NerveConfig:
    cfg = NerveConfig()
    for key, value in kw.items():
        setattr(cfg, key, value)
    return cfg


class TestDbRetentionLoop:
    @pytest.mark.asyncio
    async def test_interval_and_windows_follow_a_reload(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cfgmod, "_config", _config(
            retention=RetentionConfig(
                enabled=True, interval_hours=6, retention_days=90,
                retention_full_days=30,
            ),
        ))
        db = SimpleNamespace(run_retention=AsyncMock(return_value={}))

        def reload_during_the_first_wait(tick):
            if tick == 1:
                cfgmod.set_config(_config(
                    retention=RetentionConfig(
                        enabled=True, interval_hours=1, retention_days=7,
                        retention_full_days=3,
                    ),
                ))

        delays = _fake_clock(monkeypatch, 2, between=reload_during_the_first_wait)
        with pytest.raises(_StopLoop):
            await srv._periodic_db_retention(db)

        assert delays[:2] == [6 * 3600, 1 * 3600]
        assert db.run_retention.await_args_list[-1].kwargs == {
            "retention_days": 7, "retention_full_days": 3,
        }

    @pytest.mark.asyncio
    async def test_switching_it_off_stops_the_work_but_not_the_loop(
        self, monkeypatch,
    ):
        monkeypatch.setattr(cfgmod, "_config", _config(
            retention=RetentionConfig(enabled=True, interval_hours=6),
        ))
        db = SimpleNamespace(run_retention=AsyncMock(return_value={}))

        def disable_before_the_second_pass(tick):
            if tick == 2:
                cfgmod.set_config(_config(
                    retention=RetentionConfig(enabled=False, interval_hours=6),
                ))

        _fake_clock(monkeypatch, 3, between=disable_before_the_second_pass)
        with pytest.raises(_StopLoop):
            await srv._periodic_db_retention(db)

        # One pass before the flag went false, none after — and the loop kept
        # ticking, so turning it back on would resume.
        assert db.run_retention.await_count == 1

    @pytest.mark.asyncio
    async def test_switching_it_on_needs_a_restart(self, monkeypatch):
        """No task, nothing to notice the flag. Stated as such in the docs."""
        monkeypatch.setattr(cfgmod, "_config", _config(
            retention=RetentionConfig(enabled=False),
        ))
        delays = _fake_clock(monkeypatch, 3)
        db = SimpleNamespace(run_retention=AsyncMock(return_value={}))
        await srv._periodic_db_retention(db)  # returns immediately
        assert delays == []


class TestBackupLoop:
    @pytest.mark.asyncio
    async def test_enabling_backups_takes_effect_without_a_restart(
        self, tmp_path, monkeypatch,
    ):
        """The hourly tick runs whether or not backups are on, so unlike the
        retention loop this one can pick up an off→on flip."""
        from nerve import backup as backup_mod

        target = tmp_path / "bundles"
        target.mkdir()
        monkeypatch.setattr(cfgmod, "_config", _config(
            workspace=tmp_path / "ws", backup=BackupConfig(enabled=False),
        ))
        monkeypatch.setattr(
            backup_mod, "latest_bundle_age_seconds", lambda _t: None,
        )
        made: list = []
        monkeypatch.setattr(
            backup_mod, "create_backup",
            lambda *a, **k: made.append(a) or SimpleNamespace(
                path=tmp_path / "b.tar.zst", size=1024, file_count=3,
            ),
        )
        monkeypatch.setattr(backup_mod, "prune", lambda *a, **k: [])
        monkeypatch.setattr(backup_mod, "list_bundles", lambda _t: [])

        def enable_before_the_second_pass(tick):
            if tick == 2:
                cfgmod.set_config(_config(
                    workspace=tmp_path / "ws",
                    backup=BackupConfig(enabled=True, target_dir=str(target)),
                ))

        delays = _fake_clock(monkeypatch, 2, between=enable_before_the_second_pass)
        with pytest.raises(_StopLoop):
            await srv._periodic_backup(AsyncMock())

        assert delays[:2] == [3600, 3600]  # cadence is a fixed hourly tick
        assert len(made) == 1  # nothing on the first tick, a bundle on the second
