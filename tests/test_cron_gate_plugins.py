"""Tests for the drop-in cron gate plugin loader (nerve/cron/gate_plugins.py)."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from nerve.config import ConfigError
from nerve.cron.gate_plugins import load_gate_plugins
from nerve.cron.gates import (
    GATE_REGISTRY,
    CronGate,
    GateConfigError,
    GateContext,
    build_gate,
    evaluate_gates,
)
from nerve.cron.jobs import CronJob


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

# A valid plugin: a gate that is always satisfied, registered as "always_test".
_VALID_PLUGIN = '''
from nerve.cron.gates import CronGate


class AlwaysGate(CronGate):
    type = "always_test"

    async def is_satisfied(self, ctx):
        return True

    def describe(self):
        return "always (test plugin)"

    @classmethod
    def from_config(cls, spec):
        return cls()
'''


# A plugin that imports cleanly but defines an *abstract* gate (it forgets
# is_satisfied/from_config). It must NOT be registered: instantiating it would
# raise TypeError, which build_gates does not catch — crashing job construction.
_ABSTRACT_PLUGIN = '''
from nerve.cron.gates import CronGate


class HalfGate(CronGate):
    type = "half_test"

    def describe(self):
        return "half"
    # is_satisfied and from_config intentionally left unimplemented → abstract.
'''

# A plugin that calls sys.exit() at import time. SystemExit is a BaseException
# (not Exception), so the loader must catch it explicitly or it would escape
# and crash daemon startup.
_SYS_EXIT_PLUGIN = "import sys\nsys.exit(1)\n"


def _write(dirpath: Path, name: str, body: str) -> Path:
    p = dirpath / name
    p.write_text(body, encoding="utf-8")
    return p


def _ctx() -> GateContext:
    return GateContext(job_id="j", db=AsyncMock())


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_registers_and_builds(self, tmp_path, clean_registry):
        _write(tmp_path, "always.py", _VALID_PLUGIN)
        assert load_gate_plugins(tmp_path) == 1
        assert "always_test" in GATE_REGISTRY
        gate = build_gate({"type": "always_test"})
        assert isinstance(gate, CronGate)
        assert gate.type == "always_test"

    @pytest.mark.asyncio
    async def test_loaded_gate_evaluates(self, tmp_path, clean_registry):
        _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        gate = build_gate({"type": "always_test"})
        decision = await evaluate_gates([gate], _ctx())
        assert decision.should_run is True

    def test_multiple_gates_in_one_file(self, tmp_path, clean_registry):
        body = _VALID_PLUGIN + '''

class AlwaysGate2(CronGate):
    type = "always_test_2"

    async def is_satisfied(self, ctx):
        return True

    def describe(self):
        return "always 2"

    @classmethod
    def from_config(cls, spec):
        return cls()
'''
        _write(tmp_path, "multi.py", body)
        assert load_gate_plugins(tmp_path) == 2
        assert {"always_test", "always_test_2"} <= set(GATE_REGISTRY)


# ---------------------------------------------------------------------------
# Fail-safe isolation
# ---------------------------------------------------------------------------

class TestFailSafe:
    def test_broken_plugin_does_not_block_valid_one(
        self, tmp_path, clean_registry, caplog,
    ):
        _write(tmp_path, "broken.py", "this is not valid python !!!\n")
        _write(tmp_path, "good.py", _VALID_PLUGIN)
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)
        assert n == 1                          # only the good one
        assert "always_test" in GATE_REGISTRY
        assert "broken.py" in caplog.text      # the failure named the file

    def test_import_error_at_module_level_isolated(
        self, tmp_path, clean_registry, caplog,
    ):
        _write(tmp_path, "raises.py", "raise RuntimeError('boom at import')\n")
        _write(tmp_path, "good.py", _VALID_PLUGIN)
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)
        assert n == 1
        assert "raises.py" in caplog.text

    def test_file_without_crongate_is_skipped(self, tmp_path, clean_registry):
        _write(tmp_path, "nogate.py", "x = 1\ndef helper():\n    return 2\n")
        assert load_gate_plugins(tmp_path) == 0

    def test_abstract_gate_not_registered(self, tmp_path, clean_registry, caplog):
        # A typed-but-abstract gate imports fine but must not be registered —
        # registering it would defer a TypeError crash to job-build time.
        _write(tmp_path, "half.py", _ABSTRACT_PLUGIN)
        _write(tmp_path, "good.py", _VALID_PLUGIN)
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)
        assert n == 1                          # only the concrete gate
        assert "half_test" not in GATE_REGISTRY
        assert "always_test" in GATE_REGISTRY
        assert "half.py" in caplog.text

    def test_abstract_gate_is_refused_at_job_build(self, tmp_path, clean_registry):
        # The loader skips the abstract class, so nothing registers its type, and
        # a job naming it is refused. Containment means the daemon survives a bad
        # plugin — not that a job whose precondition is missing runs anyway.
        _write(tmp_path, "half.py", _ABSTRACT_PLUGIN)
        load_gate_plugins(tmp_path)
        with pytest.raises(GateConfigError, match="half_test"):
            CronJob(
                id="j", schedule="1h", prompt="p",
                run_if=[{"type": "half_test"}],
            )

    def test_sys_exit_at_import_is_contained(
        self, tmp_path, clean_registry, caplog,
    ):
        # sys.exit() raises SystemExit (a BaseException). The loader must catch
        # it — this call must NOT raise — and the valid plugin must still load.
        _write(tmp_path, "exiter.py", _SYS_EXIT_PLUGIN)
        _write(tmp_path, "good.py", _VALID_PLUGIN)
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)    # must not raise SystemExit
        assert n == 1
        assert "always_test" in GATE_REGISTRY
        assert "exiter.py" in caplog.text

    def test_empty_type_is_skipped(self, tmp_path, clean_registry, caplog):
        body = _VALID_PLUGIN.replace('type = "always_test"', 'type = ""')
        _write(tmp_path, "notype.py", body)
        with caplog.at_level(logging.WARNING):
            assert load_gate_plugins(tmp_path) == 0
        assert "always_test" not in GATE_REGISTRY
        assert "notype.py" in caplog.text

    def test_underscore_prefixed_file_ignored(self, tmp_path, clean_registry):
        _write(tmp_path, "_helper.py", _VALID_PLUGIN)
        assert load_gate_plugins(tmp_path) == 0
        assert "always_test" not in GATE_REGISTRY

    def test_non_py_files_ignored(self, tmp_path, clean_registry):
        _write(tmp_path, "always.txt", _VALID_PLUGIN)
        _write(tmp_path, "readme.md", "# not a plugin\n")
        assert load_gate_plugins(tmp_path) == 0


# ---------------------------------------------------------------------------
# Collisions
# ---------------------------------------------------------------------------

class TestCollisions:
    def test_builtin_collision_keeps_builtin(
        self, tmp_path, clean_registry, caplog,
    ):
        # A plugin claiming the built-in "tasks" type must not override it.
        body = (
            _VALID_PLUGIN
            .replace('"always_test"', '"tasks"')
            .replace("AlwaysGate", "FakeTasksGate")
        )
        _write(tmp_path, "collide.py", body)
        before = GATE_REGISTRY["tasks"]
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)
        assert n == 0
        assert GATE_REGISTRY["tasks"] is before     # built-in retained
        assert "tasks" in caplog.text

    def test_two_plugins_same_type_first_wins(
        self, tmp_path, clean_registry, caplog,
    ):
        first = _VALID_PLUGIN.replace("AlwaysGate", "FirstGate")
        second = (
            _VALID_PLUGIN
            .replace("AlwaysGate", "SecondGate")
            .replace('"always (test plugin)"', '"second"')
        )
        # Filenames sort so a_*.py loads before b_*.py → FirstGate wins.
        _write(tmp_path, "a_first.py", first)
        _write(tmp_path, "b_second.py", second)
        with caplog.at_level(logging.WARNING):
            n = load_gate_plugins(tmp_path)
        assert n == 1
        assert GATE_REGISTRY["always_test"].__name__ == "FirstGate"


# ---------------------------------------------------------------------------
# Empty / missing directory
# ---------------------------------------------------------------------------

class TestNoOpDirs:
    def test_missing_dir_returns_zero(self, tmp_path, clean_registry):
        assert load_gate_plugins(tmp_path / "does_not_exist") == 0

    def test_empty_dir_returns_zero(self, tmp_path, clean_registry):
        assert load_gate_plugins(tmp_path) == 0

    def test_file_path_instead_of_dir_returns_zero(self, tmp_path, clean_registry):
        f = _write(tmp_path, "always.py", _VALID_PLUGIN)
        # Pointing at a file (not a dir) is treated as "no dir" — no crash.
        assert load_gate_plugins(f) == 0

    def test_tilde_path_expanded(self, tmp_path, clean_registry, monkeypatch):
        # A "~/..." path is expanded; a non-existent one is a no-op (no raise).
        monkeypatch.setenv("HOME", str(tmp_path))
        assert load_gate_plugins(Path("~/nope/gates")) == 0


# ---------------------------------------------------------------------------
# Hot-reload (replace=True)
# ---------------------------------------------------------------------------

# Same type as _VALID_PLUGIN ("always_test") but the OPPOSITE behaviour: never
# satisfied. Simulates an operator editing a plugin's code in place.
_EDITED_PLUGIN = '''
from nerve.cron.gates import CronGate


class AlwaysGate(CronGate):
    type = "always_test"

    async def is_satisfied(self, ctx):
        return False

    def describe(self):
        return "never (edited test plugin)"

    @classmethod
    def from_config(cls, spec):
        return cls()
'''


class TestHotReloadReplace:
    @pytest.mark.asyncio
    async def test_edited_plugin_code_takes_effect_on_replace(
        self, tmp_path, clean_registry,
    ):
        # Load v1 (always satisfied), then overwrite the file with v2 (never)
        # and reload with replace=True — the new behaviour must win.
        f = _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        gate = build_gate({"type": "always_test"})
        assert (await evaluate_gates([gate], _ctx())).should_run is True

        f.write_text(_EDITED_PLUGIN, encoding="utf-8")
        assert load_gate_plugins(tmp_path, replace=True) == 1
        gate = build_gate({"type": "always_test"})
        assert (await evaluate_gates([gate], _ctx())).should_run is False

    def test_without_replace_edit_is_ignored(self, tmp_path, clean_registry):
        # The old (asymmetric) behaviour: without replace, a re-run keeps the
        # incumbent class, so an edit does NOT take effect.
        f = _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        original = GATE_REGISTRY["always_test"]
        f.write_text(_EDITED_PLUGIN, encoding="utf-8")
        assert load_gate_plugins(tmp_path) == 0        # collision → kept
        assert GATE_REGISTRY["always_test"] is original

    def test_deleted_plugin_unregisters_on_replace(
        self, tmp_path, clean_registry, caplog,
    ):
        f = _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        assert "always_test" in GATE_REGISTRY
        f.unlink()
        with caplog.at_level(logging.WARNING):
            assert load_gate_plugins(tmp_path, replace=True) == 0
        assert "always_test" not in GATE_REGISTRY   # gate is gone
        assert "always_test" in caplog.text          # and it was surfaced

    def test_replace_when_dir_deleted_still_unregisters(
        self, tmp_path, clean_registry,
    ):
        # The whole gates dir vanishing (not just a file) must still unregister.
        import shutil

        gates_dir = tmp_path / "gates"
        gates_dir.mkdir()
        _write(gates_dir, "always.py", _VALID_PLUGIN)
        load_gate_plugins(gates_dir)
        assert "always_test" in GATE_REGISTRY
        shutil.rmtree(gates_dir)
        assert load_gate_plugins(gates_dir, replace=True) == 0
        assert "always_test" not in GATE_REGISTRY

    def test_replace_preserves_builtins(self, tmp_path, clean_registry):
        _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        builtins_before = {
            t: c for t, c in GATE_REGISTRY.items()
            if not c.__module__.startswith("nerve_cron_gate_plugin_")
        }
        # A reload against an empty dir drops the plugin but keeps every built-in.
        empty = tmp_path / "empty"
        empty.mkdir()
        load_gate_plugins(empty, replace=True)
        assert "always_test" not in GATE_REGISTRY
        for t, c in builtins_before.items():
            assert GATE_REGISTRY[t] is c

    def test_replace_swaps_type_across_files(self, tmp_path, clean_registry):
        # Rename the gate's type by editing the file: old type gone, new present.
        f = _write(tmp_path, "g.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        assert "always_test" in GATE_REGISTRY
        f.write_text(
            _VALID_PLUGIN.replace('"always_test"', '"renamed_test"'),
            encoding="utf-8",
        )
        load_gate_plugins(tmp_path, replace=True)
        assert "always_test" not in GATE_REGISTRY
        assert "renamed_test" in GATE_REGISTRY

    @pytest.mark.asyncio
    async def test_edit_takes_effect_through_cron_reload(
        self, tmp_path, clean_registry,
    ):
        """The real path: CronService.reload() re-reads edited plugin code."""
        from unittest.mock import AsyncMock, MagicMock

        from nerve.cron.service import CronService

        gates_dir = tmp_path / "gates"
        gates_dir.mkdir()
        f = _write(gates_dir, "always.py", _VALID_PLUGIN)

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.gate_plugins_dir = gates_dir
        config.cron.system_file = tmp_path / "system.yaml"
        config.cron.jobs_file = tmp_path / "jobs.yaml"

        svc = CronService(config, AsyncMock(), AsyncMock())
        svc.scheduler.start(paused=True)
        svc._load_merged_jobs = MagicMock(return_value=[])

        try:
            await svc.reload()
            assert (
                await evaluate_gates([build_gate({"type": "always_test"})], _ctx())
            ).should_run is True
            f.write_text(_EDITED_PLUGIN, encoding="utf-8")
            await svc.reload()
            assert (
                await evaluate_gates([build_gate({"type": "always_test"})], _ctx())
            ).should_run is False
        finally:
            svc.scheduler.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_edited_gate_reaches_the_scheduled_job(
        self, tmp_path, clean_registry,
    ):
        """Registry-only isn't enough: the job APScheduler holds must be rebuilt.

        Editing a .py gate changes nothing in jobs.yaml, and the scheduler fires
        the CronJob object it was handed, whose gates were built when that object
        was constructed. Re-registering the class without replacing the object
        would leave the job gating on the pre-edit class forever, while the API
        reported the new one.
        """
        import yaml
        from unittest.mock import AsyncMock, MagicMock

        from nerve.cron.service import CronService

        gates_dir = tmp_path / "gates"
        gates_dir.mkdir()
        f = _write(gates_dir, "always.py", _VALID_PLUGIN)

        jobs_file = tmp_path / "jobs.yaml"
        jobs_file.write_text(yaml.safe_dump({"jobs": [{
            "id": "j1", "schedule": "1h", "prompt": "x",
            "run_if": [{"type": "always_test"}],
        }]}), encoding="utf-8")

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.gate_plugins_dir = gates_dir
        config.cron.system_file = tmp_path / "system.yaml"
        config.cron.jobs_file = jobs_file

        db = AsyncMock()
        db.get_last_successful_cron_run = AsyncMock(return_value=None)
        svc = CronService(config, AsyncMock(), db)
        svc.scheduler.start(paused=True)

        def scheduled_gates():
            return svc.scheduler.get_job("j1").args[0].gates

        try:
            await svc.reload()
            assert (await evaluate_gates(scheduled_gates(), _ctx())).should_run is True

            f.write_text(_EDITED_PLUGIN, encoding="utf-8")
            await svc.reload()

            # The object the scheduler will actually run now holds the new gate.
            assert [g.describe() for g in scheduled_gates()] == [
                "never (edited test plugin)"
            ]
            assert (await evaluate_gates(scheduled_gates(), _ctx())).should_run is False
            # ... and it agrees with what the API reports from _jobs.
            assert [g.describe() for j in svc._jobs for g in j.gates] == [
                "never (edited test plugin)"
            ]
        finally:
            svc.scheduler.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_deleted_gate_stops_gating_the_scheduled_job(
        self, tmp_path, clean_registry,
    ):
        """Deleting a plugin refuses the reload instead of ungating the job.

        This is the case the whole vanished-gate apparatus exists for. A synced
        pull that removes a gate file must not turn "only when the plugin says
        so" into "every time" on a live daemon, so the job stops building and the
        reload is refused with the running schedule intact.
        """
        import yaml
        from unittest.mock import AsyncMock, MagicMock

        from nerve.cron.service import CronService

        gates_dir = tmp_path / "gates"
        gates_dir.mkdir()
        f = _write(gates_dir, "always.py", _EDITED_PLUGIN)  # gate says "no"

        jobs_file = tmp_path / "jobs.yaml"
        jobs_file.write_text(yaml.safe_dump({"jobs": [{
            "id": "j1", "schedule": "1h", "prompt": "x",
            "run_if": [{"type": "always_test"}],
        }]}), encoding="utf-8")

        config = MagicMock()
        config.timezone = "UTC"
        config.cron.gate_plugins_dir = gates_dir
        config.cron.system_file = tmp_path / "system.yaml"
        config.cron.jobs_file = jobs_file

        db = AsyncMock()
        db.get_last_successful_cron_run = AsyncMock(return_value=None)
        svc = CronService(config, AsyncMock(), db)
        svc.scheduler.start(paused=True)

        try:
            await svc.reload()
            gates = svc.scheduler.get_job("j1").args[0].gates
            assert (await evaluate_gates(gates, _ctx())).should_run is False

            f.unlink()
            with pytest.raises(ConfigError, match="always_test"):
                await svc.reload()

            # The running schedule is untouched, and the job it is still holding
            # keeps the gate — so the job goes on being gated, not on firing.
            gates = svc.scheduler.get_job("j1").args[0].gates
            assert (await evaluate_gates(gates, _ctx())).should_run is False
            # The registry was rolled back too, so the next load off disk works
            # the moment the plugin is restored.
            _write(gates_dir, "always.py", _EDITED_PLUGIN)
            await svc.reload()
            assert (await evaluate_gates(
                svc.scheduler.get_job("j1").args[0].gates, _ctx(),
            )).should_run is False
        finally:
            svc.scheduler.shutdown(wait=False)

    def test_warn_vanished_can_be_deferred(self, tmp_path, clean_registry, caplog):
        """The unregister still happens; only the announcement is held back.

        A caller that may yet abandon the load asks for silence here and calls
        warn_vanished_gates() itself once it has committed.
        """
        from nerve.cron.gate_plugins import warn_vanished_gates

        f = _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        before = dict(GATE_REGISTRY)

        f.unlink()
        with caplog.at_level(logging.WARNING):
            load_gate_plugins(tmp_path, replace=True, warn_vanished=False)
        assert "always_test" not in GATE_REGISTRY   # dropped all the same
        assert "always_test" not in caplog.text     # but not announced

        with caplog.at_level(logging.WARNING):
            warn_vanished_gates(before)
        assert "always_test" in caplog.text

    def test_deferred_warning_ignores_builtins_in_the_snapshot(
        self, tmp_path, clean_registry, caplog,
    ):
        """The snapshot holds built-ins too, and they are never dropped.

        warn_vanished_gates() takes the whole registry rather than the set the
        loader removed, so it has to stay quiet about the built-ins in it.
        """
        from nerve.cron.gate_plugins import warn_vanished_gates

        _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        before = dict(GATE_REGISTRY)
        assert "tasks" in before, "expected a built-in in the snapshot"

        load_gate_plugins(tmp_path, replace=True, warn_vanished=False)
        with caplog.at_level(logging.WARNING):
            warn_vanished_gates(before)
        assert caplog.text == ""


# ---------------------------------------------------------------------------
# End-to-end via CronJob.run_if
# ---------------------------------------------------------------------------

class TestEndToEndViaConfig:
    def test_cronjob_run_if_builds_plugin_gate(self, tmp_path, clean_registry):
        _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        job = CronJob(
            id="j", schedule="1h", prompt="p",
            run_if=[{"type": "always_test"}],
        )
        assert len(job.gates) == 1
        assert job.gates[0].type == "always_test"

    @pytest.mark.asyncio
    async def test_cronjob_plugin_gate_evaluates(self, tmp_path, clean_registry):
        _write(tmp_path, "always.py", _VALID_PLUGIN)
        load_gate_plugins(tmp_path)
        job = CronJob(
            id="j", schedule="1h", prompt="p",
            run_if=[{"type": "always_test"}],
        )
        decision = await evaluate_gates(job.gates, _ctx())
        assert decision.should_run is True

    def test_unknown_plugin_type_refuses_the_job(self, clean_registry):
        # Nothing registered the type — the plugin was never loaded, or never
        # existed. Either way the job is refused, and the message names the job
        # and the type so the YAML entry to fix is identifiable.
        with pytest.raises(GateConfigError) as exc:
            CronJob(
                id="j", schedule="1h", prompt="p",
                run_if=[{"type": "never_loaded_gate"}],
            )
        assert "'j'" in str(exc.value)
        assert "never_loaded_gate" in str(exc.value)
