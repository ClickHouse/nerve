"""Tests for the drop-in cron gate plugin loader (nerve/cron/gate_plugins.py)."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from nerve.cron.gate_plugins import load_gate_plugins
from nerve.cron.gates import (
    GATE_REGISTRY,
    CronGate,
    GateContext,
    build_gate,
    evaluate_gates,
)
from nerve.cron.jobs import CronJob


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_registry():
    """Snapshot GATE_REGISTRY and restore it after the test.

    The loader mutates the process-global registry; without this, gates
    registered by one test would leak into the others (and into
    test_cron_gates.py, which asserts on the exact built-in set).
    """
    saved = dict(GATE_REGISTRY)
    try:
        yield
    finally:
        GATE_REGISTRY.clear()
        GATE_REGISTRY.update(saved)


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

    def test_abstract_gate_does_not_crash_job_build(self, tmp_path, clean_registry):
        # The crash vector itself: a job referencing the abstract gate's type
        # must build without raising (the unknown type is dropped, fail-open).
        _write(tmp_path, "half.py", _ABSTRACT_PLUGIN)
        load_gate_plugins(tmp_path)
        job = CronJob(
            id="j", schedule="1h", prompt="p",
            run_if=[{"type": "half_test"}],
        )
        assert job.gates == []

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

    def test_unknown_plugin_type_drops_gate_when_not_loaded(self, clean_registry):
        # Without loading the plugin, an unknown type is dropped by build_gates
        # (fail-open: the job ends up ungated) rather than raising.
        job = CronJob(
            id="j", schedule="1h", prompt="p",
            run_if=[{"type": "never_loaded_gate"}],
        )
        assert job.gates == []
