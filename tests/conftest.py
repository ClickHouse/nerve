"""Shared test fixtures for Nerve tests."""

import asyncio
import sys
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

import nerve.config  # noqa: F401  — imported so its constants can be re-pointed
from nerve import paths
from nerve.db import Database

# Machine-local paths that are already materialized by the time a fixture runs,
# keyed by attribute name -> location under the state dir.
#
# ``nerve.paths`` re-reads NERVE_HOME on every call, but a module-level
# ``X = paths.nerve_path(...)`` is evaluated when its module is imported, and
# pytest imports every test module (and everything it pulls in) before the first
# fixture executes. The env override below therefore lands too late for such a
# constant — and by then ``from nerve.config import X`` has copied the stale
# Path into each importer's namespace, so patching the definition alone misses
# them.
#
# Left alone they name the developer's live install: `nerve restart --resume`
# appends to the real resume queue and the daemon-side drainer *unlinks* it. One
# test that forgets one patch is enough to destroy state on the machine running
# the suite, which is why isolation happens here instead of per test.
_IMPORT_TIME_STATE_PATHS = {"RESUME_QUEUE_FILE": ("resume-after-restart",)}


def _repoint_import_time_state_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rewrite every in-memory copy of the constants above to the temp state dir.

    Sweeps the already-imported ``nerve`` modules rather than listing the known
    importers, so a new consumer is covered the day it is added instead of the
    day someone notices. A module imported later, inside a test body, picks up
    the patched definition in ``nerve.config`` — which is why this file imports
    it eagerly.
    """
    for attr, parts in _IMPORT_TIME_STATE_PATHS.items():
        target = paths.nerve_path(*parts)
        for name, module in list(sys.modules.items()):
            if module is None or not (name == "nerve" or name.startswith("nerve.")):
                continue
            # Type check keeps an unrelated same-named attribute from being
            # replaced with a Path behind its owner's back.
            if isinstance(getattr(module, attr, None), Path):
                monkeypatch.setattr(module, attr, target)


@pytest.fixture(scope="session")
def event_loop():
    """Use a single event loop for all tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def _isolate_nerve_state_files(tmp_path, monkeypatch):
    """Keep tests away from the real ~/.nerve state files.

    The config-dir pointer, wizard init-state, Telegram pairing file, DB,
    caches, etc. all live under ~/.nerve on a real install. Tests must never
    read or mutate them (running the suite on a live box would otherwise
    repoint the daemon's config discovery or leak pairing codes).

    Every machine-local path now funnels through ``nerve.paths.nerve_home()``,
    which honors the ``NERVE_HOME`` env var, so a single override isolates the
    whole state directory instead of patching each constant individually — with
    the exception of the paths already frozen at import time, which the second
    step rewrites by hand.
    """
    state_dir = tmp_path / "_nerve_state"
    monkeypatch.setenv("NERVE_HOME", str(state_dir))
    _repoint_import_time_state_paths(monkeypatch)


@pytest.fixture
def clean_registry():
    """Snapshot ``GATE_REGISTRY`` and restore it after the test.

    The gate-plugin loader mutates the process-global registry; without this,
    gates registered by one test would leak into the others (and into
    test_cron_gates.py, which asserts on the exact built-in set).
    """
    from nerve.cron.gates import GATE_REGISTRY

    saved = dict(GATE_REGISTRY)
    try:
        yield
    finally:
        GATE_REGISTRY.clear()
        GATE_REGISTRY.update(saved)


@pytest_asyncio.fixture
async def db(tmp_path):
    """Create a fresh in-memory-like database for each test."""
    db_path = tmp_path / "test.db"
    database = Database(db_path)
    await database.connect()
    yield database
    await database.close()
