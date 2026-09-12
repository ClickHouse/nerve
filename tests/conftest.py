"""Shared test fixtures for Nerve tests."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

import nerve.config  # noqa: F401  — imported so its constants can be re-pointed
from nerve import paths
from nerve.db import Database
from nerve.identity import Actor

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


@pytest.fixture(autouse=True)
def _unpin_jwt_secret():
    """Forget the signing secret pinned to this process.

    Startup pins the effective secret once for the life of the daemon; in the
    suite each test is its own "process", so a test that bootstraps an
    identity (or pins a secret directly) must not leave it pinned for the
    tests after it.
    """
    from nerve.gateway.auth import unpin_jwt_secret

    unpin_jwt_secret()
    yield
    unpin_jwt_secret()


@pytest.fixture(scope="session", autouse=True)
def _deterministic_umask():
    """Run the suite under umask 022, whatever the developer's shell has.

    ``Database.connect`` refuses — unrepaired — to open a state directory that
    other users can write to. A umask of 002 (Ubuntu's default with
    user-private groups) makes every plain ``mkdir()`` in a test fixture a
    0775, group-writable directory: a hazard the policy is right to refuse,
    but not what those fixtures are about. Production never depends on the
    umask (Nerve creates its state directory 0700 explicitly, see
    ``paths.ensure_nerve_home``); the tests that *are* about the umask set
    their own inside the test.
    """
    old = os.umask(0o022)
    yield
    os.umask(old)


# The actor a test that is not about authentication runs as. Obviously
# synthetic ids, UUID-shaped like the real ones, and NOT rows in ``actor_refs``
# — nothing in this version stores an actor, so nothing checks. See the PR 2
# handoff: the day an actor id becomes a foreign key, tests using this have to
# create the row.
TEST_ACTOR = Actor(
    actor_id="00000000-0000-4000-8000-00000000ac70",
    kind="human",
    account_id="00000000-0000-4000-8000-00000000acc7",
    display_name="Test Account",
)


@pytest.fixture
def request_actor() -> Actor:
    """The actor to pass a route function called directly in a test.

    ``require_auth`` returns an :class:`~nerve.identity.Actor`, so a test that
    calls a route coroutine itself (rather than through the app) supplies one.
    The same value the ``bypass_auth`` override injects.
    """
    return TEST_ACTOR


@pytest.fixture
def bypass_auth():
    """Install a stand-in for ``require_auth`` on a test app.

    There is no unauthenticated mode: with no signing secret in force every
    auth check fails closed. Route tests that are not about authentication
    therefore override the dependency on the app they build instead of
    relying on an empty ``auth.jwt_secret``. Yields a function taking the app
    (returns it, for chaining). Auth tests must not use it — they exercise the
    real dependency with real tokens.

    The override returns a real :class:`~nerve.identity.Actor`, not a stub
    dict: a route that starts reading the actor must see the same type in tests
    that it sees in production.
    """
    from nerve.gateway.auth import require_auth

    apps = []

    def _bypass(app):
        app.dependency_overrides[require_auth] = lambda: TEST_ACTOR
        apps.append(app)
        return app

    yield _bypass
    for app in apps:
        app.dependency_overrides.pop(require_auth, None)


@pytest.fixture
def open_identity_db():
    """Factory: a connected ``Database`` with the local identity bootstrapped.

    Returns ``(db, LocalIdentity)`` — the tenant, the agent and its system
    principal, and one owner account, exactly as first start creates them.
    Await it **in the event loop that will use the database**: a sync
    ``TestClient`` test should call it through ``client.portal`` so the
    connection's write lock belongs to the loop the app runs in.

    The caller closes the database.
    """

    async def _open(db_path, *, credential_source: str = "none", display_name=None):
        database = Database(Path(db_path))
        await database.connect()
        identity = await database.bootstrap_local_identity(
            credential_source=credential_source, display_name=display_name,
        )
        return database, identity

    return _open


@pytest.fixture
def wire_identity_store(monkeypatch):
    """Point the request path's actor resolution at a database.

    ``require_auth`` resolves the actor against ``get_deps().db``, which the
    gateway lifespan wires before it serves. A test that builds its own app
    wires it here instead; ``monkeypatch`` puts the previous container back
    afterwards, so one test's (closed) database can never be resolved against
    by the next.
    """
    from nerve.gateway.routes import _deps as deps_module

    def _wire(database):
        monkeypatch.setattr(
            deps_module, "_deps", deps_module.RouteDeps(engine=None, db=database),
        )
        return database

    return _wire


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
