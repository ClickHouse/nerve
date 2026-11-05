"""Shared test fixtures for Nerve tests."""

import asyncio
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest
import pytest_asyncio

import nerve.config  # imported so its constants can be re-pointed
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
    # A hosted VM seeds the agent's display name into the environment; the
    # suite must not inherit it as the profile step's prefill.
    monkeypatch.delenv("NERVE_DISPLAY_NAME", raising=False)
    _repoint_import_time_state_paths(monkeypatch)


@pytest.fixture(autouse=True)
def _no_ambient_platform_activity_socket(monkeypatch):
    """Keep the gateway's activity reporter off the machine's real socket.

    On a machine that runs on a platform with a local activity socket, the
    default ``platform.activity_socket`` exists, and every test that starts
    the gateway lifespan would report its fake turns and connections there.
    The default is resolved when a config is built, so pointing it at nothing
    here covers every config a test builds. Tests about the reporter name
    their own socket.
    """
    monkeypatch.setattr(nerve.config, "DEFAULT_PLATFORM_ACTIVITY_SOCKET", "")


@pytest.fixture(autouse=True)
def _no_ambient_provider_credential(monkeypatch):
    """Strip the developer shell's model credentials from every test.

    ``NerveConfig.credential_available()`` and ``resolve_anthropic_api_key()``
    read ``ANTHROPIC_API_KEY`` / ``CLAUDE_CODE_OAUTH_TOKEN`` so that an
    environment-provisioned instance works. A test that means "no credential"
    must therefore not inherit one from whoever runs the suite; tests about
    the environment set the variables themselves.
    """
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)


#: What the stand-in GitHub CLI probe below answers: no ``gh`` on the machine.
NO_GITHUB_CLI = {"cli": False, "authenticated": False, "login": None, "hint": None}


@pytest.fixture(autouse=True)
def _no_real_github_cli(monkeypatch):
    """Never run the developer's GitHub CLI for the onboarding's facts.

    Every onboarding start gathers facts about the instance, ``gh auth
    status`` included (:mod:`nerve.onboarding_environment`). A test that opens
    the conversation (directly, or through Finish) must neither wait for nor
    depend on whoever is signed in on the machine running the suite: the probe
    answers "not installed" unless a test replaces it.
    """
    from nerve import onboarding_environment

    monkeypatch.setattr(onboarding_environment, "_github_cli", lambda timeout: dict(NO_GITHUB_CLI))


@pytest.fixture(autouse=True)
def _reset_setup_process_state(monkeypatch):
    """The setup modules keep per-process memory (detection caches, the last
    provider test, the restart flag, the startup-config snapshot). None of it
    may leak from one test into the next, whichever file the tests live in.
    """
    from nerve import config_reload, setup_checks

    setup_checks.reset_caches()
    monkeypatch.setattr(config_reload, "_startup_config", None)
    catalog = sys.modules.get("nerve.models_catalog")
    if catalog is not None:
        catalog.reset_cache()  # the fingerprint-keyed catalog and any warm-up
    routes = sys.modules.get("nerve.gateway.routes.setup")
    if routes is not None:
        routes._bot_usernames.clear()
        monkeypatch.setattr(routes, "_restart_requested", False)
        monkeypatch.setattr(routes, "_last_provider_test", None)
        # One pending Telegram sign-in per process: never one left by another test.
        from nerve import setup_telegram_login

        monkeypatch.setattr(routes, "_telegram_login", setup_telegram_login.TelegramLogin())
    yield
    setup_checks.reset_caches()
    if catalog is not None:
        catalog.reset_cache()
    routes = sys.modules.get("nerve.gateway.routes.setup")
    if routes is not None:
        routes._bot_usernames.clear()


@pytest.fixture(autouse=True)
def _unpin_jwt_secret():
    """Clear the process signing secret before and after each test.

    Startup pins the secret once per process. A test that pins one must not
    leave it in force for the next test.
    """
    from nerve.gateway.auth import unpin_jwt_secret

    unpin_jwt_secret()
    yield
    unpin_jwt_secret()


@pytest.fixture(autouse=True)
def _unpin_auth_mode(monkeypatch):
    """Clear the authentication mode before and after each test.

    ``create_app()`` pins the mode once per process, and before a pin the mode
    comes from ``NERVE_AUTH_MODE``. Each test starts in local mode, whatever
    the shell sets, and a test that pins a mode does not leave it in force.
    """
    from nerve.gateway.auth import AUTH_MODE_ENV, unpin_auth_mode

    monkeypatch.delenv(AUTH_MODE_ENV, raising=False)
    unpin_auth_mode()
    yield
    unpin_auth_mode()


@pytest.fixture(autouse=True)
def _no_ambient_system_principal(monkeypatch):
    """Strip ``NERVE_SYSTEM_PRINCIPAL_ID`` from the developer shell.

    In external mode, the identity bootstrap gives the system actor the ID in
    that variable. Tests about it set the variable themselves.
    """
    from nerve.identity import SYSTEM_PRINCIPAL_ENV

    monkeypatch.delenv(SYSTEM_PRINCIPAL_ENV, raising=False)


@pytest.fixture(autouse=True)
def _no_ambient_timezone(monkeypatch):
    """Strip ``NERVE_TIMEZONE`` from the developer shell.

    Nerve uses it as the default time zone when no settings layer names one.
    Tests about it set the variable themselves.
    """
    monkeypatch.delenv(nerve.config.TIMEZONE_ENV, raising=False)


@pytest.fixture(scope="session", autouse=True)
def _deterministic_umask():
    """Run the suite under umask 022, whatever the developer's shell has.

    ``Database.connect`` refuses a group-writable state directory, and under
    umask 002 every plain ``mkdir()`` in a fixture makes one. Tests about the
    umask set their own.
    """
    old = os.umask(0o022)
    yield
    os.umask(old)


# The actor a test that is not about authentication runs as. Its synthetic ids
# are UUID-shaped like production ids and are not rows in ``actor_refs``
# — these fixtures do not store actor ids, so no foreign key checks them. Tests
# that persist an actor id must create the corresponding ``actor_refs`` row.
TEST_ACTOR = Actor(
    actor_id="00000000-0000-4000-8000-00000000ac70",
    kind="human",
    account_id="00000000-0000-4000-8000-00000000acc7",
    display_name="Test Account",
)


@pytest.fixture
def bypass_auth():
    """Replace ``require_auth`` on a test app.

    Without a signing secret every auth check fails closed, so route tests
    that are not about auth use this. Yields a function that takes the app
    and returns it. Auth tests use the real dependency.

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


@dataclass(frozen=True)
class _TestIdentity:
    owner_account_id: str
    owner_actor_id: str
    system_actor_id: str


@pytest.fixture
def open_identity_db():
    """Factory: a connected ``Database`` with one test account.

    Returns ``(db, identity)`` with the account, human actor, and the
    migration-created system actor ids. This intentionally uses the narrow
    internal bootstrap primitive: tests create exceptional account states
    directly instead of expanding the production Database API for fixtures.
    Await it **in the event loop that will use the database**: a sync
    ``TestClient`` test should call it through ``client.portal`` so the
    connection's write lock belongs to the loop the app runs in.

    The caller closes the database.
    """

    async def _open(db_path, *, credential_source: str = "none", display_name=None):
        database = Database(Path(db_path))
        await database.connect()
        account = await database._bootstrap_first_account(
            credential_source=credential_source, display_name=display_name,
        )
        assert account.created and account.account_id and account.actor_id
        identity = _TestIdentity(
            owner_account_id=account.account_id,
            owner_actor_id=account.actor_id,
            system_actor_id=database.system_actor_id,
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
