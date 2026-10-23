"""The small additions the setup work made to existing modules.

Kept beside the modules' own test files rather than inside them so the
contracts those files pin stay untouched:

* ``config_reload``: ``restart_pending`` compares against the startup
  snapshot; ``note_applied`` patches exactly one path;
* the Claude backend exports ``CLAUDE_CODE_OAUTH_TOKEN`` when no API key is set;
* ``TelegramChannel.bot_username`` / ``allowed_users``, and ``/pair`` marks
  its own append as applied;
* the gateway: ``/health`` carries ``generation`` and no commit (only the
  signed-in diagnostics name the build), ``/setup`` is served
  without a built UI, the startup snapshot is taken, a Telegram channel
  whose ``start()`` raises never takes the gateway down and its token never
  reaches the log or the state file, the startup notice is skipped when
  Telegram is off;
* the onboarding conversation opens once, in a web session.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from nerve import boot, config_reload, setup_state
from nerve.config import NerveConfig, load_config, set_config

_TOKEN = "123456:REPLACEabcdefghijklmnopqrstuvwxyz0123"


# --------------------------------------------------------------------------- #
#  config_reload                                                               #
# --------------------------------------------------------------------------- #


def _write_config(config_dir: Path, workspace: Path, body: str = "") -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n{body}", encoding="utf-8")


class TestRestartPending:
    @pytest.fixture(autouse=True)
    def _clear(self, monkeypatch):
        monkeypatch.setattr(config_reload, "_startup_config", None)

    def test_no_snapshot_means_nothing_to_say(self):
        assert config_reload.restart_pending() == []

    @pytest.mark.asyncio
    async def test_restart_pending_compares_against_startup_config(self, tmp_path):
        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, "timezone: UTC\n")
        config = load_config(config_dir)
        set_config(config)
        config_reload.remember_startup_config(config)
        assert config_reload.restart_pending() == []

        _write_config(config_dir, ws, "timezone: Europe/Berlin\n")
        await config_reload.reload_all(None, None, config_dir)
        assert config_reload.restart_pending() == ["timezone: 'UTC' → 'Europe/Berlin'"]

        # Reverted: the answer is empty again, nothing had to be cleared.
        _write_config(config_dir, ws, "timezone: UTC\n")
        await config_reload.reload_all(None, None, config_dir)
        assert config_reload.restart_pending() == []
        set_config(NerveConfig())

    def test_note_applied_patches_only_that_path(self):
        config = NerveConfig.from_dict({"telegram": {"allowed_users": [], "bot_token": "x"}, "timezone": "UTC"})
        config_reload.remember_startup_config(config)
        config_reload.note_applied("telegram.allowed_users", [123456789])
        snapshot = config_reload.startup_config()
        assert snapshot.telegram.allowed_users == [123456789]
        assert snapshot.telegram.bot_token == "x" and snapshot.timezone == "UTC"
        assert config.telegram.allowed_users == [], "the live object is not touched"
        # Unknown paths and no snapshot are ignored.
        config_reload.note_applied("nothing.like.this", 1)
        assert config_reload.startup_config() is snapshot
        config_reload._startup_config = None
        config_reload.note_applied("timezone", "x")
        assert config_reload.startup_config() is None

    def test_the_snapshot_is_a_copy(self):
        config = NerveConfig.from_dict({"external_agents": {"targets": []}})
        config_reload.remember_startup_config(config)
        config.external_agents.targets.append("mutated-in-place")
        assert config_reload.startup_config().external_agents.targets == []


# --------------------------------------------------------------------------- #
#  Claude backend environment                                                  #
# --------------------------------------------------------------------------- #


def _backend(*, api_key: str = "", oauth: str = ""):
    from nerve.agent.backends.claude import ClaudeBackend

    config = SimpleNamespace(
        provider=SimpleNamespace(
            is_bedrock=False, aws_region="", aws_profile="",
            aws_access_key_id="", aws_secret_access_key="",
        ),
        proxy=SimpleNamespace(enabled=False, host="", port=0),
        anthropic_base_url="",
        effective_api_key=api_key,
        claude_oauth_token=oauth,
        agent=SimpleNamespace(model_aliases={}, agent_teams=False),
    )
    return ClaudeBackend(SimpleNamespace(config=lambda: config))


class TestClaudeBackendEnv:
    def test_exports_claude_code_oauth_token_when_no_api_key(self):
        env = _backend(oauth="sk-ant-oat01-REPLACE")._build_env()
        assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "sk-ant-oat01-REPLACE"
        assert "ANTHROPIC_API_KEY" not in env

    def test_api_key_wins_over_oauth_token(self):
        env = _backend(api_key="sk-ant-REPLACE0000000000", oauth="sk-ant-oat01-REPLACE")._build_env()
        assert env["ANTHROPIC_API_KEY"] == "sk-ant-REPLACE0000000000"
        assert "CLAUDE_CODE_OAUTH_TOKEN" not in env

    def test_nothing_is_exported_without_either(self):
        env = _backend()._build_env()
        assert "ANTHROPIC_API_KEY" not in env and "CLAUDE_CODE_OAUTH_TOKEN" not in env


# --------------------------------------------------------------------------- #
#  Telegram channel                                                            #
# --------------------------------------------------------------------------- #


@pytest.fixture
def channel(tmp_path, monkeypatch):
    import nerve.pairing as pairing
    from nerve.channels.telegram import TelegramChannel

    config = NerveConfig()
    config.config_dir = tmp_path
    config.telegram.enabled = True
    config.telegram.dm_policy = "pairing"
    config.telegram.allowed_users = []
    monkeypatch.setattr(pairing, "verify_pairing_code", lambda code: True)
    monkeypatch.setattr("nerve.config.is_locked", lambda: False)
    return TelegramChannel(lambda: config, MagicMock())


def _update(user_id: int = 4242):
    reply = AsyncMock()
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id, is_bot=False),
        effective_chat=SimpleNamespace(id=user_id, type="private"),
        message=SimpleNamespace(reply_text=reply, sender_chat=None),
    ), reply


class TestTelegramChannelAdditions:
    def test_bot_username_before_and_after_initialize(self, channel):
        assert channel.bot_username is None
        channel._app = SimpleNamespace(bot=SimpleNamespace(username="example_assistant_bot"))
        assert channel.bot_username == "example_assistant_bot"

        class _Raises:
            @property
            def bot(self):
                raise RuntimeError("not initialised")

        channel._app = _Raises()
        assert channel.bot_username is None

    def test_allowed_users_is_a_frozen_copy(self, channel):
        channel._allowed_users.add(1)
        users = channel.allowed_users
        assert users == frozenset({1}) and isinstance(users, frozenset)

    @pytest.mark.asyncio
    async def test_pair_notes_the_allowlist_as_applied_after_a_successful_append(self, channel, monkeypatch):
        noted = []
        monkeypatch.setattr("nerve.config.append_telegram_allowed_user", lambda config_dir, user_id: True)
        monkeypatch.setattr(config_reload, "note_applied", lambda path, value: noted.append((path, value)))
        update, _reply = _update()
        await channel._handle_pair(update, SimpleNamespace(args=["code"]))
        assert noted == [("telegram.allowed_users", [4242])]

    @pytest.mark.asyncio
    async def test_pair_notes_the_list_as_it_landed_on_disk(self, tmp_path, monkeypatch):
        """A startup allow-list that is not ascending ([333, 111]) plus a
        pair of 222: what the handler notes equals what is now on disk, so
        the next reload reports no phantom restart debt (the C39 case)."""
        import nerve.pairing as pairing
        from nerve.channels.telegram import TelegramChannel
        from nerve.config import get_config

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text(
            f"workspace: {tmp_path / 'ws'}\ntelegram:\n  enabled: true\n", encoding="utf-8",
        )
        (config_dir / "config.local.yaml").write_text(
            "telegram:\n  bot_token: '123456:REPLACEabcdefghijklmnopqrstuvwxyz0123'\n"
            "  allowed_users: [333, 111]\n",
            encoding="utf-8",
        )
        (tmp_path / "ws" / "config").mkdir(parents=True)
        (tmp_path / "ws" / "config" / "settings.yaml").write_text("telegram:\n  dm_policy: pairing\n", encoding="utf-8")
        config = load_config(config_dir)
        set_config(config)
        config_reload.remember_startup_config(config)
        monkeypatch.setattr(pairing, "verify_pairing_code", lambda code: True)
        monkeypatch.setattr("nerve.config.is_locked", lambda: False)
        channel = TelegramChannel(get_config, MagicMock())
        update, reply = _update(222)
        await channel._handle_pair(update, SimpleNamespace(args=["code"]))
        assert "paired" in reply.call_args[0][0].lower()
        import yaml

        on_disk = yaml.safe_load((config_dir / "config.local.yaml").read_text())["telegram"]["allowed_users"]
        assert on_disk == [333, 111, 222]
        assert list(config_reload.startup_config().telegram.allowed_users) == [333, 111, 222]
        await config_reload.reload_all(None, None, config_dir)
        assert config_reload.restart_pending() == []
        # A second pair in the same process builds on the patched snapshot.
        update, _reply = _update(5)
        await channel._handle_pair(update, SimpleNamespace(args=["code"]))
        await config_reload.reload_all(None, None, config_dir)
        assert config_reload.restart_pending() == []
        set_config(NerveConfig())

    @pytest.mark.asyncio
    async def test_pair_does_not_note_after_a_failed_append(self, channel, monkeypatch):
        noted = []

        def _fail(config_dir, user_id):
            raise OSError("disk full")

        monkeypatch.setattr("nerve.config.append_telegram_allowed_user", _fail)
        monkeypatch.setattr(config_reload, "note_applied", lambda path, value: noted.append(path))
        update, reply = _update()
        await channel._handle_pair(update, SimpleNamespace(args=["code"]))
        assert noted == []
        assert "Paired for this run" in reply.call_args[0][0]


# --------------------------------------------------------------------------- #
#  Gateway                                                                     #
# --------------------------------------------------------------------------- #


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """The lifespan with fakes, as tests/test_gateway_startup.py drives it."""
    import nerve.config as config_module
    import nerve.proxy.service as proxy_module
    from nerve.gateway import server as gw

    config = NerveConfig()
    config.workspace = tmp_path / "workspace"
    config.workspace.mkdir(parents=True, exist_ok=True)
    config.config_dir = tmp_path
    config.anthropic_api_key = ""
    config.telegram.enabled = False
    config.proxy.enabled = False
    config.mcp_endpoint.enabled = False
    config.agent.model_discovery = False
    config.external_agents.enabled = False
    config.workspace_sync.enabled = False
    config_module._config = config

    monkeypatch.setattr(proxy_module, "ProxyService", lambda cfg: MagicMock(start=AsyncMock(), stop=AsyncMock()))
    engine = MagicMock()
    engine.initialize = AsyncMock()
    engine.shutdown = AsyncMock()
    engine.registry = MagicMock()
    engine.router = MagicMock()
    engine.set_notification_service = MagicMock()
    engine.resume_enrolled_sessions = AsyncMock(return_value=0)
    monkeypatch.setattr(gw, "AgentEngine", lambda cfg, db: engine)
    monkeypatch.setattr(gw, "init_langfuse", lambda cfg: None)
    monkeypatch.setattr(gw, "init_deps", lambda *a, **k: None)

    async def _init_db(*a, **k):
        return MagicMock(
            cleanup_expired_messages=AsyncMock(return_value=0),
            login_state=AsyncMock(return_value=MagicMock(passwordless=False)),
            _delete_instance_secret=AsyncMock(return_value=False),
        )

    async def _close_db():
        return None

    monkeypatch.setattr(gw, "init_db", _init_db)
    monkeypatch.setattr(gw, "close_db", _close_db)
    monkeypatch.setattr("nerve.migrate.bootstrap_identity", AsyncMock(return_value=MagicMock(identity_actions=[])))
    notifications = MagicMock(send_notification=AsyncMock(), hide_session_label_for=MagicMock())
    monkeypatch.setattr("nerve.notifications.service.NotificationService", lambda *a, **k: notifications)
    monkeypatch.setattr(gw, "set_notification_service", lambda *a, **k: None)
    monkeypatch.setattr(config_reload, "_startup_config", None)
    yield {"server": gw, "engine": engine, "config": config, "notifications": notifications}
    gw._engine = None
    gw._cron_service = None
    config_module._config = None


class _ExplodingChannel:
    instances: list["_ExplodingChannel"] = []
    #: What ``start()`` raises; tests swap in the forms a transport layer
    #: produces (h11 quotes an illegal request target as ``b'…'``).
    message: str = f"InvalidToken: The token `{_TOKEN}` was rejected by the server."

    def __init__(self, get_config, router):
        self.stopped = 0
        type(self).instances.append(self)

    def set_notification_service(self, service):
        pass

    async def start(self):
        raise RuntimeError(type(self).message)

    async def stop(self):
        self.stopped += 1


@pytest.mark.asyncio
class TestGatewayAdditions:
    async def test_startup_config_is_remembered_and_old_notes_retired(self, harness, monkeypatch):
        state = setup_state.SetupState()
        state.debts["cron"] = "x"
        state.boot = "0000000000000000"
        setup_state.save_state(state)
        # save_state stamps the current boot; rewrite the boot field to an old one.
        import json

        path = setup_state.state_file()
        raw = json.loads(path.read_text())
        raw["boot"] = "0000000000000000"
        path.write_text(json.dumps(raw))
        async with harness["server"].lifespan(MagicMock()):
            assert config_reload.startup_config() is not None
            assert config_reload.startup_config().workspace == harness["config"].workspace
        reloaded = setup_state.load_state()
        assert reloaded.debts == {} and reloaded.boot == boot.boot_id()

    async def test_lifespan_survives_bad_telegram_token(self, harness, monkeypatch, caplog):
        import nerve.channels.telegram as telegram_module

        harness["config"].telegram.enabled = True
        harness["config"].telegram.bot_token = _TOKEN
        _ExplodingChannel.instances.clear()
        monkeypatch.setattr(telegram_module, "TelegramChannel", _ExplodingChannel)
        with caplog.at_level(logging.ERROR):
            async with harness["server"].lifespan(MagicMock()):
                pass
        assert len(_ExplodingChannel.instances) == 1
        assert _ExplodingChannel.instances[0].stopped == 1
        assert harness["engine"].register_channel.call_count == 0
        assert any("[ERR] Telegram bot failed to start" in r.getMessage() for r in caplog.records)
        assert _TOKEN not in caplog.text
        errors = setup_state.load_state().boot_errors
        assert "telegram" in errors and _TOKEN not in errors["telegram"] and "<token>" in errors["telegram"]
        assert _TOKEN not in setup_state.state_file().read_text()
        # No startup notice went out either: the channel is not running.
        assert not _started_notices(harness)

    @pytest.mark.parametrize("form", [repr(_TOKEN), repr(_TOKEN.encode())], ids=["repr", "bytes-repr"])
    async def test_lifespan_scrubs_the_repr_forms_of_the_token(self, harness, monkeypatch, caplog, form):
        """The lifespan uses the one scrubber (setup_checks): an exception that
        quotes the token the way h11 quotes an illegal request target
        (``b'…'``) must not carry it into the log, the state file or the API."""
        import nerve.channels.telegram as telegram_module

        harness["config"].telegram.enabled = True
        harness["config"].telegram.bot_token = _TOKEN
        _ExplodingChannel.instances.clear()
        monkeypatch.setattr(_ExplodingChannel, "message", f"ProtocolError: illegal request target {form}")
        monkeypatch.setattr(telegram_module, "TelegramChannel", _ExplodingChannel)
        with caplog.at_level(logging.ERROR):
            async with harness["server"].lifespan(MagicMock()):
                pass
        assert _TOKEN not in caplog.text
        recorded = setup_state.load_state().boot_errors["telegram"]
        assert _TOKEN not in recorded and "<token>" in recorded
        assert _TOKEN not in setup_state.state_file().read_text()

    async def test_startup_notice_is_skipped_when_telegram_is_off(self, harness):
        async with harness["server"].lifespan(MagicMock()):
            pass
        assert not _started_notices(harness)


def _started_notices(harness) -> list:
    """The "Nerve started" notices sent (other services may notify failures)."""
    return [
        call for call in harness["notifications"].send_notification.await_args_list
        if str(call.kwargs.get("title", "")).startswith("Nerve started")
    ]


class TestHealthAndFallbackPage:
    @pytest.fixture
    def app(self, tmp_path, monkeypatch):
        from nerve.gateway import server

        config = NerveConfig()
        config.workspace = tmp_path / "ws"
        config.workspace.mkdir()
        set_config(config)
        monkeypatch.setattr(server, "lifespan", _no_lifespan)
        yield server.create_app()
        set_config(NerveConfig())

    @pytest.mark.asyncio
    async def test_health_has_generation(self, app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
            body = (await http.get("/health")).json()
        assert body["status"] == "ok"
        assert body["generation"] == boot.boot_id() and len(body["generation"]) == 16

    @pytest.mark.asyncio
    @pytest.mark.parametrize("commit", ["9f2c4e1a7b3d5f60812c4e6a8b0d2f4618a3c5e7", ""])
    async def test_only_the_signed_in_diagnostics_name_the_running_commit(
        self, tmp_path, monkeypatch, db, bypass_auth, commit,
    ):
        """``/health`` answers anyone, so it does not say which build runs. The
        diagnostics do, from the commit read at startup."""
        from nerve.gateway import server
        from nerve.gateway.routes import _deps as deps_module

        reads: list[str] = []

        def source_commit() -> str:
            reads.append(commit)
            return commit

        monkeypatch.setattr(boot, "source_commit", source_commit)
        monkeypatch.setattr(
            deps_module, "_deps", deps_module.RouteDeps(engine=SimpleNamespace(), db=db),
        )
        config = NerveConfig()
        config.workspace = tmp_path / "ws"
        config.workspace.mkdir()
        set_config(config)
        monkeypatch.setattr(server, "lifespan", _no_lifespan)
        try:
            app = bypass_auth(server.create_app())
            assert reads == [commit], "read at startup"
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
                health = (await http.get("/health")).json()
                system = (await http.get("/api/diagnostics")).json()["system"]
        finally:
            set_config(NerveConfig())

        assert health == {"status": "ok", "version": "0.1.0", "generation": boot.boot_id()}
        assert system["commit"] == (commit or None)

    def test_source_commit_is_the_checkout_head(self):
        import shutil
        import subprocess

        root = Path(boot.__file__).resolve().parent.parent
        if not (root / ".git").exists() or not shutil.which("git"):
            pytest.skip("not running from a git checkout")
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True,
            check=True,
        ).stdout.strip()
        assert boot.source_commit.__wrapped__() == head

    def test_source_commit_is_empty_when_git_cannot_say(self, monkeypatch):
        def no_git(*args, **kwargs):
            raise FileNotFoundError("git")

        monkeypatch.setattr(boot.subprocess, "run", no_git)
        assert boot.source_commit.__wrapped__() == ""

    @pytest.mark.asyncio
    async def test_setup_is_served_without_a_built_ui(self, app):
        web_dist = Path(__file__).resolve().parent.parent / "web" / "dist"
        if web_dist.exists():
            pytest.skip("web/dist is built in this checkout; the SPA serves /setup")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
            page = await http.get("/setup")
            root = await http.get("/")
            api = await http.get("/api/setup")
        assert page.status_code == 200 and "npm run build" in page.text and "<script" not in page.text
        assert root.status_code == 200
        assert api.status_code in (401, 503), "the API is not shadowed by the page"

    @pytest.mark.asyncio
    async def test_unknown_api_paths_answer_404_json(self, app, tmp_path, monkeypatch):
        """An API path no router claimed is a JSON 404 in both modes — never
        a 405 from the SPA catch-all, never index.html with a 200."""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
            trailing = await http.get("/api/setup/")
            typo = await http.get("/api/whoami")
            posted = await http.post("/api/setup/nope", json={})
            claim = await http.post("/api/setup/claim", json={})
        for response in (trailing, typo, posted):
            assert response.status_code == 404, response.text
            assert response.json() == {"detail": "Not found"}
        assert claim.status_code != 404, "local mode: the claim router is registered"

        # External mode: the claim router is not registered → 404, not 405.
        from nerve.gateway import auth as auth_module
        from nerve.gateway import server

        monkeypatch.setenv(auth_module.AUTH_MODE_ENV, auth_module.AUTH_MODE_EXTERNAL)
        auth_module.unpin_auth_mode()  # the local app above pinned the mode
        external_app = server.create_app()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=external_app), base_url="http://nerve-test") as http:
            claim = await http.post("/api/setup/claim", json={})
            trailing = await http.get("/api/setup/")
        assert claim.status_code == 404 and claim.json() == {"detail": "Not found"}
        assert trailing.status_code == 404

    def test_access_log_redacts_token_query_values(self):
        import logging.config as logging_config

        from nerve.gateway import server

        record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "WebSocket %s" [accepted]', ("127.0.0.1:50000", "/ws?token=eyJhbGciOi.secret.sig"), None,
        )
        assert server.RedactTokenQuery().filter(record) is True
        assert "secret" not in record.getMessage()
        assert record.getMessage() == '127.0.0.1:50000 - "WebSocket /ws?token=<redacted>" [accepted]'
        http_record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d', ("127.0.0.1:1", "GET", "/api/files/x?token=abc&v=1", "1.1", 200), None,
        )
        server.RedactTokenQuery().filter(http_record)
        assert "token=<redacted>&v=1" in http_record.getMessage() and "abc" not in http_record.getMessage()
        # And uvicorn's config carries it on every uvicorn logger: the HTTP
        # access line comes through uvicorn.access, the WebSocket "[accepted]"
        # line through uvicorn.error (verified live: only the former was
        # redacted with the filter on the access logger alone).
        config = server.uvicorn_log_config()
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            assert "redact_token_query" in config["loggers"][name]["filters"], name
        assert config["filters"]["redact_token_query"]["()"].endswith("RedactTokenQuery")
        logging_config.dictConfig(config)
        try:
            for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
                assert any(isinstance(f, server.RedactTokenQuery) for f in logging.getLogger(name).filters), name
        finally:
            for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
                logging.getLogger(name).filters.clear()


from contextlib import asynccontextmanager  # noqa: E402


@asynccontextmanager
async def _no_lifespan(_app):
    yield


# --------------------------------------------------------------------------- #
#  The onboarding conversation opens once, in a web session                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_onboarding_conversation_opens_once_in_a_web_session(tmp_path, monkeypatch):
    """With the real engine and session manager: one web session titled
    Onboarding, a hidden first turn, and the setup page reads it as running."""
    from nerve import onboarding
    from nerve.agent.engine import AgentEngine, TurnOutcome, TurnResult
    from nerve.db import Database
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes import setup as setup_routes
    from nerve.setup_writer import apply_mode

    ws = tmp_path / "ws"
    apply_mode(ws, "worker", "Watch the nightly build for example/repo and propose fixes.")
    config = NerveConfig(workspace=ws)
    config.codex.home_dir = tmp_path / "codex"
    db = Database(tmp_path / "nerve.db", workspace=ws)
    await db.connect()
    try:
        engine = AgentEngine(config, db)
        turns: list[dict] = []

        async def _run_turn(session_id, user_message, source, model, effort_override, actor, **kw):
            turns.append({"session_id": session_id, "prompt": user_message, "source": source,
                          "actor": actor, **kw})
            return TurnResult("Hello", TurnOutcome())

        monkeypatch.setattr(engine, "_run_turn", _run_turn)
        monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=db))

        first, second = await asyncio.gather(
            onboarding.start(engine, ws, actor=db.system_actor, cause=None),
            onboarding.start(engine, ws, actor=db.system_actor, cause=None),
        )
        assert first.status == second.status == "running"
        assert first.session_id == second.session_id
        for _ in range(20):
            if turns:
                break
            await asyncio.sleep(0.05)
        assert len(turns) == 1
        turn = turns[0]
        assert turn["session_id"] == first.session_id and turn["source"] == "web"
        assert turn["internal"] is True and turn["actor"] == db.system_actor
        assert "> Watch the nightly build for example/repo and propose fixes." in turn["prompt"]
        row = await db.get_session(first.session_id)
        assert (row["title"], row["source"]) == ("Onboarding", "web")
        assert await setup_routes._onboarding(config) == ("running", first.session_id, "conversation")
    finally:
        await db.close()
