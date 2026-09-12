"""3.5 — every install moves off ``credential_source = 'config'`` at startup.

The configured hash is *copied* onto the account row (never re-hashed, so
nobody's password changes) and the now-dead configuration value is then removed
— except on a lockdown install, whose configuration is fleet-managed and is
reported rather than rewritten.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import bcrypt
import httpx
import pytest
import pytest_asyncio
import yaml
from fastapi import FastAPI

from nerve import paths
from nerve.config import AuthConfig, NerveConfig, load_config, set_config, workspace_settings_file
from nerve.db import Database
from nerve.gateway.auth import pin_jwt_secret
from nerve.gateway.routes import accounts as accounts_routes
from nerve.gateway.routes import auth as auth_routes
from nerve.migrate import bootstrap_identity

_PASSWORD = "correct horse battery staple"
_HASH = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()
_SECRET = "configured-secret-padded-to-thirty-two-bytes"


def _install(tmp_path: Path, *, local_yaml: str, settings_yaml: str = "") -> NerveConfig:
    """A config directory with a machine-local overlay, loaded."""
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir(exist_ok=True)
    (ws / "config").mkdir(parents=True, exist_ok=True)
    (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
    (config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
    if settings_yaml:
        workspace_settings_file(ws).write_text(settings_yaml, encoding="utf-8")
    return load_config(config_dir)


def _git_repo_with_remote(ws: Path) -> None:
    """What a locked workspace is on a real box; the remote is never contacted."""
    if not shutil.which("git"):
        pytest.skip("git not available")
    subprocess.run(["git", "init", "-q"], cwd=str(ws), check=True, capture_output=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://example.invalid/config.git"],
        cwd=str(ws), check=True, capture_output=True,
    )


def _auth_section(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return raw.get("auth") or {}


@pytest.mark.asyncio
class TestTheCopy:
    async def test_the_hash_is_copied_byte_for_byte(self, db: Database, tmp_path):
        config = _install(
            tmp_path,
            local_yaml=f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: '{_SECRET}'\n",
        )
        report = await bootstrap_identity(db, config)
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "local"
        assert account["credential"] == _HASH
        assert report.migrated_config_credential
        # A re-hash would produce a different salt and a different string, and
        # would mean the migration had decided what somebody's password is.
        assert bcrypt.checkpw(_PASSWORD.encode(), account["credential"].encode())

    async def test_it_is_idempotent(self, db: Database, tmp_path):
        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        first = await bootstrap_identity(db, config)
        assert first.migrated_config_credential and first.scrubbed_config_password

        # The config object was emptied in step with the file it rewrote, so a
        # second pass in the same process is a no-op...
        second = await bootstrap_identity(db, config)
        assert not second.migrated_config_credential
        assert not second.scrubbed_config_password
        # ...and so is a fresh load from the rewritten files.
        third = await bootstrap_identity(db, load_config(config.config_dir))
        assert not third.did_bootstrap
        (account,) = await db.list_accounts()
        assert account["credential"] == _HASH

    async def test_a_local_credential_is_left_alone(self, db: Database, tmp_path):
        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        await bootstrap_identity(db, config)
        (account,) = await db.list_accounts()
        await db.set_account_credential(
            account["id"], credential_source="local", credential="$2b$12$the-accounts-own",
        )
        reloaded = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        report = await bootstrap_identity(db, reloaded)
        assert not report.migrated_config_credential
        (account,) = await db.list_accounts()
        assert account["credential"] == "$2b$12$the-accounts-own"

    async def test_a_passwordless_install_is_untouched(self, db: Database, tmp_path):
        config = _install(tmp_path, local_yaml="auth:\n  jwt_secret: 'x' \n")
        report = await bootstrap_identity(db, config)
        assert not report.migrated_config_credential
        assert not report.scrubbed_config_password
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "none"
        assert account["credential"] is None


@pytest.mark.asyncio
class TestTheScrub:
    async def test_the_key_is_removed_and_the_rest_of_the_file_survives(
        self, db: Database, tmp_path,
    ):
        config = _install(
            tmp_path,
            local_yaml=(
                "# Nerve — machine-local secrets & overrides (gitignored).\n\n"
                f"anthropic_api_key: an-obviously-fake-value\n"
                f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: '{_SECRET}'\n"
            ),
        )
        local_yaml = config.config_dir / "config.local.yaml"
        await bootstrap_identity(db, config)

        text = local_yaml.read_text(encoding="utf-8")
        assert "password_hash" not in text
        assert _HASH not in text
        assert _auth_section(local_yaml) == {"jwt_secret": _SECRET}
        assert yaml.safe_load(text)["anthropic_api_key"] == "an-obviously-fake-value"
        # The file's own header is kept rather than replaced.
        assert text.startswith("# Nerve —")

    async def test_the_file_stays_owner_only(self, db: Database, tmp_path):
        """It still holds the signing secret and every API key the wizard
        collected, so it is rewritten through the fail-closed private writer."""
        config = _install(
            tmp_path,
            local_yaml=f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: '{_SECRET}'\n",
        )
        local_yaml = config.config_dir / "config.local.yaml"
        local_yaml.chmod(0o600)
        await bootstrap_identity(db, config)
        assert local_yaml.stat().st_mode & 0o077 == 0

    async def test_an_empty_auth_section_is_dropped_rather_than_left_null(
        self, db: Database, tmp_path,
    ):
        """A bare ``auth:`` key is an empty overlay, not an eraser — but leaving
        one behind for no reason invites the question."""
        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        local_yaml = config.config_dir / "config.local.yaml"
        await bootstrap_identity(db, config)
        assert "auth" not in (yaml.safe_load(local_yaml.read_text(encoding="utf-8")) or {})

    async def test_config_yaml_is_scrubbed_too(self, db: Database, tmp_path):
        """Both machine-local layers can carry it, and both are gitignored."""
        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {ws}\nauth:\n  password_hash: '{_HASH}'\n", encoding="utf-8",
        )
        (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")
        config = load_config(config_dir)
        assert config.auth.password_hash == _HASH

        report = await bootstrap_identity(db, config)
        assert report.scrubbed_config_password
        text = (config_dir / "config.yaml").read_text(encoding="utf-8")
        assert "password_hash" not in text
        assert f"workspace: {ws}" in text

    async def test_the_process_stops_believing_in_the_configured_password(
        self, db: Database, tmp_path,
    ):
        """The in-memory config is emptied with the file, so the login route and
        the status descriptor do not go on reading a value that exists nowhere
        until the next restart."""
        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        await bootstrap_identity(db, config)
        assert config.auth.password_hash == ""

    async def test_a_write_that_cannot_be_private_reports_and_keeps_the_login(
        self, db: Database, tmp_path, monkeypatch,
    ):
        """A tidy-up that fails must not stop a start, and must not be silent —
        the credential is already on the row, so the configured value is inert
        either way."""
        from nerve import migrate as migrate_mod

        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        local_yaml = config.config_dir / "config.local.yaml"
        before = local_yaml.read_text(encoding="utf-8")

        def _refuse(path, text):
            raise paths.InsecureFileError(f"{path} cannot be created owner-only")

        monkeypatch.setattr(migrate_mod.paths, "write_private_text", _refuse)
        report = await bootstrap_identity(db, config)

        (account,) = await db.list_accounts()
        assert account["credential"] == _HASH          # the copy happened first
        assert not report.scrubbed_config_password
        assert local_yaml.read_text(encoding="utf-8") == before
        assert any("could not be removed" in w for w in report.warnings)
        # ...and the value is still in force for this process, because it is
        # still in the file.
        assert config.auth.password_hash == _HASH


@pytest.mark.asyncio
class TestLockdown:
    def _locked(self, tmp_path, monkeypatch) -> NerveConfig:
        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        _git_repo_with_remote(ws)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        workspace_settings_file(ws).write_text(
            "lockdown: true\nauth:\n  password_hash: ${NERVE_TEST_PASSWORD_HASH}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("NERVE_TEST_PASSWORD_HASH", _HASH)
        return load_config(config_dir)

    async def test_nothing_is_written_and_the_warning_names_the_file_and_key(
        self, db: Database, tmp_path, monkeypatch,
    ):
        config = self._locked(tmp_path, monkeypatch)
        settings = workspace_settings_file(config.workspace)
        before = settings.read_bytes()

        report = await bootstrap_identity(db, config)

        (account,) = await db.list_accounts()
        assert account["credential"] == _HASH
        assert not report.scrubbed_config_password
        assert settings.read_bytes() == before
        assert config.auth.password_hash == _HASH      # still there, still inert
        warning = " ".join(report.warnings)
        assert str(settings) in warning
        assert "auth.password_hash" in warning
        assert "no longer authenticates anybody" in warning
        assert "fleet-managed" in warning

    async def test_the_env_reference_in_the_file_is_left_as_a_reference(
        self, db: Database, tmp_path, monkeypatch,
    ):
        config = self._locked(tmp_path, monkeypatch)
        settings = workspace_settings_file(config.workspace)
        await bootstrap_identity(db, config)
        assert "${NERVE_TEST_PASSWORD_HASH}" in settings.read_text(encoding="utf-8")


@pytest.mark.asyncio
class TestATrackedValueIsReportedNotRewritten:
    async def test_shared_configuration_is_never_edited(self, db: Database, tmp_path):
        """Not a lockdown install, but the value lives in the tracked settings
        file — shared configuration, possibly under version control."""
        config = _install(
            tmp_path,
            local_yaml="{}\n",
            settings_yaml=f"auth:\n  password_hash: '{_HASH}'\n",
        )
        settings = workspace_settings_file(config.workspace)
        before = settings.read_bytes()

        report = await bootstrap_identity(db, config)

        (account,) = await db.list_accounts()
        assert account["credential"] == _HASH
        assert settings.read_bytes() == before
        assert not report.scrubbed_config_password
        assert any(str(settings) in w for w in report.warnings)


@pytest.mark.asyncio
class TestTheStaleValueWarning:
    async def test_a_hash_no_account_reads_is_called_out_once(
        self, db: Database, tmp_path, caplog,
    ):
        """Spec 1.3: a stale auth.password_hash that no longer does anything is
        exactly what an operator debugs for an hour."""
        actor = await db.create_actor_ref(kind="human")
        await db.create_account(
            actor_id=actor["id"], credential_source="local", credential="$2b$12$own",
        )
        config = NerveConfig(
            auth=AuthConfig(password_hash=_HASH),
            config_dir=Path("/nonexistent/nerve-test-config-dir"),
        )
        with caplog.at_level("WARNING"):
            report = await bootstrap_identity(db, config)
        assert any("no account uses it" in w for w in report.warnings)
        assert any("no account uses it" in r.getMessage() for r in caplog.records)

    async def test_nothing_is_said_when_the_value_still_authenticates(
        self, db: Database, tmp_path,
    ):
        config = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        report = await bootstrap_identity(db, config)
        # It was copied and then removed, so there is no stale value to warn
        # about — only the actions that describe what happened.
        assert not any("no account uses it" in w for w in report.warnings)


# --------------------------------------------------------------------------- #
#  The whole point: nobody's password changes                                  #
# --------------------------------------------------------------------------- #


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_routes.router)
    app.include_router(accounts_routes.router)
    return app


@pytest_asyncio.fixture
async def _clean_config():
    yield
    set_config(NerveConfig())


@pytest.mark.asyncio
class TestTheOldPasswordStillWorks:
    async def test_login_after_the_migration(
        self, db: Database, tmp_path, wire_identity_store, _clean_config,
    ):
        config = _install(
            tmp_path,
            local_yaml=f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: '{_SECRET}'\n",
        )
        set_config(config)
        pin_jwt_secret(_SECRET)
        await bootstrap_identity(db, config)
        wire_identity_store(db)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=_app()), base_url="http://nerve-test",
        ) as client:
            good = await client.post("/api/auth/login", json={"password": _PASSWORD})
            bad = await client.post("/api/auth/login", json={"password": "not it"})
            status = await client.get("/api/auth/status")

        assert good.status_code == 200, good.text
        assert bad.status_code == 401
        # ...and the instance is still "one account, password only" — the
        # migration changed where the hash lives, nothing else.
        assert status.json()["login"] == "password"
        assert status.json()["setup_pending"] is False
