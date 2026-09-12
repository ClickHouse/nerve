"""The configuration-aware identity bootstrap in ``nerve.migrate``.

What 1.9 of the local multi-user sequence asks for: idempotency across
repeated starts, the three upgrade shapes, a lockdown install whose password
hash is an environment reference, the exactly-one-account invariant, a
disabled account staying disabled, an existing ``auth.jwt_secret`` being kept
and a missing one generated once — plus the CLI surface (``nerve migrate
--dry-run`` shows the bootstrap before it happens and writes nothing).
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
from pathlib import Path

import bcrypt
import pytest
from click.testing import CliRunner

from nerve import paths
from nerve.config import AuthConfig, NerveConfig, load_config, workspace_settings_file
from nerve.db import Database
from nerve.db.accounts import JWT_SECRET_NAME, read_instance_secret
from nerve.gateway.auth import effective_jwt_secret, pinned_jwt_secret
from nerve.migrate import (
    MigrationReport,
    bootstrap_identity,
    ensure_jwt_secret,
    maybe_migrate,
    migrate,
)

_HASH = bcrypt.hashpw(b"correct horse battery staple", bcrypt.gensalt(rounds=4)).decode()
_SECRET = "configured-secret-padded-to-thirty-two-bytes"


def _cfg(
    *, password_hash: str = "", jwt_secret: str = "", lockdown: bool = False,
    config_dir: Path | None = None,
) -> NerveConfig:
    # config_dir is where the 3.5 retirement step looks for (and on an ordinary
    # install rewrites) auth.password_hash. It defaults to the *caller's working
    # directory* on a real NerveConfig, so a test that does not care still has
    # to point it somewhere that cannot exist.
    return NerveConfig(
        auth=AuthConfig(password_hash=password_hash, jwt_secret=jwt_secret),
        lockdown=lockdown,
        config_dir=config_dir or Path("/nonexistent/nerve-test-config-dir"),
    )


async def _snapshot(db: Database) -> dict:
    """Every identity row, keyed so two snapshots compare by id."""
    out: dict = {}
    for table in ("tenants", "agents", "actor_refs", "accounts",
                  "tenant_memberships", "agent_grants"):
        async with db.db.execute(f"SELECT * FROM {table} ORDER BY id") as cur:
            out[table] = [dict(r) async for r in cur]
    return out


# --------------------------------------------------------------------------- #
#  The three shapes                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestUpgradeShapes:
    async def test_a_configured_password_is_copied_onto_the_account(self, db: Database):
        """The account is created on `config` — PR 1's shape, which copies
        nothing — and 3.5 then moves it onto its own credential in the same
        start. The hash is *copied*, so nobody's password changes."""
        report = await bootstrap_identity(db, _cfg(password_hash=_HASH, jwt_secret=_SECRET))
        assert report.bootstrapped_account
        assert report.migrated_config_credential
        assert not report.generated_jwt_secret
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "local"
        assert account["credential"] == _HASH      # copied, byte for byte
        assert account["username"] is None
        assert account["enabled"] is True
        assert any("credential_source=config" in a for a in report.identity_actions)
        assert any("copied auth.password_hash" in a for a in report.identity_actions)

    async def test_passwordless_install_gives_credential_source_none(self, db: Database):
        report = await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "none"
        assert account["username"] is None
        assert any("credential_source=none" in a for a in report.identity_actions)

    async def test_fresh_install_gives_passwordless_account_and_a_secret(self, db: Database):
        """No password, no secret: the shape a headless install that omitted
        NERVE_PASSWORD lands in. One passwordless account, and a signing secret
        generated so the instance does not serve open."""
        report = await bootstrap_identity(db, NerveConfig())
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "none"
        assert report.bootstrapped_account and report.generated_jwt_secret
        assert await db.get_instance_secret(JWT_SECRET_NAME)

    async def test_owner_display_name_is_null_unless_supplied(self, db: Database):
        await bootstrap_identity(db, NerveConfig())
        (account,) = await db.list_accounts()
        assert (await db.get_actor_ref(account["actor_id"]))["display_name"] is None

    async def test_owner_display_name_passthrough(self, db: Database):
        await bootstrap_identity(db, NerveConfig(), display_name="alice")
        (account,) = await db.list_accounts()
        owner = await db.get_actor_ref(account["actor_id"])
        assert owner["display_name"] == "alice"
        assert owner["kind"] == "human"
        assert owner["email"] is None


# --------------------------------------------------------------------------- #
#  Idempotency and the lockout rule                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestIdempotency:
    async def test_repeated_starts_find_the_same_rows(self, db: Database):
        config = _cfg(password_hash=_HASH)
        first = await bootstrap_identity(db, config)
        before = await _snapshot(db)
        secret = await db.get_instance_secret(JWT_SECRET_NAME)

        for _ in range(3):
            again = await bootstrap_identity(db, config)
            assert not again.did_bootstrap
            assert again.identity_actions == []

        assert await _snapshot(db) == before
        assert await db.get_instance_secret(JWT_SECRET_NAME) == secret
        assert first.bootstrapped_account and first.generated_jwt_secret

    async def test_exactly_one_account_whatever_the_config_does(self, db: Database):
        await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        for config in (_cfg(password_hash=_HASH), NerveConfig(), _cfg(password_hash=_HASH, jwt_secret=_SECRET)):
            await bootstrap_identity(db, config)
            assert await db.count_accounts() == 1
        assert len(await db.list_actor_refs(kind="human")) == 1
        assert len(await db.list_actor_refs(kind="system")) == 1

    async def test_a_disabled_account_is_not_resurrected(self, db: Database):
        await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        await db.set_account_enabled(account["id"], False)

        report = await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))

        assert not report.bootstrapped_account
        (still,) = await db.list_accounts()
        assert still["id"] == account["id"]
        assert still["enabled"] is False
        assert still["disabled_at"] is not None

    async def test_dry_run_reports_and_writes_nothing(self, db: Database):
        report = await bootstrap_identity(db, _cfg(password_hash=_HASH), dry_run=True)
        assert report.bootstrapped_account and report.generated_jwt_secret
        assert report.identity_actions == [
            "create the local owner account in nerve.db (credential_source=config, no username yet)",
            "copy auth.password_hash onto the account in nerve.db and set "
            "credential_source=local (the hash is copied, not re-hashed — no "
            "password changes)",
            "generate a JWT signing secret into nerve.db — auth.jwt_secret is not "
            "configured (set it to override)",
        ]
        assert await db.count_accounts() == 0
        assert await db.get_local_identity() is None
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        assert pinned_jwt_secret() == ""

        # The real thing then reports in the past tense.
        report = await bootstrap_identity(db, _cfg(password_hash=_HASH))
        assert report.identity_actions[0].startswith("created the local owner account")
        assert report.identity_actions[1].startswith("copied auth.password_hash")
        assert report.identity_actions[2].startswith("generated a JWT signing secret")


@pytest.mark.asyncio
class TestCredentialSourceMirrorsConfiguration:
    """While the owner's credential lives in configuration (``config`` or
    ``none``), the row says which of the two it is right now. Only ``local``
    is the account's own and is never touched."""

    async def test_password_added_after_bootstrap_flips_none_to_config_then_local(
        self, db: Database,
    ):
        """The mirror and 3.5 run in one pass, so a passwordless install that
        gains auth.password_hash does not linger on the transitional value."""
        await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        report = await bootstrap_identity(db, _cfg(password_hash=_HASH, jwt_secret=_SECRET))
        assert report.updated_credential_source
        assert report.migrated_config_credential
        assert not report.bootstrapped_account
        assert report.identity_actions == [
            "set credential_source none → config on the local owner account "
            "(auth.password_hash is now configured)",
            "copied auth.password_hash onto the account in nerve.db and set "
            "credential_source=local (the hash is copied, not re-hashed — no "
            "password changes)",
        ]
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "local"
        assert account["credential"] == _HASH

    async def test_password_removed_flips_config_to_none(self, db: Database):
        """A row still on `config` — an install between this release and the
        last, or one 3.5 could not finish — goes back to `none` when the
        configured hash is removed."""
        await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        await db.set_account_credential(account["id"], credential_source="config")
        report = await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        assert report.updated_credential_source
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "none"

    async def test_a_local_credential_is_never_touched(self, db: Database):
        await bootstrap_identity(db, _cfg(password_hash=_HASH, jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        await db.set_account_credential(
            account["id"], credential_source="local", credential="$2b$12$own-hash",
        )
        for config in (_cfg(jwt_secret=_SECRET), _cfg(password_hash=_HASH, jwt_secret=_SECRET)):
            report = await bootstrap_identity(db, config)
            assert not report.did_bootstrap
        (account,) = await db.list_accounts()
        assert (account["credential_source"], account["credential"]) == ("local", "$2b$12$own-hash")

    async def test_the_mirror_does_not_enable_or_create(self, db: Database):
        await bootstrap_identity(db, _cfg(jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        await db.set_account_enabled(account["id"], False)
        await bootstrap_identity(db, _cfg(password_hash=_HASH, jwt_secret=_SECRET))
        (account,) = await db.list_accounts()
        assert account["credential_source"] == "local"
        assert account["enabled"] is False


# --------------------------------------------------------------------------- #
#  auth.jwt_secret                                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestJwtSecret:
    async def test_configured_secret_is_kept_and_nothing_is_stored(self, db: Database):
        config = _cfg(jwt_secret=_SECRET)
        secret = await ensure_jwt_secret(db, config)
        assert secret == _SECRET
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        assert effective_jwt_secret(config) == _SECRET

    async def test_missing_secret_is_generated_once_and_published(self, db: Database):
        config = NerveConfig()
        report = MigrationReport()
        first = await ensure_jwt_secret(db, config, report=report)
        assert report.generated_jwt_secret
        assert len(first) == 64 and all(c in "0123456789abcdef" for c in first)
        assert await db.get_instance_secret(JWT_SECRET_NAME) == first
        assert effective_jwt_secret(config) == first
        # Every consumer outside the process reads the same value.
        assert read_instance_secret(db.db_path, JWT_SECRET_NAME) == first

        again = MigrationReport()
        assert await ensure_jwt_secret(db, config, report=again) == first
        assert not again.generated_jwt_secret and again.identity_actions == []

    async def test_a_configured_secret_added_later_takes_effect_at_the_next_start(
        self, db: Database,
    ):
        from nerve.gateway.auth import unpin_jwt_secret

        generated = await ensure_jwt_secret(db, NerveConfig())
        config = _cfg(jwt_secret=_SECRET)
        # Same process: the pin holds; the new configured value is reported by
        # a reload and waits for a restart. The stored key is retired at once,
        # though — it is superseded the moment configuration supplies one.
        assert await ensure_jwt_secret(db, config) == generated
        assert effective_jwt_secret(config) == generated
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        # "Restart": the configured value wins from then on.
        unpin_jwt_secret()
        assert await ensure_jwt_secret(db, config) == _SECRET
        assert effective_jwt_secret(config) == _SECRET
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None

    async def test_rotating_to_a_configured_secret_retires_the_stored_one_for_good(
        self, db: Database,
    ):
        """S1 generated → S2 configured → S2 removed again. The old stored key
        must not come back: a token signed with S1 stays invalid under S2 and
        under the fresh S3 the next unconfigured start generates."""
        from fastapi import HTTPException

        from nerve.gateway.auth import (
            SYSTEM_SUBJECT,
            create_system_token,
            decode_token,
            unpin_jwt_secret,
        )

        s1 = await ensure_jwt_secret(db, NerveConfig())
        old_token = create_system_token(s1)
        assert decode_token(old_token, effective_jwt_secret())["sub"] == SYSTEM_SUBJECT

        unpin_jwt_secret()  # restart, S2 configured
        report = MigrationReport()
        assert await ensure_jwt_secret(db, _cfg(jwt_secret=_SECRET), report=report) == _SECRET
        assert report.retired_stored_secret and report.did_bootstrap
        assert report.identity_actions == [
            "retired the database-held signing secret from nerve.db "
            "(auth.jwt_secret is configured and supersedes it)",
        ]
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        assert read_instance_secret(db.db_path, JWT_SECRET_NAME) == ""
        with pytest.raises(HTTPException):
            decode_token(old_token, effective_jwt_secret())

        # A second configured start has nothing left to retire.
        unpin_jwt_secret()
        again = MigrationReport()
        await ensure_jwt_secret(db, _cfg(jwt_secret=_SECRET), report=again)
        assert not again.retired_stored_secret and again.identity_actions == []

        unpin_jwt_secret()  # restart, the key removed from configuration
        s3 = await ensure_jwt_secret(db, NerveConfig())
        assert s3 != s1 and s3 != _SECRET
        assert await db.get_instance_secret(JWT_SECRET_NAME) == s3
        with pytest.raises(HTTPException):
            decode_token(old_token, effective_jwt_secret())

    async def test_dry_run_reports_the_retirement_without_doing_it(self, db: Database):
        from nerve.gateway.auth import unpin_jwt_secret

        generated = await ensure_jwt_secret(db, NerveConfig())
        unpin_jwt_secret()
        report = MigrationReport(dry_run=True)
        await ensure_jwt_secret(db, _cfg(jwt_secret=_SECRET), report=report, dry_run=True)
        assert report.retired_stored_secret
        assert report.identity_actions[0].startswith("retire the database-held signing secret")
        assert await db.get_instance_secret(JWT_SECRET_NAME) == generated
        assert pinned_jwt_secret() == ""

    async def test_lockdown_without_a_configured_secret_also_generates(self, db: Database):
        """1.6 is unconditional: a locked box with no auth.jwt_secret used to
        answer 503 to everything; it now runs with a machine-local secret."""
        config = _cfg(lockdown=True)
        secret = await ensure_jwt_secret(db, config)
        assert secret and effective_jwt_secret(config) == secret

    async def test_dry_run_generates_nothing(self, db: Database):
        report = MigrationReport(dry_run=True)
        assert await ensure_jwt_secret(db, NerveConfig(), report=report, dry_run=True) == ""
        assert report.generated_jwt_secret
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        assert pinned_jwt_secret() == ""


# --------------------------------------------------------------------------- #
#  Lockdown with an environment-referenced hash                                #
# --------------------------------------------------------------------------- #


def _git_repo_with_remote(ws: Path) -> None:
    """What a locked workspace is on a real box; the remote is never contacted."""
    if not shutil.which("git"):
        pytest.skip("git not available")
    subprocess.run(["git", "init", "-q"], cwd=str(ws), check=True, capture_output=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://example.invalid/config.git"],
        cwd=str(ws), check=True, capture_output=True,
    )


@pytest.mark.asyncio
class TestLockdownInstall:
    async def test_env_referenced_hash_is_used_in_place_and_nothing_is_rewritten(
        self, db: Database, tmp_path, monkeypatch,
    ):
        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        _git_repo_with_remote(ws)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        settings = workspace_settings_file(ws)
        settings.write_text(
            "lockdown: true\n"
            "auth:\n"
            "  password_hash: ${NERVE_TEST_PASSWORD_HASH}\n"
            "  jwt_secret: ${NERVE_TEST_JWT_SECRET}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("NERVE_TEST_PASSWORD_HASH", _HASH)
        monkeypatch.setenv("NERVE_TEST_JWT_SECRET", _SECRET)
        before = {p: p.read_bytes() for p in (settings, config_dir / "config.yaml")}

        config = load_config(config_dir)
        assert config.lockdown and config.auth.password_hash == _HASH

        report = await bootstrap_identity(db, config)

        (account,) = await db.list_accounts()
        # 3.5 runs here too: the account gets its own copy of the hash, which is
        # what stops the fleet-managed value being the only credential in play.
        assert account["credential_source"] == "local"
        assert account["credential"] == _HASH
        assert not report.generated_jwt_secret           # the env supplies the secret
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        # **Nothing was written.** Configuration is fleet-managed and the value
        # may be an ${ENV_VAR} the next push reasserts.
        assert {p: p.read_bytes() for p in before} == before
        assert not (config_dir / "config.local.yaml").exists()
        assert not report.scrubbed_config_password
        # ...and the operator is told the value is now inert, by file and key.
        warning = " ".join(report.warnings)
        assert "auth.password_hash" in warning
        assert str(settings) in warning
        assert "lockdown" in warning
        assert "no longer authenticates anybody" in warning

    async def test_a_lockdown_dry_run_shows_the_copy_and_no_write(
        self, db: Database, tmp_path, monkeypatch,
    ):
        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (ws / "config").mkdir(parents=True)
        _git_repo_with_remote(ws)
        (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
        settings = workspace_settings_file(ws)
        settings.write_text(
            "lockdown: true\n"
            "auth:\n"
            "  password_hash: ${NERVE_TEST_PASSWORD_HASH}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("NERVE_TEST_PASSWORD_HASH", _HASH)
        before = {p: p.read_bytes() for p in (settings, config_dir / "config.yaml")}
        config = load_config(config_dir)

        report = await bootstrap_identity(db, config, dry_run=True)

        assert report.migrated_config_credential
        assert not report.scrubbed_config_password
        assert any("copy auth.password_hash" in a for a in report.identity_actions)
        assert any(str(settings) in w for w in report.warnings)
        assert {p: p.read_bytes() for p in before} == before
        assert await db.count_accounts() == 0


# --------------------------------------------------------------------------- #
#  The synchronous path: nerve migrate / maybe_migrate                         #
# --------------------------------------------------------------------------- #


def _install(tmp_path, *, local_yaml: str | None = "{}\n") -> tuple[Path, Path]:
    """A post-wizard install: config.yaml + config.local.yaml, nothing legacy."""
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (ws / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
    if local_yaml is not None:
        (config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
    return config_dir, ws


def _db_rows(db_path: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


class TestSyncPath:
    def test_dry_run_inspects_without_creating_the_database(self, tmp_path):
        config_dir, ws = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        report = migrate(config_dir, workspace=ws, dry_run=True)
        assert not report.did_anything                  # nothing legacy to move
        assert report.bootstrapped_account and report.generated_jwt_secret
        assert report.identity_actions[0].startswith("create the local owner account")
        assert "credential_source=config" in report.identity_actions[0]
        assert not paths.db_path().exists()

    def test_real_run_bootstraps_through_its_own_connection(self, tmp_path):
        config_dir, ws = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        report = migrate(config_dir, workspace=ws)
        assert report.did_bootstrap and not report.did_anything
        assert report.identity_actions[0].startswith("created the local owner account")
        rows = _db_rows(paths.db_path(), "SELECT credential_source, credential, username FROM accounts")
        assert rows == [("local", _HASH, None)]
        # ...and the now-dead configured value is gone from the file it was in,
        # which is still owner-only.
        local_yaml = config_dir / "config.local.yaml"
        assert "password_hash" not in local_yaml.read_text(encoding="utf-8")
        assert local_yaml.stat().st_mode & 0o077 == 0
        assert report.scrubbed_config_password
        assert any("removed auth.password_hash" in a for a in report.identity_actions)
        assert read_instance_secret(paths.db_path(), JWT_SECRET_NAME)

        # Second pass: same rows, nothing reported.
        again = migrate(config_dir, workspace=ws)
        assert not again.did_bootstrap and again.identity_actions == []
        assert _db_rows(paths.db_path(), "SELECT COUNT(*) FROM accounts") == [(1,)]

    def test_fresh_install_is_left_to_the_wizard(self, tmp_path):
        """No config.local.yaml means the wizard has not run. What it writes
        shapes the account, so nothing is bootstrapped from here — and a
        docker launch from the host never leaves a host-side nerve.db behind."""
        config_dir, ws = _install(tmp_path, local_yaml=None)
        report = migrate(config_dir, workspace=ws)
        assert not report.did_bootstrap
        assert not paths.db_path().exists()

    def test_maybe_migrate_bootstraps_and_never_raises(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        report = maybe_migrate(config_dir, workspace=ws)
        assert report is not None and report.bootstrapped_account
        assert report.error is None
        assert _db_rows(paths.db_path(), "SELECT credential_source FROM accounts") == [("none",)]

    def test_a_config_object_is_honoured_when_passed(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        report = migrate(
            config_dir, workspace=ws,
            config=_cfg(password_hash=_HASH, jwt_secret=_SECRET, config_dir=config_dir),
        )
        assert report.bootstrapped_account and not report.generated_jwt_secret
        assert _db_rows(paths.db_path(), "SELECT credential_source FROM accounts") == [("local",)]
        assert read_instance_secret(paths.db_path(), JWT_SECRET_NAME) == ""

    def test_dry_run_on_an_existing_database_sees_the_mirror_too(self, tmp_path):
        config_dir, ws = _install(tmp_path)
        migrate(config_dir, workspace=ws)  # passwordless account exists now
        report = migrate(
            config_dir, workspace=ws, dry_run=True,
            config=_cfg(password_hash=_HASH, config_dir=config_dir),
        )
        assert not report.bootstrapped_account
        assert report.updated_credential_source
        assert report.identity_actions == [
            "set credential_source none → config on the local owner account "
            "(auth.password_hash is now configured)",
            "copy auth.password_hash onto the account in nerve.db and set "
            "credential_source=local (the hash is copied, not re-hashed — no "
            "password changes)",
        ]
        assert not report.generated_jwt_secret  # already stored by the first run
        assert _db_rows(paths.db_path(), "SELECT credential_source FROM accounts") == [("none",)]


class TestCli:
    def test_migrate_dry_run_shows_the_bootstrap_and_writes_nothing(self, tmp_path):
        from nerve.cli import main

        config_dir, _ws = _install(tmp_path, local_yaml=f"auth:\n  password_hash: '{_HASH}'\n")
        result = CliRunner().invoke(main, ["-c", str(config_dir), "migrate", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "would create the local owner account" in result.output
        assert "credential_source=config" in result.output
        assert "would generate a JWT signing secret" in result.output
        assert "Dry run — no changes written." in result.output
        assert not paths.db_path().exists()

    def test_migrate_bootstraps_then_reports_nothing_to_do(self, tmp_path):
        from nerve.cli import main

        config_dir, _ws = _install(tmp_path)
        result = CliRunner().invoke(main, ["-c", str(config_dir), "migrate"])
        assert result.exit_code == 0, result.output
        assert "created the local owner account" in result.output
        assert "generated a JWT signing secret" in result.output
        assert "Migration complete." in result.output
        assert _db_rows(paths.db_path(), "SELECT credential_source FROM accounts") == [("none",)]

        result = CliRunner().invoke(main, ["-c", str(config_dir), "migrate"])
        assert result.exit_code == 0, result.output
        assert "Nothing to migrate" in result.output


# --------------------------------------------------------------------------- #
#  Two bootstraps at once                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestConcurrentBootstrap:
    async def test_two_connections_create_one_owner_and_report_it_once(self, tmp_path):
        """A `nerve migrate` beside a starting daemon: two connections, one
        database. BEGIN IMMEDIATE lets exactly one create the owner, and the
        report has to say what each transaction did — not what a count taken
        before it suggested."""
        import asyncio

        path = tmp_path / "shared.db"
        a, b = Database(path), Database(path)
        await a.connect()
        await b.connect()
        try:
            config = _cfg(jwt_secret=_SECRET)
            ra, rb = await asyncio.gather(
                bootstrap_identity(a, config), bootstrap_identity(b, config),
            )
            winners = [r for r in (ra, rb) if r.bootstrapped_account]
            assert len(winners) == 1
            loser = rb if ra.bootstrapped_account else ra
            assert loser.identity_actions == []
            assert not loser.updated_credential_source

            assert await a.count_accounts() == 1
            assert len(await a.list_actor_refs()) == 2
            ia, ib = await a.get_local_identity(), await b.get_local_identity()
            assert (ia.tenant_id, ia.agent_id, ia.system_actor_id) == (
                ib.tenant_id, ib.agent_id, ib.system_actor_id,
            )
        finally:
            await a.close()
            await b.close()

    async def test_the_caller_that_finds_the_owner_still_mirrors_its_configuration(
        self, tmp_path,
    ):
        """The second half of the race, run sequentially so the outcome is
        deterministic: a caller whose configuration snapshot differs from the
        creator's brings credential_source in line after the transaction."""
        path = tmp_path / "shared.db"
        a, b = Database(path), Database(path)
        await a.connect()
        await b.connect()
        try:
            ra = await bootstrap_identity(a, _cfg(jwt_secret=_SECRET))
            assert ra.bootstrapped_account
            rb = await bootstrap_identity(b, _cfg(password_hash=_HASH, jwt_secret=_SECRET))
            assert not rb.bootstrapped_account
            assert rb.updated_credential_source
            (account,) = await b.list_accounts()
            # ...and 3.5 finishes the move in the same pass.
            assert account["credential_source"] == "local"
        finally:
            await a.close()
            await b.close()
