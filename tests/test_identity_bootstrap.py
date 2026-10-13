"""Configuration-aware first-account and signing-secret bootstrap."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from click.testing import CliRunner

from nerve import paths
from nerve.config import AuthConfig, NerveConfig
from nerve.db import Database
from nerve.db.accounts import JWT_SECRET_NAME, read_instance_secret
from nerve.gateway.auth import effective_jwt_secret, pinned_jwt_secret, unpin_jwt_secret
from nerve.migrate import MigrationReport, bootstrap_identity, ensure_jwt_secret, migrate

_HASH = "$2b$12$abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTU"
_SECRET = "configured-secret-padded-to-thirty-two-bytes"


def _config(*, password: bool = False, secret: str = "") -> NerveConfig:
    return NerveConfig(
        auth=AuthConfig(
            password_hash=_HASH if password else "",
            jwt_secret=secret,
        )
    )


async def _accounts(db: Database) -> list[dict]:
    return await db._account_rows()


async def _actors(db: Database, kind: str) -> list[dict]:
    async with db.db.execute(
        "SELECT * FROM actor_refs WHERE kind = ? ORDER BY created_at, id", (kind,)
    ) as cursor:
        return [dict(row) async for row in cursor]


@pytest.mark.asyncio
@pytest.mark.parametrize(("password", "source"), [(True, "local"), (False, "none")])
async def test_bootstrap_creates_only_the_first_human_account(
    db: Database, password, source,
):
    system_id = db.system_actor_id
    report = await bootstrap_identity(
        db, _config(password=password, secret=_SECRET), display_name="alice"
    )

    assert report.bootstrapped_account and not report.generated_jwt_secret
    assert len(await _actors(db, "system")) == 1
    assert db.system_actor_id == system_id
    (human,) = await _actors(db, "human")
    (account,) = await _accounts(db)
    assert account["id"] != account["actor_id"] == human["id"]
    assert (human["display_name"], account["credential_source"]) == ("alice", source)
    assert account["credential"] == (_HASH if password else None)
    assert account["enabled"] is True and account["created_at"]

    again = await bootstrap_identity(db, _config(password=password, secret=_SECRET))
    assert not again.did_bootstrap
    assert [row["id"] for row in await _accounts(db)] == [account["id"]]
    assert [row["id"] for row in await _actors(db, "human")] == [human["id"]]


@pytest.mark.asyncio
async def test_bootstrap_rejects_database_held_credentials(db: Database):
    with pytest.raises(ValueError, match="bootstrap credential_source"):
        await db._bootstrap_first_account(credential_source="local")
    assert await _accounts(db) == []


@pytest.mark.asyncio
async def test_disabled_account_is_not_recreated_and_mirror_preserves_state(db: Database):
    await bootstrap_identity(db, _config(secret=_SECRET))
    (account,) = await _accounts(db)
    await db._write("UPDATE accounts SET enabled = 0 WHERE id = ?", (account["id"],))

    report = await bootstrap_identity(db, _config(password=True, secret=_SECRET))
    (still,) = await _accounts(db)
    assert not report.bootstrapped_account and report.updated_credential_source
    assert report.migrated_config_credential
    assert still["id"] == account["id"]
    assert still["enabled"] is False
    assert (still["credential_source"], still["credential"]) == ("local", _HASH)


@pytest.mark.asyncio
async def test_local_credential_is_never_mirrored_from_configuration(db: Database):
    await bootstrap_identity(db, _config(secret=_SECRET))
    (account,) = await _accounts(db)
    await db._write(
        "UPDATE accounts SET credential_source = 'local', credential = ? WHERE id = ?",
        ("$2b$12$owned", account["id"]),
    )
    for config in (_config(password=True, secret=_SECRET), _config(secret=_SECRET)):
        assert not (await bootstrap_identity(db, config)).updated_credential_source
    (account,) = await _accounts(db)
    assert (account["credential_source"], account["credential"]) == (
        "local", "$2b$12$owned",
    )


@pytest.mark.asyncio
async def test_dry_run_reports_without_writing(db: Database):
    report = await bootstrap_identity(db, _config(password=True), dry_run=True)
    assert report.bootstrapped_account and report.generated_jwt_secret
    assert report.migrated_config_credential
    assert await _accounts(db) == []
    assert await _actors(db, "human") == []
    assert await db._get_instance_secret(JWT_SECRET_NAME) is None
    assert pinned_jwt_secret() == ""


@pytest.mark.asyncio
async def test_generated_secret_is_stable_and_visible_to_other_processes(db: Database):
    report = MigrationReport()
    first = await ensure_jwt_secret(db, NerveConfig(), report=report)
    assert report.generated_jwt_secret
    assert len(first) == 64
    assert await db._get_instance_secret(JWT_SECRET_NAME) == first
    assert read_instance_secret(db.db_path, JWT_SECRET_NAME) == first
    assert effective_jwt_secret() == first

    again = MigrationReport()
    assert await ensure_jwt_secret(db, NerveConfig(), report=again) == first
    assert not again.did_bootstrap


@pytest.mark.asyncio
async def test_instance_secret_first_writer_wins(db: Database):
    assert read_instance_secret(db.db_path, JWT_SECRET_NAME) == ""
    assert await db._ensure_instance_secret(JWT_SECRET_NAME, "first") == "first"
    assert await db._ensure_instance_secret(JWT_SECRET_NAME, "second") == "first"


@pytest.mark.asyncio
async def test_configured_secret_retires_stored_secret_permanently(db: Database):
    first = await ensure_jwt_secret(db, NerveConfig())
    unpin_jwt_secret()
    report = MigrationReport()
    assert await ensure_jwt_secret(db, _config(secret=_SECRET), report=report) == _SECRET
    assert report.retired_stored_secret
    assert await db._get_instance_secret(JWT_SECRET_NAME) is None

    unpin_jwt_secret()
    replacement = await ensure_jwt_secret(db, NerveConfig())
    assert replacement not in {first, _SECRET}


@pytest.mark.asyncio
async def test_concurrent_bootstrap_has_exactly_one_winner(tmp_path):
    path = tmp_path / "nerve.db"
    seed = Database(path)
    await seed.connect()
    system_id = seed.system_actor_id
    await seed.close()

    left, right = Database(path), Database(path)
    await asyncio.gather(left.connect(), right.connect())
    try:
        reports = await asyncio.gather(
            bootstrap_identity(left, _config(secret=_SECRET)),
            bootstrap_identity(right, _config(secret=_SECRET)),
        )
        assert sum(report.bootstrapped_account for report in reports) == 1
        assert len(await _accounts(left)) == 1
        assert len(await _actors(left, "human")) == 1
        assert left.system_actor_id == right.system_actor_id == system_id
    finally:
        await asyncio.gather(left.close(), right.close())


def _install(tmp_path: Path, *, local_yaml: str = "{}\n") -> tuple[Path, Path]:
    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (workspace / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\n", encoding="utf-8"
    )
    (config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
    return config_dir, workspace


@pytest.mark.parametrize("dry_run", [True, False], ids=["dry-run", "real"])
def test_migrate_reports_existing_identity_without_writing(tmp_path, dry_run):
    """Only the gateway and ``nerve init`` write identity state. ``migrate``
    reports what the gateway will do, in both modes."""
    from nerve.migrate import bootstrap_identity_sync

    config_dir, workspace = _install(tmp_path)
    bootstrap_identity_sync(_config())
    before = paths.db_path().read_bytes()

    report = migrate(
        config_dir,
        workspace=workspace,
        config=_config(password=True),
        dry_run=dry_run,
    )

    assert not report.bootstrapped_account
    assert report.updated_credential_source
    assert report.migrated_config_credential
    assert not report.generated_jwt_secret
    assert paths.db_path().read_bytes() == before


@pytest.mark.parametrize("argv", [["migrate", "--dry-run"], ["migrate"]], ids=["dry-run", "real"])
def test_cli_migrate_reports_bootstrap_without_creating_database(tmp_path, argv):
    from nerve.cli import main

    config_dir, _workspace = _install(
        tmp_path,
        local_yaml=f"auth:\n  password_hash: '{_HASH}'\n",
    )
    result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])

    assert result.exit_code == 0, result.output
    assert "At its next start, the gateway will:" in result.output
    assert "- create the local owner account" in result.output
    assert "credential_source=config" in result.output
    assert "- copy auth.password_hash" in result.output
    assert "- generate a JWT signing secret" in result.output
    assert not paths.db_path().exists()
