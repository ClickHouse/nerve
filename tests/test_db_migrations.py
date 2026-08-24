"""The upgrade path an existing installation actually takes.

A fresh database applies every migration file, whatever version each one
claims, so a duplicated version number is invisible there. The runner skips
any migration at or below the version already recorded, which is why only an
upgrade from an older database shows a collision.
"""

from __future__ import annotations

import importlib

import aiosqlite
import pytest

from nerve.db import Database
from nerve.db.migrations.runner import discover_migrations

# The migration under test, found by suffix so that renumbering the file
# still selects it — and still fails these tests if the number is too low.
_SLACK_MIGRATION_SUFFIX = "_slack_notifications"


def _slack_migration() -> tuple[int, str]:
    found = [
        (v, name) for v, name in discover_migrations()
        if name.endswith(_SLACK_MIGRATION_SUFFIX)
    ]
    assert len(found) == 1, f"expected one Slack migration, found {found}"
    return found[0]


async def _build_database_without(path, skipped: str) -> int:
    """Create the schema an install had before *skipped* was written.

    Applies every other migration in order and stamps the version at the
    highest of them — the state a running installation upgrades from.
    Returns that version.
    """
    others = [(v, name) for v, name in discover_migrations() if name != skipped]
    assert others, "no migrations discovered"
    stamp = max(v for v, _ in others)
    async with aiosqlite.connect(str(path)) as db:
        for _version, module_name in others:
            module = importlib.import_module(f"nerve.db.migrations.{module_name}")
            await module.up(db)
        await db.execute(
            "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
            (stamp,),
        )
        await db.commit()
    return stamp


async def _columns(db: Database, table: str) -> set[str]:
    async with db.db.execute(f"PRAGMA table_info({table})") as cursor:
        return {row[1] for row in await cursor.fetchall()}


class TestMigrationVersions:
    def test_no_two_migrations_claim_the_same_version(self):
        # Two files at the same version both run on a fresh database and the
        # second is skipped on every upgrade, so the collision only shows up
        # on installs that already exist.
        versions = [v for v, _ in discover_migrations()]
        assert len(versions) == len(set(versions))

@pytest.mark.asyncio
class TestSlackNotificationUpgrade:
    async def test_an_existing_database_gains_the_slack_delivery_columns(
        self, tmp_path,
    ):
        path = tmp_path / "upgrade.db"
        await _build_database_without(path, _slack_migration()[1])

        db = Database(path)
        await db.connect()
        try:
            columns = await _columns(db, "notifications")
        finally:
            await db.close()
        assert {"slack_message_id", "slack_channel_id"} <= columns

    async def test_delivery_ids_can_be_written_after_the_upgrade(self, tmp_path):
        # Without the columns the Slack post still succeeds and only the
        # follow-up write fails, so a card the workspace can see is recorded
        # as undelivered and expiry edits lose their target.
        path = tmp_path / "upgrade.db"
        await _build_database_without(path, _slack_migration()[1])

        db = Database(path)
        await db.connect()
        try:
            await db.create_notification("n1", "s1", "question", "Ship it?")
            await db.update_notification(
                "n1", slack_message_id="1699887766.123456", slack_channel_id="C1",
            )
            row = await db.get_notification("n1")
        finally:
            await db.close()

        assert row is not None
        assert row["slack_message_id"] == "1699887766.123456"
        assert row["slack_channel_id"] == "C1"
