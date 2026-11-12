"""Boot a throwaway, passwordless Nerve with synthetic data for UI snapshot tests.

Usage: python scripts/ui_snapshot_server.py <state-dir> <port>

Everything lives under <state-dir>, which is wiped first. Timestamps are fixed
so screenshots do not change from run to run.
"""

import asyncio
import json
import os
import shutil
import sys
from pathlib import Path


def write_config(root: Path, port: int) -> Path:
    config_dir = root / "config"
    workspace = root / "workspace"
    (workspace / "config").mkdir(parents=True)
    config_dir.mkdir()
    (workspace / "SOUL.md").write_text("# SOUL.md - Who You Are\n")
    (workspace / "config" / "settings.yaml").write_text(json.dumps({
        "timezone": "UTC",
        "gateway": {"host": "127.0.0.1", "port": port},
        "agent": {"model_discovery": False},
        "sync": {"github": {"enabled": False}},
    }))
    (config_dir / "config.yaml").write_text(json.dumps({"workspace": str(workspace)}))
    # Without it `start -f` takes the directory for a fresh install and runs the setup wizard.
    (config_dir / "config.local.yaml").write_text("{}\n")
    return config_dir


def minute(m: int) -> str:
    return f"2026-01-15T10:{m:02d}:00+00:00"


async def seed(db_path: Path, workspace: Path) -> None:
    from nerve.db import Database
    from nerve.identity import actor_for_account
    from nerve.session_origin import (
        DETAIL_FORK_OF, DETAIL_HOOK, TRIGGER_API, TRIGGER_CHAT,
        Origin, channel_origin, external_origin, plan_origin,
    )

    db = Database(db_path, workspace=workspace)
    await db.connect()
    try:
        owner = (await db.list_accounts())[0]
        user = await actor_for_account(db, owner["id"])

        sessions = [
            ("snap-release", "Ship release 1.4", 18, None, "web", Origin(TRIGGER_CHAT)),
            ("snap-fork", "Shorter changelog", 16, "snap-release", "web",
             Origin(TRIGGER_CHAT, {DETAIL_FORK_OF: "snap-release"})),
            ("snap-slack", "Release sign-off", 14, "snap-release", "slack",
             channel_origin("slack", "slack:C0SNAP:1700000000.000100", "release")),
            ("snap-plan", "Pin the clock in the date parser test", 12, "snap-release", "web",
             plan_origin("plan-snap-1", "task-snap-1")),
            ("snap-chat", "Fix the flaky date parser test", 10, "snap-plan", "web", Origin(TRIGGER_CHAT)),
            ("snap-codex", "Tidy the date helpers", 8, "snap-release", "external", external_origin("codex")),
            ("snap-deploy", "Nightly deploy failed", 6, None, "hook", Origin(TRIGGER_API, {DETAIL_HOOK: "deploy"})),
            ("snap-telegram", "Weekend trip ideas", 4, None, "telegram",
             channel_origin("telegram", "telegram:1001", "Trips")),
            ("snap-trip-fork", "A cheaper trip", 3, "snap-telegram", "web",
             Origin(TRIGGER_CHAT, {DETAIL_FORK_OF: "snap-telegram"})),
            ("snap-email", "Invoice from the print shop", 2, None, "email", channel_origin("email", "email:1001")),
            ("snap-older", "Draft the release notes", 0, None, "web", Origin(TRIGGER_CHAT)),
        ]
        for session_id, title, _last_activity_minute, parent, source, origin in sessions:
            await db.create_session(session_id, title=title, source=source, parent_session_id=parent,
                                    actor=user, origin=origin)

        await db.add_message(
            "snap-chat", "user", "The date parser test fails about once a week in CI. Can you find out why?",
            created_at=minute(1), actor=user,
        )
        await db.add_message(
            "snap-chat", "assistant",
            "The test builds its expected value from the local clock, so it fails when CI runs across midnight UTC.",
            blocks=[
                {"type": "tool_call", "tool": "Bash", "input": {"command": "grep -rn 'parse_date' tests/"},
                 "tool_use_id": "tu1", "result": "tests/test_dates.py:12: assert parse_date(s) == today()",
                 "is_error": False},
                {"type": "tool_call", "tool": "Read", "input": {"file_path": "tests/test_dates.py"},
                 "tool_use_id": "tu2", "result": "def test_parse():\n    s = today().isoformat()\n    ...",
                 "is_error": False},
                {"type": "text", "content": (
                    "The test builds its expected value from the local clock, so it fails when CI runs "
                    "across midnight UTC.\n\nFix: pass a fixed date into the test instead of `today()`."
                )},
            ],
            created_at=minute(2), actor=None,
        )
        await db.add_message("snap-older", "user", "Draft release notes from the merged PRs.",
                             created_at=minute(0), actor=user)

        await db.upsert_task("task-snap-1", "memory/tasks/active/task-snap-1.md",
                             "Pin the clock in the date parser test", status="pending", tags="tests")
        await db.upsert_task("task-snap-2", "memory/tasks/active/task-snap-2.md",
                             "Draft the release notes", status="in_progress", tags="docs")
        await db.create_plan("plan-snap-1", "task-snap-1",
                             "# Plan\n\n1. Inject a fixed date.\n2. Run the test 100 times.")
        await db.create_notification("notif-snap-1", "snap-deploy", "question",
                                     "Roll back the nightly deploy?",
                                     body="The smoke test failed on two of five hosts.",
                                     options=["Roll back", "Wait"])
        await db.update_notification("notif-snap-1", channels_delivered='["web"]')
        # Otherwise every browser's first WebSocket connect creates a session with a random id.
        await db.set_channel_session("web:default", "snap-chat")

        await pin_timestamps(db, {session_id: minute(m) for session_id, _, m, *_ in sessions})
    finally:
        await db.close()


async def pin_timestamps(db, session_times: dict[str, str]) -> None:
    for session_id, at in session_times.items():
        await db.db.execute(
            "UPDATE sessions SET created_at = ?, updated_at = ?, last_activity_at = ? WHERE id = ?",
            (at, at, at, session_id),
        )
    for table, columns in {
        "tasks": ("created_at", "updated_at"),
        "plans": ("created_at",),
        "notifications": ("created_at",),
    }.items():
        for column in columns:
            await db.db.execute(f"UPDATE {table} SET {column} = ?", (minute(0),))
    await db.db.commit()


def main() -> None:
    root, port = Path(sys.argv[1]).resolve(), int(sys.argv[2])
    shutil.rmtree(root, ignore_errors=True)
    config_dir = write_config(root, port)
    os.environ["NERVE_HOME"] = str(root / "home")
    (root / "home").mkdir(mode=0o700)

    from nerve.config import load_config, set_config
    from nerve.migrate import bootstrap_identity_sync
    from nerve.paths import db_path

    config = load_config(config_dir)
    set_config(config)
    bootstrap_identity_sync(config, display_name="Alice", passwordless=True)
    asyncio.run(seed(db_path(), root / "workspace"))

    os.execvp(sys.executable, [sys.executable, "-m", "nerve", "-c", str(config_dir), "start", "-f"])


if __name__ == "__main__":
    main()
