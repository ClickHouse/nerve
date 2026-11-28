"""Owner upgrades preserve populated scopes and keep runtime roles DML-only."""

import json
import os
from pathlib import Path
import uuid

import pytest

from nerve.db.postgres.version import SCHEMA_VERSION

psycopg = pytest.importorskip("psycopg")
sql = psycopg.sql
make_conninfo = psycopg.conninfo.make_conninfo

SQL_DIR = Path(__file__).parents[2] / "nerve/db/postgres"
LATEST_UPGRADE = (SQL_DIR / "migrations/003_operational_v073.sql").read_text()
UPGRADE = (SQL_DIR / "migrations/002_operational_v061.sql").read_text()


@pytest.fixture
def old_database():
    owner_dsn = os.environ.get("NERVE_TEST_POSTGRES_OWNER_DSN")
    if not owner_dsn:
        pytest.skip("NERVE_TEST_POSTGRES_OWNER_DSN is required for owner upgrade tests")
    name = "nerve_upgrade_" + uuid.uuid4().hex
    with psycopg.connect(owner_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_conninfo(owner_dsn, dbname=name)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            initial = (SQL_DIR / "schema.sql").read_text().split("\\ir ")[0]
            conn.execute(initial)
            conn.execute("SET search_path=nerve_pg,public")
            yield conn
    finally:
        with psycopg.connect(owner_dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def test_upgrade_preserves_scopes_and_backfills_deliveries(old_database):
    conn = old_database
    for workflow in ("one", "two"):
        scope = json.dumps(["test", workflow], separators=(",", ":"))
        conn.execute("SELECT set_config('nerve.scope',%s,false)", (scope,))
        conn.execute("INSERT INTO scopes DEFAULT VALUES")
        conn.execute(
            "INSERT INTO tasks (id,file_path,title,content) VALUES ('t','t.md','Kept','durable')"
        )
        conn.execute(
            "INSERT INTO notifications (id,session_id,type,title,telegram_chat_id,telegram_message_id) VALUES ('n','session','info','Kept','chat','42')"
        )
        conn.execute(
            "INSERT INTO task_statuses (name,label,color,description,is_system,sort_order) VALUES ('pending','Custom','#123456','Keep me',0,7)"
        )
        if workflow == "two":
            conn.execute(
                "INSERT INTO task_statuses (name,label,color,description,is_system,sort_order) VALUES ('backlog','My backlog','#654321','Keep too',1,8)"
            )
    conn.execute(UPGRADE)
    conn.execute(UPGRADE)
    assert conn.execute("SELECT version FROM storage_version").fetchall() == [(2,)]
    assert (
        conn.execute("SELECT content,revision,comment_count FROM tasks").fetchall()
        == [("durable", 1, 0)] * 2
    )
    assert (
        conn.execute(
            "SELECT channel,target,message_id FROM notification_deliveries"
        ).fetchall()
        == [("telegram", "chat", "42")] * 2
    )
    assert (
        conn.execute(
            "SELECT label,description FROM task_statuses WHERE name='pending'"
        ).fetchall()
        == [("Custom", "Keep me")] * 2
    )
    assert conn.execute(
        "SELECT label,description FROM task_statuses WHERE name='backlog' ORDER BY _scope"
    ).fetchall() == [
        ("Backlog", "Not ready for work yet. No built-in job picks it up."),
        ("My backlog", "Keep too"),
    ]
    rows = conn.execute(
        "SELECT _scope,array_agg(name ORDER BY sort_order) FROM task_statuses GROUP BY _scope ORDER BY _scope"
    ).fetchall()
    assert rows[0][1] == ["backlog", "pending", "plan_review"]
    assert rows[1][1] == ["pending", "plan_review", "backlog"]
    assert conn.execute(
        "SELECT count(*) FROM pg_class WHERE relnamespace='nerve_pg'::regnamespace AND relname IN ('task_comments','session_agents','channel_observations') AND relrowsecurity AND relforcerowsecurity"
    ).fetchone() == (3,)


def test_failed_upgrade_is_atomic_and_can_be_retried(old_database):
    conn = old_database
    conn.execute("CREATE TABLE task_comments (collision text)")
    with pytest.raises(psycopg.errors.DuplicateTable):
        conn.execute(UPGRADE)
    conn.execute("ROLLBACK")
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (1,)
    assert conn.execute(
        "SELECT to_regclass('nerve_pg.channel_observations')"
    ).fetchone() == (None,)
    conn.execute("DROP TABLE task_comments")
    conn.execute(UPGRADE)
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (2,)


def test_unknown_version_is_refused(old_database):
    conn = old_database
    conn.execute("UPDATE storage_version SET version=999")
    with pytest.raises(
        psycopg.errors.RaiseException, match="Expected PostgreSQL schema 1"
    ):
        conn.execute(UPGRADE)
    conn.execute("ROLLBACK")
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (999,)


@pytest.mark.asyncio
async def test_runtime_refuses_old_schema_then_uses_upgraded_data(old_database):
    from psycopg.conninfo import conninfo_to_dict
    from nerve.db.postgres import PostgresDatabase

    conn = old_database
    runtime_dsn = make_conninfo(
        os.environ["NERVE_TEST_POSTGRES_DSN"], dbname=conn.info.dbname
    )
    role = sql.Identifier(conninfo_to_dict(runtime_dsn)["user"])

    def grant_runtime():
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA nerve_pg TO {}").format(role))
        conn.execute(
            sql.SQL(
                "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA nerve_pg TO {}"
            ).format(role)
        )
        conn.execute(
            sql.SQL(
                "REVOKE INSERT,UPDATE,DELETE ON nerve_pg.storage_version FROM {}"
            ).format(role)
        )
        conn.execute(
            sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA nerve_pg TO {}").format(
                role
            )
        )

    grant_runtime()
    db = PostgresDatabase(runtime_dsn)
    with pytest.raises(RuntimeError, match="apply owner migrations"):
        await db.connect()
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (1,)
    conn.execute(UPGRADE)
    with pytest.raises(RuntimeError, match="apply owner migrations"):
        await db.connect()
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (2,)
    conn.execute(LATEST_UPGRADE)
    grant_runtime()
    await db.connect()
    try:
        await db.upsert_task("t", "t.md", "Upgraded", content="retained body")
        comment = await db.add_task_comment("t", "Reviewed")
        assert comment["body"] == "Reviewed"
        assert (await db.get_task("t"))["comment_count"] == 1
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await db._write("CREATE TABLE nerve_pg.not_allowed (id integer)")
    finally:
        await db.close()


def test_v3_backfills_each_scope_and_is_retryable(old_database):
    conn = old_database
    conn.execute(UPGRADE)
    for workflow in ("one", "two"):
        scope = json.dumps(["test", workflow], separators=(",", ":"))
        conn.execute("SELECT set_config('nerve.scope',%s,false)", (scope,))
        conn.execute("INSERT INTO scopes DEFAULT VALUES")
        conn.execute(
            "INSERT INTO sessions (id,sdk_session_id) VALUES ('parent',%s)", (workflow,)
        )
        for fork, own in (
            ("fork-pending", workflow),
            ("fork-independent", "own"),
            ("fork-mapped", None),
        ):
            conn.execute(
                "INSERT INTO sessions (id,parent_session_id,sdk_session_id) VALUES (%s,'parent',%s)",
                (fork, own),
            )
        message = conn.execute(
            "INSERT INTO messages (session_id,role,native_turn_id) VALUES ('parent','user',%s) RETURNING id",
            (workflow + "-turn",),
        ).fetchone()[0]
        conn.execute(
            "UPDATE sessions SET forked_from_message=%s WHERE _scope=%s AND id='fork-mapped'",
            (str(message), scope),
        )
        conn.execute(
            "INSERT INTO tasks (id,file_path,title) VALUES ('t','t.md','Kept')"
        )
        conn.execute(
            "INSERT INTO task_events (task_id,to_status,actor,created_at) VALUES ('t','pending','parent','2026-01-01')"
        )
        conn.execute(
            "INSERT INTO session_events (session_id,event_type,details) VALUES ('parent','title_changed',%s)",
            (
                json.dumps(
                    {"reason": "generated" if workflow == "one" else "placeholder"}
                ),
            ),
        )
        conn.execute(
            "INSERT INTO cron_logs (job_id,output) VALUES ('source:inbox','12 ingested, 3 dropped by guardrail')"
        )
        conn.execute(
            "INSERT INTO cron_logs (job_id,output) VALUES ('agent','12 ingested')"
        )
    conn.execute(LATEST_UPGRADE)
    conn.execute(LATEST_UPGRADE)
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (
        SCHEMA_VERSION,
    )
    assert conn.execute(
        "SELECT records FROM cron_logs WHERE job_id='source:inbox'"
    ).fetchall() == [(15,), (15,)]
    assert conn.execute(
        "SELECT records FROM cron_logs WHERE job_id='agent'"
    ).fetchall() == [(None,), (None,)]
    assert conn.execute("SELECT created_session_id FROM tasks").fetchall() == [
        ("parent",),
        ("parent",),
    ]
    for workflow in ("one", "two"):
        scope = json.dumps(["test", workflow], separators=(",", ":"))
        rows = conn.execute(
            "SELECT id,sdk_session_id,fork_source_native_id,fork_source_turn_id,title_source FROM sessions WHERE _scope=%s ORDER BY id",
            (scope,),
        ).fetchall()
        assert rows == [
            ("fork-independent", "own", None, None, None),
            ("fork-mapped", None, workflow, workflow + "-turn", None),
            ("fork-pending", None, workflow, None, None),
            (
                "parent",
                workflow,
                None,
                None,
                "generated" if workflow == "one" else "placeholder",
            ),
        ]
    assert conn.execute(
        "SELECT count(*) FROM pg_class WHERE relnamespace='nerve_pg'::regnamespace AND relname IN ('push_subscriptions','channel_reply_routes','widget_states') AND relrowsecurity AND relforcerowsecurity"
    ).fetchone() == (3,)


def test_v3_rolls_back_before_retry(old_database):
    conn = old_database
    conn.execute(UPGRADE)
    conn.execute("CREATE TABLE widget_states (collision text)")
    with pytest.raises(psycopg.errors.DuplicateTable):
        conn.execute(LATEST_UPGRADE)
    conn.execute("ROLLBACK")
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (2,)
    assert conn.execute(
        "SELECT to_regprocedure('nerve_pg.json_valid(text)')"
    ).fetchone() == (None,)
    assert conn.execute(
        "SELECT to_regclass('nerve_pg.push_subscriptions')"
    ).fetchone() == (None,)
    conn.execute("DROP TABLE widget_states")
    conn.execute(LATEST_UPGRADE)
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (
        SCHEMA_VERSION,
    )


def test_v3_rejects_wrong_version(old_database):
    conn = old_database
    with pytest.raises(
        psycopg.errors.RaiseException, match="Expected PostgreSQL schema 2"
    ):
        conn.execute(LATEST_UPGRADE)
    conn.execute("ROLLBACK")
    assert conn.execute("SELECT version FROM storage_version").fetchone() == (1,)
