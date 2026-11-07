from nerve.db.postgres.connection import translate


def test_placeholders_leave_literals_and_comments_intact():
    sql, _ = translate("SELECT '?', '100%', \"?\" FROM tasks WHERE id=? -- ?")
    assert sql == "SELECT '?', '100%%', \"?\" FROM tasks WHERE id=%s -- ?"


def test_upsert_includes_scope():
    sql, _ = translate(
        "INSERT INTO tasks (id,title) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET title=excluded.title"
    )
    assert "ON CONFLICT (_scope, id)" in sql
