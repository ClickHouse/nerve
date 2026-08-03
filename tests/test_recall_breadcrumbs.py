"""Tests for recall category breadcrumbs + memory_expand_category drill-down.

memU categories are rolled-up topic *documents* (often 5–20KB). recall must
surface them as short navigable breadcrumbs, never dump the document — that
blows past the harness tool-output limit. These tests lock in that contract
and the drill-down path that replaces the lost detail.
"""

from __future__ import annotations

import gc
import json
import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.memory import (
    _clip_to_budget,
    memory_expand_category_handler,
    memory_recall_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import MemUBridge, _category_breadcrumb


def _make_config(tmp_path: Path) -> NerveConfig:
    config = NerveConfig()
    config.memory = MemoryConfig(sqlite_dsn=f"sqlite:///{tmp_path / 'memu.sqlite'}")
    config.anthropic_api_key = "test-key"
    return config


def _stub_bridge(config: NerveConfig) -> MemUBridge:
    """A MemUBridge marked available with a mockable _service."""
    bridge = MemUBridge(config)
    bridge._available = True
    bridge._service = MagicMock()
    return bridge


def _ctx(bridge) -> ToolContext:
    return ToolContext(
        session_id="s-1",
        workspace=Path("/tmp/ws"),
        db=None,
        memory_bridge=bridge,
        config=None,
    )


# --- pure helpers ----------------------------------------------------------


def test_breadcrumb_prefers_description() -> None:
    crumb = _category_breadcrumb(
        name="preferences",
        description="Communication style and tool preferences",
        summary="# preferences\n\n## A\n- huge\n" + ("x" * 5000),
    )
    assert crumb == "Communication style and tool preferences"
    assert len(crumb) < 200


def test_breadcrumb_falls_back_to_first_summary_line() -> None:
    crumb = _category_breadcrumb(
        name="agent_ops",
        description="",
        summary="# agent_ops\n\n- First real fact [ref:abc123]\n- second",
    )
    # header '#' skipped, first bullet used, [ref:] stripped
    assert "First real fact" in crumb
    assert "ref:" not in crumb
    assert "#" not in crumb


def test_breadcrumb_truncates_long_text() -> None:
    crumb = _category_breadcrumb("c", "y" * 500, "")
    assert len(crumb) <= 200
    assert crumb.endswith("…")


def test_clip_to_budget_passes_small_text() -> None:
    assert _clip_to_budget("hello", max_bytes=100) == "hello"


def test_clip_to_budget_truncates_large_text() -> None:
    out = _clip_to_budget("a" * 50_000, max_bytes=1000)
    assert len(out.encode("utf-8")) <= 1000 + 64
    assert "truncated" in out


# --- bridge.recall() -------------------------------------------------------


@pytest.mark.asyncio
async def test_recall_categories_become_breadcrumbs(tmp_path) -> None:
    config = _make_config(tmp_path)
    bridge = _stub_bridge(config)

    fat_summary = "# preferences\n\n" + ("- a giant bullet\n" * 2000)  # ~30KB
    bridge._service.retrieve = AsyncMock(return_value={
        "items": [
            {"id": "i1", "memory_type": "profile", "summary": "Alice lives in Metropolis"},
            {"id": "i2", "memory_type": "behavior", "summary": "Prefers dark mode"},
        ],
        "categories": [
            {
                "id": "c1",
                "name": "preferences",
                "description": "How things should be done",
                "summary": fat_summary,
            },
        ],
    })

    out = await bridge.recall("prefs", limit=10, category_limit=5)

    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    assert len(items) == 2
    assert items[0]["summary"] == "Alice lives in Metropolis"  # full content kept
    assert len(cats) == 1
    cat = cats[0]
    assert cat["id"] == "cat:c1"
    assert cat["name"] == "preferences"
    assert cat["summary"] == "How things should be done"  # breadcrumb, not the doc
    # The fat document must not leak anywhere into the result.
    assert "giant bullet" not in repr(out)


@pytest.mark.asyncio
async def test_recall_caps_items_and_categories(tmp_path) -> None:
    config = _make_config(tmp_path)
    bridge = _stub_bridge(config)
    bridge._service.retrieve = AsyncMock(return_value={
        "items": [
            {"id": f"i{n}", "memory_type": "knowledge", "summary": f"fact {n}"}
            for n in range(20)
        ],
        "categories": [
            {"id": f"c{n}", "name": f"cat{n}", "description": f"desc {n}", "summary": "x"}
            for n in range(20)
        ],
    })

    out = await bridge.recall("q", limit=3, category_limit=2)
    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    assert len(items) == 3
    assert len(cats) == 2
    # items come before categories
    assert out[0]["type"] != "category"


# --- bridge.expand_category() ---------------------------------------------


def _create_category_schema(db_path: str) -> None:
    db = sqlite3.connect(db_path)
    db.executescript(
        """
        CREATE TABLE memu_memory_items (
            id TEXT PRIMARY KEY, memory_type TEXT, summary TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE memu_memory_categories (
            id TEXT PRIMARY KEY, name TEXT, description TEXT, summary TEXT
        );
        CREATE TABLE memu_category_items (
            id TEXT PRIMARY KEY, item_id TEXT, category_id TEXT
        );
        """
    )
    db.commit()
    db.close()


def _seed_category(db_path: str) -> None:
    db = sqlite3.connect(db_path)
    db.execute(
        "INSERT INTO memu_memory_categories (id, name, description) VALUES (?,?,?)",
        ("cat-pref", "preferences", "How things should be done"),
    )
    rows = [
        ("it-1", "profile", "Likes the color teal", "2026-06-01 10:00:00"),
        ("it-2", "behavior", "Prefers dark mode", "2026-06-02 10:00:00"),
        ("it-3", "profile", "Drinks black coffee", "2026-05-30 10:00:00"),
    ]
    for iid, mt, summ, upd in rows:
        db.execute(
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at, updated_at) "
            "VALUES (?,?,?,?,?)",
            (iid, mt, summ, upd, upd),
        )
        db.execute(
            "INSERT INTO memu_category_items (id, item_id, category_id) VALUES (?,?,?)",
            (f"link-{iid}", iid, "cat-pref"),
        )
    db.commit()
    db.close()


@pytest.mark.asyncio
async def test_expand_category_returns_recent_items(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat:cat-pref", limit=2)

    assert result["name"] == "preferences"
    assert result["total"] == 3
    assert len(result["items"]) == 2
    # most-recent-first: it-2 (Jun 2) then it-1 (Jun 1)
    assert [i["id"] for i in result["items"]] == ["it-2", "it-1"]


@pytest.mark.asyncio
async def test_expand_category_keyword_filter(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat-pref", query="coffee", limit=10)
    assert [i["id"] for i in result["items"]] == ["it-3"]


@pytest.mark.asyncio
async def test_expand_category_unknown_id(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat:nope")
    assert result["name"] is None
    assert result["items"] == []


# --- read isolation: one logical result must come from ONE snapshot ---------
#
# Python sqlite3 with the default isolation_level="" emits no BEGIN for reads,
# so every SELECT on a connection is its own snapshot. These tests interleave a
# COMMITTED write between the statements of one logical read and assert the
# parts still agree. Each runs in both journal modes, because the writer's
# ability to proceed while a read transaction is open is the property the
# explicit-BEGIN approach rests on.

JOURNAL_MODES = ["wal", "delete"]

# The full memU read surface the gateway snapshot scans, which is wider than
# the three tables `_create_category_schema` needs.
_SNAPSHOT_SCHEMA = """
CREATE TABLE memu_memory_items (
    id TEXT PRIMARY KEY, memory_type TEXT, summary TEXT, resource_id TEXT,
    created_at TEXT, updated_at TEXT, happened_at TEXT
);
CREATE TABLE memu_memory_categories (
    id TEXT PRIMARY KEY, name TEXT, description TEXT, summary TEXT
);
CREATE TABLE memu_category_items (
    id TEXT PRIMARY KEY, item_id TEXT, category_id TEXT
);
CREATE TABLE memu_resources (
    id TEXT PRIMARY KEY, url TEXT, modality TEXT, caption TEXT, created_at TEXT
);
"""


def _seed_snapshot(db_path: str, journal_mode: str) -> None:
    """Three items, each linked to one category and one resource."""
    db = sqlite3.connect(db_path)
    # PRAGMA journal_mode returns the mode now in force. Assert it took: a silent
    # no-op would invert the wal/delete writer-progress expectations below.
    effective = db.execute(f"PRAGMA journal_mode={journal_mode}").fetchone()[0]
    assert effective.lower() == journal_mode, (
        f"asked for journal_mode={journal_mode} but the database is in {effective}"
    )
    db.executescript(_SNAPSHOT_SCHEMA)
    db.execute(
        "INSERT INTO memu_memory_categories (id, name, description) VALUES (?,?,?)",
        ("cat-pref", "preferences", "How things should be done"),
    )
    for n in (1, 2, 3):
        stamp = f"2026-06-0{n} 10:00:00"
        db.execute(
            "INSERT INTO memu_memory_items "
            "(id, memory_type, summary, resource_id, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?)",
            (f"it-{n}", "profile", f"summary {n}", f"res-{n}", stamp, stamp),
        )
        db.execute(
            "INSERT INTO memu_category_items (id, item_id, category_id) VALUES (?,?,?)",
            (f"link-{n}", f"it-{n}", "cat-pref"),
        )
        db.execute(
            "INSERT INTO memu_resources (id, url, modality, caption, created_at) "
            "VALUES (?,?,?,?,?)",
            (f"res-{n}", f"http://x/{n}", "text", "cap", stamp),
        )
    db.commit()
    db.close()


class _WriteBetweenStatements:
    """Commit a write on another thread just before a chosen statement runs.

    Patches `sqlite3.connect` so every connection the code under test opens gets
    a trace callback. The callback fires once, when a statement containing
    `needle` is about to execute, and blocks until the writer's COMMIT has
    returned or `wait` elapses. The writer uses the busy_timeout every memU
    writer sets, so under an open read transaction it waits rather than failing.
    """

    def __init__(self, db_path: str, needle: str, statements: list[str], wait: float = 30.0):
        self.db_path = db_path
        self.needle = needle
        self.statements = statements
        self.wait = wait
        self.fired = 0
        self.committed_before_next_statement = False
        self.writer_error: str | None = None
        self._original_connect = sqlite3.connect
        self._done = threading.Event()
        self._thread: threading.Thread | None = None

    def _write(self) -> None:
        try:
            w = sqlite3.connect(self.db_path, timeout=self.wait)
            w.execute("PRAGMA busy_timeout=30000")
            for sql in self.statements:
                w.execute(sql)
            w.commit()
            w.close()
        except Exception as e:  # pragma: no cover - reported by assertions
            self.writer_error = f"{type(e).__name__}: {e}"
        self._done.set()

    def _trace(self, statement: str) -> None:
        if self.fired or self.needle not in statement:
            return
        self.fired = 1
        self._thread = threading.Thread(target=self._write)
        self._thread.start()
        # Under BEGIN the writer cannot commit until the reader releases, so a
        # timeout here is expected and correct; the read must still be coherent.
        self.committed_before_next_statement = self._done.wait(timeout=1.0)

    def __enter__(self) -> _WriteBetweenStatements:
        def _connect(*args, **kwargs):
            conn = self._original_connect(*args, **kwargs)
            conn.set_trace_callback(self._trace)
            return conn

        sqlite3.connect = _connect
        return self

    def __exit__(self, *exc) -> bool:
        sqlite3.connect = self._original_connect
        if self._thread is not None:
            self._thread.join(timeout=self.wait + 5)
        return False


class _RetainConnections:
    """Keep every connection the code under test opens, so release is observable.

    The release tests below must decide whether a read transaction was released,
    and the only GC-independent way to do that is to look at the connection
    itself. Same patching technique as `_WriteBetweenStatements`, restored in
    `__exit__`.

    Retaining the connection deliberately defeats garbage collection for the
    duration of the assertion. That is the point: without it, whether a leaked
    transaction is still observable depends on the cyclic collector (measured -
    see `assert_released`), which is not a property a test can control.
    """

    def __init__(self) -> None:
        self.connections: list[sqlite3.Connection] = []
        self._original_connect = sqlite3.connect

    def __enter__(self) -> _RetainConnections:
        def _connect(*args, **kwargs):
            conn = self._original_connect(*args, **kwargs)
            self.connections.append(conn)
            return conn

        sqlite3.connect = _connect
        return self

    def __exit__(self, *exc) -> bool:
        sqlite3.connect = self._original_connect
        return False

    def assert_released(self) -> None:
        """Every retained connection is closed, or holds no open transaction.

        Exact and immediate. A closed connection raises `ProgrammingError` on any
        attribute use, which is itself proof of release; an open one reports
        `in_transaction` directly.
        """
        assert self.connections, "the code under test opened no connection"
        for n, conn in enumerate(self.connections):
            try:
                in_txn = conn.in_transaction
            except sqlite3.ProgrammingError:
                continue  # closed, so the transaction is gone
            assert not in_txn, (
                f"connection {n} of {len(self.connections)} is still open with a "
                "transaction in progress, so the read did not release it"
            )

    def assert_all_closed(self) -> None:
        """Every retained connection is CLOSED, not merely free of a transaction.

        `assert_released` accepts an open connection with no transaction, which is
        exactly the state a `close()` moved back out of `finally` leaves behind:
        the exception rolls back and skips `close()`, leaking the connection.
        """
        assert self.connections, "the code under test opened no connection"
        for n, conn in enumerate(self.connections):
            try:
                conn.in_transaction
            except sqlite3.ProgrammingError:
                continue  # closed, which is what this asserts
            raise AssertionError(
                f"connection {n} of {len(self.connections)} is still open, so the "
                "read did not close it"
            )


def _assert_writer_progress(writer: _WriteBetweenStatements, journal_mode: str) -> None:
    """Assert the writer did (wal) or did not (delete) commit while the txn was open.

    This is the one observable that separates the transaction this change adds
    from a lock-taking one. Without it every arm ends `ok` and the suite cannot
    tell `BEGIN` from `BEGIN EXCLUSIVE` (measured: with EXCLUSIVE the wal writer
    waits out the whole read, and still succeeds).

    Only valid for a read that opens a DEFERRED read transaction: a reader with
    no transaction at all never blocks a writer in either mode.
    """
    expected = journal_mode == "wal"
    assert writer.committed_before_next_statement is expected, (
        f"journal_mode={journal_mode}: expected the interleaved writer to "
        f"{'commit while' if expected else 'wait until after'} the read transaction "
        f"{'was open' if expected else 'closed'}, but "
        f"committed_before_next_statement={writer.committed_before_next_statement}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
async def test_expand_category_total_and_items_agree_under_concurrent_delete(
    tmp_path, journal_mode
) -> None:
    """`total` must not describe a different state than `items`.

    Unfixed this yields total=3 len=2, which the handler renders as
    "showing 2 of 3 items" for an UNPAGED category.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="JOIN memu_memory_items",  # the listing, i.e. after the count
        statements=["DELETE FROM memu_category_items WHERE item_id = 'it-3'"],
    ) as writer:
        result = await bridge.expand_category("cat:cat-pref", limit=10)

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    assert result["total"] == len(result["items"]), (
        f"total={result['total']} but len(items)={len(result['items'])} "
        "- the count and the listing came from different snapshots"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_memu_snapshot_links_are_subset_of_items(tmp_path, journal_mode) -> None:
    """Every exported `category_items` id must be present in the same payload.

    Unfixed this exports ['it-9'], an item committed after the item scan. Note a
    SQL JOIN on the relation scan does NOT fix this: the JOIN runs in its own
    later snapshot and happily returns the new link (pinned below).
    """
    from nerve.gateway.routes.memory import _read_memu_snapshot_sync

    db_path = str(tmp_path / "memu.sqlite")
    _seed_snapshot(db_path, journal_mode)

    inserts = [
        "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
        "VALUES ('it-9', 'profile', 'summary 9', '2026-07-01 10:00:00')",
        "INSERT INTO memu_category_items (id, item_id, category_id) "
        "VALUES ('link-9', 'it-9', 'cat-pref')",
    ]
    with _WriteBetweenStatements(
        db_path,
        needle="FROM memu_category_items",  # the relation scan, i.e. after items
        statements=inserts,
    ) as writer:
        payload = json.loads(_read_memu_snapshot_sync(db_path))

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    item_ids = {i["id"] for i in payload["items"]}
    dangling = [
        item_id
        for linked in payload["category_items"].values()
        for item_id in linked
        if item_id not in item_ids
    ]
    assert dangling == [], (
        f"category_items references {dangling}, absent from the same payload's items"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_local_join_reconstruction_shows_a_join_is_not_an_isolation_fix(
    tmp_path, journal_mode
) -> None:
    """Pin that joining the relation scan to items does NOT close the gap.

    `read_with_joined_relation_scan` below is a LOCAL RECONSTRUCTION of the
    shape PR #252 ships for a different property (dangling rows); it is not this
    repo's code, and this test calls no production function. It exists only to
    show the JOIN is not an isolation guarantee, so it cannot go red for a wrong
    reason and it pins nothing about `nerve/`.

    The reconstruction opens NO transaction, which is exactly why the JOIN runs
    in its own later snapshot and happily returns a link to an item committed
    after the item scan. That also makes it the one interleaving test where the
    writer progresses in BOTH journal modes, so `_assert_writer_progress` does
    not apply - the assertion below is its negation, and it fails if anyone adds
    a `BEGIN` here and leaves the docstring claiming a JOIN alone suffices.
    """
    db_path = str(tmp_path / "memu.sqlite")
    _seed_snapshot(db_path, journal_mode)

    def read_with_joined_relation_scan() -> dict:
        db = sqlite3.connect(db_path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            items = [
                dict(row)
                for row in db.execute(
                    "SELECT id, resource_id FROM memu_memory_items ORDER BY created_at DESC"
                )
            ]
            cat_items: dict[str, list[str]] = {}
            for row in db.execute(
                "SELECT ci.category_id, ci.item_id FROM memu_category_items ci "
                "JOIN memu_memory_items i ON i.id = ci.item_id"
            ):
                cat_items.setdefault(row["category_id"], []).append(row["item_id"])
        finally:
            db.close()
        return {"items": items, "category_items": cat_items}

    inserts = [
        "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
        "VALUES ('it-9', 'profile', 'summary 9', '2026-07-01 10:00:00')",
        "INSERT INTO memu_category_items (id, item_id, category_id) "
        "VALUES ('link-9', 'it-9', 'cat-pref')",
    ]
    with _WriteBetweenStatements(
        db_path, needle="JOIN memu_memory_items", statements=inserts
    ) as writer:
        payload = read_with_joined_relation_scan()

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.committed_before_next_statement is True, (
        f"journal_mode={journal_mode}: this reconstruction opens no transaction, so "
        "the writer must commit before the next statement in BOTH modes; a False "
        "here means a BEGIN was added and the JOIN claim below no longer holds"
    )
    item_ids = {i["id"] for i in payload["items"]}
    dangling = [
        item_id
        for linked in payload["category_items"].values()
        for item_id in linked
        if item_id not in item_ids
    ]
    assert dangling == ["it-9"], (
        "expected the JOIN to still leak the post-scan link it-9, so that it is "
        f"not mistaken for an isolation fix; got {dangling}"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_memu_snapshot_resource_refs_are_subset_of_resources(
    tmp_path, journal_mode
) -> None:
    """Every non-null `items[].resource_id` must resolve inside the payload.

    The sibling invariant of the link one: a resource deleted between the item
    scan and the resource scan leaves every item pointing at nothing. Unfixed
    this yields ['it-3', 'it-2', 'it-1'].
    """
    from nerve.gateway.routes.memory import _read_memu_snapshot_sync

    db_path = str(tmp_path / "memu.sqlite")
    _seed_snapshot(db_path, journal_mode)

    with _WriteBetweenStatements(
        db_path,
        needle="FROM memu_resources",  # the resource scan, i.e. after items
        statements=["DELETE FROM memu_resources"],
    ) as writer:
        payload = json.loads(_read_memu_snapshot_sync(db_path))

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    resource_ids = {r["id"] for r in payload["resources"]}
    dangling = [
        i["id"]
        for i in payload["items"]
        if i["resource_id"] and i["resource_id"] not in resource_ids
    ]
    assert dangling == [], (
        f"items {dangling} reference a resource absent from the same payload"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_db_stats_counts_are_one_snapshot(tmp_path, journal_mode) -> None:
    """The type distribution must sum to `total_items`.

    Unfixed an item inserted between the items count and the GROUP BY yields
    total_items=3 with sum(type_distribution)=4 in the diagnostics payload.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="GROUP BY memory_type",  # the distribution, i.e. after the counts
        statements=[
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
            "VALUES ('it-9', 'behavior', 'summary 9', '2026-07-01 10:00:00')"
        ],
    ) as writer:
        stats = bridge._get_db_stats_sync()

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    assert sum(stats["type_distribution"].values()) == stats["total_items"], (
        f"type_distribution sums to {sum(stats['type_distribution'].values())} "
        f"but total_items={stats['total_items']}"
    )


# The five tests below cover the statement boundaries the four above leave
# open, so that every statement of all three reads is either the first statement
# or has an interleaving needle firing at it. The rule that makes that coverage
# complete: `expand_category` runs 3 statements, the gateway snapshot 4 scans and
# `_get_db_stats_sync` 5, and each test names in its docstring the (N-1)->(N)
# boundary its needle breaks, so the boundaries are enumerable from this file
# alone.
#
# The needle fires from a trace callback, which sqlite3 invokes BEFORE the
# statement executes, and a write committed inside it IS visible to that same
# statement (measured). So a needle on statement N breaks the (N-1)->(N)
# boundary, and a needle on statement 1 breaks nothing: the write lands before
# the whole read and every statement sees one consistent state. Each test below
# therefore needles the LATER statement of the pair it covers, and its writer
# must change what the EARLIER statement already read.


@pytest.mark.asyncio
@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
async def test_expand_category_name_and_total_agree_under_concurrent_delete(
    tmp_path, journal_mode
) -> None:
    """The category row behind `name` must still exist for the `total` that follows.

    Covers `expand_category`'s (1)->(2) boundary: the name lookup and the count.
    The writer deletes the category and its links, so unfixed the read reports a
    named category with total=0 - a category that, in the state `total` was
    counted from, does not exist. Deleting only a link cannot cover this
    boundary: it leaves statement 1's own result unchanged, so total and the
    listing still agree with each other and only the (2)->(3) boundary (covered
    by the sibling test above) can observe it.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="count(*) FROM memu_category_items",  # statement 2, i.e. after the name
        statements=[
            "DELETE FROM memu_memory_categories WHERE id = 'cat-pref'",
            "DELETE FROM memu_category_items WHERE category_id = 'cat-pref'",
        ],
    ) as writer:
        result = await bridge.expand_category("cat:cat-pref", limit=10)

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    assert result["name"] == "preferences", (
        f"the category was found before the write, so it must stay named: {result!r}"
    )
    assert result["total"] == 3, (
        f"name={result['name']!r} came from a snapshot with 3 links but total="
        f"{result['total']} - the lookup and the count came from different snapshots"
    )
    assert result["total"] == len(result["items"])


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_memu_snapshot_link_keys_are_subset_of_categories(
    tmp_path, journal_mode
) -> None:
    """Every `category_items` KEY must be a category present in the same payload.

    Covers the gateway's (1)->(2) boundary: the category scan and the item scan.
    The writer commits a whole new category with an item and a link, so unfixed
    the payload exports `category_items['cat-new']` while `categories` has no
    such row - the mirror of the item-side invariant, on the key rather than the
    value. Needling the item scan (statement 2) is what puts the write after the
    category scan; needling the category scan itself covers no boundary.
    """
    from nerve.gateway.routes.memory import _read_memu_snapshot_sync

    db_path = str(tmp_path / "memu.sqlite")
    _seed_snapshot(db_path, journal_mode)

    with _WriteBetweenStatements(
        db_path,
        # statement 2, the item scan, i.e. after the category scan
        needle="FROM memu_memory_items ORDER BY created_at DESC",
        statements=[
            "INSERT INTO memu_memory_categories (id, name, description) "
            "VALUES ('cat-new', 'fresh', 'committed mid-read')",
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
            "VALUES ('it-9', 'profile', 'summary 9', '2026-07-01 10:00:00')",
            "INSERT INTO memu_category_items (id, item_id, category_id) "
            "VALUES ('link-9', 'it-9', 'cat-new')",
        ],
    ) as writer:
        payload = json.loads(_read_memu_snapshot_sync(db_path))

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    category_ids = {c["id"] for c in payload["categories"]}
    dangling = sorted(k for k in payload["category_items"] if k not in category_ids)
    assert dangling == [], (
        f"category_items is keyed by {dangling}, absent from the same payload's categories"
    )
    item_ids = {i["id"] for i in payload["items"]}
    dangling_items = sorted(
        {i for linked in payload["category_items"].values() for i in linked if i not in item_ids}
    )
    assert dangling_items == [], (
        f"category_items references {dangling_items}, absent from the same payload's items"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_db_stats_events_missing_agrees_with_distribution(
    tmp_path, journal_mode
) -> None:
    """`events_missing_happened_at` must not exceed the same snapshot's event count.

    Covers `_get_db_stats_sync`'s (4)->(5) boundary, i.e. the last statement,
    which no other test puts on the far side of an interleaved write. The writer
    commits one `memory_type='event'` row with a NULL `happened_at`; unfixed the
    fifth statement counts it (1) while the distribution from the fourth has no
    `event` key at all (0), so the payload reports more events missing a
    timestamp than it reports events.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="happened_at IS NULL",  # statement 5, i.e. after the distribution
        statements=[
            "INSERT INTO memu_memory_items "
            "(id, memory_type, summary, created_at, happened_at) "
            "VALUES ('ev-9', 'event', 'summary 9', '2026-07-01 10:00:00', NULL)"
        ],
    ) as writer:
        stats = bridge._get_db_stats_sync()

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    events = stats["type_distribution"].get("event", 0)
    assert stats["events_missing_happened_at"] <= events, (
        f"events_missing_happened_at={stats['events_missing_happened_at']} exceeds the "
        f"same snapshot's event count {events} - the last statement and the "
        "distribution came from different snapshots"
    )
    assert sum(stats["type_distribution"].values()) == stats["total_items"]


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_db_stats_total_items_agrees_with_distribution_across_the_second_count(
    tmp_path, journal_mode
) -> None:
    """Covers `_get_db_stats_sync`'s (1)->(2) boundary: the item count and the
    category count. The writer inserts an item, so unfixed `total_items` comes
    from before the write and the distribution at statement 4 from after it.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="COUNT(*) FROM memu_memory_categories",  # statement 2
        statements=[
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
            "VALUES ('it-9', 'behavior', 'summary 9', '2026-07-01 10:00:00')"
        ],
    ) as writer:
        stats = bridge._get_db_stats_sync()

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    assert sum(stats["type_distribution"].values()) == stats["total_items"], (
        f"type_distribution sums to {sum(stats['type_distribution'].values())} "
        f"but total_items={stats['total_items']}"
    )


@pytest.mark.parametrize("journal_mode", JOURNAL_MODES)
def test_db_stats_total_items_agrees_with_distribution_across_the_third_count(
    tmp_path, journal_mode
) -> None:
    """Covers `_get_db_stats_sync`'s (2)->(3) boundary: the category count and
    the resource count. The writer inserts an item, so unfixed `total_items`
    comes from before the write and the distribution at statement 4 from after
    it.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode)

    bridge = _stub_bridge(config)
    with _WriteBetweenStatements(
        db_path,
        needle="COUNT(*) FROM memu_resources",  # statement 3
        statements=[
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at) "
            "VALUES ('it-9', 'behavior', 'summary 9', '2026-07-01 10:00:00')"
        ],
    ) as writer:
        stats = bridge._get_db_stats_sync()

    assert writer.fired == 1, "the interleaved write never ran"
    assert writer.writer_error is None, f"writer failed: {writer.writer_error}"
    _assert_writer_progress(writer, journal_mode)
    assert sum(stats["type_distribution"].values()) == stats["total_items"], (
        f"type_distribution sums to {sum(stats['type_distribution'].values())} "
        f"but total_items={stats['total_items']}"
    )


@pytest.mark.asyncio
async def test_expand_category_total_stays_unfiltered_by_query(tmp_path) -> None:
    """A read transaction must not change what `total` counts.

    `total` is the category's whole link count; `query` filters only the listing.
    Pinned because folding `total` into the listing statement was the rejected
    alternative and would have filtered it.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode="wal")

    bridge = _stub_bridge(config)
    matches_one = await bridge.expand_category("cat-pref", query="summary 1", limit=10)
    assert [i["id"] for i in matches_one["items"]] == ["it-1"]
    assert matches_one["total"] == 3

    matches_none = await bridge.expand_category("cat-pref", query="nothing", limit=10)
    assert matches_none["items"] == []
    assert matches_none["total"] == 3


@pytest.mark.asyncio
async def test_expand_category_all_links_dangling(tmp_path) -> None:
    """A category whose every link points at a deleted item still reports total.

    Boundary shape for the read transaction: the listing is empty while the
    count is not, with no concurrent writer involved.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode="wal")
    db = sqlite3.connect(db_path)
    db.execute("DELETE FROM memu_memory_items")
    db.commit()
    db.close()

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat-pref", limit=10)
    assert result["name"] == "preferences"
    assert result["total"] == 3
    assert result["items"] == []


@pytest.mark.asyncio
async def test_expand_category_releases_transaction_on_unknown_id(tmp_path) -> None:
    """The early return for an unknown category must not leak a read transaction.

    `expand_category` returns before its other two statements when the category
    does not exist, and the deferred transaction is already open by then (the
    lookup itself reads, and a no-row result still takes the lock). A leaked
    reader is exactly the long-lived transaction that starves WAL passive
    checkpoints, so a writer must still be able to commit afterwards.

    This is a boundary/parity test, not a test of `rollback()`: the site's
    `finally: db.close()` predates this change and `close()` alone releases the
    transaction (measured). It guards the release path against a future edit that
    returns early without unwinding through `finally`.

    The oracle is the retained connection, not the clock: `assert_released` is
    exact and immediate, while a timed writer only reports release indirectly and
    depends on both the garbage collector and the scheduler.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _seed_snapshot(db_path, journal_mode="delete")

    bridge = _stub_bridge(config)
    with _RetainConnections() as opened:
        result = await bridge.expand_category("cat:nope")
    assert result["name"] is None
    opened.assert_released()

    # Corroborating only, and deliberately generous: the assertion above is the
    # oracle. A leaked reader in delete mode blocks this writer for the whole
    # busy_timeout, so the threshold sits well below it.
    started = time.monotonic()
    w = sqlite3.connect(db_path, timeout=5)
    w.execute("PRAGMA busy_timeout=5000")
    w.execute("DELETE FROM memu_category_items WHERE item_id = 'it-3'")
    w.commit()
    w.close()
    elapsed = time.monotonic() - started
    assert elapsed < 4.0, (
        f"writer took {elapsed:.1f}s, so the early return left its transaction open"
    )


def test_db_stats_releases_transaction_on_error(tmp_path) -> None:
    """A stats query that raises mid-read must still release the transaction.

    `_get_db_stats_sync` keeps `db.close()` in a `finally`; before that it sat at
    the end of the `try` body, so an exception skipped it and left a read
    transaction open on a connection only the garbage collector could reclaim.

    Two fixture properties make the error path reachable at all:

    * `memu_memory_items` must EXIST so the first COUNT succeeds. `BEGIN` is
      deferred, so it acquires no lock until a statement actually reads; a
      fixture whose first statement fails never opens a transaction to leak.
    * `memu_memory_categories` must be MISSING so the second COUNT raises while
      the transaction is open.

    The oracle is the retained connection. A timed writer cannot decide this:
    production writes the error path as `except Exception as e:`, which drops `e`
    at block exit (PEP 3110), so once the traceback stops pinning the frame that
    pins `db` the connection becomes cyclic garbage and release depends on the
    cyclic collector. Measured against the pre-fix flow with no retention: a
    forced `gc.collect()` makes a `writer < 2.0s` assertion pass 6/6 on unfixed
    code, and without one it fails 6/6. `assert_released` holds the connection so
    no collection can hide the leak, and is exact either way.
    """
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    db = sqlite3.connect(db_path)
    db.execute("PRAGMA journal_mode=delete")
    db.execute("CREATE TABLE memu_memory_items (id TEXT, memory_type TEXT, happened_at TEXT)")
    db.execute("INSERT INTO memu_memory_items (id, memory_type) VALUES ('it-1', 'profile')")
    db.commit()
    db.close()

    bridge = _stub_bridge(config)
    with _RetainConnections() as opened:
        stats = bridge._get_db_stats_sync()
    # The read reached the missing table, so the error path ran and left the
    # remaining fields at their defaults.
    assert stats["total_items"] == 1
    assert stats["total_categories"] == 0
    assert stats["type_distribution"] == {}

    # A forced collection here would release a leaked connection through the
    # cyclic collector; the assertion below must hold regardless, which is what
    # makes it independent of GC timing.
    gc.collect()
    opened.assert_released()
    # Release and closure are different properties: `assert_released` accepts an
    # open connection with no transaction, which is what a `close()` moved back
    # out of the `finally` leaks. Only the pair pins both halves of the change.
    opened.assert_all_closed()

    # Corroborating only, and deliberately generous: see the sibling test.
    started = time.monotonic()
    w = sqlite3.connect(db_path, timeout=5)
    w.execute("PRAGMA busy_timeout=5000")
    w.execute("INSERT INTO memu_memory_items (id, memory_type) VALUES ('it-2', 'profile')")
    w.commit()
    w.close()
    elapsed = time.monotonic() - started
    assert elapsed < 4.0, (
        f"writer took {elapsed:.1f}s, so the failed read left its transaction open"
    )


# --- handlers --------------------------------------------------------------


@pytest.mark.asyncio
async def test_recall_handler_renders_two_sections(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.recall = AsyncMock(return_value=[
        {"id": "i1", "type": "profile", "summary": "Alice lives in Metropolis"},
        {"id": "cat:c1", "type": "category", "name": "preferences",
         "summary": "How things should be done"},
    ])
    result = await memory_recall_handler(_ctx(bridge), {"query": "x"})
    text = result.content[0]["text"]
    assert "Recalled 1 memories" in text
    assert "1 related topics" in text
    assert "Alice lives in Metropolis" in text
    assert "memory_expand_category" in text
    assert "cat:c1" in text


@pytest.mark.asyncio
async def test_recall_handler_passes_category_limit(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.recall = AsyncMock(return_value=[])
    await memory_recall_handler(_ctx(bridge), {"query": "x", "category_limit": 2})
    _, kwargs = bridge.recall.call_args
    assert kwargs["category_limit"] == 2


@pytest.mark.asyncio
async def test_expand_category_handler(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.expand_category = AsyncMock(return_value={
        "name": "preferences",
        "total": 5,
        "items": [{"id": "it-1", "type": "profile", "summary": "Likes the color teal"}],
    })
    result = await memory_expand_category_handler(
        _ctx(bridge), {"category_id": "cat:cat-pref"}
    )
    text = result.content[0]["text"]
    assert "preferences" in text
    assert "1 of 5" in text
    assert "Likes the color teal" in text


@pytest.mark.asyncio
async def test_expand_category_handler_requires_id(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    result = await memory_expand_category_handler(_ctx(bridge), {"category_id": ""})
    assert result.is_error
