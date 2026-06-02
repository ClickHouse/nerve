"""Tests for the migration runner — discovery, ordering, idempotency."""

from __future__ import annotations

import pytest

from nerve.db import Database
from nerve.db.migrations.runner import (
    _check_no_duplicate_versions,
    discover_migrations,
)


# ---------------------------------------------------------------------------
# Duplicate-version guard
# ---------------------------------------------------------------------------


class TestNoDuplicateVersions:
    def test_unique_versions_ok(self):
        _check_no_duplicate_versions([(1, "v001_a"), (2, "v002_b"), (3, "v003_c")])

    def test_duplicate_raises(self):
        # The exact bug that broke usage tracking: two v027 files coexisted,
        # one silently won, the other was skipped forever because the runner
        # tracks MAX(version) only.
        with pytest.raises(RuntimeError, match="Duplicate migration version 27"):
            _check_no_duplicate_versions(
                [(27, "v027_cache_ttl_split"), (27, "v027_session_last_rotated")]
            )

    def test_duplicate_at_zero(self):
        with pytest.raises(RuntimeError, match="Duplicate migration version 0"):
            _check_no_duplicate_versions([(0, "v000_x"), (0, "v000_y")])

    def test_current_migrations_have_no_duplicates(self):
        # Regression check on the real on-disk set — if anyone adds a
        # collision later, this lights up before it ships.
        discovered = discover_migrations()
        versions = [v for v, _ in discovered]
        assert len(versions) == len(set(versions)), (
            f"Duplicate versions in nerve/db/migrations/: {discovered}"
        )

    def test_discovered_list_is_sorted(self):
        discovered = discover_migrations()
        versions = [v for v, _ in discovered]
        assert versions == sorted(versions)


# ---------------------------------------------------------------------------
# v030 cache TTL split — idempotent re-application
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestV030Idempotent:
    async def test_running_v030_twice_is_noop(self, db: Database):
        # Fresh DB fixture has already rolled forward to head, which
        # includes v030. Running it again must not raise "duplicate column"
        # — DBs hand-patched between the buggy ship and this fix will hit
        # this exact case when the runner picks v030 up under its new number.
        from nerve.db.migrations import v030_cache_ttl_split

        await v030_cache_ttl_split.up(db.db)  # second run, should be a no-op

        async with db.db.execute("PRAGMA table_info(session_usage)") as cur:
            cols = {row[1] async for row in cur}
        assert "cache_creation_5m_input_tokens" in cols
        assert "cache_creation_1h_input_tokens" in cols
