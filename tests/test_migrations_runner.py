"""Tests for the migration runner's duplicate-version guard."""

from __future__ import annotations

import pytest

from nerve.db.migrations.runner import (
    _check_no_duplicate_versions,
    discover_migrations,
)


class TestNoDuplicateVersions:
    def test_unique_versions_ok(self):
        _check_no_duplicate_versions([(1, "v001_a"), (2, "v002_b"), (3, "v003_c")])

    def test_duplicate_raises(self):
        with pytest.raises(RuntimeError, match="Duplicate migration version 27"):
            _check_no_duplicate_versions([(27, "v027_alpha"), (27, "v027_beta")])

    def test_duplicate_at_zero(self):
        with pytest.raises(RuntimeError, match="Duplicate migration version 0"):
            _check_no_duplicate_versions([(0, "v000_x"), (0, "v000_y")])

    def test_error_names_both_files(self):
        # Without both names the maintainer still has to go hunting for
        # which two files collided.
        with pytest.raises(RuntimeError) as exc:
            _check_no_duplicate_versions([(5, "v005_first"), (5, "v005_second")])
        assert "v005_first" in str(exc.value)
        assert "v005_second" in str(exc.value)

    def test_triplicate_reports_the_first_collision(self):
        with pytest.raises(RuntimeError, match="Duplicate migration version 9"):
            _check_no_duplicate_versions(
                [(9, "v009_a"), (9, "v009_b"), (9, "v009_c")]
            )

    def test_current_migrations_have_no_duplicates(self):
        # Regression check on the real on-disk set, so a collision added
        # later fails here rather than silently at a user's startup.
        discovered = discover_migrations()
        versions = [v for v, _ in discovered]
        assert len(versions) == len(set(versions)), (
            f"Duplicate versions in nerve/db/migrations/: {discovered}"
        )

    def test_discovery_still_returns_sorted_versions(self):
        versions = [v for v, _ in discover_migrations()]
        assert versions == sorted(versions)
