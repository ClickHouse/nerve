"""The title-similarity duplicate guard: the index, its cache, and task_create.

The guard used to be an FTS5 OR query over titles and bodies with a fixed
BM25 cut-off. BM25 grows with query length and corpus size, so on a large
install every create was refused as a duplicate (12.6K-task corpus: 153 of
153 unconfirmed creates in 48 h). The replacement scores titles against
titles with IDF-weighted token overlap, which is 0..1 whatever the corpus.
"""

from __future__ import annotations

import pytest

from nerve.agent.tools.handlers.tasks import (
    DUPLICATE_SIMILARITY,
    RELATED_SIMILARITY,
    task_create_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.tasks.similarity import TitleIndex, title_tokens

# A small synthetic corpus: one long templated alert, a short chore, three
# tasks sharing a CI template, and unrelated filler.
ROWS = [
    {"id": "t1", "status": "pending", "title": (
        "Service alerts — @bob: the nightly backup job fails with a timeout "
        "after the storage upgrade"
    )},
    {"id": "t2", "status": "pending", "title": "Rotate the staging database credentials"},
    {"id": "t3", "status": "done", "title": "CI: test_alpha failing in nightly (debug)"},
    {"id": "t4", "status": "pending", "title": "CI: test_beta failing in nightly (debug)"},
    {"id": "t5", "status": "pending", "title": "CI: test_gamma failing in nightly (debug)"},
    {"id": "t6", "status": "pending", "title": "Renew the office internet contract before March"},
    {"id": "t7", "status": "done", "title": "Write the quarterly report for the board"},
    {"id": "t8", "status": "pending", "title": "Upgrade the storage cluster firmware"},
]


class TestTitleTokens:
    def test_drops_dates_counts_time_stamps_and_stop_words(self):
        tokens = title_tokens(
            "GitHub: acme/widgets#4242 — @alice 2026-03-04 0930Z: fix the 3 flaky checks in CI"
        )
        assert tokens == {
            "github", "acme", "widgets", "4242", "alice", "fix", "flaky", "checks", "ci",
        }

    def test_keeps_identifiers_and_long_numbers(self):
        # Underscored identifiers stay whole — they are the most distinctive
        # tokens a title has — and issue/PR-sized numbers identify work.
        assert title_tokens("01234_some_test_name fails on build 98765") == {
            "01234_some_test_name", "fails", "build", "98765",
        }

    def test_case_insensitive_and_empty(self):
        assert title_tokens("Fix Backup") == title_tokens("fix backup")
        assert title_tokens("") == frozenset()
        assert title_tokens("2026-01-01 at 10") == frozenset()


class TestTitleIndex:
    def test_same_words_score_one_in_any_order(self):
        index = TitleIndex(ROWS)
        [match] = index.similar("Rotate staging database credentials", min_score=0.5)
        assert match.id == "t2"
        assert match.score == 1.0
        assert index.similar("credentials database staging rotate")[0].score == 1.0

    def test_a_short_restatement_of_a_long_task_is_a_duplicate(self):
        index = TitleIndex(ROWS)
        best = index.similar("Nightly backup job fails with a timeout")[0]
        assert best.id == "t1"
        # Containment carries it: every word of the new title is in the old
        # one, even though the old title says much more.
        assert best.containment == 1.0
        assert best.dice < DUPLICATE_SIMILARITY
        assert best.score >= DUPLICATE_SIMILARITY

    def test_a_reworded_title_lands_in_the_related_band(self):
        # No stemming: "rotation" is not "rotate", so a reworded copy shares
        # three of four weighted words — a look-alike to report, not a refusal.
        index = TitleIndex(ROWS)
        best = index.similar("Credentials rotation for the staging database")[0]
        assert best.id == "t2"
        assert RELATED_SIMILARITY <= best.score < DUPLICATE_SIMILARITY
        assert set(best.shared) == {"credentials", "staging", "database"}

    def test_same_area_different_work_scores_low(self):
        index = TitleIndex(ROWS)
        best = index.similar("Pay the office internet bill")[0]
        assert best.id == "t6"
        assert best.score < RELATED_SIMILARITY
        assert best.shared == ("internet", "office")

    def test_a_shared_rare_identifier_surfaces_a_task_whatever_its_score(self):
        rows = ROWS + [
            {"id": "i1", "status": "done", "title": "Investigate issue #12345: cold start takes a minute"},
            {"id": "i2", "status": "pending", "title": "Perf: 00812_cold_start_timing regressed on the arm runner"},
        ]
        index = TitleIndex(rows)
        matches = index.similar("Follow-up to #12345: add the cold-start regression test", min_score=0.5)
        assert [m.id for m in matches][:1] == ["i1"]
        assert matches[0].shared_identifiers == ("12345",)
        assert matches[0].score < 0.5
        # The rule needs an identifier: a shared plain word that happens to be
        # rare does not qualify, and a number that many titles carry is not rare.
        assert all(m.id != "i2" for m in index.similar("Timing of the cold start on arm", min_score=0.5))
        crowded = rows + [
            {"id": f"p{i}", "status": "done", "title": f"Part {i} of #12345"} for i in range(8)
        ]
        hits = TitleIndex(crowded).similar("Yet another #12345 item", min_score=0.5)
        assert all(not m.shared_identifiers for m in hits)

    def test_template_words_weigh_less_the_more_tasks_share_them(self):
        """What every title has says nothing about which task this is."""
        query = "CI: test_delta failing in nightly (debug)"
        few = TitleIndex(ROWS).similar(query)[0].score
        many_rows = ROWS + [
            {"id": f"c{i}", "status": "done", "title": f"CI: test_{i} failing in nightly (debug)"}
            for i in range(40)
        ]
        crowded = TitleIndex(many_rows).similar(query)
        assert few > crowded[0].score if crowded else True
        assert all(m.score < RELATED_SIMILARITY for m in crowded)

    def test_shared_tokens_most_distinctive_first(self):
        index = TitleIndex(ROWS)
        best = index.similar("Nightly backup job fails with a timeout")[0]
        # "nightly" is in four titles; the rest are in one each.
        assert best.shared[-1] == "nightly"
        assert set(best.shared) == {"backup", "fails", "job", "timeout", "nightly"}

    def test_min_score_limit_and_exclude(self):
        index = TitleIndex(ROWS)
        query = "CI: test_delta failing in nightly (debug)"
        assert len(index.similar(query, limit=2)) == 2
        assert index.similar(query, min_score=0.99) == []
        ids = {m.id for m in index.similar(query, exclude="t3")}
        assert "t3" not in ids and {"t4", "t5"} <= ids

    def test_compare_scores_one_known_task(self):
        index = TitleIndex(ROWS)
        match = index.compare("Rotate staging database credentials", "t2")
        assert match is not None and match.score == 1.0
        unrelated = index.compare("Rotate staging database credentials", "t7")
        assert unrelated is not None and unrelated.score == 0.0 and unrelated.shared == ()
        assert index.compare("anything", "missing") is None

    def test_empty_title_and_empty_index(self):
        assert TitleIndex(ROWS).similar("2026-01-01") == []
        assert TitleIndex([]).similar("Rotate the credentials") == []
        assert len(TitleIndex([])) == 0


@pytest.mark.asyncio
class TestTitleIndexCache:
    async def _seed(self, db, task_id: str, title: str) -> None:
        await db.upsert_task(
            task_id=task_id, file_path=f"memory/tasks/active/{task_id}.md",
            title=title, status="pending", content="",
        )

    async def test_unchanged_corpus_reuses_the_index(self, db):
        await self._seed(db, "2026-01-01-alpha", "Alpha task")
        first = await db.title_index()
        assert first is await db.title_index()

    async def test_a_new_task_rebuilds(self, db):
        await self._seed(db, "2026-01-01-alpha", "Alpha task")
        first = await db.title_index()
        await self._seed(db, "2026-01-02-beta", "Beta task")
        second = await db.title_index()
        assert second is not first
        assert "2026-01-02-beta" in second and "2026-01-02-beta" not in first

    async def test_a_changed_title_rebuilds(self, db):
        await self._seed(db, "2026-01-01-alpha", "Alpha task")
        first = await db.title_index()
        await self._seed(db, "2026-01-01-alpha", "Alpha task, renamed")
        second = await db.title_index()
        assert second is not first
        assert second.similar("Alpha task renamed")[0].score == 1.0


@pytest.mark.asyncio
class TestTaskCreateGuard:
    """task_create refuses near-duplicates and same-source tasks; it reports
    look-alikes with the outcome instead of refusing over them."""

    async def _ctx(self, db, tmp_path) -> ToolContext:
        return ToolContext(session_id="test", db=db, workspace=tmp_path)

    async def _seed(self, ctx) -> None:
        for row in ROWS:
            result = await task_create_handler(ctx, {
                "title": row["title"], "content": "body", "confirm_duplicate": True,
            })
            assert result.structured["created"], result.text_content

    async def test_near_identical_title_is_refused_with_the_evidence(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        await self._seed(ctx)

        result = await task_create_handler(ctx, {
            "title": "Rotate staging database credentials", "content": "again",
        })

        outcome = result.structured
        assert outcome["created"] is False and outcome["reason"] == "duplicate"
        [dupe] = outcome["duplicates"]
        assert dupe["title"] == "Rotate the staging database credentials"
        assert dupe["similarity"] >= DUPLICATE_SIMILARITY
        assert dupe["same_source"] is False
        assert "credentials" in dupe["shared"]
        text = result.text_content
        assert "Task NOT created" in text and "title similarity 1.00" in text
        assert "shares: credentials, database, rotate, staging" in text

    async def test_look_alikes_do_not_block_and_are_reported(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        await self._seed(ctx)

        result = await task_create_handler(ctx, {
            "title": "Credentials rotation for the staging database", "content": "quarterly",
        })

        outcome = result.structured
        assert outcome["created"] is True
        [related] = outcome["related"]
        assert related["title"] == "Rotate the staging database credentials"
        assert RELATED_SIMILARITY <= related["similarity"] < DUPLICATE_SIMILARITY
        assert "Possibly related tasks" in result.text_content
        assert "Task created:" in result.text_content

    async def test_a_shared_rare_identifier_is_reported_by_name(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        await self._seed(ctx)
        await task_create_handler(ctx, {
            "title": "Investigate issue #12345: cold start takes a minute", "content": "x",
            "confirm_duplicate": True,
        })

        result = await task_create_handler(ctx, {
            "title": "Follow-up to #12345: add the regression test", "content": "x",
        })

        outcome = result.structured
        assert outcome["created"] is True
        [related] = outcome["related"]
        assert related["shared_identifiers"] == ["12345"]
        assert "shares the identifier 12345" in result.text_content

    async def test_nothing_alike_creates_quietly(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        await self._seed(ctx)

        result = await task_create_handler(ctx, {
            "title": "Order new badges for the summer interns", "content": "x",
        })

        assert result.structured["created"] is True
        assert "related" not in result.structured
        assert "related" not in result.text_content.lower()

    async def test_same_source_url_is_refused_whatever_the_title(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        url = "https://example.invalid/issues/7"
        await task_create_handler(ctx, {
            "title": "Original task", "content": "x", "source_url": url,
            "confirm_duplicate": True,
        })

        result = await task_create_handler(ctx, {
            "title": "A different title, same source", "content": "x", "source_url": url,
        })

        outcome = result.structured
        assert outcome["created"] is False
        [dupe] = outcome["duplicates"]
        assert dupe["same_source"] is True and dupe["title"] == "Original task"
        assert "same source_url" in result.text_content

    async def test_confirm_creates_and_keeps_the_matches_in_the_result(self, db, tmp_path):
        ctx = await self._ctx(db, tmp_path)
        await self._seed(ctx)

        result = await task_create_handler(ctx, {
            "title": "Rotate staging database credentials", "content": "again",
            "confirm_duplicate": True,
        })

        outcome = result.structured
        assert outcome["created"] is True
        assert [r["title"] for r in outcome["related"]] == [
            "Rotate the staging database credentials",
        ]
        assert "Possibly related tasks" in result.text_content
