"""HTTP tests for ``/api/tasks*`` (gateway/routes/tasks.py).

These are the first route-level tests for the task API — until the board
went in, everything under ``/api/tasks`` was covered only through the
handler layer, so the routes' own behaviour (status codes, clamping,
declaration order, the response envelopes the frontend reads) was
unverified.

Pattern follows ``TestWorkflowRunRoutes`` in test_workflow_routes.py: a
minimal FastAPI app with the real router, auth disabled via an empty
jwt_secret, and a real tool registry behind a stub engine so the routes
exercise the genuine handler path rather than a mock of it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.db import Database


@pytest.mark.asyncio
class TestTaskBoardRoutes:
    @pytest_asyncio.fixture
    async def setup(self, db: Database, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.agent.tools import build_default_registry
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.tasks import router as tasks_router

        workspace = tmp_path / "ws"
        (workspace / "memory" / "tasks" / "active").mkdir(parents=True)
        (workspace / "memory" / "tasks" / "done").mkdir(parents=True)

        # require_auth reads get_config().auth.jwt_secret — an empty secret
        # makes it a no-op.
        cfg = NerveConfig()
        cfg.workspace = workspace
        cfg.auth.jwt_secret = ""
        cfg_mod._config = cfg

        # build_route_tool_context() pulls collaborators off the engine; the
        # task handlers only need workspace/db/config, so the rest are None.
        engine = SimpleNamespace(
            config=cfg,
            registry=build_default_registry(),
            _memory_bridge=None,
            _xmemory_bridge=None,
            _skill_manager=None,
        )
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(tasks_router)

        yield SimpleNamespace(
            client=TestClient(app), db=db, workspace=workspace, cfg=cfg,
        )

        cfg_mod._config = None

    async def _create(self, setup, title: str, **body) -> str:
        body.setdefault("content", "body")
        body.setdefault("confirm_duplicate", True)
        resp = setup.client.post("/api/tasks", json={"title": title, **body})
        assert resp.status_code == 200, resp.text
        return resp.json()["task"]["id"]

    # ── Board envelope ───────────────────────────────────────────────────

    async def test_board_returns_a_lane_per_configured_status(self, setup):
        resp = setup.client.get("/api/tasks/board")

        assert resp.status_code == 200
        body = resp.json()
        lane_names = [lane["status"] for lane in body["lanes"]]
        assert lane_names == [s["name"] for s in body["statuses"]]
        # Lanes follow the configured display order, so the board can render
        # them without sorting.
        assert "pending" in lane_names and "done" in lane_names

    async def test_board_lane_carries_tasks_and_a_true_total(self, setup):
        for i in range(3):
            await self._create(setup, f"Board task {i}")

        body = setup.client.get("/api/tasks/board?limit=2").json()
        pending = next(l for l in body["lanes"] if l["status"] == "pending")

        assert len(pending["tasks"]) == 2, "lane should honour the page limit"
        assert pending["total"] == 3, "total must count beyond the page"

    async def test_board_orders_lanes_by_position(self, setup):
        first = await self._create(setup, "Board alpha")
        second = await self._create(setup, "Board beta")

        setup.client.post(f"/api/tasks/{second}/move", json={"before_id": first})

        body = setup.client.get("/api/tasks/board").json()
        pending = next(l for l in body["lanes"] if l["status"] == "pending")
        assert [t["id"] for t in pending["tasks"]] == [first, second]

    async def test_board_limit_is_clamped(self, setup):
        await self._create(setup, "Clamp me")
        # Out-of-range values must not reach the DB as-is.
        assert setup.client.get("/api/tasks/board?limit=99999").status_code == 200
        assert setup.client.get("/api/tasks/board?limit=0").status_code == 200

    async def test_board_tag_filter_narrows_lanes_and_totals(self, setup):
        await self._create(setup, "Tagged one", tags="backend")
        await self._create(setup, "Untagged one")

        body = setup.client.get("/api/tasks/board?tag=backend").json()
        pending = next(l for l in body["lanes"] if l["status"] == "pending")

        assert [t["title"] for t in pending["tasks"]] == ["Tagged one"]
        # The total has to respect the filter too, or the lane offers to load
        # "+N more" tasks that the filter would exclude.
        assert pending["total"] == 1

    async def test_board_route_is_not_shadowed_by_the_id_route(self, setup):
        """/board and /tags must stay declared above /{task_id}."""
        body = setup.client.get("/api/tasks/board").json()
        assert "lanes" in body, "GET /api/tasks/board resolved to the detail route"

        tags = setup.client.get("/api/tasks/tags").json()
        assert "tags" in tags, "GET /api/tasks/tags resolved to the detail route"

    # ── Board search ─────────────────────────────────────────────────────

    async def test_board_search_filters_every_lane(self, setup):
        await self._create(setup, "Fix the widget encoder")
        await self._create(setup, "Unrelated chore")

        body = setup.client.get("/api/tasks/board?q=encoder").json()
        titles = [t["title"] for lane in body["lanes"] for t in lane["tasks"]]

        assert titles == ["Fix the widget encoder"]

    async def test_board_search_reports_matching_totals(self, setup):
        await self._create(setup, "Fix the widget encoder")
        await self._create(setup, "Unrelated chore")

        body = setup.client.get("/api/tasks/board?q=encoder").json()
        pending = next(l for l in body["lanes"] if l["status"] == "pending")

        # A stale total would offer "+N more" for tasks the search excluded.
        assert pending["total"] == 1

    async def test_board_search_spans_lanes(self, setup):
        keep = await self._create(setup, "Encoder work in progress")
        setup.client.patch(f"/api/tasks/{keep}", json={"status": "in_progress"})
        await self._create(setup, "Encoder work pending")

        body = setup.client.get("/api/tasks/board?q=encoder").json()
        by_lane = {l["status"]: len(l["tasks"]) for l in body["lanes"]}

        # Search narrows lanes; it does not collapse them into one list.
        assert by_lane["pending"] == 1
        assert by_lane["in_progress"] == 1

    async def test_board_search_keeps_lane_order_not_relevance_order(self, setup):
        first = await self._create(setup, "Encoder alpha")
        second = await self._create(setup, "Encoder beta")
        # Put them in an order relevance ranking would not produce.
        setup.client.post(f"/api/tasks/{second}/move", json={"before_id": first})

        body = setup.client.get("/api/tasks/board?q=encoder").json()
        pending = next(l for l in body["lanes"] if l["status"] == "pending")

        assert [t["id"] for t in pending["tasks"]] == [first, second]

    async def test_board_search_finds_a_task_beyond_the_lane_page(self, setup):
        """Why this is server-side rather than a client-side filter.

        The board holds one page per lane, so filtering what the client
        already has would silently miss anything deeper and report no
        results for a task that exists.
        """
        for i in range(4):
            await self._create(setup, f"Filler task {i}")
        await self._create(setup, "Buried encoder task")

        body = setup.client.get("/api/tasks/board?limit=2&q=encoder").json()
        titles = [t["title"] for lane in body["lanes"] for t in lane["tasks"]]

        assert titles == ["Buried encoder task"]

    async def test_board_search_with_no_matches_returns_empty_lanes(self, setup):
        await self._create(setup, "Something else entirely")

        body = setup.client.get("/api/tasks/board?q=nonexistentterm").json()

        # Lanes still present, just empty — the client needs the columns to
        # render its "no matches" state in the right shape.
        assert body["lanes"]
        assert all(len(l["tasks"]) == 0 for l in body["lanes"])

    async def test_board_search_combines_with_a_tag_filter(self, setup):
        await self._create(setup, "Encoder backend work", tags="backend")
        await self._create(setup, "Encoder frontend work", tags="frontend")

        body = setup.client.get("/api/tasks/board?q=encoder&tag=backend").json()
        titles = [t["title"] for lane in body["lanes"] for t in lane["tasks"]]

        assert titles == ["Encoder backend work"]

    # ── Tag facets ───────────────────────────────────────────────────────

    async def test_tags_endpoint_counts_and_ranks(self, setup):
        await self._create(setup, "Tag task one", tags="backend,ui")
        await self._create(setup, "Tag task two", tags="backend")

        tags = setup.client.get("/api/tasks/tags").json()["tags"]

        assert tags[0] == {"name": "backend", "count": 2}
        assert {"name": "ui", "count": 1} in tags

    async def test_tags_endpoint_excludes_done_by_default(self, setup):
        task_id = await self._create(setup, "Finish me", tags="ephemeral")
        setup.client.patch(f"/api/tasks/{task_id}", json={"status": "done"})

        assert setup.client.get("/api/tasks/tags").json()["tags"] == []
        included = setup.client.get("/api/tasks/tags?include_done=true").json()["tags"]
        assert included == [{"name": "ephemeral", "count": 1}]

    # ── Move ─────────────────────────────────────────────────────────────

    async def test_move_returns_the_full_updated_row(self, setup):
        task_id = await self._create(setup, "Move me")

        resp = setup.client.post(
            f"/api/tasks/{task_id}/move", json={"status": "in_progress"},
        )

        assert resp.status_code == 200
        task = resp.json()["task"]
        # The client reconciles its optimistic update against this, so it
        # needs the whole row — not {"moved": true}.
        assert task["id"] == task_id
        assert task["status"] == "in_progress"
        assert "position" in task

    async def test_move_reorders_within_a_lane(self, setup):
        top = await self._create(setup, "Order top")
        bottom = await self._create(setup, "Order bottom")

        setup.client.post(f"/api/tasks/{top}/move", json={"before_id": bottom})

        listing = setup.client.get("/api/tasks?sort=position").json()["tasks"]
        assert [t["id"] for t in listing] == [bottom, top]

    async def test_move_out_of_done_moves_the_file_back(self, setup):
        """The drag-out-of-Done path, end to end through HTTP."""
        task_id = await self._create(setup, "Round trip")
        setup.client.patch(f"/api/tasks/{task_id}", json={"status": "done"})
        done_dir = setup.workspace / "memory" / "tasks" / "done"
        assert (done_dir / f"{task_id}.md").exists()

        resp = setup.client.post(
            f"/api/tasks/{task_id}/move", json={"status": "pending"},
        )

        assert resp.status_code == 200
        active_dir = setup.workspace / "memory" / "tasks" / "active"
        assert (active_dir / f"{task_id}.md").exists()
        assert not (done_dir / f"{task_id}.md").exists()

    async def test_move_rejects_an_unknown_status(self, setup):
        task_id = await self._create(setup, "Bad lane")

        resp = setup.client.post(
            f"/api/tasks/{task_id}/move", json={"status": "not_a_status"},
        )

        assert resp.status_code == 422
        assert (await setup.db.get_task(task_id))["status"] == "pending"

    async def test_move_on_a_missing_task_is_404(self, setup):
        resp = setup.client.post("/api/tasks/nope/move", json={"status": "pending"})
        assert resp.status_code == 404

    # ── History ──────────────────────────────────────────────────────────

    async def test_events_endpoint_returns_the_transition_history(self, setup):
        task_id = await self._create(setup, "Tracked task")
        setup.client.patch(f"/api/tasks/{task_id}", json={"status": "in_progress"})

        resp = setup.client.get(f"/api/tasks/{task_id}/events")

        assert resp.status_code == 200
        events = resp.json()["events"]
        assert [(e["from_status"], e["to_status"]) for e in events] == [
            (None, "pending"), ("pending", "in_progress"),
        ]

    async def test_events_route_is_not_shadowed_by_the_id_route(self, setup):
        task_id = await self._create(setup, "Shadow check")
        body = setup.client.get(f"/api/tasks/{task_id}/events").json()
        assert "events" in body, "resolved to the task-detail route instead"

    async def test_move_via_http_is_recorded_with_the_web_actor(self, setup):
        task_id = await self._create(setup, "Dragged task")

        setup.client.post(f"/api/tasks/{task_id}/move", json={"status": "in_progress"})

        events = setup.client.get(f"/api/tasks/{task_id}/events").json()["events"]
        assert events[-1]["to_status"] == "in_progress"
        # Distinguishes a human dragging a card from the agent moving it.
        assert events[-1]["actor"] == "web"

    async def test_reorder_within_a_lane_records_no_event(self, setup):
        first = await self._create(setup, "Reorder one")
        second = await self._create(setup, "Reorder two")

        setup.client.post(f"/api/tasks/{second}/move", json={"before_id": first})

        events = setup.client.get(f"/api/tasks/{second}/events").json()["events"]
        assert len(events) == 1, "a pure reorder is not a status change"

    async def test_board_reports_when_cards_entered_their_status(self, setup):
        task_id = await self._create(setup, "Aging card")

        body = setup.client.get("/api/tasks/board").json()

        # Drives the card aging indicator; absent means "unknown", not zero.
        assert task_id in body["status_since"]

    # ── Create ───────────────────────────────────────────────────────────

    async def test_create_returns_the_structured_task(self, setup):
        resp = setup.client.post("/api/tasks", json={
            "title": "Structured create",
            "content": "details",
            "tags": "alpha,beta",
            "confirm_duplicate": True,
        })

        assert resp.status_code == 200
        task = resp.json()["task"]
        assert task["title"] == "Structured create"
        assert task["tags"] == "alpha,beta"

    async def test_create_honours_an_initial_status(self, setup):
        resp = setup.client.post("/api/tasks", json={
            "title": "Starts in progress",
            "content": "details",
            "status": "in_progress",
            "confirm_duplicate": True,
        })

        assert resp.json()["task"]["status"] == "in_progress"

    # The duplicate guard has two strategies; these tests drive the
    # ``source_url`` one because it is an exact match. The fuzzy fallback is
    # BM25-ranked against a threshold, and BM25 is corpus-relative — on a
    # two-document test index the IDF term collapses and even an identical
    # title scores above the cutoff. Exercising the ranking itself belongs
    # with the handler tests; what matters here is the route's status code.
    async def test_duplicate_refusal_is_a_409_with_the_matches(self, setup):
        url = "https://example.invalid/issues/1"
        await self._create(setup, "Original task", source_url=url)

        resp = setup.client.post("/api/tasks", json={
            "title": "A different title, same source",
            "content": "details",
            "source_url": url,
        })

        # Previously a 200 carrying an apology in a text blob, which no
        # client could distinguish from success.
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["reason"] == "duplicate"
        assert detail["duplicates"], "the 409 must name what it collided with"
        assert detail["duplicates"][0]["title"] == "Original task"

    async def test_confirm_duplicate_overrides_the_refusal(self, setup):
        url = "https://example.invalid/issues/2"
        await self._create(setup, "Original task", source_url=url)

        resp = setup.client.post("/api/tasks", json={
            "title": "Deliberate second copy",
            "content": "details",
            "source_url": url,
            "confirm_duplicate": True,
        })

        assert resp.status_code == 200

    async def test_create_rejects_an_unknown_status_as_422(self, setup):
        """A bad field is not a collision, and must not answer like one.

        Both refusals arrive as ``created: false``, so a client that only
        reads the status code would be told to offer "create anyway" for a
        status that does not exist — a retry that cannot succeed. ``/move``
        already answers 422 for the same mistake; this keeps the two routes
        saying the same thing.
        """
        resp = setup.client.post("/api/tasks", json={
            "title": "Task in a status that does not exist",
            "content": "details",
            "status": "nope",
        })

        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert detail["reason"] == "invalid_status"
        assert "nope" in detail["message"]

    # ── Patch ────────────────────────────────────────────────────────────

    async def test_patch_returns_the_full_row(self, setup):
        task_id = await self._create(setup, "Patch me")

        resp = setup.client.patch(
            f"/api/tasks/{task_id}", json={"title": "Patched title"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["task"]["title"] == "Patched title"
        # Legacy keys stay for existing callers.
        assert body["task_id"] == task_id and body["updated"] is True

    async def test_patch_writes_content_before_flipping_status(self, setup):
        """Ordering guard for routes/tasks.py.

        ``done`` moves the file out of ``active/`` and unlinks the source,
        so a status flip applied before the content write would write into
        a path that is about to disappear — losing the edit.
        """
        task_id = await self._create(setup, "Content then status")

        resp = setup.client.patch(f"/api/tasks/{task_id}", json={
            "content": f"# Content then status\n\nfinal body\n",
            "status": "done",
        })

        assert resp.status_code == 200
        done_file = setup.workspace / "memory" / "tasks" / "done" / f"{task_id}.md"
        assert done_file.exists()
        assert "final body" in done_file.read_text()

    async def test_patch_can_clear_a_deadline(self, setup):
        task_id = await self._create(setup, "Dated", deadline="2026-12-01")
        assert (await setup.db.get_task(task_id))["deadline"] == "2026-12-01"

        resp = setup.client.patch(f"/api/tasks/{task_id}", json={"deadline": ""})

        assert resp.status_code == 200
        assert not (await setup.db.get_task(task_id))["deadline"]

    async def test_patch_can_clear_all_tags(self, setup):
        task_id = await self._create(setup, "Tagged", tags="alpha")

        setup.client.patch(f"/api/tasks/{task_id}", json={"tags": ""})

        assert (await setup.db.get_task(task_id))["tags"] == ""

    async def test_patch_leaves_omitted_fields_alone(self, setup):
        task_id = await self._create(
            setup, "Untouched", tags="keepme", deadline="2026-12-01",
        )

        setup.client.patch(f"/api/tasks/{task_id}", json={"note": "just a note"})

        row = await setup.db.get_task(task_id)
        assert row["tags"] == "keepme"
        assert row["deadline"] == "2026-12-01"

    async def test_patch_surfaces_an_invalid_status(self, setup):
        task_id = await self._create(setup, "Bad status")

        resp = setup.client.patch(
            f"/api/tasks/{task_id}", json={"status": "not_a_status"},
        )

        # Previously returned 200 {"updated": true} while changing nothing.
        assert resp.status_code == 400
        assert (await setup.db.get_task(task_id))["status"] == "pending"

    async def test_patch_on_a_missing_task_is_404(self, setup):
        resp = setup.client.patch("/api/tasks/nope", json={"status": "pending"})
        assert resp.status_code == 404

    # ── List ─────────────────────────────────────────────────────────────

    async def test_list_accepts_a_tag_filter(self, setup):
        await self._create(setup, "Listed tagged", tags="infra")
        await self._create(setup, "Listed plain")

        body = setup.client.get("/api/tasks?tag=infra").json()

        assert [t["title"] for t in body["tasks"]] == ["Listed tagged"]
        assert body["total"] == 1

    async def test_list_accepts_position_sort(self, setup):
        first = await self._create(setup, "Sort one")
        second = await self._create(setup, "Sort two")

        body = setup.client.get("/api/tasks?sort=position").json()

        # Newest first by default rank; an unknown sort would silently fall
        # back to deadline order, which here is the same — so assert the
        # explicit reorder instead.
        setup.client.post(f"/api/tasks/{second}/move", json={"before_id": first})
        body = setup.client.get("/api/tasks?sort=position").json()
        assert [t["id"] for t in body["tasks"]] == [first, second]
