"""HTTP tests for ``GET /api/notifications``.

A minimal FastAPI app with the real notifications router and auth replaced by
``bypass_auth``. The counts beside the list ignore the filters and the limit
of the list: the panel shows "!" in the tab title from
``pending_answer_count``, also while the Notifications page shows answered
rows only.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.db import Database


@pytest.mark.asyncio
class TestListNotificationsRoute:
    @pytest_asyncio.fixture
    async def client(self, db: Database, bypass_auth):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.notifications import router

        init_deps(engine=SimpleNamespace(), db=db)  # type: ignore[arg-type]
        app = FastAPI()
        app.include_router(router)
        bypass_auth(app)
        return TestClient(app)

    async def _row(self, db: Database, nid: str, type: str, *, status: str = "pending",
                   channels: tuple[str, ...] = ("web",)) -> None:
        await db.create_notification(
            notification_id=nid, session_id="cron:nightly", type=type,
            title=f"{type} {nid}",
        )
        await db.update_notification(
            nid, status=status, channels_delivered=json.dumps(list(channels)),
        )

    async def test_counts_ignore_the_filters_and_the_limit(self, client, db: Database):
        await self._row(db, "q1", "question")
        await self._row(db, "q2", "question", channels=("web", "telegram"))
        await self._row(db, "a1", "approval")
        await self._row(db, "n1", "notify")
        await self._row(db, "q3", "question", status="answered")
        await self._row(db, "q4", "question", status="answered")
        await self._row(db, "a2", "approval", status="expired")
        # Not delivered to the web channel: the panel does not show it.
        await self._row(db, "q5", "question", channels=("telegram",))

        body = client.get("/api/notifications?status=answered&limit=1").json()

        assert [n["status"] for n in body["notifications"]] == ["answered"]
        assert body["pending_count"] == 4
        assert body["pending_answer_count"] == 3

        body = client.get("/api/notifications?type=notify").json()

        assert [n["id"] for n in body["notifications"]] == ["n1"]
        assert body["pending_answer_count"] == 3

    async def test_no_pending_answer(self, client, db: Database):
        await self._row(db, "n1", "notify")
        await self._row(db, "q1", "question", status="answered")

        body = client.get("/api/notifications").json()

        assert body["pending_count"] == 1
        assert body["pending_answer_count"] == 0
