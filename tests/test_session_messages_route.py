"""HTTP tests for ``GET /api/sessions/{id}/messages``.

Harness mirrors test_session_parent_patch.py: a minimal FastAPI app with the
real sessions router and auth replaced by ``bypass_auth``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.agent.sessions import SessionManager
from nerve.db import Database

from tests.actor_rows import ensure_actor_row
from tests.conftest import TEST_ACTOR


@pytest.mark.asyncio
class TestSessionMessagesRoute:
    @pytest_asyncio.fixture
    async def setup(self, db: Database, bypass_auth):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.sessions import router as sessions_router

        cfg = NerveConfig()
        cfg_mod._config = cfg
        await ensure_actor_row(db, TEST_ACTOR)

        sm = SessionManager(db)
        engine = SimpleNamespace(
            config=cfg, sessions=sm,
            has_live_background_tasks=lambda sid: False,
        )
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(sessions_router)
        bypass_auth(app)
        yield SimpleNamespace(client=TestClient(app), db=db, sm=sm)

        cfg_mod._config = None

    async def test_thinking_column_is_not_sent(self, setup):
        await setup.sm.get_or_create("s1", source="web", actor=None)
        await setup.db.add_message("s1", "user", "hello", actor=None)
        await setup.db.add_message(
            "s1", "assistant", "the answer",
            thinking="a long chain of thought",
            blocks=[
                {"type": "thinking", "content": "a long chain of thought"},
                {"type": "text", "content": "the answer"},
            ],
            actor=None,
        )

        r = setup.client.get("/api/sessions/s1/messages")

        assert r.status_code == 200
        messages = r.json()["messages"]
        assert [m["role"] for m in messages] == ["user", "assistant"]
        assert all("thinking" not in m for m in messages)
        assistant = messages[1]
        assert assistant["content"] == "the answer"
        assert assistant["blocks"][0] == {
            "type": "thinking", "content": "a long chain of thought",
        }
