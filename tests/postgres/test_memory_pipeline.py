"""Real memU extraction/retrieval with deterministic, offline model responses."""

import os
from xml.sax.saxutils import escape

import pytest

from nerve.config import NerveConfig


class FixtureModel:
    chat_model = "fixture"
    embed_model = "fixture"

    async def chat(self, prompt, **kwargs):
        if prompt.startswith("EXTRACT\n"):
            return (
                "<knowledge><memory><content>"
                + escape(prompt.removeprefix("EXTRACT\n").removesuffix("\nEND"))
                + "</content><categories><category>facts</category></categories></memory></knowledge>"
            )
        return "A synthetic retained fact."

    async def embed(self, inputs):
        return [[1.0, 0.0, 0.0] for _ in inputs]


@pytest.mark.asyncio
async def test_extract_reconnect_retrieve(db_scope, tmp_path):
    from nerve.memory.postgres import MemoryService, MemoryScope, MemoryStore

    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": os.environ["NERVE_TEST_POSTGRES_DSN"],
            "workflow_id": db_scope,
        }
    )
    service = MemoryService(
        user_config={"model": MemoryScope},
        database_config={"metadata_store": {"provider": "inmemory"}},
        blob_config={"resources_dir": str(tmp_path / "resources")},
        llm_profiles={
            "default": {"base_url": "https://fixture.invalid", "api_key": "unused"}
        },
        memorize_config={
            "memory_types": ["knowledge"],
            "memory_type_prompts": {"knowledge": "EXTRACT\n{resource}\nEND"},
            "memory_categories": [{"name": "facts", "description": "Facts"}],
            "enable_item_reinforcement": True,
        },
        retrieve_config={
            "route_intention": False,
            "sufficiency_check": False,
            "category": {"enabled": False},
            "resource": {"enabled": False},
        },
    )
    service._llm_clients.update(default=FixtureModel(), embedding=FixtureModel())
    service.database = MemoryStore(config)
    source = tmp_path / "source.txt"
    source.write_text("The synthetic service uses port 1234.")
    try:
        async with service.database.guard():
            await service.memorize(resource_url=str(source), modality="text", user={})
        assert service.database.items
        service.database.close()
        source.unlink()
        service.database = MemoryStore(config)
        async with service.database.guard():
            service.database.restore_resources(tmp_path / "new-cache")
            result = await service.retrieve(
                queries=[{"role": "user", "content": {"text": "Which port?"}}], where={}
            )
        assert result["items"]
        assert "1234" in str(result)
    finally:
        service.database.close()


@pytest.mark.asyncio
async def test_recall_does_not_restore_or_wait_for_extraction(
    db_scope, tmp_path, monkeypatch
):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import Mock

    from nerve.memory.memu_bridge import MemUBridge
    from nerve.memory.postgres import MemoryStore

    config = NerveConfig(
        use_postgresql=True,
        postgresql_dsn=os.environ["NERVE_TEST_POSTGRES_DSN"],
        workflow_id=db_scope,
    )
    store = MemoryStore(config)
    entered, release = asyncio.Event(), asyncio.Event()
    store.restore_resources(tmp_path / "cache")
    restore = Mock(side_effect=AssertionError("must not restore per call"))
    monkeypatch.setattr(store, "restore_resources", restore)
    monkeypatch.setattr(
        store, "refresh", Mock(side_effect=AssertionError("must not reload corpus"))
    )

    async def memorize(**kwargs):
        entered.set()
        await release.wait()
        return "memorized"

    async def retrieve(**kwargs):
        return "recalled"

    bridge = object.__new__(MemUBridge)
    bridge.config = config
    bridge._service = SimpleNamespace(
        database=store, memorize=memorize, retrieve=retrieve
    )
    pending = asyncio.create_task(bridge._storage_call("memorize"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert await asyncio.wait_for(bridge._storage_call("retrieve"), 2) == "recalled"
        assert not pending.done()
        release.set()
        assert await pending == "memorized"
        assert await bridge._storage_call("memorize") == "memorized"
        restore.assert_not_called()
    finally:
        release.set()
        await pending
        store.close()
