"""Real memU extraction/retrieval with deterministic, offline model responses."""

import os
from xml.sax.saxutils import escape

import pytest

from nerve.config import NerveConfig
from nerve.memory.postgres import MemoryService, MemoryStore, MemoryScope


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
