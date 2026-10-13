"""No network without a credential — and an environment credential counts.

``NerveConfig.credential_available()`` / ``resolve_anthropic_api_key()`` are
the one answer the setup page, memU, title generation and the model catalogue
share (C31). This pins their semantics and the memU recall gate; the warm-up
gate inside ``MemUBridge._initialize_impl`` reads the same predicate and is
exercised by the scripted gateway boot (no 401s with nothing configured).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nerve.config import NerveConfig
from nerve.memory.memu_bridge import MemUBridge

_KEY = "sk-ant-REPLACE0000000000"


class TestCredentialHelpers:
    def test_nothing_configured_means_no_credential(self):
        config = NerveConfig()
        assert config.credential_available() is False
        assert config.resolve_anthropic_api_key() == ""

    def test_config_key_wins(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-REPLACE-ENV00000000")
        config = NerveConfig(anthropic_api_key=_KEY)
        assert config.credential_available() is True
        assert config.resolve_anthropic_api_key() == _KEY

    def test_env_api_key_counts_as_credential(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        config = NerveConfig()
        assert config.credential_available() is True
        assert config.resolve_anthropic_api_key() == _KEY

    def test_oauth_token_is_a_credential_but_not_an_api_key(self, monkeypatch):
        config = NerveConfig(claude_oauth_token="sk-ant-oat01-REPLACE")
        assert config.credential_available() is True
        assert config.resolve_anthropic_api_key() == ""
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "sk-ant-oat01-REPLACE-ENV")
        assert NerveConfig().credential_available() is True
        assert NerveConfig().resolve_anthropic_api_key() == ""

    def test_bedrock_and_proxy_take_no_key_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        bedrock = NerveConfig.from_dict({"provider": {"type": "bedrock"}})
        assert bedrock.credential_available() is True
        assert bedrock.resolve_anthropic_api_key() == ""
        proxy = NerveConfig.from_dict({"proxy": {"enabled": True}})
        assert proxy.credential_available() is True
        assert proxy.resolve_anthropic_api_key() == proxy.effective_api_key

    def test_claude_oauth_token_is_read_from_the_private_file(self, tmp_path):
        from nerve.config import load_config, validate_config_keys

        (tmp_path / "config.yaml").write_text(f"workspace: {tmp_path / 'ws'}\n", encoding="utf-8")
        (tmp_path / "config.local.yaml").write_text("claude_oauth_token: sk-ant-oat01-REPLACE\n", encoding="utf-8")
        config = load_config(tmp_path)
        assert config.claude_oauth_token == "sk-ant-oat01-REPLACE"
        assert validate_config_keys({"claude_oauth_token": "x"}) == []


@pytest.mark.asyncio
class TestRecallGate:
    async def _bridge(self, config: NerveConfig) -> tuple[MemUBridge, MagicMock]:
        bridge = MemUBridge(config)
        bridge._available = True
        service = MagicMock()
        bridge._service = service
        calls = []

        async def _submit(coro):
            calls.append(coro)
            coro.close()
            return {"items": [], "categories": []}

        bridge._submit = _submit  # type: ignore[method-assign]
        bridge._calls = calls  # type: ignore[attr-defined]
        return bridge, service

    async def test_recall_returns_nothing_quietly_without_a_credential(self, caplog):
        import logging

        bridge, service = await self._bridge(NerveConfig())
        with caplog.at_level(logging.INFO, logger="nerve.memory.memu_bridge"):
            assert await bridge.recall("anything") == []
            assert await bridge.recall("again") == []
        assert service.retrieve.call_count == 0
        assert sum("no model provider configured" in r.getMessage() for r in caplog.records) == 1

    async def test_recall_runs_with_an_environment_key(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", _KEY)
        bridge, service = await self._bridge(NerveConfig())
        service.retrieve = MagicMock(side_effect=lambda **kw: _done({"items": [], "categories": []}))
        assert await bridge.recall("anything") == []
        assert service.retrieve.call_count == 1


async def _done(value):
    return value


@pytest.mark.asyncio
class TestApplyCategoriesLive:
    """The Mode step hands ``memory.categories`` to the running service."""

    class _Repo:
        def __init__(self):
            self.categories: dict = {}
            self.created: list[tuple[str, str]] = []

        def list_categories(self, where=None):
            return dict(self.categories)

        def get_or_create_category(self, *, name, description, embedding, user_data):
            self.created.append((name, description))
            cat = self.categories.get(name)
            if cat is None:
                from types import SimpleNamespace

                cat = SimpleNamespace(id=f"id-{name}", name=name, description=description)
                self.categories[name] = cat
            return cat

    def _service(self):
        from types import SimpleNamespace

        repo = self._Repo()
        ctx = SimpleNamespace(categories_ready=False, category_ids=[], category_name_to_id={})
        svc = SimpleNamespace(
            memorize_config=SimpleNamespace(memory_categories=[]),
            category_configs=[],
            category_config_map={},
            _category_prompt_str="",
            _format_categories_for_prompt=lambda cfgs: "\n".join(c.name for c in cfgs),
            _context=ctx,
            _get_context=lambda: ctx,
            database=SimpleNamespace(memory_category_repo=repo),
        )
        return svc, repo, ctx

    async def test_not_running_answers_false(self):
        bridge = MemUBridge(NerveConfig())
        assert await bridge.apply_categories([]) is False

    async def test_applies_the_set_like_a_boot_would(self, caplog):
        from nerve.config import MemoryCategoryConfig

        bridge = MemUBridge(NerveConfig())  # no embedding provider → no LLM call
        svc, repo, ctx = self._service()
        bridge._service = svc
        bridge._available = True
        repo.categories["work"] = type("C", (), {"id": "id-work", "name": "work", "description": "old"})()
        wanted = [
            MemoryCategoryConfig(name="work", description="Job, projects"),
            MemoryCategoryConfig(name="people", description="Facts about people"),
        ]
        assert await bridge._apply_categories_impl(wanted) is True
        # Only the missing one was created; the prompt list is the full set.
        assert repo.created == [("people", "Facts about people")]
        assert [c.name for c in svc.category_configs] == ["work", "people"]
        assert set(svc.category_config_map) == {"work", "people"}
        assert svc.memorize_config.memory_categories == svc.category_configs
        assert svc._category_prompt_str == "work\npeople"
        assert ctx.categories_ready is True
        assert ctx.category_name_to_id == {"work": "id-work", "people": "id-people"}
        assert sorted(ctx.category_ids) == ["id-people", "id-work"]
        # Idempotent: a second application creates nothing and keeps one entry each.
        assert await bridge._apply_categories_impl(wanted) is True
        assert repo.created == [("people", "Facts about people")]
        assert [c.name for c in svc.category_configs] == ["work", "people"]
