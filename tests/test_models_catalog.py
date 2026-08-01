"""Tests for the Anthropic model-catalog discovery (nerve/models_catalog.py).

The composer's model picker must offer what the configured credentials can
actually reach, and must degrade to the built-in list — never to an error or
a hang — when the Models API is unusable (Bedrock, no key, unreachable API).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from nerve import models_catalog
from nerve.config import NerveConfig


class FakeModels:
    """Stand-in for ``client.models`` — records calls, replays a page."""

    def __init__(self, ids: list[str], error: Exception | None = None):
        self._ids = ids
        self._error = error
        self.calls: list[dict] = []

    def list(self, **kwargs):
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return iter([SimpleNamespace(id=i) for i in self._ids])


class FakeClient:
    def __init__(self, ids: list[str], error: Exception | None = None):
        self.models = FakeModels(ids, error)
        self.closed = False

    def close(self):
        self.closed = True


def make_config(
    ids: list[str] | None = None,
    *,
    error: Exception | None = None,
    api_key: str = "sk-ant-test-0000000000",
    bedrock: bool = False,
) -> tuple[NerveConfig, list[FakeClient]]:
    """A config whose Anthropic client is a fake. Returns (config, clients)."""
    cfg = NerveConfig()
    cfg.anthropic_api_key = api_key
    if bedrock:
        cfg.provider.type = "bedrock"

    clients: list[FakeClient] = []

    def _create(timeout: float = 60.0):
        client = FakeClient(ids or [], error)
        clients.append(client)
        return client

    cfg.create_anthropic_client = _create  # type: ignore[method-assign]
    return cfg, clients


@pytest.fixture(autouse=True)
def _clean_cache():
    models_catalog.reset_cache()
    yield
    models_catalog.reset_cache()


class TestFetchModels:
    def test_returns_ids_in_api_order(self):
        cfg, clients = make_config(
            ["claude-opus-5", "claude-fable-5", "claude-sonnet-4-6"],
        )
        assert models_catalog.fetch_models(cfg) == [
            "claude-opus-5", "claude-fable-5", "claude-sonnet-4-6",
        ]
        assert clients[0].closed is True

    def test_filters_non_claude_and_dedupes(self):
        """A proxy-served catalog can carry other upstreams — drop them."""
        cfg, _ = make_config([
            "claude-opus-5", "llama3:8b", "claude-opus-5", "gpt-5.6-sol",
            "claude-haiku-4-5-20251001",
        ])
        assert models_catalog.fetch_models(cfg) == [
            "claude-opus-5", "claude-haiku-4-5-20251001",
        ]

    def test_bedrock_never_queries(self):
        cfg, clients = make_config(["claude-opus-5"], bedrock=True)
        assert models_catalog.fetch_models(cfg) == []
        assert clients == []

    def test_no_api_key_never_queries(self):
        cfg, clients = make_config(["claude-opus-5"], api_key="")
        assert models_catalog.fetch_models(cfg) == []
        assert clients == []

    def test_api_error_is_swallowed(self):
        cfg, clients = make_config(error=RuntimeError("connection refused"))
        assert models_catalog.fetch_models(cfg) == []
        assert clients[0].closed is True  # client still cleaned up


class TestGetModels:
    @pytest.mark.asyncio
    async def test_caches_between_calls(self):
        cfg, clients = make_config(["claude-opus-5", "claude-fable-5"])

        first = await models_catalog.get_models(cfg)
        second = await models_catalog.get_models(cfg)

        assert first == second == ["claude-opus-5", "claude-fable-5"]
        assert len(clients) == 1  # second call served from cache

    @pytest.mark.asyncio
    async def test_failure_is_backed_off_not_retried(self):
        cfg, clients = make_config(error=RuntimeError("boom"))

        assert await models_catalog.get_models(cfg) == []
        assert await models_catalog.get_models(cfg) == []
        assert len(clients) == 1  # unreachable API is not hammered

    @pytest.mark.asyncio
    async def test_credential_change_invalidates_cache(self):
        cfg, clients = make_config(["claude-opus-5"])
        assert await models_catalog.get_models(cfg) == ["claude-opus-5"]

        cfg.anthropic_api_key = "sk-ant-other-1111111111"
        await models_catalog.get_models(cfg)

        assert len(clients) == 2  # a different account gets its own catalog

    @pytest.mark.asyncio
    async def test_stale_entry_is_served_while_refreshing(self, monkeypatch):
        cfg, clients = make_config(["claude-opus-5"])
        assert await models_catalog.get_models(cfg) == ["claude-opus-5"]

        # Age the entry past the TTL — the stale list must still come back
        # immediately (the refresh happens in the background).
        monkeypatch.setitem(
            models_catalog._cache, "checked_at",
            models_catalog._cache["checked_at"] - models_catalog._TTL_SECONDS - 1,
        )
        assert await models_catalog.get_models(cfg) == ["claude-opus-5"]

        task = models_catalog._refresh_task
        assert task is not None
        await task
        assert len(clients) == 2

    @pytest.mark.asyncio
    async def test_prime_never_raises(self):
        cfg, _ = make_config(error=RuntimeError("boom"))
        assert await models_catalog.prime(cfg) == []
