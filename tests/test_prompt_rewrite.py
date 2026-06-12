"""Tests for the prompt rewrite feature.

Covers the PromptRewriteConfig section and the /api/prompt-rewrite
routes. The Anthropic client is faked — no network calls are made.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from nerve.config import AgentConfig, NerveConfig, PromptRewriteConfig


# ------------------------------------------------------------------ #
#  Config parsing                                                      #
# ------------------------------------------------------------------ #


class TestPromptRewriteConfig:
    def test_defaults(self):
        cfg = PromptRewriteConfig()
        assert cfg.enabled is True
        assert cfg.model == ""
        assert cfg.max_tokens == 1024
        assert cfg.timeout_seconds == 20.0

    def test_from_dict_overrides(self):
        cfg = PromptRewriteConfig.from_dict({
            "enabled": False,
            "model": "some-fast-model",
            "max_tokens": 512,
            "timeout_seconds": 5,
        })
        assert cfg.enabled is False
        assert cfg.model == "some-fast-model"
        assert cfg.max_tokens == 512
        assert cfg.timeout_seconds == 5.0

    def test_nested_in_agent_config(self):
        agent = AgentConfig.from_dict({
            "model": "claude-opus-4-8",
            "prompt_rewrite": {"enabled": False, "model": "m"},
        })
        assert agent.prompt_rewrite.enabled is False
        assert agent.prompt_rewrite.model == "m"

    def test_agent_config_without_section_uses_defaults(self):
        agent = AgentConfig.from_dict({"model": "claude-opus-4-8"})
        assert agent.prompt_rewrite.enabled is True
        assert agent.prompt_rewrite.model == ""


# ------------------------------------------------------------------ #
#  Routes                                                              #
# ------------------------------------------------------------------ #


class _FakeMessages:
    """Async stand-in for anthropic client .messages namespace."""

    def __init__(self, reply_text: str | None, error: Exception | None = None):
        self.reply_text = reply_text
        self.error = error
        self.calls: list[dict] = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        content = []
        if self.reply_text is not None:
            content.append(SimpleNamespace(type="text", text=self.reply_text))
        return SimpleNamespace(content=content)


class _FakeClient:
    def __init__(self, reply_text: str | None, error: Exception | None = None):
        self.messages = _FakeMessages(reply_text, error)


@pytest.fixture
def rewrite_app(tmp_path):
    """Minimal FastAPI app with the prompt-rewrite router and a clean
    global config (no jwt_secret → require_auth is a no-op)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import nerve.config as cfg_mod

    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.auth.jwt_secret = ""
    cfg.anthropic_api_key = "test-key"
    cfg_mod._config = cfg

    from nerve.gateway.routes.prompt_rewrite import router

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    yield SimpleNamespace(client=client, config=cfg)

    cfg_mod._config = None


def _install_fake_client(cfg: NerveConfig, fake: _FakeClient) -> None:
    cfg.create_async_anthropic_client = lambda timeout=60.0: fake  # type: ignore[method-assign]


class TestPromptRewriteRoutes:
    def test_status_reports_enabled_and_model(self, rewrite_app):
        resp = rewrite_app.client.get("/api/prompt-rewrite/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is True
        # No explicit model configured — falls back to title_model.
        assert body["model"] == rewrite_app.config.agent.title_model

    def test_status_respects_model_override(self, rewrite_app):
        rewrite_app.config.agent.prompt_rewrite.model = "custom-model"
        resp = rewrite_app.client.get("/api/prompt-rewrite/status")
        assert resp.json()["model"] == "custom-model"

    def test_rewrite_disabled_returns_403(self, rewrite_app):
        rewrite_app.config.agent.prompt_rewrite.enabled = False
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        assert resp.status_code == 403

    def test_empty_prompt_returns_400(self, rewrite_app):
        resp = rewrite_app.client.post("/api/prompt-rewrite", json={"prompt": "   "})
        assert resp.status_code == 400

    def test_no_credentials_returns_503(self, rewrite_app):
        rewrite_app.config.anthropic_api_key = ""
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "rewrite me please"},
        )
        assert resp.status_code == 503

    def test_overlong_prompt_returned_unchanged_without_model_call(self, rewrite_app):
        fake = _FakeClient("should never be used")
        _install_fake_client(rewrite_app.config, fake)
        long_prompt = "x" * 7000
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": long_prompt},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["changed"] is False
        assert body["rewritten"] == long_prompt
        assert fake.messages.calls == []

    def test_happy_path_returns_rewritten(self, rewrite_app):
        fake = _FakeClient("Refined: do the thing, step by step.")
        _install_fake_client(rewrite_app.config, fake)
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["changed"] is True
        assert body["rewritten"] == "Refined: do the thing, step by step."
        assert body["model"] == rewrite_app.config.agent.title_model
        # The original prompt is the only user message sent to the model.
        assert len(fake.messages.calls) == 1
        call = fake.messages.calls[0]
        assert call["messages"] == [{"role": "user", "content": "do the thing"}]
        assert "rewrite" in call["system"].lower()

    def test_unchanged_reply_reports_changed_false(self, rewrite_app):
        fake = _FakeClient("do the thing")
        _install_fake_client(rewrite_app.config, fake)
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        body = resp.json()
        assert body["changed"] is False
        assert body["rewritten"] == "do the thing"

    def test_empty_model_reply_reports_changed_false(self, rewrite_app):
        fake = _FakeClient(None)
        _install_fake_client(rewrite_app.config, fake)
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        body = resp.json()
        assert body["changed"] is False
        assert body["rewritten"] == "do the thing"

    def test_model_error_returns_502(self, rewrite_app):
        fake = _FakeClient(None, error=RuntimeError("boom"))
        _install_fake_client(rewrite_app.config, fake)
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        assert resp.status_code == 502

    def test_rewrite_model_override_used_in_call(self, rewrite_app):
        rewrite_app.config.agent.prompt_rewrite.model = "custom-model"
        fake = _FakeClient("better prompt")
        _install_fake_client(rewrite_app.config, fake)
        resp = rewrite_app.client.post(
            "/api/prompt-rewrite", json={"prompt": "do the thing"},
        )
        assert resp.json()["model"] == "custom-model"
        assert fake.messages.calls[0]["model"] == "custom-model"
