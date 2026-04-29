"""Tests for nerve.observability.langfuse — covers no-op behavior when
keys are absent, redaction, init wiring with mocked SDK, and flush.

These tests deliberately avoid importing the real langfuse package by
patching ``sys.modules`` before ``init_langfuse`` runs. That way they
pass regardless of whether the optional observability deps are installed.
"""

from __future__ import annotations

import os
import re
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from nerve.observability import langfuse as lf


@pytest.fixture(autouse=True)
def _reset_lf_state(monkeypatch):
    """Reset module state and clear LANGFUSE_* env vars between tests."""
    lf._enabled = False
    lf._host = ""
    lf._redact_patterns = []
    lf._last_flush_at = None
    lf._auth_ok = False
    for var in (
        "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST",
        "LANGSMITH_OTEL_ENABLED", "LANGSMITH_OTEL_ONLY", "LANGSMITH_TRACING",
    ):
        monkeypatch.delenv(var, raising=False)
    yield
    lf._enabled = False
    lf._redact_patterns = []


def _config_with(**kwargs):
    """Build a minimal config-shaped object with a ``langfuse`` attr."""
    from nerve.config import LangfuseConfig
    cfg = SimpleNamespace()
    cfg.langfuse = LangfuseConfig(**kwargs)
    return cfg


# ---------------------------------------------------------------------------
# init_langfuse — disabled paths
# ---------------------------------------------------------------------------


def test_init_noop_without_keys():
    assert lf.init_langfuse(_config_with()) is False
    assert lf.is_enabled() is False
    status = lf.get_status()
    assert status["enabled"] is False
    assert status["auth_ok"] is False
    assert status["host"] is None


def test_init_noop_when_only_public_key():
    assert lf.init_langfuse(_config_with(public_key="pk-lf-x")) is False


def test_init_noop_when_only_secret_key():
    assert lf.init_langfuse(_config_with(secret_key="sk-lf-x")) is False


def test_init_noop_when_config_lacks_langfuse_attr():
    """If the config object has no ``langfuse`` field at all, init bails."""
    bare = SimpleNamespace()
    assert lf.init_langfuse(bare) is False


# ---------------------------------------------------------------------------
# init_langfuse — happy path with mocked SDK
# ---------------------------------------------------------------------------


def _install_fake_langfuse(monkeypatch, *, auth_ok: bool = True):
    """Install a fake langfuse + langsmith + opentelemetry-instrumentation
    into sys.modules so ``init_langfuse`` doesn't touch the real packages.

    Returns the configure_claude_agent_sdk and AnthropicInstrumentor mocks
    so tests can assert they were called.
    """
    fake_client = MagicMock()
    fake_client.auth_check.return_value = auth_ok
    fake_client.flush = MagicMock()

    fake_lf = MagicMock()
    fake_lf.get_client = MagicMock(return_value=fake_client)
    # propagate_attributes returns a context manager — use MagicMock's
    # __enter__/__exit__ defaults.
    fake_lf.propagate_attributes = MagicMock(return_value=MagicMock(
        __enter__=MagicMock(return_value=None),
        __exit__=MagicMock(return_value=False),
    ))

    fake_configure = MagicMock()
    fake_langsmith_int = MagicMock()
    fake_langsmith_int.configure_claude_agent_sdk = fake_configure

    fake_instrumentor_inst = MagicMock()
    fake_instrumentor_cls = MagicMock(return_value=fake_instrumentor_inst)
    fake_anthropic_instr = MagicMock()
    fake_anthropic_instr.AnthropicInstrumentor = fake_instrumentor_cls

    monkeypatch.setitem(sys.modules, "langfuse", fake_lf)
    monkeypatch.setitem(sys.modules, "langsmith", MagicMock())
    monkeypatch.setitem(sys.modules, "langsmith.integrations", MagicMock())
    monkeypatch.setitem(
        sys.modules, "langsmith.integrations.claude_agent_sdk",
        fake_langsmith_int,
    )
    monkeypatch.setitem(sys.modules, "opentelemetry", MagicMock())
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation", MagicMock())
    monkeypatch.setitem(
        sys.modules, "opentelemetry.instrumentation.anthropic",
        fake_anthropic_instr,
    )
    return SimpleNamespace(
        client=fake_client,
        configure=fake_configure,
        instrumentor_inst=fake_instrumentor_inst,
        instrumentor_cls=fake_instrumentor_cls,
    )


def test_init_with_keys_sets_env_and_calls_instrumentors(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)

    cfg = _config_with(
        public_key="pk-lf-test",
        secret_key="sk-lf-test",
        host="https://cloud.langfuse.com",
    )
    assert lf.init_langfuse(cfg) is True
    assert lf.is_enabled() is True

    # Env vars set
    assert os.environ["LANGFUSE_PUBLIC_KEY"] == "pk-lf-test"
    assert os.environ["LANGFUSE_SECRET_KEY"] == "sk-lf-test"
    assert os.environ["LANGFUSE_HOST"] == "https://cloud.langfuse.com"
    assert os.environ["LANGSMITH_OTEL_ENABLED"] == "true"
    assert os.environ["LANGSMITH_OTEL_ONLY"] == "true"
    assert os.environ["LANGSMITH_TRACING"] == "true"

    # Instrumentors called
    fakes.client.auth_check.assert_called_once()
    fakes.configure.assert_called_once()
    fakes.instrumentor_cls.assert_called_once()
    fakes.instrumentor_inst.instrument.assert_called_once()


def test_init_disabled_when_auth_check_fails(monkeypatch):
    _install_fake_langfuse(monkeypatch, auth_ok=False)
    cfg = _config_with(public_key="pk-lf-bad", secret_key="sk-lf-bad")
    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False


def test_init_disabled_when_auth_check_raises(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.client.auth_check.side_effect = RuntimeError("network down")
    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")
    assert lf.init_langfuse(cfg) is False
    assert lf.is_enabled() is False


def test_init_continues_when_anthropic_instrumentor_fails(monkeypatch):
    """Failing to instrument Anthropic SDK shouldn't disable the rest."""
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    fakes.instrumentor_inst.instrument.side_effect = RuntimeError("boom")

    cfg = _config_with(public_key="pk-lf-x", secret_key="sk-lf-x")
    assert lf.init_langfuse(cfg) is True  # still enabled overall
    assert lf.is_enabled() is True


# ---------------------------------------------------------------------------
# attributes()
# ---------------------------------------------------------------------------


def test_attributes_nullcontext_when_disabled():
    """When disabled, attributes() must yield without touching the SDK."""
    with lf.attributes(session_id="s1", tags=["a"]):
        pass  # Just verify no crash and the block runs.


def test_attributes_calls_propagate_when_enabled(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    with lf.attributes(
        session_id="s1",
        user_id="u1",
        tags=["source:web", "model:opus-4-7"],
        metadata={"k": "v", "ignored": None},
    ):
        pass

    # Called once with the expected kwargs (None values stripped).
    sys.modules["langfuse"].propagate_attributes.assert_called_once()
    call_kwargs = sys.modules["langfuse"].propagate_attributes.call_args.kwargs
    assert call_kwargs["session_id"] == "s1"
    assert call_kwargs["user_id"] == "u1"
    assert call_kwargs["tags"] == ["source:web", "model:opus-4-7"]
    assert call_kwargs["metadata"] == {"k": "v"}


# ---------------------------------------------------------------------------
# redact()
# ---------------------------------------------------------------------------


def test_redact_noop_when_disabled():
    # Even with patterns set, when not enabled redact returns input unchanged.
    lf._redact_patterns = [re.compile(r"sk-ant-\w+")]
    assert lf.redact("hello sk-ant-AAAA") == "hello sk-ant-AAAA"


def test_redact_strips_anthropic_key_when_enabled():
    lf._enabled = True
    lf._redact_patterns = [re.compile(r"sk-ant-[A-Za-z0-9_\-]+")]
    assert lf.redact("API key: sk-ant-AAAABBBBCC123") == "API key: [REDACTED]"


def test_redact_handles_empty_input():
    lf._enabled = True
    lf._redact_patterns = [re.compile(r"x+")]
    assert lf.redact("") == ""


def test_redact_handles_no_patterns():
    lf._enabled = True
    lf._redact_patterns = []
    assert lf.redact("anything") == "anything"


# ---------------------------------------------------------------------------
# flush()
# ---------------------------------------------------------------------------


def test_flush_noop_when_disabled():
    # Should not raise even though no SDK is loaded.
    lf.flush()
    assert lf._last_flush_at is None


def test_flush_calls_client_flush_when_enabled(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    lf.flush()
    fakes.client.flush.assert_called_once()
    assert lf._last_flush_at is not None
    # ISO timestamp shape
    assert "T" in lf._last_flush_at


def test_flush_swallows_errors(monkeypatch):
    fakes = _install_fake_langfuse(monkeypatch, auth_ok=True)
    cfg = _config_with(public_key="pk-lf", secret_key="sk-lf")
    assert lf.init_langfuse(cfg) is True

    fakes.client.flush.side_effect = RuntimeError("network gone")
    # Must not raise.
    lf.flush()
