"""The Anthropic client factories use the credential the checks accept.

``credential_available()`` and ``resolve_anthropic_api_key()`` take
``ANTHROPIC_API_KEY`` from the environment, so an environment-provisioned
install is "configured" to the setup page, memory and the model catalogue.
The clients those callers build must carry the same key: an explicit
``api_key`` argument — ``""`` included — stops the SDK from reading the
environment itself, so a factory that passed only the config-file key sent
an empty ``X-Api-Key`` and every call ended in a 401.
"""

from __future__ import annotations

from nerve.config import NerveConfig

_ENV_KEY = "sk-ant-REPLACE-from-the-environment"
_FILE_KEY = "sk-ant-REPLACE-from-config-local"


def test_environment_only_key_reaches_both_clients(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", _ENV_KEY)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    config = NerveConfig.from_dict({})
    assert config.credential_available()
    assert config.resolve_anthropic_api_key() == _ENV_KEY
    assert config.create_anthropic_client().api_key == _ENV_KEY
    assert config.create_async_anthropic_client().api_key == _ENV_KEY


def test_configured_key_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", _ENV_KEY)
    config = NerveConfig.from_dict({"anthropic_api_key": _FILE_KEY})
    assert config.create_anthropic_client().api_key == _FILE_KEY
    assert config.create_async_anthropic_client().api_key == _FILE_KEY


def test_no_key_anywhere_still_builds_a_client(monkeypatch):
    """Construction never raises: callers that run without a credential keep
    failing at request time, as before, not at import or wiring time."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    config = NerveConfig.from_dict({})
    assert not config.credential_available()
    assert config.create_anthropic_client().api_key == ""
    assert config.create_async_anthropic_client().api_key == ""
