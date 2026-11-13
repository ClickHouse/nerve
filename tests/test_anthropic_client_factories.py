"""The Anthropic client factories use the credential the checks accept.

``credential_available()`` and ``resolve_anthropic_api_key()`` take
``ANTHROPIC_API_KEY`` from the environment, so an environment-provisioned
install is "configured" to the setup page, memory and the model catalogue.
The clients those callers build must carry the same key: an explicit
``api_key`` argument — ``""`` included — stops the SDK from reading the
environment itself, so a factory that passed only the config-file key sent
an empty key and every call ended in a 401.
"""

from __future__ import annotations

import pytest

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


# --------------------------------------------------------------------------- #
#  Bedrock region                                                              #
# --------------------------------------------------------------------------- #


@pytest.fixture
def no_aws_region(monkeypatch, tmp_path):
    """No region in the environment and no AWS config file."""
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    config_file = tmp_path / "aws-config"
    config_file.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))
    return config_file


def _bedrock(**provider) -> NerveConfig:
    return NerveConfig.from_dict({"provider": {
        "type": "bedrock", "aws_access_key_id": "test", "aws_secret_access_key": "test",
        **provider,
    }})


def _regions(config: NerveConfig) -> set[str]:
    """The region of each Bedrock client that Nerve makes."""
    from nerve.memory.memu_bridge import MemUBridge

    return {
        config.create_anthropic_client().aws_region,
        config.create_async_anthropic_client().aws_region,
        MemUBridge(config)._make_bedrock_client("model")._bedrock.aws_region,
    }


def test_bedrock_without_any_region_uses_us_east_1(no_aws_region):
    """The SDK refuses to make a Bedrock client without a region. Nerve
    gives it us-east-1 when nothing names one."""
    assert _regions(_bedrock()) == {"us-east-1"}


def test_bedrock_region_comes_from_the_environment(no_aws_region, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    assert _bedrock().bedrock_region() == ""
    assert _regions(_bedrock()) == {"eu-west-1"}
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    assert _regions(_bedrock()) == {"ap-southeast-2"}


def test_bedrock_region_comes_from_the_aws_profile(no_aws_region):
    no_aws_region.write_text("[profile nerve]\nregion = eu-central-1\n")
    config = NerveConfig.from_dict({"provider": {"type": "bedrock", "aws_profile": "nerve"}})
    assert config.bedrock_region() == ""
    assert _regions(config) == {"eu-central-1"}


def test_the_configured_bedrock_region_wins(no_aws_region, monkeypatch):
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    assert _regions(_bedrock(aws_region="us-west-2")) == {"us-west-2"}
