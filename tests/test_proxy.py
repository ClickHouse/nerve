"""Tests for CLIProxyAPI integration — config, service, bootstrap."""

from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
import yaml

from nerve.config import NerveConfig, ProxyConfig, load_config


# ------------------------------------------------------------------ #
#  ProxyConfig                                                        #
# ------------------------------------------------------------------ #


class TestProxyConfigDefaults:
    """ProxyConfig should have sane defaults."""

    def test_defaults(self) -> None:
        pc = ProxyConfig()
        assert pc.enabled is False
        assert pc.port == 8317
        assert pc.host == "127.0.0.1"
        assert pc.api_key == "sk-nerve-local-proxy"
        assert pc.binary_path == Path("~/.nerve/bin/cli-proxy-api")
        assert pc.auth_dir == Path("~/.nerve/cli-proxy-auth")
        assert pc.log_file == Path("~/.nerve/proxy.log")

    def test_from_dict_empty(self) -> None:
        pc = ProxyConfig.from_dict({})
        assert pc.enabled is False
        assert pc.port == 8317

    def test_from_dict_custom(self) -> None:
        pc = ProxyConfig.from_dict({
            "enabled": True,
            "port": 9999,
            "host": "0.0.0.0",
            "api_key": "sk-custom-key",
            "binary_path": "/opt/bin/cli-proxy-api",
            "auth_dir": "/opt/auth",
            "log_file": "/var/log/proxy.log",
        })
        assert pc.enabled is True
        assert pc.port == 9999
        assert pc.host == "0.0.0.0"
        assert pc.api_key == "sk-custom-key"
        assert pc.binary_path == Path("/opt/bin/cli-proxy-api")
        assert pc.auth_dir == Path("/opt/auth")
        assert pc.log_file == Path("/var/log/proxy.log")

    def test_from_dict_path_expansion(self) -> None:
        pc = ProxyConfig.from_dict({"binary_path": "~/my-bin/proxy"})
        assert str(pc.binary_path).startswith("/")  # ~ was expanded
        assert "~" not in str(pc.binary_path)


# ------------------------------------------------------------------ #
#  NerveConfig — proxy properties                                     #
# ------------------------------------------------------------------ #


class TestNerveConfigProxyProperties:
    """Test anthropic_api_base_url and effective_api_key."""

    def test_proxy_disabled_uses_direct_url(self) -> None:
        cfg = NerveConfig.from_dict({"anthropic_api_key": "sk-ant-real"})
        assert cfg.anthropic_api_base_url == "https://api.anthropic.com/v1/"
        assert cfg.effective_api_key == "sk-ant-real"

    def test_proxy_disabled_empty_key(self) -> None:
        cfg = NerveConfig.from_dict({})
        assert cfg.effective_api_key == ""
        assert cfg.anthropic_api_base_url == "https://api.anthropic.com/v1/"

    def test_proxy_enabled_overrides_url(self) -> None:
        cfg = NerveConfig.from_dict({"proxy": {"enabled": True, "port": 8317}})
        assert cfg.anthropic_api_base_url == "http://127.0.0.1:8317/v1/"

    def test_proxy_enabled_overrides_key(self) -> None:
        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "api_key": "sk-local"},
            "anthropic_api_key": "sk-ant-ignored",
        })
        assert cfg.effective_api_key == "sk-local"
        # Real key still accessible directly if needed:
        assert cfg.anthropic_api_key == "sk-ant-ignored"

    def test_proxy_custom_host_port(self) -> None:
        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "host": "10.0.0.5", "port": 4000},
        })
        assert cfg.anthropic_api_base_url == "http://10.0.0.5:4000/v1/"

    def test_proxy_default_api_key(self) -> None:
        cfg = NerveConfig.from_dict({"proxy": {"enabled": True}})
        assert cfg.effective_api_key == "sk-nerve-local-proxy"


# ------------------------------------------------------------------ #
#  Config loading from YAML files                                     #
# ------------------------------------------------------------------ #


class TestProxyConfigYAML:
    """Test proxy config round-trips through YAML files."""

    def test_load_with_proxy_section(self, tmp_path: Path) -> None:
        (tmp_path / "config.yaml").write_text(yaml.dump({
            "proxy": {"enabled": True, "port": 7777},
        }))
        cfg = load_config(tmp_path)
        assert cfg.proxy.enabled is True
        assert cfg.proxy.port == 7777

    def test_load_without_proxy_section(self, tmp_path: Path) -> None:
        (tmp_path / "config.yaml").write_text("workspace: ~/ws\n")
        cfg = load_config(tmp_path)
        assert cfg.proxy.enabled is False
        assert cfg.proxy.port == 8317

    def test_proxy_in_local_overrides_base(self, tmp_path: Path) -> None:
        (tmp_path / "config.yaml").write_text(yaml.dump({
            "proxy": {"enabled": False, "port": 8317},
        }))
        (tmp_path / "config.local.yaml").write_text(yaml.dump({
            "proxy": {"enabled": True},
        }))
        cfg = load_config(tmp_path)
        assert cfg.proxy.enabled is True
        # Port from base config survives the merge:
        assert cfg.proxy.port == 8317


# ------------------------------------------------------------------ #
#  ProxyService                                                       #
# ------------------------------------------------------------------ #


class TestProxyServiceInit:
    """Test ProxyService initialization and config writing."""

    def test_creates_config_file(self, tmp_path: Path) -> None:
        from nerve.proxy.service import ProxyService

        cfg = NerveConfig.from_dict({
            "proxy": {
                "enabled": True,
                "port": 9000,
                "host": "127.0.0.1",
                "auth_dir": str(tmp_path / "auth"),
                "api_key": "sk-test-key",
            },
        })
        svc = ProxyService(cfg)
        svc._config_path = tmp_path / "proxy-config.yaml"

        written_path = svc._write_proxy_config()

        assert written_path.exists()
        data = yaml.safe_load(written_path.read_text())
        assert data["host"] == "127.0.0.1"
        assert data["port"] == 9000
        assert data["api-keys"] == ["sk-test-key"]
        assert data["auth-dir"] == str(tmp_path / "auth")
        assert (tmp_path / "auth").is_dir()  # auth dir was created

    def test_detect_asset_suffix(self) -> None:
        from nerve.proxy.service import _detect_asset_suffix

        # Should not raise on the current platform.
        suffix = _detect_asset_suffix()
        assert isinstance(suffix, str)
        assert "_" in suffix  # e.g. "linux_arm64"


class TestProxyServiceBinary:
    """Test binary detection and download."""

    @pytest.mark.asyncio
    async def test_ensure_binary_exists(self, tmp_path: Path) -> None:
        from nerve.proxy.service import ProxyService

        # Create a fake binary.
        binary = tmp_path / "cli-proxy-api"
        binary.write_bytes(b"#!/bin/sh\necho ok\n")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "binary_path": str(binary)},
        })
        svc = ProxyService(cfg)
        result = await svc.ensure_binary()
        assert result == binary  # No download needed.

    @pytest.mark.asyncio
    async def test_ensure_binary_downloads(self, tmp_path: Path) -> None:
        from nerve.proxy.service import ProxyService

        binary = tmp_path / "bin" / "cli-proxy-api"
        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "binary_path": str(binary)},
        })
        svc = ProxyService(cfg)

        with patch.object(svc, "_download_binary", new_callable=AsyncMock) as mock_dl:
            result = await svc.ensure_binary()
            mock_dl.assert_called_once_with(binary)

    @pytest.mark.asyncio
    async def test_download_binary_extracts(self, tmp_path: Path) -> None:
        """Verify _download_binary creates an executable at the destination."""
        import io
        import tarfile

        from nerve.proxy.service import ProxyService

        # Build a fake tar.gz containing a "cli-proxy-api" binary.
        archive_buf = io.BytesIO()
        with tarfile.open(fileobj=archive_buf, mode="w:gz") as tar:
            data = b"#!/bin/sh\necho fake-proxy\n"
            info = tarfile.TarInfo(name="cli-proxy-api")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        archive_bytes = archive_buf.getvalue()

        # Mock httpx to return our fake archive.
        mock_release = {
            "tag_name": "v1.0.0",
            "assets": [{
                "name": "CLIProxyAPI_1.0.0_linux_arm64.tar.gz",
                "browser_download_url": "https://example.com/fake.tar.gz",
            }],
        }

        mock_response_release = MagicMock()
        mock_response_release.json.return_value = mock_release
        mock_response_release.raise_for_status = MagicMock()

        mock_response_download = MagicMock()
        mock_response_download.content = archive_bytes
        mock_response_download.raise_for_status = MagicMock()

        async def mock_get(url, **kwargs):
            if "api.github.com" in url:
                return mock_response_release
            return mock_response_download

        mock_client = AsyncMock()
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        dest = tmp_path / "bin" / "cli-proxy-api"
        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "binary_path": str(dest)},
        })
        svc = ProxyService(cfg)

        with patch("httpx.AsyncClient", return_value=mock_client), \
             patch("nerve.proxy.service._detect_asset_suffix", return_value="linux_arm64"):
            await svc._download_binary(dest)

        assert dest.exists()
        assert dest.stat().st_mode & stat.S_IXUSR  # executable
        assert dest.read_bytes() == b"#!/bin/sh\necho fake-proxy\n"


class TestProxyServiceHealth:
    """Test health checking."""

    @pytest.mark.asyncio
    async def test_healthy_returns_true(self) -> None:
        from nerve.proxy.service import ProxyService

        cfg = NerveConfig.from_dict({"proxy": {"enabled": True, "port": 55555}})
        svc = ProxyService(cfg)

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            assert await svc.is_healthy() is True

    @pytest.mark.asyncio
    async def test_unhealthy_returns_false(self) -> None:
        from nerve.proxy.service import ProxyService

        cfg = NerveConfig.from_dict({"proxy": {"enabled": True, "port": 55555}})
        svc = ProxyService(cfg)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ConnectionError("refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            assert await svc.is_healthy() is False


class TestProxyServiceLifecycle:
    """Test start/stop with mocked subprocess."""

    @pytest.mark.asyncio
    async def test_start_launches_subprocess(self, tmp_path: Path) -> None:
        from nerve.proxy.service import ProxyService

        # Create a fake binary.
        binary = tmp_path / "cli-proxy-api"
        binary.write_bytes(b"#!/bin/sh\nwhile true; do sleep 1; done\n")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        cfg = NerveConfig.from_dict({
            "proxy": {
                "enabled": True,
                "binary_path": str(binary),
                "auth_dir": str(tmp_path / "auth"),
                "log_file": str(tmp_path / "proxy.log"),
            },
        })
        svc = ProxyService(cfg)
        svc._config_path = tmp_path / "proxy-config.yaml"

        # Mock health check to succeed immediately.
        with patch.object(svc, "_wait_for_healthy", new_callable=AsyncMock, return_value=True):
            await svc.start()

        assert svc._process is not None
        assert svc._process.returncode is None  # still running
        # Config file was written.
        assert (tmp_path / "proxy-config.yaml").exists()

        # Cleanup
        await svc.stop()
        assert svc._process is None

    @pytest.mark.asyncio
    async def test_start_raises_on_unhealthy(self, tmp_path: Path) -> None:
        from nerve.proxy.service import ProxyService

        binary = tmp_path / "cli-proxy-api"
        binary.write_bytes(b"#!/bin/sh\nexit 1\n")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        cfg = NerveConfig.from_dict({
            "proxy": {
                "enabled": True,
                "binary_path": str(binary),
                "auth_dir": str(tmp_path / "auth"),
                "log_file": str(tmp_path / "proxy.log"),
            },
        })
        svc = ProxyService(cfg)
        svc._config_path = tmp_path / "proxy-config.yaml"

        with patch.object(svc, "_wait_for_healthy", new_callable=AsyncMock, return_value=False):
            with pytest.raises(RuntimeError, match="failed to become healthy"):
                await svc.start()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self) -> None:
        from nerve.proxy.service import ProxyService

        cfg = NerveConfig.from_dict({"proxy": {"enabled": True}})
        svc = ProxyService(cfg)
        # No process running — stop should not raise.
        await svc.stop()
        assert svc._process is None


# ------------------------------------------------------------------ #
#  Bootstrap — proxy mode                                             #
# ------------------------------------------------------------------ #


class TestBootstrapProxy:
    """Test setup wizard proxy integration."""

    def test_setup_choices_has_proxy_field(self) -> None:
        from nerve.bootstrap import SetupChoices
        c = SetupChoices()
        assert c.use_proxy is False

    def test_non_interactive_proxy_mode(self, tmp_path: Path) -> None:
        """NERVE_USE_PROXY=1 should enable proxy without API key."""
        from nerve.bootstrap import run_non_interactive

        env = {
            "NERVE_USE_PROXY": "1",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert choices.use_proxy is True
        assert choices.anthropic_api_key == ""

        # config.yaml should have proxy section.
        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["proxy"]["enabled"] is True
        assert config["proxy"]["port"] == 8317

        # config.local.yaml should NOT have anthropic_api_key.
        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert "anthropic_api_key" not in local

    def test_non_interactive_proxy_with_api_key(self, tmp_path: Path) -> None:
        """Proxy mode should allow optional API key as fallback."""
        from nerve.bootstrap import run_non_interactive

        env = {
            "NERVE_USE_PROXY": "1",
            "ANTHROPIC_API_KEY": "sk-ant-api03-optional",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        with patch.dict(os.environ, env, clear=False):
            choices = run_non_interactive(tmp_path)

        assert choices.use_proxy is True
        assert choices.anthropic_api_key == "sk-ant-api03-optional"

    def test_non_interactive_requires_key_without_proxy(self, tmp_path: Path) -> None:
        """Without proxy, API key is still required."""
        from nerve.bootstrap import run_non_interactive

        env = {"NERVE_MODE": "personal", "NERVE_WORKSPACE": str(tmp_path / "ws")}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("NERVE_USE_PROXY", None)
            with pytest.raises(Exception, match="ANTHROPIC_API_KEY.*required"):
                run_non_interactive(tmp_path)

    def test_apply_with_proxy_writes_config(self, tmp_path: Path) -> None:
        """_apply() with use_proxy should create proxy config section."""
        from nerve.bootstrap import SetupWizard

        wizard = SetupWizard(tmp_path)
        wizard.choices.use_proxy = True
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert config["proxy"]["enabled"] is True

        # No API key in local config.
        local = yaml.safe_load((tmp_path / "config.local.yaml").read_text())
        assert "anthropic_api_key" not in local

    def test_apply_without_proxy_no_proxy_section(self, tmp_path: Path) -> None:
        """_apply() without proxy should not write proxy section."""
        from nerve.bootstrap import SetupWizard

        wizard = SetupWizard(tmp_path)
        wizard.choices.anthropic_api_key = "sk-ant-api03-test"
        wizard.choices.workspace_path = tmp_path / "workspace"
        wizard.choices.mode = "personal"

        wizard._apply()

        config = yaml.safe_load((tmp_path / "config.yaml").read_text())
        assert "proxy" not in config

    def test_non_interactive_error_message_mentions_proxy(self, tmp_path: Path) -> None:
        """Error when no API key should mention NERVE_USE_PROXY."""
        from nerve.bootstrap import run_non_interactive

        env = {"NERVE_MODE": "personal", "NERVE_WORKSPACE": str(tmp_path / "ws")}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("NERVE_USE_PROXY", None)
            with pytest.raises(Exception, match="NERVE_USE_PROXY"):
                run_non_interactive(tmp_path)


# ------------------------------------------------------------------ #
#  Subsystem integration — verify config helpers are wired correctly  #
# ------------------------------------------------------------------ #


class TestSubsystemConfigWiring:
    """Verify subsystems use the config helper properties."""

    def test_registry_uses_effective_key(self) -> None:
        """build_source_runners should use config factory with effective_api_key."""
        from nerve.sources.registry import build_source_runners

        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "api_key": "sk-proxy-key"},
            "anthropic_api_key": "sk-ant-should-be-ignored",
        })
        # Enable a source so condense factory gets used.
        cfg.sync.github.enabled = True

        mock_db = MagicMock()
        mock_db.get_consumer_cursor = AsyncMock(return_value=None)

        runners = build_source_runners(cfg, mock_db)

        # At least one runner should exist (GitHub).
        assert len(runners) >= 1
        runner = runners[0]
        # Model should be set from config.memory.fast_model
        assert runner.condense_model == "claude-haiku-4-5-20251001"
        # Factory should be set — delegates to config.create_async_anthropic_client
        assert runner._client_factory is not None
        # Calling the factory should produce a client with the proxy base URL
        client = runner._client_factory()
        assert "127.0.0.1" in str(client.base_url)

    def test_registry_no_proxy_uses_raw_key(self) -> None:
        """Without proxy, condense factory should create client with raw API key."""
        from nerve.sources.registry import build_source_runners

        cfg = NerveConfig.from_dict({
            "anthropic_api_key": "sk-ant-real-key",
        })
        cfg.sync.github.enabled = True

        mock_db = MagicMock()
        runners = build_source_runners(cfg, mock_db)
        assert len(runners) >= 1
        runner = runners[0]
        assert runner.condense_model == "claude-haiku-4-5-20251001"
        assert runner._client_factory is not None
        client = runner._client_factory()
        assert "api.anthropic.com" in str(client.base_url)


class TestCondenseClientBaseUrl:
    """Verify condense client (via factory) strips /v1 suffix to avoid /v1/v1/messages."""

    @pytest.mark.asyncio
    async def test_condense_client_strips_v1_from_proxy_url(self) -> None:
        """Proxy base_url includes /v1/ — factory must not double it."""
        from nerve.sources.runner import SourceRunner

        cfg = NerveConfig.from_dict({
            "proxy": {"enabled": True, "port": 8317},
        })
        factory = lambda: cfg.create_async_anthropic_client(timeout=60.0)

        runner = SourceRunner(
            source=MagicMock(),
            db=MagicMock(),
            condense=True,
            condense_model="claude-haiku-4-5-20251001",
            condense_client_factory=factory,
        )
        client = await runner._get_condense_client()
        assert client is not None
        base = str(client.base_url)
        # Must NOT contain /v1/v1
        assert "/v1/v1" not in base
        # Must end with the proxy host (no /v1 path)
        assert base.rstrip("/") == "http://127.0.0.1:8317"

    @pytest.mark.asyncio
    async def test_condense_client_strips_v1_from_direct_url(self) -> None:
        """Direct API base_url also includes /v1/ — same fix applies."""
        from nerve.sources.runner import SourceRunner

        cfg = NerveConfig.from_dict({
            "anthropic_api_key": "sk-test",
        })
        factory = lambda: cfg.create_async_anthropic_client(timeout=60.0)

        runner = SourceRunner(
            source=MagicMock(),
            db=MagicMock(),
            condense=True,
            condense_model="claude-haiku-4-5-20251001",
            condense_client_factory=factory,
        )
        client = await runner._get_condense_client()
        assert client is not None
        base = str(client.base_url)
        assert "/v1/v1" not in base
        assert base.rstrip("/") == "https://api.anthropic.com"

    @pytest.mark.asyncio
    async def test_condense_client_no_factory(self) -> None:
        """Without a client factory, _get_condense_client returns None."""
        from nerve.sources.runner import SourceRunner

        runner = SourceRunner(
            source=MagicMock(),
            db=MagicMock(),
            condense=True,
            condense_model="claude-haiku-4-5-20251001",
        )
        client = await runner._get_condense_client()
        assert client is None
