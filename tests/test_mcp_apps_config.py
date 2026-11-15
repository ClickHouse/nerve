"""The ``mcp_apps`` configuration section: one flag, off by default."""

from __future__ import annotations

from nerve.config import McpAppsConfig, NerveConfig, load_config, validate_config_keys


class TestConfig:
    def test_off_by_default(self):
        config = NerveConfig.from_dict({})
        assert config.mcp_apps.enabled is False
        assert config.mcp_apps_active is False

    def test_on_with_the_flag_alone(self):
        config = NerveConfig.from_dict({"mcp_apps": {"enabled": True}})
        assert config.mcp_apps_active is True

    def test_the_section_has_one_setting(self):
        """The flag is all of it: the sandbox page is served from the gateway's own address."""
        assert [f for f in McpAppsConfig.__dataclass_fields__] == ["enabled"]

    def test_environment_references_arrive_as_strings(self):
        assert NerveConfig.from_dict({"mcp_apps": {"enabled": "true"}}).mcp_apps_active is True
        assert NerveConfig.from_dict({"mcp_apps": {"enabled": "false"}}).mcp_apps_active is False

    def test_a_bare_section_is_the_default(self):
        assert NerveConfig.from_dict({"mcp_apps": None}).mcp_apps_active is False

    def test_the_documented_environment_form_end_to_end(self, tmp_path, monkeypatch):
        """docs/config.md: per machine, and under lockdown, the section is
        stated with a ``${VAR}`` reference."""
        (tmp_path / "config.yaml").write_text(
            f"workspace: {tmp_path / 'ws'}\n"
            "mcp_apps:\n"
            "  enabled: ${NERVE_MCP_APPS:-false}\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("NERVE_MCP_APPS", raising=False)
        assert load_config(tmp_path).mcp_apps_active is False
        monkeypatch.setenv("NERVE_MCP_APPS", "true")
        assert load_config(tmp_path).mcp_apps_active is True

    def test_the_key_is_known(self):
        assert validate_config_keys({"mcp_apps": {"enabled": True}}) == []
        (warning,) = validate_config_keys({"mcp_apps": {"no_such_setting": 1}})
        assert "unknown key 'mcp_apps.no_such_setting'" in warning

    def test_a_key_the_section_does_not_have_changes_nothing(self):
        """It is ignored, with the warning above: the flag decides."""
        on = NerveConfig.from_dict({"mcp_apps": {"enabled": True, "no_such_setting": 1}})
        assert on.mcp_apps_active is True
        assert NerveConfig.from_dict({"mcp_apps": {"no_such_setting": 1}}).mcp_apps_active is False
