"""Regression tests for Codex HTTP MCP header config (issue #605)."""

from __future__ import annotations

import tomllib

from nerve.agent.backends.codex import CodexBackend
from nerve.config import McpServerConfig


def test_http_mcp_headers_use_one_inline_table_and_no_plaintext_secrets():
    headers = {
        "Authorization": "Bearer test-secret",
        "X-Api.Key": "second-test-secret",
    }
    server = McpServerConfig(
        name="remote-api", type="http", url="https://mcp.example.invalid/v1",
        headers=headers,
    )

    overrides = CodexBackend._translate_mcp_server(server)
    assert len(overrides) == 2
    assert overrides[0] == (
        'mcp_servers.remote-api.url="https://mcp.example.invalid/v1"'
    )
    assert overrides[1].startswith("mcp_servers.remote-api.env_http_headers={")

    parsed = tomllib.loads(overrides[1])
    mapping = parsed["mcp_servers"]["remote-api"]["env_http_headers"]
    assert mapping == {
        header: CodexBackend._mcp_secret_env_name(
            "remote-api", header, "HTTP",
        )
        for header in headers
    }
    assert all(
        name.startswith("NERVE_CODEX_MCP_EXTERNAL_HTTP_")
        for name in mapping.values()
    )
    assert all(secret not in "\n".join(overrides) for secret in headers.values())


def test_http_mcp_without_headers_emits_only_url():
    server = McpServerConfig(
        name="public-api", type="http", url="https://mcp.example.invalid/v1",
    )
    assert CodexBackend._translate_mcp_server(server) == [
        'mcp_servers.public-api.url="https://mcp.example.invalid/v1"',
    ]
