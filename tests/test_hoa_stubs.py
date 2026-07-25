"""Tests for the houseofagents deprecation stubs.

houseofagents was retired in favor of workflow runs. The three hoa_* tool
names stay registered as no-op stubs (visibility still gated by
``include_hoa``) so stale clients and saved prompts get a pointer to
``workflow_run_start`` instead of an unknown-tool error.
"""

from __future__ import annotations

import pytest

from nerve.agent.tools import ToolContext, build_default_registry
from nerve.agent.tools.handlers.hoa import (
    hoa_execute_handler,
    hoa_list_pipelines_handler,
    hoa_status_handler,
)

HOA_STUB_NAMES = {"hoa_status", "hoa_list_pipelines", "hoa_execute"}


class TestHoaStubHandlers:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "handler",
        [hoa_status_handler, hoa_list_pipelines_handler, hoa_execute_handler],
        ids=["hoa_status", "hoa_list_pipelines", "hoa_execute"],
    )
    async def test_stub_returns_retirement_pointer(self, handler):
        result = await handler(ToolContext(session_id="s"), {})
        assert result.is_error is False
        text = result.content[0]["text"]
        assert "retired" in text
        assert "workflow_run" in text

    @pytest.mark.asyncio
    async def test_execute_stub_points_at_workflow_run_start(self):
        # Legacy args are accepted and ignored — the stub never executes.
        result = await hoa_execute_handler(
            ToolContext(session_id="s"),
            {"prompt": "do things", "mode": "relay", "iterations": 3},
        )
        assert result.is_error is False
        assert "workflow_run_start" in result.content[0]["text"]
        assert "docs/workflow-runs.md" in result.content[0]["text"]


class TestHoaStubRegistration:
    def test_default_registry_still_registers_stub_names(self):
        registry = build_default_registry()
        names = {s.name for s in registry.list(include_hoa=True)}
        missing = HOA_STUB_NAMES - names
        assert not missing, f"Stub tools missing from default registry: {missing}"

    def test_stubs_stay_hidden_without_opt_in(self):
        registry = build_default_registry()
        hidden = {s.name for s in registry.list(include_hoa=False)}
        assert not (HOA_STUB_NAMES & hidden)
