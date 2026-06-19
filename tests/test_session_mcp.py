"""Tests for per-session MCP server — session isolation for notification tools.

Verifies that create_session_mcp_server() binds the correct session_id
into notify/ask_user tool closures, preventing the race condition where
concurrent sessions could overwrite a shared global.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.tools import (
    _notify_impl,
    _ask_user_impl,
    create_session_mcp_server,
)


@pytest.mark.asyncio
class TestSessionMCPIsolation:
    """Verify that per-session MCP servers bind the correct session_id."""

    async def test_create_session_mcp_server_returns_server(self):
        """Factory returns an MCP server object."""
        server = create_session_mcp_server("test-session-1")
        assert server is not None

    async def test_different_sessions_get_different_servers(self):
        """Each call produces a distinct server instance."""
        server_a = create_session_mcp_server("session-a")
        server_b = create_session_mcp_server("session-b")
        assert server_a is not server_b

    async def test_nerve_server_marked_always_load(self):
        """The nerve MCP must opt into alwaysLoad so its core tools
        (memory_recall, task_*, plan_*, notify) skip CLI tool-search
        deferral and are available on the first turn."""
        from nerve.agent.tools import create_nerve_mcp_server
        session_server = create_session_mcp_server("session-load-test")
        legacy_server = create_nerve_mcp_server()
        assert session_server.get("alwaysLoad") is True
        assert legacy_server.get("alwaysLoad") is True
        # Must remain a valid SDK server config — type and name preserved.
        assert session_server.get("type") == "sdk"
        assert session_server.get("name") == "nerve"

    async def test_notify_impl_passes_correct_session_id(self):
        """_notify_impl forwards the given session_id to the notification service."""
        mock_service = AsyncMock()
        mock_service.send_notification = AsyncMock(return_value="notif-abc")
        # The handler reads the row back to detect silenced/force outcomes;
        # a plain None keeps it on the "sent" path for this unit test.
        mock_service.db.get_notification = AsyncMock(return_value=None)

        with patch("nerve.agent.tools._notification_service", mock_service):
            result = await _notify_impl(
                {"title": "Test", "body": "hello", "priority": "normal"},
                "session-xyz",
            )

        mock_service.send_notification.assert_called_once_with(
            session_id="session-xyz",
            title="Test",
            body="hello",
            priority="normal",
            force=False,
        )
        assert "notif-abc" in result["content"][0]["text"]

    async def test_ask_user_impl_passes_correct_session_id(self):
        """_ask_user_impl forwards the given session_id to the notification service."""
        mock_service = AsyncMock()
        mock_service.ask_question = AsyncMock(
            return_value={"notification_id": "ask-def"}
        )

        with patch("nerve.agent.tools._notification_service", mock_service):
            result = await _ask_user_impl(
                {"title": "Question?", "body": "details", "priority": "high"},
                "session-999",
            )

        mock_service.ask_question.assert_called_once_with(
            session_id="session-999",
            title="Question?",
            body="details",
            options=None,
            priority="high",
        )
        assert "ask-def" in result["content"][0]["text"]

    async def test_concurrent_sessions_use_correct_ids(self):
        """Simulate two concurrent sessions and verify each uses its own session_id."""
        captured_session_ids: list[str] = []

        async def fake_send_notification(session_id, **kwargs):
            # Add a small delay to simulate real async work and increase
            # chance of interleaving if isolation were broken
            await asyncio.sleep(0.01)
            captured_session_ids.append(session_id)
            return f"notif-{session_id}"

        mock_service = AsyncMock()
        mock_service.send_notification = AsyncMock(side_effect=fake_send_notification)
        mock_service.db.get_notification = AsyncMock(return_value=None)

        with patch("nerve.agent.tools._notification_service", mock_service):
            # Both sessions call notify concurrently
            results = await asyncio.gather(
                _notify_impl({"title": "From A"}, "session-A"),
                _notify_impl({"title": "From B"}, "session-B"),
            )

        # Each call should have used its own session_id
        assert "session-A" in captured_session_ids
        assert "session-B" in captured_session_ids
        assert len(captured_session_ids) == 2

    async def test_ask_user_parses_json_options(self):
        """Options passed as JSON array string are parsed correctly."""
        mock_service = AsyncMock()
        mock_service.ask_question = AsyncMock(
            return_value={"notification_id": "ask-opts"}
        )

        with patch("nerve.agent.tools._notification_service", mock_service):
            await _ask_user_impl(
                {
                    "title": "Pick one",
                    "options": '["Yes", "No", "Maybe"]',
                    "priority": "normal",
                },
                "session-opts",
            )

        call_kwargs = mock_service.ask_question.call_args[1]
        assert call_kwargs["options"] == ["Yes", "No", "Maybe"]

    async def test_ask_user_parses_comma_separated_options(self):
        """Options passed as comma-separated string are parsed correctly."""
        mock_service = AsyncMock()
        mock_service.ask_question = AsyncMock(
            return_value={"notification_id": "ask-csv"}
        )

        with patch("nerve.agent.tools._notification_service", mock_service):
            await _ask_user_impl(
                {
                    "title": "Pick one",
                    "options": "Yes, No, Maybe",
                    "priority": "normal",
                },
                "session-csv",
            )

        call_kwargs = mock_service.ask_question.call_args[1]
        assert call_kwargs["options"] == ["Yes", "No", "Maybe"]

    async def test_notify_without_service_returns_error(self):
        """notify gracefully fails when notification service is not initialized."""
        with patch("nerve.agent.tools._notification_service", None):
            result = await _notify_impl({"title": "Test"}, "any-session")

        assert "not available" in result["content"][0]["text"]
