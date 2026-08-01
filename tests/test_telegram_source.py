"""Tests for the Telegram source's session-file location."""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve import paths
from nerve.sources.telegram import TelegramSource


class TestSessionPath:
    """Where Telethon's ``.session`` file lands.

    The session is the authenticated login; putting it somewhere unexpected
    means the interactive ``nerve sync telegram`` writes one file and the cron
    run looks for another, so the sync just keeps reporting that it is not
    authenticated.
    """

    def test_default_is_under_the_state_dir(self):
        src = TelegramSource({})
        assert src._session_path() == str(paths.nerve_path("telegram_sync"))

    def test_configured_path_is_honored(self, tmp_path):
        src = TelegramSource({"session_path": str(tmp_path / "tg")})
        assert src._session_path() == str(tmp_path / "tg")

    def test_tilde_expands(self):
        src = TelegramSource({"session_path": "~/tg-session"})
        assert src._session_path() == str(Path.home() / "tg-session")

    @pytest.mark.parametrize("blank", ["", "   ", "\t", "\n", " \t "])
    def test_blank_falls_back_to_the_default(self, blank):
        """Whitespace is truthy, so without the strip this becomes " .session".

        Relative to the cwd, so the file also moves with whatever directory the
        daemon was started in.
        """
        src = TelegramSource({"session_path": blank})
        assert src._session_path() == str(paths.nerve_path("telegram_sync"))

    def test_surrounding_whitespace_is_trimmed_not_baked_in(self, tmp_path):
        src = TelegramSource({"session_path": f"  {tmp_path / 'tg'}  "})
        assert src._session_path() == str(tmp_path / "tg")
