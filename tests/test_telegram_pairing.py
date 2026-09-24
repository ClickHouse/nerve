"""``/pair`` reports whether authorization was persisted.

Pairing authorizes a Telegram user in memory and adds them to
``telegram.allowed_users`` in ``config.local.yaml``. The writer refuses when
the file cannot be kept owner-only. The reply must then say the pairing lasts
only until restart.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve import paths
from nerve.channels.telegram import TelegramChannel
from nerve.config import NerveConfig


@pytest.fixture
def channel(tmp_path, monkeypatch):
    """A channel whose /pair path reaches the persistence call."""
    import nerve.pairing as pairing

    config = NerveConfig()
    config.config_dir = tmp_path
    config.telegram.enabled = True
    config.telegram.dm_policy = "pairing"
    config.telegram.allowed_users = []

    monkeypatch.setattr(pairing, "verify_pairing_code", lambda code: True)
    monkeypatch.setattr("nerve.config.is_locked", lambda: False)
    return TelegramChannel(lambda: config, MagicMock())


def _update(user_id: int = 4242):
    reply = AsyncMock()
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_chat=SimpleNamespace(id=user_id),
        message=SimpleNamespace(reply_text=reply),
    ), reply


@pytest.mark.asyncio
async def test_a_saved_pairing_is_reported_as_paired(channel, monkeypatch):
    saved: list[int] = []
    monkeypatch.setattr(
        "nerve.config.append_telegram_allowed_user",
        lambda config_dir, user_id: (saved.append(user_id), True)[1],
    )
    update, reply = _update()

    await channel._handle_pair(update, SimpleNamespace(args=["code"]))

    assert saved == [4242]
    assert 4242 in channel._allowed_users
    assert "✓ Paired" in reply.call_args[0][0]


@pytest.mark.asyncio
async def test_a_pairing_the_filesystem_would_not_let_us_save_says_so(
    channel, tmp_path, monkeypatch, caplog,
):
    """When the secrets file cannot be kept owner-only, the pairing holds for
    this run only, and the reply says so and how to make it permanent."""
    import logging

    monkeypatch.setattr(paths, "_mode_is_private", lambda st_mode: False)
    update, reply = _update()

    with caplog.at_level(logging.ERROR, logger="nerve.channels.telegram"):
        await channel._handle_pair(update, SimpleNamespace(args=["code"]))

    text = reply.call_args[0][0]
    assert "✓ Paired" not in text
    assert "Paired for this run" in text and "saving to config failed" in text
    assert "telegram.allowed_users" in text  # the manual remedy
    assert 4242 in channel._allowed_users  # authorized for this run, as promised
    assert any("failed to persist" in r.getMessage() for r in caplog.records)
    assert not (tmp_path / "config.local.yaml").exists()  # and nothing was written


@pytest.mark.asyncio
async def test_a_locked_instance_revokes_the_in_memory_authorization(
    channel, monkeypatch,
):
    """The other failure branch, unchanged: locked means not paired at all."""
    from nerve.config import LockdownError

    def refuse(config_dir, user_id):
        raise LockdownError("locked")

    monkeypatch.setattr("nerve.config.append_telegram_allowed_user", refuse)
    update, reply = _update()

    await channel._handle_pair(update, SimpleNamespace(args=["code"]))

    assert 4242 not in channel._allowed_users
    assert "locked" in reply.call_args[0][0]
