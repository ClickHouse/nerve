"""The per-frame WebSocket re-check decides on the actor's kind."""

from __future__ import annotations

import pytest

from nerve.gateway.server import WebSocketConnection, _connection_still_authorised
from nerve.identity import ACTOR_KIND_HUMAN, ACTOR_KIND_SYSTEM, Actor


def _socket(actor: Actor) -> WebSocketConnection:
    return WebSocketConnection(
        client_id="client-1", actor=actor, session_epoch=actor.session_epoch,
    )


@pytest.mark.asyncio
async def test_the_system_principal_stays_valid():
    actor = Actor(actor_id="system-actor", kind=ACTOR_KIND_SYSTEM)
    assert await _connection_still_authorised(_socket(actor)) is True


@pytest.mark.asyncio
async def test_a_human_without_an_account_is_refused():
    actor = Actor(actor_id="human-actor", kind=ACTOR_KIND_HUMAN)
    assert await _connection_still_authorised(_socket(actor)) is False


@pytest.mark.asyncio
async def test_a_human_with_an_enabled_account_stays_valid(
    tmp_path, open_identity_db, wire_identity_store,
):
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    try:
        actor = Actor(
            actor_id=identity.owner_actor_id,
            kind=ACTOR_KIND_HUMAN,
            account_id=identity.owner_account_id,
            session_epoch=0,
        )
        assert await _connection_still_authorised(_socket(actor)) is True
    finally:
        await database.close()
