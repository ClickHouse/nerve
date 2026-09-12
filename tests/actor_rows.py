"""Make a test's synthetic actor a real ``actor_refs`` row.

``sessions.created_by_actor_id`` and ``messages.actor_id`` reference
``actor_refs(id)`` (v048), so an id that resolves to nothing can never be
stored. That is the point of the constraint — attribution nobody can look up
renders as a blank name — but it means the synthetic actors the suite uses to
stand in for a logged-in person (``conftest.TEST_ACTOR``, the per-file
``_ACTOR`` constants) need their row to exist wherever a test actually
*persists* one: a route called through ``bypass_auth``, or called directly
with the ``request_actor`` fixture, that goes on to create a session or a
message.

Tests that only read, or that pass ``actor=None`` (a deliberately unattributed
row, which is what all pre-v048 history looks like), need nothing from here:
NULL is exempt from foreign-key checks.

This lives outside ``conftest.py`` on purpose — it is a helper a handful of
files call explicitly, not something that should quietly happen to every test
that opens a database. Several tests count ``actor_refs`` rows.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from nerve.identity import Actor

# A synthetic system principal for tests whose database is a mock. Obviously
# not a real id, and never written anywhere — a mock has no schema to violate.
FAKE_SYSTEM_PRINCIPAL = {
    "id": "00000000-0000-4000-8000-00000000515e",
    "kind": "system",
    "display_name": "nerve",
}


async def ensure_actor_row(db, *actors: Actor) -> None:
    """Create the ``actor_refs`` row for each actor that does not have one.

    Idempotent, so a per-test call is safe however the database was built
    (including one that has already been bootstrapped).
    """
    for actor in actors:
        if await db.get_actor_ref(actor.actor_id) is None:
            await db.create_actor_ref(
                kind=actor.kind,
                display_name=actor.display_name,
                actor_id=actor.actor_id,
            )


async def ensure_system_principal(db) -> str:
    """Give a test database the local identity every real database has.

    The ``db`` fixture opens a schema-current database and stops there, which
    is a state production cannot reach: every opener bootstraps the identity
    before anything serves (PR 1). Autonomous code — cron, workflow legs, MCP
    satellites, the Codex sync — resolves the system principal *before* it
    writes and fails the run if it cannot, so a test driving those paths needs
    the rows the instance it stands in for would have.

    Idempotent. Returns the system principal's actor id.
    """
    identity = await db.get_local_identity()
    if identity is None:
        identity = await db.bootstrap_local_identity(credential_source="none")
    return identity.system_actor_id


def mock_system_principal(db) -> str:
    """The same, for a test whose database is a ``MagicMock``.

    Without this the mock answers ``get_system_principal()`` with another mock,
    and building an :class:`Actor` out of it fails in a way that says nothing
    about the test. Returns the actor id it will resolve to, so a caller can
    assert against it.
    """
    db.get_system_principal = AsyncMock(return_value=dict(FAKE_SYSTEM_PRINCIPAL))
    return FAKE_SYSTEM_PRINCIPAL["id"]
