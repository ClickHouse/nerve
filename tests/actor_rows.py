"""Explicit actor rows for tests that persist synthetic identities.

Tests call these helpers explicitly because some count actor rows, and ``NULL``
attribution needs no parent row.
"""

from __future__ import annotations

from datetime import datetime, timezone

from nerve.identity import ACTOR_KIND_SYSTEM, Actor

# Used only by tests whose database is a mock; never persisted.
FAKE_SYSTEM_ACTOR = Actor(
    actor_id="00000000-0000-4000-8000-00000000515e",
    kind=ACTOR_KIND_SYSTEM,
    display_name="nerve",
)


async def ensure_actor_row(db, *actors: Actor) -> None:
    """Create the ``actor_refs`` row for each actor that does not have one.

    Idempotent, so a per-test call is safe however the database was built
    (including one that has already been bootstrapped).
    """
    for actor in actors:
        if await db.get_actor_ref(actor.actor_id) is None:
            await db._write(
                """INSERT INTO actor_refs (id, kind, display_name, created_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    actor.actor_id,
                    actor.kind,
                    actor.display_name,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )


async def ensure_system_principal(db) -> str:
    """Return the migration-guaranteed system actor id."""
    return db.system_actor_id


def mock_system_principal(db) -> str:
    """Configure a ``MagicMock`` database's system actor."""
    db.system_actor = FAKE_SYSTEM_ACTOR
    return FAKE_SYSTEM_ACTOR.actor_id
