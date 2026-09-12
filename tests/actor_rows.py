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

from nerve.identity import Actor


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
