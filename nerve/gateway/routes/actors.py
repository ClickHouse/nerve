"""Actor lookup — turn a stored actor id into a name to show.

``sessions.created_by_actor_id`` and ``messages.actor_id`` hold an identity,
not a name: a rename must change what is displayed without rewriting a single
stored row (RFC 3.3), so the name has to be looked up at render time. This is
where the UI looks it up.

Two deliberate properties:

* **Identity only.** The response is built field by field from ``actor_refs``,
  so nothing from ``accounts`` — a username, whether a password is set, the
  enabled flag — can start travelling here because someone added a column or a
  join later. Account state belongs to ``/api/accounts``, which is a different
  question with different answers (an actor may have no account at all: the
  agent's system principal is one).
* **No email.** ``actor_refs.email`` is always NULL in local mode and carries a
  display email in external mode. Nothing in the UI needs it to label a
  message, so it is not published; adding it later is a deliberate act rather
  than an accident of ``SELECT *``.

Authenticated like every other read: knowing who else uses this instance is
not public.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter()


def actor_out(row: dict) -> dict:
    """The public shape of an actor: an id, what kind it is, and a name.

    ``profile_version`` rides along so a client can tell a stale cached name
    from a current one without comparing strings.
    """
    return {
        "id": row["id"],
        "kind": row["kind"],
        "display_name": row["display_name"],
        "profile_version": row["profile_version"],
    }


@router.get("/api/actors")
async def list_actors(actor: Actor = Depends(require_auth)):
    """Every actor this instance knows, for resolving a list of rows at once.

    Small by construction — one row per person plus the system principal — so
    a client can fetch it once and label a whole session list from it.
    """
    deps = get_deps()
    rows = await deps.db.list_actor_refs()
    return {"actors": [actor_out(row) for row in rows]}


@router.get("/api/actors/{actor_id}")
async def get_actor(actor_id: str, actor: Actor = Depends(require_auth)):
    """One actor, for a single stored id.

    404 means no such actor, which is not the same as a row with no actor:
    unattributed history carries NULL and is never looked up at all.
    """
    deps = get_deps()
    row = await deps.db.get_actor_ref(actor_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown actor")
    return actor_out(row)
