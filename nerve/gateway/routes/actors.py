"""Bulk lookup for the actor ids stored on sessions and messages.

The response is built field by field from ``actor_refs`` so account and
credential state cannot leak into attribution display.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter()


def actor_out(row: dict) -> dict:
    """The public shape of an actor: an id, what kind it is, and a name."""
    return {
        "id": row["id"],
        "kind": row["kind"],
        "display_name": row["display_name"],
    }


@router.get("/api/actors")
async def list_actors(actor: Actor = Depends(require_auth)):
    """Every actor this instance knows, for resolving a list of rows at once.

    Returns one row per person plus the system principal, allowing a client to
    resolve all names in a session list with one request.
    """
    deps = get_deps()
    rows = await deps.db.list_actor_refs()
    return {"actors": [actor_out(row) for row in rows]}
