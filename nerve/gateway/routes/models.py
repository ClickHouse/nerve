"""Model discovery routes — which chat models the UI can offer.

Exposes the configured Anthropic chat model plus any locally-installed
Ollama models (auto-discovered from the running Ollama server). The web
composer's model picker calls GET /api/models to populate its options.

Ollama models are only listed when they are actually routable
(``config.ollama_routable`` — Ollama enabled *and* the proxy running),
so the picker never offers a model that would fail on send.
"""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends

from nerve.anthropic import discover_models as discover_anthropic_models
from nerve.anthropic import latest_per_family
from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.ollama import discover_models

logger = logging.getLogger(__name__)

router = APIRouter()

# Anthropic discovery cache — unlike the local Ollama/Codex probes this call
# leaves the box, and /api/models fires on every composer mount. Successful
# lists are reused for 15 minutes; failures retry after 60 seconds so a
# keyless or offline box doesn't pay the discovery timeout on every mount.
_ANTHROPIC_TTL_OK = 15 * 60.0
_ANTHROPIC_TTL_EMPTY = 60.0
_anthropic_cache: tuple[float, list[str]] = (0.0, [])


async def _discovered_anthropic_ids(api_key: str) -> list[str]:
    global _anthropic_cache
    cached_at, ids = _anthropic_cache
    ttl = _ANTHROPIC_TTL_OK if ids else _ANTHROPIC_TTL_EMPTY
    if cached_at and time.monotonic() - cached_at < ttl:
        return ids
    # Discovery does blocking I/O (stdlib urllib) — keep the event loop free.
    found = latest_per_family(
        await asyncio.to_thread(discover_anthropic_models, api_key),
    )
    _anthropic_cache = (time.monotonic(), found)
    return found


@router.get("/api/models")
async def list_models(user: dict = Depends(require_auth)):
    """List selectable chat models for the composer's model picker.

    Returns:
        {
          "default": "<anthropic model id>",
          "models": [{"id", "provider"}...],
          "ollama": {"enabled", "routable", "available"}
        }

    ``provider`` is ``"anthropic"`` or ``"ollama"``; the frontend formats
    display labels. Discovery is best-effort — if the Ollama server is
    unreachable the list simply contains no Ollama entries.
    """
    config = get_config()
    deps = get_deps()
    default_model = config.agent.model

    # Default model first, then the newest live-discovered model of each
    # Anthropic family (opus/sonnet/haiku/...), de-duplicated. Discovery is
    # best-effort — a keyless (OAuth/Bedrock) or offline box simply offers
    # only the default model.
    discovered = await _discovered_anthropic_ids(config.anthropic_api_key)
    anthropic_ids: list[str] = [default_model]
    for m in discovered:
        if m and m not in anthropic_ids:
            anthropic_ids.append(m)

    codex_backend = deps.engine._backends.get("codex")
    codex_preflight = (
        await codex_backend.preflight() if codex_backend is not None
        else {"available": False, "reason": "Codex backend is not registered"}
    )

    # Advertise unavailable backends as disabled options with diagnostics, so
    # configuration problems are visible before the user's first turn.
    options = [
        {
            "id": "claude", "label": "Claude", "model": config.agent.model,
            "models": anthropic_ids, "available": True,
        },
    ]
    options.append({
        "id": "codex",
        "label": "Codex",
        "model": config.codex.model,
        "models": codex_preflight.get("models") or [config.codex.model],
        "available": bool(codex_preflight.get("available")),
        "reason": codex_preflight.get("reason"),
    })
    backends = {
        "default": (
            config.agent.backend
            if any(
                item["id"] == config.agent.backend and item.get("available")
                for item in options
            )
            else "claude"
        ),
        "options": options,
        "diagnostics": {"codex": codex_preflight},
    }

    models: list[dict[str, str]] = [
        {"id": m, "provider": "anthropic", "backend": "claude"} for m in anthropic_ids
    ]
    if codex_preflight.get("available"):
        models.extend({
            "id": model_id, "provider": "openai", "backend": "codex",
        } for model_id in codex_preflight.get("models") or [config.codex.model])

    ollama_available = False
    if config.ollama_routable:
        # Discovery does blocking I/O (stdlib urllib) — keep the event loop free.
        names = await asyncio.to_thread(discover_models, config.ollama.base_url)
        ollama_available = bool(names)
        models.extend({
            "id": name, "provider": "ollama", "backend": "claude",
        } for name in names)

    return {
        "default": default_model,
        "defaults": {
            "claude": default_model,
            "codex": config.codex.model,
        },
        "backends": backends,
        "models": models,
        "ollama": {
            "enabled": config.ollama.enabled,
            "routable": config.ollama_routable,
            "available": ollama_available,
        },
    }
