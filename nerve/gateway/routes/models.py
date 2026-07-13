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

from fastapi import APIRouter, Depends

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.ollama import discover_models

logger = logging.getLogger(__name__)

router = APIRouter()


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
            "models": [config.agent.model], "available": True,
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
        {"id": default_model, "provider": "anthropic", "backend": "claude"},
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
