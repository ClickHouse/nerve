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
    default_model = config.agent.model

    # Agent backends for the new-chat selector. Both are always offered —
    # construction is unconditional server-side; a codex pick without
    # auth configured surfaces a clear, actionable error on first message.
    backends = {
        "default": config.agent.backend,
        "options": [
            {"id": "claude", "label": "Claude", "model": config.agent.model},
            {"id": "codex", "label": "Codex", "model": config.codex.model},
        ],
    }

    models: list[dict[str, str]] = [
        {"id": default_model, "provider": "anthropic"},
    ]

    ollama_available = False
    if config.ollama_routable:
        # Discovery does blocking I/O (stdlib urllib) — keep the event loop free.
        names = await asyncio.to_thread(discover_models, config.ollama.base_url)
        ollama_available = bool(names)
        models.extend({"id": name, "provider": "ollama"} for name in names)

    return {
        "default": default_model,
        "backends": backends,
        "models": models,
        "ollama": {
            "enabled": config.ollama.enabled,
            "routable": config.ollama_routable,
            "available": ollama_available,
        },
    }
