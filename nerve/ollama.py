"""Local Ollama integration helpers.

Ollama exposes an OpenAI-compatible API at ``/v1`` and a native API at
``/api/*``. The Claude Agent SDK only speaks the Anthropic Messages API,
so Ollama models are reached through the bundled CLIProxyAPI (registered
as an ``openai-compatibility`` upstream). This module only handles model
*discovery* — querying which models are installed on the local server so
they can be offered in the model picker.

Discovery is best-effort and never raises: if the Ollama server is down or
unreachable, callers get an empty list and Ollama simply contributes no
models to the picker.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# Short, bounded timeout — discovery runs on the request path (model
# picker) and on proxy-config writes; we never want it to hang the UI or
# block startup if Ollama is installed-but-not-running.
_DISCOVERY_TIMEOUT = 3.0


def discover_models(base_url: str, timeout: float = _DISCOVERY_TIMEOUT) -> list[str]:
    """Return model names installed on a local Ollama server.

    Queries Ollama's native ``GET /api/tags`` endpoint. Returns a sorted,
    de-duplicated list of model names, or an empty list (never raises) when
    the server is unreachable or the response is malformed.

    Uses the stdlib ``urllib`` so it is safe to call synchronously from a
    worker thread (e.g. the proxy-config writer) without pulling in an
    async HTTP client.

    Args:
        base_url: Native Ollama base URL, e.g. ``http://127.0.0.1:11434``.
        timeout: Per-request timeout in seconds.
    """
    url = base_url.rstrip("/") + "/api/tags"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as e:
        logger.warning("Ollama model discovery failed at %s: %s", url, e)
        return []

    if not isinstance(payload, dict):
        return []

    names: set[str] = set()
    for entry in payload.get("models") or []:
        if isinstance(entry, dict):
            name = entry.get("name")
            if name:
                names.add(str(name))
    return sorted(names)
