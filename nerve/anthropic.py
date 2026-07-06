"""Anthropic API integration helpers.

This module only handles model *discovery* — querying which chat models
the configured Anthropic API key can use (``GET /v1/models``) so the web
composer's model picker can offer them without a hand-maintained list.

Discovery is best-effort and never raises: without an API key (OAuth or
Bedrock setups) or with the API unreachable, callers get an empty list
and the picker falls back to the configured models.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

_MODELS_URL = "https://api.anthropic.com/v1/models?limit=100"
_API_VERSION = "2023-06-01"

# Short, bounded timeout — discovery runs on the request path (model
# picker); a slow or unreachable API must never hang the UI.
_DISCOVERY_TIMEOUT = 3.0


def discover_models(api_key: str, timeout: float = _DISCOVERY_TIMEOUT) -> list[str]:
    """Return chat model ids available to an Anthropic API key.

    Queries ``GET /v1/models`` and preserves the API's newest-first order.
    Returns an empty list (never raises) when the key is missing, the API
    is unreachable, or the response is malformed.

    Uses the stdlib ``urllib`` so it is safe to call synchronously from a
    worker thread without pulling in an async HTTP client.
    """
    if not api_key:
        return []
    try:
        req = urllib.request.Request(_MODELS_URL, headers={
            "x-api-key": api_key,
            "anthropic-version": _API_VERSION,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as e:
        logger.warning("Anthropic model discovery failed: %s", e)
        return []

    if not isinstance(payload, dict):
        return []

    ids: list[str] = []
    for entry in payload.get("data") or []:
        if isinstance(entry, dict):
            model_id = entry.get("id")
            if model_id and str(model_id) not in ids:
                ids.append(str(model_id))
    return ids


def latest_per_family(model_ids: list[str]) -> list[str]:
    """Keep the first (newest, in API order) model of each family.

    The family is the first alphabetic token after the ``claude-`` prefix
    (``opus``, ``sonnet``, ``haiku``, ``fable``, ...), which also covers
    legacy ``claude-3-5-sonnet-...`` ids where the version precedes it.
    Ids with no recognizable family are kept as their own family.
    """
    seen: set[str] = set()
    kept: list[str] = []
    for model_id in model_ids:
        family = _family(model_id)
        if family in seen:
            continue
        seen.add(family)
        kept.append(model_id)
    return kept


def _family(model_id: str) -> str:
    for token in model_id.removeprefix("claude-").split("-"):
        if token.isalpha():
            return token
    return model_id
