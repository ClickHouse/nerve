"""Anthropic model-catalog discovery for the composer's model picker.

The Claude side of ``GET /api/models`` used to be a hardcoded list
(:data:`nerve.config.DEFAULT_CLAUDE_MODELS`) plus whatever ``agent.model`` /
``agent.models`` named, so every model Anthropic released after that constant
was written stayed invisible in the UI even when the configured credentials
could use it. This module asks the Anthropic Models API (``GET /v1/models``)
which models the account can actually reach, so new releases show up without
a code change or a config edit.

Discovery is best-effort and never raises. It returns an empty list — and the
caller falls back to the built-in list — when:

* the provider is Bedrock (``AnthropicBedrock`` exposes no Models API, and
  Bedrock IDs are region-prefixed anyway),
* no API key is configured (e.g. OAuth-token installs), or
* the API is unreachable / returns something unexpected.

Results are cached in-process: primed once at gateway startup, re-fetched in
the background when older than :data:`_TTL_SECONDS`, so a long-lived gateway
picks up a newly released model without a restart.
"""

from __future__ import annotations

import asyncio
import logging
import time
from itertools import islice
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

# Bounded: discovery runs at startup and, when the cache is cold, on the
# /api/models request path — it must never hang the picker.
_DISCOVERY_TIMEOUT = 8.0

# The catalog only changes when Anthropic ships a model, so hours of
# staleness are fine. A stale entry is still served immediately; the refresh
# happens in the background.
_TTL_SECONDS = 6 * 3600.0

# After a failure, serve the built-in fallback for a while instead of
# retrying the unreachable API on every request.
_FAILURE_BACKOFF_SECONDS = 300.0

# Safety net against an unexpectedly long paginated response.
_MAX_MODELS = 200

# Cache shape: fingerprint identifies the credentials/endpoint the entry was
# fetched with, so a config reload that switches provider, key or base URL
# invalidates it instead of serving another account's catalog.
_cache: dict[str, Any] = {
    "fingerprint": None,   # tuple | None
    "models": [],          # list[str]
    "checked_at": 0.0,     # monotonic timestamp of the last attempt
    "ok": False,           # whether that attempt returned models
}

# Background refresh handle — kept module-level so the task is not garbage
# collected mid-flight.
_refresh_task: asyncio.Task[Any] | None = None


def reset_cache() -> None:
    """Drop the cached catalog (used by tests and config reloads)."""
    _cache.update({"fingerprint": None, "models": [], "checked_at": 0.0, "ok": False})


def _fingerprint(config: NerveConfig) -> tuple[str, str, str]:
    """Identity of the credentials/endpoint a cached catalog belongs to."""
    key = config.effective_api_key or ""
    return (
        "bedrock" if config.provider.is_bedrock else "anthropic",
        config.anthropic_api_base_url,
        key[-6:],  # tail only — never log or store the whole key
    )


def fetch_models(
    config: NerveConfig, timeout: float = _DISCOVERY_TIMEOUT,
) -> list[str]:
    """Return Claude model IDs the configured credentials can reach.

    Queries ``GET /v1/models`` (newest first, as the API orders it). Blocking
    — call it from a worker thread. Never raises: returns ``[]`` when
    discovery is impossible or fails, and the caller falls back to the
    built-in list.
    """
    if config.provider.is_bedrock:
        # The Bedrock client has no `.models` resource, and Bedrock IDs are
        # region-prefixed — configured models are the only sane source there.
        return []
    if not config.effective_api_key:
        logger.debug("Claude model discovery skipped: no API key configured")
        return []

    client = None
    try:
        client = config.create_anthropic_client(timeout=timeout)
        # Iterating the page object auto-paginates; islice bounds it.
        raw = [
            str(m.id)
            for m in islice(client.models.list(limit=100), _MAX_MODELS)
            if getattr(m, "id", None)
        ]
    except Exception as e:
        logger.warning("Claude model discovery failed: %s", e)
        return []
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # pragma: no cover - defensive
                pass

    # Keep Claude chat models only. With `proxy.enabled` the request is
    # served by CLIProxyAPI, whose catalog can include non-Anthropic
    # upstreams (Ollama, ...) that the picker already lists separately.
    models: list[str] = []
    seen: set[str] = set()
    for model_id in raw:
        if "claude" in model_id.lower() and model_id not in seen:
            seen.add(model_id)
            models.append(model_id)
    return models


async def _refresh(config: NerveConfig, fingerprint: tuple[str, str, str]) -> list[str]:
    """Fetch in a worker thread and update the cache. Never raises."""
    try:
        models = await asyncio.to_thread(fetch_models, config)
    except Exception as e:  # pragma: no cover - to_thread itself failing
        logger.warning("Claude model discovery failed: %s", e)
        models = []
    _cache.update({
        "fingerprint": fingerprint,
        "models": models,
        "checked_at": time.monotonic(),
        "ok": bool(models),
    })
    return models


def _schedule_refresh(config: NerveConfig, fingerprint: tuple[str, str, str]) -> None:
    """Kick off a background refresh unless one is already running."""
    global _refresh_task
    if _refresh_task is not None and not _refresh_task.done():
        return
    try:
        _refresh_task = asyncio.create_task(_refresh(config, fingerprint))
    except RuntimeError:  # pragma: no cover - no running loop
        pass


async def get_models(config: NerveConfig) -> list[str]:
    """Cached Claude catalog for the model picker (``[]`` when unavailable).

    A usable entry is served immediately, refreshing in the background once
    stale. Only a cold cache waits on the network, and a recent failure is
    remembered so a down API is not retried on every request.

    Two concurrent cold callers may both fetch — the request is an idempotent
    GET and startup priming makes it rare, so this is left unsynchronized on
    purpose.
    """
    fingerprint = _fingerprint(config)
    if _cache["fingerprint"] == fingerprint:
        age = time.monotonic() - float(_cache["checked_at"])
        if _cache["ok"]:
            if age < _TTL_SECONDS:
                return list(_cache["models"])
            _schedule_refresh(config, fingerprint)
            return list(_cache["models"])  # stale but usable
        if age < _FAILURE_BACKOFF_SECONDS:
            return []
    return await _refresh(config, fingerprint)


async def prime(config: NerveConfig) -> list[str]:
    """Warm the catalog at startup so the first picker render is complete."""
    models = await get_models(config)
    if models:
        logger.info(
            "Discovered %d Claude models from the Anthropic API (default: %s)",
            len(models), config.agent.model,
        )
    else:
        logger.info(
            "Claude model discovery returned nothing — the model picker "
            "falls back to the configured/built-in list",
        )
    return models
