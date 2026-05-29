"""Renderers for external agent memory bundles.

A renderer is a small object that takes a :class:`~nerve.external_agents.registry.FileTarget`
and returns the final string written to disk. Renderers are
intentionally narrow: bundle composition lives here, agent-specific
write semantics (TOML, JSON merge, ...) live in the agent module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nerve.external_agents.renderers.base import StyleRenderer
from nerve.external_agents.renderers.claude_code import ClaudeCodeRenderer
from nerve.external_agents.renderers.codex_global import CodexGlobalRenderer
from nerve.external_agents.renderers.passthrough import PassthroughRenderer

if TYPE_CHECKING:  # pragma: no cover
    pass


STYLE_REGISTRY: dict[str, StyleRenderer] = {
    "codex-global": CodexGlobalRenderer(),
    "claude-code": ClaudeCodeRenderer(),
    "passthrough": PassthroughRenderer(),
}


def get_renderer(style: str) -> StyleRenderer:
    """Look up a renderer by style key. Falls back to passthrough so
    unknown styles still produce a sensible bundle instead of crashing
    the sync loop."""
    return STYLE_REGISTRY.get(style, STYLE_REGISTRY["passthrough"])


__all__ = [
    "ClaudeCodeRenderer",
    "CodexGlobalRenderer",
    "PassthroughRenderer",
    "STYLE_REGISTRY",
    "StyleRenderer",
    "get_renderer",
]
