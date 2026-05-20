"""Verbatim renderer — concatenates the included files unchanged.

Useful for targets that just need a no-frills bundle (or when no
agent-specific renderer is registered for a custom style).
"""

from __future__ import annotations

from pathlib import Path

from nerve.external_agents.registry import FileTarget
from nerve.external_agents.renderers.base import StyleRenderer


class PassthroughRenderer(StyleRenderer):
    """Concatenate the included files with ``---`` dividers."""

    def render(self, target: FileTarget, *, workspace: Path) -> str:
        body = self._concat_includes(target, workspace, with_headings=True)
        return body.rstrip() + "\n" if body else ""
