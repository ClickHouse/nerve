"""Base renderer abstractions for external-agent memory bundles."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from nerve.external_agents.registry import FileTarget


SECTION_DIVIDER = "\n\n---\n\n"


def read_workspace_file(workspace: Path, relative: str) -> str:
    """Read ``workspace/relative`` and return its content, or ``""``
    if the file is missing or unreadable.

    The sync service runs even in fresh workspaces where, say,
    ``TOOLS.md`` doesn't exist yet — silently skipping missing files
    is the right behaviour because the user will fill them in later.
    """
    path = workspace / relative
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


class StyleRenderer(ABC):
    """Render a :class:`FileTarget` into the bytes written to disk.

    Subclasses compose the final bundle from the target's ``includes``
    plus an optional header/footer (e.g. the mandatory-first-action
    note for external agents).
    """

    @abstractmethod
    def render(self, target: FileTarget, *, workspace: Path) -> str:
        """Return the full rendered text for ``target``."""
        raise NotImplementedError

    # ---- Helpers ---------------------------------------------------

    def _concat_includes(
        self,
        target: FileTarget,
        workspace: Path,
        *,
        with_headings: bool = True,
    ) -> str:
        """Glue together the included files, separated by ``---``.

        Files that don't exist or are empty are silently dropped so an
        incomplete workspace still produces something useful for the
        agent.
        """
        parts: list[str] = []
        for filename in target.includes:
            content = read_workspace_file(workspace, filename)
            if not content.strip():
                continue
            if with_headings:
                parts.append(f"# {filename}\n\n{content.strip()}\n")
            else:
                parts.append(content.strip() + "\n")
        return SECTION_DIVIDER.join(parts)
