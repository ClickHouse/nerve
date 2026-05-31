"""External agent registry — plug-in surface for Codex, Claude Code, ...

Each agent is a small class implementing :class:`ExternalAgent`. The
registry exposes them by stable name so the bootstrap wizard and sync
service can iterate them without importing every implementation by hand.

Adding a new agent:

1. Subclass :class:`ExternalAgent` in :mod:`nerve.external_agents.agents`.
2. Register an instance in :data:`AGENT_REGISTRY` below.
3. Provide a renderer in :mod:`nerve.external_agents.renderers` if the
   default ``passthrough`` style isn't right.

Implementations stay narrow on purpose — pure functions over a
:class:`~nerve.external_agents.writer.ConfigWriter` so the wizard and
the cron path share exactly one code path for writes.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - circular import only at type-check time
    from nerve.external_agents.writer import ConfigWriter

logger = logging.getLogger(__name__)


@dataclass
class FileTarget:
    """One memory-bundle file the sync service keeps fresh.

    ``output`` is the absolute path the rendered bundle is written to.
    ``includes`` is the ordered list of workspace-relative filenames
    that get concatenated into the bundle (e.g.
    ``["SOUL.md", "USER.md", "MEMORY.md"]``). Missing files are skipped
    silently — letting agents survive a workspace where, say, TASK.md
    only exists in worker mode.

    ``style`` selects the renderer (see
    :mod:`nerve.external_agents.renderers`); ``header_template`` is an
    optional Jinja template name used by the renderer for the prefix.
    """

    output: Path
    includes: list[str]
    style: str
    header_template: str | None = None
    footer_template: str | None = None


@dataclass
class AgentSetupResult:
    """Return value of :meth:`ExternalAgent.write_config`.

    Surfaced by the bootstrap wizard's review screen so the user sees
    exactly what was written and where the ``.nerve-backup-<ts>``
    copies of clobbered originals are.
    """

    agent: str
    config_files_written: list[Path]
    backups_created: list[Path]
    token: str
    warnings: list[str] = field(default_factory=list)


class ExternalAgent(ABC):
    """Base class for an external chat agent Nerve can configure.

    Concrete subclasses know:

    - what filesystem paths the agent owns (used for conflict detection)
    - what default memory files should be synced into the agent
    - how to write the agent's config file (TOML, JSON, ...)
    - whether the host has the agent installed (smoke check)
    """

    #: Stable registry key. Used in YAML config and CLI flags.
    name: str = ""

    #: Human-friendly label rendered in the wizard.
    display_name: str = ""

    #: ``which`` lookup target. ``None`` = no smoke check available.
    cli_command: str | None = None

    @abstractmethod
    def default_config_paths(self) -> list[Path]:
        """All filesystem paths this agent owns.

        Used by the wizard to detect pre-existing files so the user can
        choose a conflict policy (backup / skip / merge).
        """
        raise NotImplementedError

    @abstractmethod
    def default_file_targets(self, workspace: Path) -> list[FileTarget]:
        """Memory bundle targets the sync service should keep fresh."""
        raise NotImplementedError

    @abstractmethod
    async def write_config(
        self,
        *,
        nerve_url: str,
        mcp_token: str,
        workspace: Path,
        writer: ConfigWriter,
    ) -> AgentSetupResult:
        """Apply the one-shot bootstrap: config file + initial memory bundle.

        The implementation MUST go through ``writer`` for every file it
        creates so backup / allowlist / atomic-write semantics are
        applied uniformly.
        """
        raise NotImplementedError

    def smoke_check(self) -> str | None:
        """Return the agent's version string if its CLI is on PATH.

        Returns ``None`` when the CLI is missing or the version probe
        fails — never raises, since this is best-effort UI sugar for
        the wizard's "(installed)" hint.
        """
        if not self.cli_command:
            return None
        if not shutil.which(self.cli_command):
            return None
        try:
            r = subprocess.run(
                [self.cli_command, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0:
                return r.stdout.strip() or r.stderr.strip() or "installed"
            return None
        except (subprocess.TimeoutExpired, OSError) as e:
            logger.debug("Smoke check for %s failed: %s", self.cli_command, e)
            return None


# Lazy module-level cache to avoid circular imports at import time.
# Populated by ``_build_registry()`` the first time ``AGENT_REGISTRY``
# is accessed by client code.
_REGISTRY_CACHE: dict[str, ExternalAgent] | None = None


def _build_registry() -> dict[str, ExternalAgent]:
    """Construct the agent registry. Lives in a function so we can
    keep ``agents/codex.py`` and ``agents/claude_code.py`` importing
    from this module without circular fireworks."""
    # Imports here, not at module top, to dodge circular imports.
    from nerve.external_agents.agents.claude_code import ClaudeCodeAgent
    from nerve.external_agents.agents.codex import CodexAgent

    agents: list[ExternalAgent] = [CodexAgent(), ClaudeCodeAgent()]
    return {a.name: a for a in agents}


class _RegistryProxy:
    """Lazy dict-like proxy so ``from .registry import AGENT_REGISTRY``
    doesn't trigger agent module imports at package import time.

    Keeps the surface area identical to a plain dict — ``in``,
    ``.get()``, ``.values()``, ``.items()``, ``for k in registry`` all
    work transparently. The first access materialises the cache.
    """

    def _ensure(self) -> dict[str, ExternalAgent]:
        global _REGISTRY_CACHE
        if _REGISTRY_CACHE is None:
            _REGISTRY_CACHE = _build_registry()
        return _REGISTRY_CACHE

    def __getitem__(self, key: str) -> ExternalAgent:
        return self._ensure()[key]

    def __contains__(self, key: object) -> bool:
        return key in self._ensure()

    def __iter__(self):
        return iter(self._ensure())

    def __len__(self) -> int:
        return len(self._ensure())

    def get(self, key: str, default: ExternalAgent | None = None) -> ExternalAgent | None:
        return self._ensure().get(key, default)

    def keys(self):
        return self._ensure().keys()

    def values(self):
        return self._ensure().values()

    def items(self):
        return self._ensure().items()


AGENT_REGISTRY: _RegistryProxy = _RegistryProxy()
