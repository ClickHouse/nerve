"""Scaffold a workspace into a shareable git config repo.

``nerve config init-repo`` drops the files a config repo needs — the CI
validation workflow, a secrets-aware ``.gitignore``, a README, and a portable
settings layer — into the workspace, after which the CLI prints the remaining
manual git/``gh`` + instance steps. Scaffolding is **idempotent**: an existing
file is never overwritten (it's reported as skipped), so re-running is safe and
won't clobber local edits.

The workspace root *is* the config repo root (it holds ``config/`` and
``skills/``), so these files land at the top level of the workspace.
"""

from __future__ import annotations

import importlib.resources
from dataclasses import dataclass, field
from pathlib import Path

# Destination path (relative to the workspace root) -> source path under
# nerve/templates/. Order is display order.
_SCAFFOLD: dict[str, str] = {
    ".github/workflows/validate-config.yml": "config-repo/validate-config.yml",
    ".gitignore": "config-repo/gitignore",
    "README.md": "config-repo/README.md",
    # The same commented scaffold `nerve init` writes. A repo with no config/ at
    # all fails the workflow above on its very first commit: that job validates
    # the portable layer only, and finding no file to open is an error there, not
    # a pass. An instance's workspace already has this file, and it is skipped.
    "config/settings.yaml": "config/settings.yaml",
}


@dataclass
class ScaffoldResult:
    """What ``scaffold_config_repo`` did (or would do, under ``dry_run``)."""

    created: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    is_git_repo: bool = False


def _template_dir() -> Path:
    """Resolve nerve/templates/ in both source and installed layouts."""
    try:
        ref = importlib.resources.files("nerve") / "templates"
        p = Path(str(ref))
        if p.is_dir():
            return p
    except (TypeError, FileNotFoundError):
        pass
    p = Path(__file__).parent / "templates"
    if p.is_dir():
        return p
    raise FileNotFoundError("nerve templates not found")


def scaffold_config_repo(workspace: Path, dry_run: bool = False) -> ScaffoldResult:
    """Write the config-repo scaffold files into ``workspace``.

    Never overwrites an existing file (reported in ``skipped``). With
    ``dry_run=True`` nothing is written but the created/skipped split is still
    computed. Returns a :class:`ScaffoldResult`.
    """
    workspace = Path(workspace)
    tmpl = _template_dir()
    result = ScaffoldResult(is_git_repo=(workspace / ".git").exists())

    for rel, src_name in _SCAFFOLD.items():
        dst = workspace / rel
        if dst.exists():
            result.skipped.append(rel)
            continue
        result.created.append(rel)
        body = (tmpl / src_name).read_text(encoding="utf-8")
        if dry_run:
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(body, encoding="utf-8")

    return result
