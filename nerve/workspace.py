"""Workspace initialization — copies mode-appropriate templates into a fresh workspace."""

from __future__ import annotations

import importlib.resources
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

VALID_MODES = ("personal", "worker")


def _get_template_dir(mode: str) -> Path:
    """Resolve the templates directory for the given mode.

    Works both in development (source tree) and when installed as a package.
    """
    # Try importlib.resources first (works for installed packages)
    try:
        ref = importlib.resources.files("nerve") / "templates" / mode
        # Materialize to a real path
        template_path = Path(str(ref))
        if template_path.is_dir():
            return template_path
    except (TypeError, FileNotFoundError):
        pass

    # Fallback: resolve relative to this file (development mode)
    template_path = Path(__file__).parent / "templates" / mode
    if template_path.is_dir():
        return template_path

    raise FileNotFoundError(f"Template directory not found for mode '{mode}'")


def read_manifest(mode: str) -> list[tuple[str, str]]:
    """Read the MANIFEST file for a mode, returning (filename, description) pairs."""
    template_dir = _get_template_dir(mode)
    manifest_path = template_dir / "MANIFEST"

    if not manifest_path.exists():
        raise FileNotFoundError(f"MANIFEST not found: {manifest_path}")

    entries = []
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t", 1)
        filename = parts[0].strip()
        description = parts[1].strip() if len(parts) > 1 else ""
        entries.append((filename, description))

    return entries


def get_expected_files(mode: str) -> list[str]:
    """Return list of expected workspace files for the given mode."""
    return [filename for filename, _ in read_manifest(mode)]


def _get_bundled_skills_dir() -> Path | None:
    """Resolve the bundled skills directory (nerve/templates/skills/).

    Returns None if no bundled skills directory exists.
    """
    # Try importlib.resources first (works for installed packages)
    try:
        ref = importlib.resources.files("nerve") / "templates" / "skills"
        skills_path = Path(str(ref))
        if skills_path.is_dir():
            return skills_path
    except (TypeError, FileNotFoundError):
        pass

    # Fallback: resolve relative to this file (development mode)
    skills_path = Path(__file__).parent / "templates" / "skills"
    if skills_path.is_dir():
        return skills_path

    return None


def install_bundled_skills(workspace_path: Path) -> list[str]:
    """Copy bundled skills into the workspace skills directory.

    Only copies skills that don't already exist — never overwrites.
    Each subdirectory of templates/skills/ that contains a SKILL.md
    is treated as a skill to install.

    Args:
        workspace_path: Target workspace directory.

    Returns:
        List of skill IDs that were installed.
    """
    bundled_dir = _get_bundled_skills_dir()
    if bundled_dir is None:
        logger.debug("No bundled skills directory found — skipping")
        return []

    skills_dir = workspace_path / "skills"
    skills_dir.mkdir(parents=True, exist_ok=True)

    installed = []
    for skill_src in sorted(bundled_dir.iterdir()):
        if not skill_src.is_dir():
            continue
        if not (skill_src / "SKILL.md").exists():
            continue

        skill_id = skill_src.name
        skill_dst = skills_dir / skill_id

        if skill_dst.exists():
            logger.debug("Skipping skill %s — already exists", skill_id)
            continue

        shutil.copytree(skill_src, skill_dst)
        logger.info("Installed bundled skill: %s", skill_id)
        installed.append(skill_id)

    return installed


def _get_config_scaffold_dir() -> Path | None:
    """Resolve the shared config scaffold directory (nerve/templates/config/).

    Returns None if it doesn't exist.
    """
    try:
        ref = importlib.resources.files("nerve") / "templates" / "config"
        cfg_path = Path(str(ref))
        if cfg_path.is_dir():
            return cfg_path
    except (TypeError, FileNotFoundError):
        pass

    cfg_path = Path(__file__).parent / "templates" / "config"
    if cfg_path.is_dir():
        return cfg_path

    return None


def install_config_scaffold(workspace_path: Path) -> list[str]:
    """Create the git-syncable ``workspace/config/`` subtree.

    Copies the shared scaffold (currently ``settings.yaml``) into
    ``workspace/config/`` without overwriting anything that already exists.
    Later stories add ``cron/`` and ``sources.yaml`` under the same subtree.

    Returns the list of files created (relative to the workspace).
    """
    scaffold_dir = _get_config_scaffold_dir()
    config_dir = workspace_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    if scaffold_dir is None:
        logger.debug("No config scaffold directory found — created empty config/")
        return []

    created: list[str] = []
    for src in sorted(scaffold_dir.iterdir()):
        if not src.is_file():
            continue
        dst = config_dir / src.name
        if dst.exists():
            logger.debug("Skipping config/%s — already exists", src.name)
            continue
        shutil.copy2(src, dst)
        logger.info("Created config/%s", src.name)
        created.append(f"config/{src.name}")

    return created


def initialize_workspace(workspace_path: Path, mode: str) -> list[str]:
    """Copy mode-appropriate template files into a workspace directory.

    Only copies files that don't already exist — never overwrites.

    Args:
        workspace_path: Target workspace directory.
        mode: Deployment mode ("personal" or "worker").

    Returns:
        List of filenames that were created.
    """
    if mode not in VALID_MODES:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of: {', '.join(VALID_MODES)}")

    template_dir = _get_template_dir(mode)
    manifest = read_manifest(mode)

    # Ensure workspace directory exists
    workspace_path.mkdir(parents=True, exist_ok=True)

    created = []
    for filename, description in manifest:
        src = template_dir / filename
        dst = workspace_path / filename

        if dst.exists():
            logger.debug("Skipping %s — already exists", dst)
            continue

        if not src.exists():
            logger.warning("Template file missing: %s", src)
            continue

        shutil.copy2(src, dst)
        logger.info("Created %s (%s)", filename, description)
        created.append(filename)

    return created
