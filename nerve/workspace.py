"""Workspace initialization — copies mode-appropriate templates into a fresh workspace."""

from __future__ import annotations

import importlib.resources
import logging
import shutil
from collections.abc import Collection
from pathlib import Path

import nerve

logger = logging.getLogger(__name__)

VALID_MODES = ("personal", "worker")

# The worker guide the onboarding conversation of a worker is given in full. The
# checkout's ``docs/`` copy is the source of truth; the copy under
# ``templates/guides/`` ships in the wheel for installs without the repository
# and must stay byte-identical (``tests/test_worker_onboarding.py`` says so).
WORKER_GUIDE = "worker-best-practices.md"


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


def template_file(mode: str, filename: str) -> Path:
    """The packaged template ``filename`` for ``mode``.

    For writers that render a template rather than copy it (a worker's
    ``TASK.md`` gets the task description substituted in). Raises
    :class:`FileNotFoundError` when the file is not shipped.
    """
    path = _get_template_dir(mode) / filename
    if not path.is_file():
        raise FileNotFoundError(f"Template file not found: {path}")
    return path


def worker_guide_paths() -> tuple[Path, Path]:
    """Where :data:`WORKER_GUIDE` is read from, in order of preference.

    The source root's ``docs/`` first (a checkout, the editable install the
    Docker image runs), then the packaged copy for a wheel install, where no
    ``docs/`` directory exists next to the package.
    """
    package = Path(nerve.__file__).resolve().parent
    return (
        package.parent / "docs" / WORKER_GUIDE,
        package / "templates" / "guides" / WORKER_GUIDE,
    )


def read_worker_guide() -> str:
    """The full text of the worker guide, or ``""`` when neither copy is readable."""
    for path in worker_guide_paths():
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            continue
    logger.warning("Worker guide not found at any of %s", [str(p) for p in worker_guide_paths()])
    return ""


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


def _write_template(src: Path, dst: Path, relative: str, memory_backend: str) -> None:
    """Copy one template file to ``dst``. Under the Loom backend a template
    that names Nerve's memory tools gets Loom's wording instead
    (:func:`nerve.memory.loom_prompts.template_text`)."""
    if memory_backend == "loom":
        from nerve.memory.loom_prompts import template_text

        text = src.read_text(encoding="utf-8")
        loom_text = template_text(relative, text)
        if loom_text != text:
            dst.write_text(loom_text, encoding="utf-8")
            return
    shutil.copy2(src, dst)


def install_bundled_skills(workspace_path: Path, *, memory_backend: str = "memu") -> list[str]:
    """Copy bundled skills into the workspace skills directory.

    Only copies skills that don't already exist — never overwrites.
    Each subdirectory of templates/skills/ that contains a SKILL.md
    is treated as a skill to install.

    Args:
        workspace_path: Target workspace directory.
        memory_backend: ``memory.backend``; under ``loom`` a skill that
            names Nerve's memory tools names Loom's instead.

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
        if memory_backend == "loom":
            _write_template(
                skill_src / "SKILL.md", skill_dst / "SKILL.md", f"skills/{skill_id}/SKILL.md",
                memory_backend,
            )
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


def initialize_workspace(
    workspace_path: Path, mode: str, *, skip: Collection[str] = (), memory_backend: str = "memu",
) -> list[str]:
    """Copy mode-appropriate template files into a workspace directory.

    Only copies files that don't already exist — never overwrites.

    Args:
        workspace_path: Target workspace directory.
        mode: Deployment mode ("personal" or "worker").
        skip: MANIFEST entries not to copy. The installers pass ``TASK.md``:
            they write it themselves from the template with the description
            in place (:func:`nerve.setup_writer.apply_task_md`), and a bare
            copy would put the literal ``{{TASK_DESCRIPTION}}`` token into
            every prompt of a worker that was given no description.
        memory_backend: ``memory.backend``; under ``loom`` the templates that
            name Nerve's memory tools (``AGENTS.md``, ``TOOLS.md``,
            ``MEMORY.md``) are written with Loom's instead.

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
        if filename in skip:
            continue
        src = template_dir / filename
        dst = workspace_path / filename

        if dst.exists():
            logger.debug("Skipping %s — already exists", dst)
            continue

        if not src.exists():
            logger.warning("Template file missing: %s", src)
            continue

        _write_template(src, dst, f"{mode}/{filename}", memory_backend)
        logger.info("Created %s (%s)", filename, description)
        created.append(filename)

    return created
