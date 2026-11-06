"""Skill manager — discovers, loads, and manages skills from the filesystem.

Skills are directories containing a SKILL.md file with YAML frontmatter.
Compatible with Claude SDK's skill format. The filesystem is the source of
truth; the DB indexes metadata and tracks usage statistics.

Directory structure:
    workspace/skills/<skill-id>/
        SKILL.md          (required — YAML frontmatter + markdown body)
        references/       (optional — documentation loaded on demand)
        scripts/          (optional — executable code)
        assets/           (optional — templates, images, etc.)

Platform skills have the same structure in ``<config_dir>/skills/<skill-id>/``,
next to ``platform.md``. The operator or the hosting platform writes them;
Nerve only reads them. When both directories have a skill with the same id,
the platform skill is the one in effect. A locked instance does not read them.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nerve.config import ensure_not_locked, is_locked
from nerve.db import Database

logger = logging.getLogger(__name__)

# Where a skill comes from: ``<workspace>/skills`` or ``<config_dir>/skills``.
SOURCE_WORKSPACE = "workspace"
SOURCE_PLATFORM = "platform"

PLATFORM_SKILL_READ_ONLY = (
    "This skill is provided by the platform and cannot be changed here."
)


@dataclass
class SkillMeta:
    """Skill metadata extracted from SKILL.md frontmatter."""
    id: str
    name: str
    description: str
    version: str = "1.0.0"
    enabled: bool = True
    user_invocable: bool = True
    model_invocable: bool = True
    allowed_tools: list[str] | None = None
    has_references: bool = False
    has_scripts: bool = False
    has_assets: bool = False
    metadata: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    source: str = SOURCE_WORKSPACE


@dataclass
class SkillContent(SkillMeta):
    """Full skill including SKILL.md body content."""
    content: str = ""   # Markdown body (after frontmatter)
    raw: str = ""       # Full SKILL.md file


def _slugify(name: str) -> str:
    """Convert a name to a valid directory slug."""
    slug = name.lower().replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:60] or "unnamed-skill"


# One path component and nothing else: no separator, on either platform, and no
# drive or UNC prefix. Deliberately wider than what _slugify emits, because the
# filesystem is the source of truth here and discover() adopts whatever directory
# names it finds — a pre-existing `My_Skill` must stay editable.
_SKILL_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")


class SkillIdError(ValueError):
    """Raised for a skill id that could not name a directory under ``skills/``."""


class PlatformSkillError(RuntimeError):
    """Raised for a change to a skill that the platform provides."""


def _skill_id(skill_id: str) -> str:
    """Validate a caller-supplied skill id before it is joined to a path.

    ``_slugify`` runs on *create* only; every other entry point takes the id
    straight from an HTTP path segment or a tool argument and joins it to
    ``skills_dir``. That made ``..`` a working path component — and since delete
    removes the whole directory tree it names, ``DELETE /api/skills/../config``
    took out the workspace's tracked config subtree.

    Checked as a *shape* rather than by containing the result afterwards: a skill
    id names one directory directly under ``skills/``, so anything that is not a
    single path component is not a skill id, whatever it would have resolved to.
    A leading dot is refused too, which keeps ``.`` and ``..`` out without
    special-casing them.
    """
    if not isinstance(skill_id, str) or not _SKILL_ID_RE.match(skill_id):
        raise SkillIdError(
            f"invalid skill id {skill_id!r}: a skill id names a single directory "
            f"under skills/ — letters, digits, '.', '_', '+' and '-', not "
            f"starting with a dot"
        )
    return skill_id


def _parse_skill_md(raw: str) -> tuple[dict, str]:
    """Parse SKILL.md into (frontmatter_dict, body_content).

    Frontmatter is delimited by --- lines at the top of the file.
    """
    frontmatter: dict = {}
    body = raw

    stripped = raw.strip()
    if stripped.startswith("---"):
        # Find the closing ---
        end_idx = stripped.find("---", 3)
        if end_idx != -1:
            yaml_block = stripped[3:end_idx].strip()
            body = stripped[end_idx + 3:].strip()
            try:
                frontmatter = yaml.safe_load(yaml_block) or {}
            except yaml.YAMLError as e:
                logger.warning("Failed to parse SKILL.md frontmatter: %s", e)

    return frontmatter, body


def _build_skill_md(name: str, description: str, body: str = "", version: str = "1.0.0", **extra) -> str:
    """Build a SKILL.md file from components."""
    fm: dict[str, Any] = {"name": name, "description": description, "version": version}
    fm.update(extra)
    yaml_str = yaml.dump(fm, default_flow_style=False, allow_unicode=True).strip()
    parts = [f"---\n{yaml_str}\n---"]
    if body:
        parts.append(body)
    return "\n\n".join(parts) + "\n"


class SkillManager:
    """Discovers, loads, and manages skills from the filesystem."""

    def __init__(self, workspace: Path, db: Database, config_dir: Path | None = None):
        self.skills_dir = workspace / "skills"
        # Platform skills: ``<config_dir>/skills``, next to ``platform.md``.
        # When the config directory is the workspace, the two are one
        # directory, and its skills are workspace skills.
        self.platform_dir: Path | None = None
        if config_dir is not None:
            platform_dir = Path(config_dir) / "skills"
            if platform_dir.resolve() != self.skills_dir.resolve():
                self.platform_dir = platform_dir
        self.db = db
        self._cache: dict[str, SkillMeta] = {}

    def _platform_root(self) -> Path | None:
        """The platform skills directory to read, or ``None``.

        A locked instance runs only what its workspace repository was
        reviewed for, so it does not read platform skills.
        """
        if self.platform_dir is None or is_locked():
            return None
        return self.platform_dir

    def _platform_skill_dir(self, skill_id: str) -> Path | None:
        """The platform's directory for ``skill_id`` when it has a SKILL.md.

        Another party owns this directory. ``os.path.isfile`` gives False for
        a path that Nerve cannot search, where ``Path.exists`` raises
        ``PermissionError``, so such a path means "no platform skill" and the
        lookup of every other skill still works.
        """
        root = self._platform_root()
        if root is None or not _SKILL_ID_RE.match(skill_id):
            return None
        skill_dir = root / skill_id
        return skill_dir if os.path.isfile(skill_dir / "SKILL.md") else None

    def skill_dir(self, skill_id: str) -> Path:
        """The directory of the skill in effect for ``skill_id``: the
        platform's when it has the skill, else the workspace's."""
        skill_id = _skill_id(skill_id)
        return self._platform_skill_dir(skill_id) or self.skills_dir / skill_id

    def source(self, skill_id: str) -> str:
        """``"platform"`` when the skill in effect for ``skill_id`` is a
        platform skill, else ``"workspace"``."""
        if self._platform_skill_dir(skill_id) is not None:
            return SOURCE_PLATFORM
        return SOURCE_WORKSPACE

    def _refuse_platform_skill(self, skill_id: str) -> None:
        if self._platform_skill_dir(skill_id) is not None:
            raise PlatformSkillError(PLATFORM_SKILL_READ_ONLY)

    async def discover(self) -> list[SkillMeta]:
        """Scan the skill directories for SKILL.md files, parse frontmatter,
        sync to DB.

        Returns all discovered skills. Also removes DB entries for skills
        that no longer exist on the filesystem. A missing platform directory
        is not an error.
        """
        await asyncio.to_thread(self.skills_dir.mkdir, parents=True, exist_ok=True)
        discovered: list[SkillMeta] = []
        found_ids: set[str] = set()
        platform_root = self._platform_root()
        if (
            platform_root is None
            and self.platform_dir is not None
            and os.path.isdir(self.platform_dir)
        ):
            logger.info(
                "This instance is in lockdown: platform skills in %s are not read",
                self.platform_dir,
            )

        def _skill_dirs(root: Path, source: str) -> list[Path]:
            """The entries of ``root``. A platform directory that is missing
            or cannot be read has none."""
            if source == SOURCE_WORKSPACE:
                return sorted(root.iterdir())
            try:
                return sorted(root.iterdir()) if root.is_dir() else []
            except OSError as e:
                logger.error("Failed to read platform skills in %s: %s", root, e)
                return []

        def _is_platform_skill(sdir: Path) -> bool:
            """Whether ``sdir`` holds a platform skill to index. An entry
            that cannot be read is logged and skipped: it never stops the
            scan, so the workspace skills still load."""
            try:
                if not (sdir / "SKILL.md").is_file():
                    return False
            except OSError as e:
                logger.error("Failed to read platform skill %s: %s", sdir.name, e)
                return False
            if not _SKILL_ID_RE.match(sdir.name):
                logger.warning(
                    "Skipping platform skill %r: not a valid skill id", sdir.name,
                )
                return False
            return True

        def _scan_skill_dirs() -> list[tuple[Path, str, str]]:
            """Collect (skill_dir, raw SKILL.md, source) off the event loop.

            One entry for each id. The platform directory is read first, so
            its skill is the one in effect for an id that is in both.
            """
            roots = [(self.skills_dir, SOURCE_WORKSPACE)]
            if platform_root is not None:
                roots.insert(0, (platform_root, SOURCE_PLATFORM))
            claimed: dict[str, Path] = {}
            out: list[tuple[Path, str, str]] = []
            for root, source in roots:
                for sdir in _skill_dirs(root, source):
                    smd = sdir / "SKILL.md"
                    if source == SOURCE_PLATFORM:
                        if not _is_platform_skill(sdir):
                            continue
                    elif not sdir.is_dir() or not smd.exists():
                        continue
                    if sdir.name in claimed:
                        logger.warning(
                            "Skill %s is in %s and in %s: the platform skill is in effect",
                            sdir.name, claimed[sdir.name], sdir,
                        )
                        continue
                    claimed[sdir.name] = sdir
                    try:
                        out.append((sdir, smd.read_text(encoding="utf-8"), source))
                    except OSError as e:
                        logger.error("Failed to read skill %s: %s", sdir.name, e)
            return sorted(out, key=lambda entry: entry[0].name)

        for skill_dir, raw, source in await asyncio.to_thread(_scan_skill_dirs):
            skill_id = skill_dir.name
            found_ids.add(skill_id)

            try:
                fm, body = _parse_skill_md(raw)

                name = fm.get("name", skill_id)
                description = fm.get("description", "")
                if not description:
                    # Use first non-empty line of body as fallback
                    for line in body.split("\n"):
                        line = line.strip()
                        if line and not line.startswith("#"):
                            description = line[:200]
                            break

                version = fm.get("version", "1.0.0")
                user_invocable = fm.get("user-invocable", True)
                model_invocable = not fm.get("disable-model-invocation", False)
                allowed_tools_raw = fm.get("allowed-tools")
                allowed_tools = None
                if allowed_tools_raw:
                    if isinstance(allowed_tools_raw, str):
                        allowed_tools = [t.strip() for t in allowed_tools_raw.split(",")]
                    elif isinstance(allowed_tools_raw, list):
                        allowed_tools = allowed_tools_raw

                # Check for optional subdirectories
                has_references = (skill_dir / "references").is_dir()
                has_scripts = (skill_dir / "scripts").is_dir()
                has_assets = (skill_dir / "assets").is_dir()

                # Extra metadata (everything not in known fields)
                known_keys = {"name", "description", "version", "user-invocable",
                              "disable-model-invocation", "allowed-tools", "license",
                              "argument-hint", "context", "agent"}
                extra_meta = {k: v for k, v in fm.items() if k not in known_keys}

                meta = SkillMeta(
                    id=skill_id,
                    name=name,
                    description=description,
                    version=str(version),
                    user_invocable=user_invocable,
                    model_invocable=model_invocable,
                    allowed_tools=allowed_tools,
                    has_references=has_references,
                    has_scripts=has_scripts,
                    has_assets=has_assets,
                    metadata=extra_meta,
                    source=source,
                )
                discovered.append(meta)
                self._cache[skill_id] = meta

                # Check if DB has the skill and preserve its enabled state
                existing = await self.db.get_skill_row(skill_id)
                enabled = existing["enabled"] if existing else True

                # Sync to DB
                await self.db.upsert_skill(
                    skill_id=skill_id,
                    name=name,
                    description=description,
                    version=str(version),
                    enabled=enabled,
                    user_invocable=user_invocable,
                    model_invocable=model_invocable,
                    allowed_tools=allowed_tools,
                    metadata=extra_meta,
                )

            except Exception as e:
                logger.error("Failed to load skill %s: %s", skill_id, e)

        # Clean up DB entries for skills that no longer exist on filesystem
        db_skills = await self.db.list_skills()
        for db_skill in db_skills:
            if db_skill["id"] not in found_ids:
                logger.info("Removing stale skill from DB: %s", db_skill["id"])
                await self.db.delete_skill_row(db_skill["id"])
        # The system prompt's skill list reads the cache.
        for skill_id in set(self._cache) - found_ids:
            del self._cache[skill_id]

        logger.info("Discovered %d skills", len(discovered))
        return discovered

    async def get_skill(self, skill_id: str) -> SkillContent | None:
        """Load full SKILL.md content + metadata for a skill."""
        skill_dir = self.skill_dir(skill_id)
        source = self.source(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        raw = await asyncio.to_thread(skill_md.read_text, encoding="utf-8")
        fm, body = _parse_skill_md(raw)

        # Get metadata from cache or DB
        cached = self._cache.get(skill_id)
        if cached:
            return SkillContent(
                id=cached.id, name=cached.name, description=cached.description,
                version=cached.version, enabled=cached.enabled,
                user_invocable=cached.user_invocable,
                model_invocable=cached.model_invocable,
                allowed_tools=cached.allowed_tools,
                has_references=cached.has_references,
                has_scripts=cached.has_scripts,
                has_assets=cached.has_assets,
                metadata=cached.metadata,
                created_at=cached.created_at,
                updated_at=cached.updated_at,
                source=source,
                content=body,
                raw=raw,
            )

        # Fallback: parse from file
        name = fm.get("name", skill_id)
        description = fm.get("description", "")
        return SkillContent(
            id=skill_id, name=name, description=description,
            version=fm.get("version", "1.0.0"),
            content=body, raw=raw,
            has_references=(skill_dir / "references").is_dir(),
            has_scripts=(skill_dir / "scripts").is_dir(),
            has_assets=(skill_dir / "assets").is_dir(),
            source=source,
        )

    async def create_skill(
        self,
        name: str,
        description: str,
        content: str = "",
        version: str = "1.0.0",
    ) -> SkillMeta:
        """Create a new skill directory + SKILL.md and index in DB."""
        ensure_not_locked("create a skill")
        skill_id = _slugify(name)
        skill_dir = self.skills_dir / _skill_id(skill_id)
        if self._platform_skill_dir(skill_id) is not None:
            raise PlatformSkillError(
                f"A skill with the id {skill_id!r} is provided by the platform "
                f"and cannot be changed here. Choose a different name."
            )

        # Build SKILL.md
        raw = _build_skill_md(name, description, content, version)

        def _write_skill() -> None:
            skill_dir.mkdir(parents=True, exist_ok=True)
            (skill_dir / "SKILL.md").write_text(raw, encoding="utf-8")

        await asyncio.to_thread(_write_skill)

        # Sync to DB
        await self.db.upsert_skill(
            skill_id=skill_id, name=name, description=description,
            version=version,
        )

        meta = SkillMeta(
            id=skill_id, name=name, description=description,
            version=version,
        )
        self._cache[skill_id] = meta
        return meta

    async def update_skill(self, skill_id: str, content: str) -> SkillMeta | None:
        """Update SKILL.md content (full raw file), re-parse frontmatter, sync DB."""
        ensure_not_locked("update a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        self._refuse_platform_skill(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        await asyncio.to_thread(skill_md.write_text, content, encoding="utf-8")

        # Re-parse and sync
        fm, body = _parse_skill_md(content)
        name = fm.get("name", skill_id)
        description = fm.get("description", "")
        version = fm.get("version", "1.0.0")

        await self.db.upsert_skill(
            skill_id=skill_id, name=name, description=description,
            version=str(version),
            user_invocable=fm.get("user-invocable", True),
            model_invocable=not fm.get("disable-model-invocation", False),
        )

        meta = SkillMeta(
            id=skill_id, name=name, description=description,
            version=str(version),
            has_references=(skill_dir / "references").is_dir(),
            has_scripts=(skill_dir / "scripts").is_dir(),
            has_assets=(skill_dir / "assets").is_dir(),
        )
        self._cache[skill_id] = meta
        return meta

    async def delete_skill(self, skill_id: str) -> bool:
        """Remove skill directory and DB record."""
        ensure_not_locked("delete a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        self._refuse_platform_skill(skill_id)
        if skill_dir.exists():
            await asyncio.to_thread(shutil.rmtree, skill_dir)
        await self.db.delete_skill_row(skill_id)
        self._cache.pop(skill_id, None)
        return True

    async def toggle_skill(self, skill_id: str, enabled: bool) -> bool:
        """Enable or disable a skill."""
        ensure_not_locked("toggle a skill")
        existing = await self.db.get_skill_row(skill_id)
        if not existing:
            return False
        await self.db.update_skill_enabled(skill_id, enabled)
        if skill_id in self._cache:
            self._cache[skill_id].enabled = enabled
        return True

    async def list_references(self, skill_id: str) -> list[str]:
        """List reference files in a skill's references/ directory."""
        refs_dir = self.skill_dir(skill_id) / "references"
        if not refs_dir.is_dir():
            return []

        def _walk() -> list[str]:
            return sorted(
                str(f.relative_to(refs_dir))
                for f in refs_dir.rglob("*")
                if f.is_file()
            )

        return await asyncio.to_thread(_walk)

    async def read_reference(self, skill_id: str, rel_path: str) -> str | None:
        """Read a reference file from a skill."""
        skill_dir = self.skill_dir(skill_id)
        ref_file = skill_dir / "references" / rel_path
        # Prevent path traversal
        try:
            ref_file.resolve().relative_to(skill_dir.resolve())
        except ValueError:
            return None
        if not ref_file.exists() or not ref_file.is_file():
            return None
        return await asyncio.to_thread(ref_file.read_text, encoding="utf-8")

    async def run_script(self, skill_id: str, rel_path: str, args: str = "") -> str:
        """Execute a script from a skill's scripts/ directory."""
        skill_dir = self.skill_dir(skill_id)
        script_file = skill_dir / "scripts" / rel_path
        # Prevent path traversal
        try:
            script_file.resolve().relative_to(skill_dir.resolve())
        except ValueError:
            return "Error: path traversal detected"
        if not script_file.exists() or not script_file.is_file():
            return f"Error: script not found: {rel_path}"

        # Detect interpreter
        suffix = script_file.suffix.lower()
        if suffix == ".py":
            cmd = ["python3", str(script_file)]
        elif suffix in (".sh", ".bash"):
            cmd = ["bash", str(script_file)]
        else:
            cmd = [str(script_file)]

        if args:
            cmd.extend(args.split())

        try:
            result = await asyncio.to_thread(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(skill_dir),
            )
            output = result.stdout
            if result.returncode != 0:
                output += f"\nSTDERR: {result.stderr}\nExit code: {result.returncode}"
            return output
        except subprocess.TimeoutExpired:
            return "Error: script timed out (30s)"
        except Exception as e:
            return f"Error running script: {e}"

    async def get_enabled_summaries(self) -> list[dict]:
        """Return name+description+source for all enabled model-invocable skills.

        Used for system prompt injection (progressive disclosure level 1).
        """
        db_skills = await self.db.list_skills()
        summaries = []
        for s in db_skills:
            if s["enabled"] and s["model_invocable"]:
                summaries.append({
                    "id": s["id"],
                    "name": s["name"],
                    "description": s["description"],
                    "source": self.source(s["id"]),
                })
        return summaries

    async def record_usage(
        self,
        skill_id: str,
        session_id: str | None = None,
        invoked_by: str = "model",
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log a skill invocation for statistics."""
        await self.db.record_skill_usage(
            skill_id=skill_id,
            session_id=session_id,
            invoked_by=invoked_by,
            duration_ms=duration_ms,
            success=success,
            error=error,
        )
