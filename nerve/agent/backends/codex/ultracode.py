"""Managed Ultracode plugin lifecycle for Nerve's isolated Codex home.

Ultracode is third-party code and deliberately not imported from the user's
normal ``~/.codex``.  Nerve installs one reviewed git revision through a local
marketplace manifest, disables the plugin's autonomous updater, and supplies a
worker wrapper that re-attaches Nerve's per-session MCP configuration without
writing bearer tokens to disk.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import signal
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MARKETPLACE = "nerve-ultracode"
_INSTALL_LOCK = asyncio.Lock()
_OVERLAY_VERSION = 2
_RUN_ID_RE = re.compile(r"^ultra-[A-Za-z0-9][A-Za-z0-9_-]{0,191}$")
_MAX_RUN_BYTES = 16 * 1024 * 1024
_SENSITIVE_KEY_RE = re.compile(
    r"(?:^|[_-])(?:authorization|bearer|cookie|credential|password|secret|token)"
    r"(?:$|[_-])",
    re.IGNORECASE,
)
_DASHBOARD_FIELDS = (
    "id", "name", "display_name", "slug", "kind", "status", "task", "cwd",
    "started_at", "completed_at", "updated_at", "duration_ms", "workers",
    "steps", "events", "aggregate_usage", "options", "result", "aggregate",
    "error", "running", "pending", "completed", "failed", "cancelled",
)

_ENGINE_CONCURRENCY_OLD = '''function normalizeConcurrency(value) {
  if (value === undefined || value === null || value === "") return defaultConcurrency();
  return Math.max(1, Math.min(16, Math.floor(Number(value)) || 1));
}'''
_ENGINE_CONCURRENCY_NEW = '''function normalizeConcurrency(value) {
  const policyCap = Math.max(
    1,
    Math.min(16, Math.floor(Number(process.env.ULTRACODE_MAX_CONCURRENCY || 16)) || 1)
  );
  if (value === undefined || value === null || value === "") {
    return Math.min(policyCap, defaultConcurrency());
  }
  return Math.min(policyCap, Math.max(1, Math.min(16, Math.floor(Number(value)) || 1)));
}'''
_ENGINE_BUDGET_OLD = '''  const budgetTotal =
    opts.budgetTokens === undefined || opts.budgetTokens === null || opts.budgetTokens === ""
      ? null
      : Math.max(0, Math.floor(Number(opts.budgetTokens)));'''
_ENGINE_BUDGET_NEW = '''  const requestedBudget =
    opts.budgetTokens === undefined || opts.budgetTokens === null || opts.budgetTokens === ""
      ? null
      : Math.max(0, Math.floor(Number(opts.budgetTokens)));
  const policyBudgetRaw = Math.floor(Number(process.env.ULTRACODE_DEFAULT_TOKEN_BUDGET || 0));
  const policyBudget = policyBudgetRaw > 0 ? policyBudgetRaw : null;
  const budgetTotal = requestedBudget === null
    ? policyBudget
    : (policyBudget === null ? requestedBudget : Math.min(requestedBudget, policyBudget));'''
_ENGINE_AGENTS_OLD = '''    maxAgents: opts.maxAgents ? Math.max(1, Math.floor(Number(opts.maxAgents))) : DEFAULT_MAX_AGENTS,'''
_ENGINE_AGENTS_NEW = '''    maxAgents: Math.min(
      Math.max(1, Math.floor(Number(process.env.ULTRACODE_MAX_AGENTS || DEFAULT_MAX_AGENTS)) || 1),
      opts.maxAgents ? Math.max(1, Math.floor(Number(opts.maxAgents))) : DEFAULT_MAX_AGENTS
    ),'''
_CLI_UI_OLD = '''  if (UI_COMMANDS.has(command) && options.ui === undefined) {
    const env = parseBool(process.env.ULTRACODE_UI);
    options.ui = env === undefined ? true : env;
  }'''
_CLI_UI_NEW = '''  if (UI_COMMANDS.has(command)) {
    const env = parseBool(process.env.ULTRACODE_UI);
    if (env === false) options.ui = false;
    else if (options.ui === undefined) options.ui = env === undefined ? true : env;
  }'''
_JOURNAL_WRITE_OLD = '''  await fs.writeFile(tmpPath, `${JSON.stringify(value, null, 2)}\\n`, "utf8");'''
_JOURNAL_WRITE_NEW = '''  await fs.writeFile(
    tmpPath,
    `${JSON.stringify(value, null, 2)}\\n`,
    { encoding: "utf8", mode: 0o600 }
  );'''


def managed_dir(home: str | Path) -> Path:
    return Path(home).expanduser() / "nerve-managed" / "ultracode"


def _atomic_write(path: Path, content: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(content, encoding="utf-8")
    os.chmod(tmp, mode)
    tmp.replace(path)
    os.chmod(path, mode)


def materialize_worker_wrapper(home: str | Path) -> Path:
    """Write an owner-only executable pointing at Nerve's wrapper module."""
    path = managed_dir(home) / "codex-worker"
    script = (
        f"#!{sys.executable}\n"
        "from nerve.agent.backends.codex.worker_wrapper import main\n"
        "raise SystemExit(main())\n"
    )
    _atomic_write(path, script, mode=0o700)
    return path


def _replace_overlay(path: Path, old: str, new: str) -> None:
    content = path.read_text(encoding="utf-8")
    if new in content:
        return
    if content.count(old) != 1:
        raise RuntimeError(
            f"Pinned Ultracode source drifted; policy overlay does not match {path}"
        )
    mode = path.stat().st_mode & 0o777
    _atomic_write(path, content.replace(old, new), mode=mode)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_policy_overlay(config: Any, plugin_root: str | Path) -> dict[str, Any]:
    """Apply Nerve's deterministic safety cap to the pinned upstream tree.

    The exact upstream git HEAD remains independently verified. Replacements
    fail closed if the reviewed source shape changes, and hashes in the marker
    detect later tampering/drift.
    """
    root = Path(plugin_root)
    engine = root / "scripts" / "ultracode-engine.js"
    cli = root / "scripts" / "ultracode-cli.js"
    script_runner = root / "scripts" / "ultracode-script-runner.js"
    _replace_overlay(engine, _ENGINE_CONCURRENCY_OLD, _ENGINE_CONCURRENCY_NEW)
    _replace_overlay(engine, _ENGINE_BUDGET_OLD, _ENGINE_BUDGET_NEW)
    _replace_overlay(engine, _ENGINE_AGENTS_OLD, _ENGINE_AGENTS_NEW)
    _replace_overlay(engine, _JOURNAL_WRITE_OLD, _JOURNAL_WRITE_NEW)
    _replace_overlay(cli, _CLI_UI_OLD, _CLI_UI_NEW)
    _replace_overlay(script_runner, _JOURNAL_WRITE_OLD, _JOURNAL_WRITE_NEW)
    marker = {
        "overlay_version": _OVERLAY_VERSION,
        "revision": config.codex.ultracode.revision,
        "files": {
            "scripts/ultracode-engine.js": _sha256(engine),
            "scripts/ultracode-cli.js": _sha256(cli),
            "scripts/ultracode-script-runner.js": _sha256(script_runner),
        },
    }
    _atomic_write(
        managed_dir(config.codex.home_dir) / "policy-overlay.json",
        json.dumps(marker, indent=2, sort_keys=True) + "\n",
    )
    return marker


def _overlay_valid(config: Any, plugin_root: Path) -> bool:
    marker_path = managed_dir(config.codex.home_dir) / "policy-overlay.json"
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        if marker.get("overlay_version") != _OVERLAY_VERSION:
            return False
        if marker.get("revision") != config.codex.ultracode.revision:
            return False
        for relative, digest in (marker.get("files") or {}).items():
            if _sha256(plugin_root / relative) != digest:
                return False
        return len(marker.get("files") or {}) == 3
    except (OSError, ValueError, TypeError):
        return False


def _marketplace_manifest(config: Any) -> dict[str, Any]:
    uc = config.codex.ultracode
    return {
        "name": _MARKETPLACE,
        "interface": {"displayName": "Nerve managed plugins"},
        "plugins": [{
            "name": "ultracode",
            "source": {
                "source": "url",
                "url": uc.repository,
                "ref": uc.revision,
            },
            "policy": {
                "installation": "AVAILABLE",
                "authentication": "ON_INSTALL",
            },
            "category": "Developer Tools",
        }],
    }


async def _run(*args: str, env: dict[str, str], timeout: float = 90.0) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except BaseException as e:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        await proc.wait()
        if isinstance(e, asyncio.TimeoutError):
            raise RuntimeError(
                f"{' '.join(args[:4])} timed out after {timeout:.0f}s"
            ) from None
        raise
    text = stdout.decode("utf-8", errors="replace")
    err = stderr.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError((err or text or f"exit {proc.returncode}").strip()[:2000])
    return text


def installation_status(config: Any) -> dict[str, Any]:
    """Inspect the isolated home without invoking network or the plugin."""
    home = Path(config.codex.home_dir).expanduser()
    expected = config.codex.ultracode.version
    manifests = sorted([
        *home.glob("plugins/cache/**/.codex-plugin/plugin.json"),
        *home.glob(".tmp/plugins*/**/.codex-plugin/plugin.json"),
    ], key=lambda path: (expected not in str(path), str(path)))
    for manifest_path in manifests:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("name") != "ultracode":
            continue
        version = str(manifest.get("version") or "")
        plugin_root = manifest_path.parent.parent
        try:
            actual_revision = (plugin_root / ".git" / "HEAD").read_text(
                encoding="utf-8",
            ).strip()
        except OSError:
            actual_revision = ""
        overlay = _overlay_valid(config, plugin_root)
        return {
            "enabled": bool(config.codex.ultracode.enabled),
            "installed": (
                version == expected
                and actual_revision == config.codex.ultracode.revision
                and overlay
            ),
            "version": version,
            "expected_version": expected,
            "revision": actual_revision or None,
            "expected_revision": config.codex.ultracode.revision,
            "path": str(plugin_root),
            "auto_update": False,
            "policy_overlay": overlay,
            "policy": {
                "max_concurrency": config.codex.ultracode.max_concurrency,
                "default_token_budget": config.codex.ultracode.default_token_budget,
                "max_agents": config.codex.ultracode.max_agents,
                "dashboard": config.codex.ultracode.dashboard,
                "ui": config.codex.ultracode.ui,
            },
        }
    return {
        "enabled": bool(config.codex.ultracode.enabled),
        "installed": False,
        "version": None,
        "expected_version": expected,
        "revision": None,
        "expected_revision": config.codex.ultracode.revision,
        "path": None,
        "auto_update": False,
        "policy_overlay": False,
        "policy": {
            "max_concurrency": config.codex.ultracode.max_concurrency,
            "default_token_budget": config.codex.ultracode.default_token_budget,
            "max_agents": config.codex.ultracode.max_agents,
            "dashboard": config.codex.ultracode.dashboard,
            "ui": config.codex.ultracode.ui,
        },
    }


async def ensure_installed(config: Any) -> dict[str, Any]:
    """Install/repair the pinned plugin snapshot when explicitly enabled."""
    uc = config.codex.ultracode
    if not uc.enabled:
        return installation_status(config)
    materialize_worker_wrapper(config.codex.home_dir)
    status = installation_status(config)
    if status["installed"] or not uc.auto_install:
        return status

    async with _INSTALL_LOCK:
        status = installation_status(config)
        if status["installed"]:
            return status
        if (
            status.get("version") == uc.version
            and status.get("revision") == uc.revision
            and status.get("path")
        ):
            apply_policy_overlay(config, status["path"])
            return installation_status(config)

        root = managed_dir(config.codex.home_dir) / "marketplace"
        manifest_path = root / ".agents" / "plugins" / "marketplace.json"
        _atomic_write(
            manifest_path,
            json.dumps(_marketplace_manifest(config), indent=2) + "\n",
        )
        env = {
            **os.environ,
            "CODEX_HOME": str(Path(config.codex.home_dir).expanduser()),
            "ULTRACODE_NO_AUTO_UPDATE": "1",
        }
        try:
            await _run(
                config.codex.bin_path, "plugin", "marketplace", "add",
                str(root), "--json", env=env,
            )
        except RuntimeError as e:
            # Re-adding the same managed marketplace is harmless and some CLI
            # versions report it as an error rather than an idempotent result.
            if "already" not in str(e).lower():
                raise
        await _run(
            config.codex.bin_path, "plugin", "add",
            f"ultracode@{_MARKETPLACE}", "--json", env=env,
        )
        status = installation_status(config)
        if (
            status.get("path")
            and status.get("version") == uc.version
            and status.get("revision") == uc.revision
        ):
            apply_policy_overlay(config, status["path"])
        status = installation_status(config)
        if not status["installed"]:
            raise RuntimeError(
                "Codex reported Ultracode installed but the pinned manifest "
                f"version {uc.version!r} was not found"
            )
        logger.info(
            "Installed managed Ultracode %s at %s (revision %s)",
            status["version"], status["path"], uc.revision,
        )
        return status


def _runs_dir(config: Any) -> Path:
    return Path(config.codex.home_dir).expanduser() / "ultracode" / "runs"


def _read_run_file(path: Path) -> dict[str, Any] | None:
    """Read one bounded, ordinary journal file.

    Journals are written by the managed plugin but are still treated as
    untrusted input at the HTTP boundary: symlinks, oversized files, malformed
    JSON, and non-object roots are ignored.
    """
    try:
        if path.is_symlink() or not path.is_file():
            return None
        metadata = path.stat()
        if metadata.st_size > _MAX_RUN_BYTES:
            return None
        if metadata.st_mode & 0o077:
            return None
        if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _dashboard_value(value: Any, *, depth: int = 0) -> Any:
    """Bound arbitrary journal values and redact credential-shaped keys."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value if len(value) <= 100_000 else value[:100_000] + "\n…"
    if depth >= 8:
        return "[maximum depth reached]"
    if isinstance(value, list):
        return [
            _dashboard_value(item, depth=depth + 1)
            for item in value[:500]
        ]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for raw_key, item in list(value.items())[:200]:
            key = str(raw_key)
            if _SENSITIVE_KEY_RE.search(key):
                result[key] = "[redacted]"
            else:
                result[key] = _dashboard_value(item, depth=depth + 1)
        return result
    return str(value)[:10_000]


def _dashboard_record(
    record: dict[str, Any], run_id: str, *, detail: bool,
) -> dict[str, Any] | None:
    recorded_id = str(record.get("id") or run_id)
    if recorded_id != run_id or not _RUN_ID_RE.fullmatch(recorded_id):
        return None
    fields = _DASHBOARD_FIELDS if detail else (
        "id", "name", "display_name", "slug", "kind", "status", "task",
        "started_at", "completed_at", "updated_at", "duration_ms",
        "aggregate_usage", "running", "pending", "completed", "failed",
        "cancelled",
    )
    result = {
        key: _dashboard_value(record[key])
        for key in fields
        if key in record
    }
    result["id"] = recorded_id
    if not detail:
        workers = record.get("workers")
        result["workers"] = len(workers) if isinstance(workers, list) else 0
    return result


def read_dashboard_run(config: Any, workflow_id: str) -> dict[str, Any] | None:
    """Return one sanitized run for the authenticated read-only dashboard."""
    if not _RUN_ID_RE.fullmatch(workflow_id):
        return None
    path = _runs_dir(config) / f"{workflow_id}.json"
    record = _read_run_file(path)
    return _dashboard_record(record, workflow_id, detail=True) if record else None


def read_verified_run_journal(
    config: Any, workflow_id: str,
) -> dict[str, Any] | None:
    """Load a raw journal after path, ownership, mode, size, and id checks."""
    if not _RUN_ID_RE.fullmatch(workflow_id):
        return None
    record = _read_run_file(_runs_dir(config) / f"{workflow_id}.json")
    if record is None or str(record.get("id") or "") != workflow_id:
        return None
    return record


def list_dashboard_runs(config: Any, limit: int = 50) -> list[dict[str, Any]]:
    """Return newest sanitized run summaries without starting Ultracode."""
    runs_dir = _runs_dir(config)
    if not runs_dir.is_dir():
        return []
    bounded_limit = max(1, min(int(limit), 200))
    candidates: list[tuple[float, Path]] = []
    for path in runs_dir.glob("ultra-*.json"):
        try:
            candidates.append((path.stat().st_mtime, path))
        except OSError:
            continue
    out: list[dict[str, Any]] = []
    for _, path in sorted(candidates, key=lambda item: item[0], reverse=True):
        if len(out) >= bounded_limit:
            break
        if not _RUN_ID_RE.fullmatch(path.stem):
            continue
        record = _read_run_file(path)
        if record is None:
            continue
        summary = _dashboard_record(record, path.stem, detail=False)
        if summary is not None:
            out.append(summary)
    return out


def recoverable_runs(config: Any) -> list[dict[str, Any]]:
    """List non-terminal Ultracode journals for operator recovery UI."""
    runs_dir = Path(config.codex.home_dir).expanduser() / "ultracode" / "runs"
    if not runs_dir.is_dir():
        return []
    out: list[dict[str, Any]] = []
    terminal = {"completed", "failed", "cancelled", "partial", "refuted"}
    for path in sorted(runs_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        status = str(record.get("status") or "unknown").lower()
        if status in terminal:
            continue
        out.append({
            "workflow_id": record.get("id") or path.stem,
            "status": status,
            "task": record.get("task"),
            "updated_at": record.get("updated_at"),
            "journal": str(path),
            "aggregate_usage": record.get("aggregate_usage"),
        })
    return out[:50]
