"""Drop-in cron gate plugins — auto-register custom gates without editing core.

Built-in gates live in :mod:`nerve.cron.gates` and are registered directly in
:data:`nerve.cron.gates.GATE_REGISTRY`. To add a *custom* gate **without
editing core source**, drop a ``.py`` file into the gate-plugins directory
(``<workspace>/config/cron/gates/`` by default — falling back to the legacy
``~/.nerve/cron/gates/`` for un-migrated installs — overridable via the
``cron.gate_plugins_dir`` config key). On daemon startup each file is imported and every
:class:`~nerve.cron.gates.CronGate` subclass it defines with a non-empty
``type`` is registered into ``GATE_REGISTRY`` — after which ``jobs.yaml`` can
reference it via ``run_if: [{type: <name>, ...}]`` exactly like a built-in.

Because the loader never edits ``gates.py``, custom gates don't conflict when
pulling Nerve upstream; the loader itself is generic and upstreamable.

A plugin file looks like any other module defining a gate::

    # <workspace>/config/cron/gates/stale_tasks.py
    from nerve.cron.gates import CronGate, GateContext

    class StaleTasksGate(CronGate):
        type = "stale_tasks"

        def __init__(self, min_age_minutes: int = 30):
            self.min_age_minutes = min_age_minutes

        async def is_satisfied(self, ctx: GateContext) -> bool:
            ...                       # ctx gives {job_id, db} — DB-only

        def describe(self) -> str:
            return f"stale tasks older than {self.min_age_minutes}m"

        @classmethod
        def from_config(cls, spec: dict) -> "StaleTasksGate":
            return cls(min_age_minutes=int(spec.get("min_age_minutes", 30)))

Rules (all fail-safe — a bad plugin never crashes the daemon):

* Files whose name starts with ``_`` (and ``__pycache__``) are skipped.
* A plugin ``type`` that collides with an already-registered gate is skipped
  with a warning: a **built-in always wins**, and among two plugins the
  **first loaded (filename-sorted) wins**.
* Any import/exec error in a plugin file is logged (naming the file) and the
  file is skipped; the remaining files still load.
* A gate gets only ``GateContext{job_id, db}`` (DB-only). A liveness/registry
  based gate is out of scope for this loader — it would need the context
  widened, a separate change.
* Hot-reload: ``load_gate_plugins(dir, replace=True)`` (used by
  :meth:`nerve.cron.service.CronService.reload`) re-reads the directory from
  scratch — a *new* file is registered, an *edited* file's new code replaces the
  old class, and a *deleted* file's gate is unregistered. Reload then rebuilds
  every job's gates from the refreshed registry and swaps the rebuilt job into
  the scheduler, so the change reaches the running job and not just the
  registry. Without ``replace`` the first-registered class wins (used on fresh
  startup, and by validation so a candidate bundle can't swap the live
  registry). A reload that is then refused rolls the registry back, so the same
  rule holds for it: only a change that is actually applied reaches the live
  registry.

**Trust model.** Files in the gate-plugins directory are imported (i.e.
executed) at daemon startup. This is the same trust model as ``config.yaml``,
configured MCP servers, and cron prompt files — all user-controlled code/config
the daemon already loads. Only place files you trust in this directory.
"""

from __future__ import annotations

import importlib.util
import inspect
import logging
from pathlib import Path

from nerve.cron.gates import GATE_REGISTRY, CronGate

logger = logging.getLogger(__name__)

#: Module-name prefix stamped on every plugin-loaded module (see
#: :func:`_import_module`). It's how we tell a plugin-registered gate class apart
#: from a built-in (whose ``__module__`` is ``nerve.cron.gates``) — used by
#: ``replace=True`` to drop only plugin-owned entries on reload.
_PLUGIN_MODULE_PREFIX = "nerve_cron_gate_plugin_"


def load_gate_plugins(
    plugins_dir: Path, *, replace: bool = False, warn_vanished: bool = True,
) -> int:
    """Discover and register :class:`CronGate` subclasses from *plugins_dir*.

    Returns the number of gate classes newly registered into
    :data:`GATE_REGISTRY`. A missing directory is a no-op (returns ``0``).

    With ``replace=True`` the directory is re-read from scratch: every
    plugin-owned entry is dropped from the registry first, so an *edited*
    plugin's new code takes effect, a *deleted* plugin's gate is unregistered,
    and a *renamed* type is moved — the registry ends up reflecting exactly the
    files on disk. This is the hot-reload path (:meth:`CronService.reload`).
    Built-ins are never dropped. The default (``replace=False``) is
    first-registered-wins and is used on fresh startup and by config validation
    (so validating a candidate bundle can't mutate the live registry).

    ``warn_vanished=False`` suppresses the gate-disappeared warning. A caller
    that can still abandon the load — :meth:`CronService.reload` rolls the
    registry back if the rest of the reload is refused — passes ``False`` here
    and calls :func:`warn_vanished_gates` once the change is committed, so the
    warning describes what happened rather than what was attempted.

    Never raises: a broken plugin file is logged and skipped so it can't take
    down daemon startup (mirrors :func:`nerve.cron.gates.build_gates`' existing
    tolerance of a bad spec).
    """
    try:
        plugins_dir = Path(plugins_dir).expanduser()
    except Exception as e:  # noqa: BLE001 — defensive; never block startup
        logger.warning("Invalid cron gate plugins dir %r: %s", plugins_dir, e)
        return 0

    # Drop previously plugin-loaded gates before rescanning so edits/deletes are
    # reflected. Do this even if the dir has since disappeared (deleted plugins
    # must still unregister). Built-ins stay put.
    removed_types = _unregister_plugin_gates() if replace else set()

    if not plugins_dir.is_dir():
        # Missing dir is the normal case — most installs have no custom gates.
        if warn_vanished:
            _warn_vanished(removed_types)
        return 0

    registered = 0
    for path in sorted(plugins_dir.glob("*.py")):
        if path.name.startswith("_") or not path.is_file():
            # Skip private modules and the odd case of a directory named "*.py".
            continue
        registered += _load_file(path)

    if registered:
        logger.info(
            "Loaded %d custom cron gate(s) from %s", registered, plugins_dir,
        )
    if replace and warn_vanished:
        _warn_vanished(removed_types)
    return registered


def warn_vanished_gates(before: dict[str, type[CronGate]]) -> None:
    """Warn about gate types in *before* that are no longer registered.

    The deferred half of ``load_gate_plugins(..., warn_vanished=False)``: pass
    the registry snapshot taken before the load and call this once the reload
    that prompted it has committed. Built-ins are never dropped, so including
    them in the snapshot cannot produce a spurious warning.
    """
    _warn_vanished(set(before))


def _unregister_plugin_gates() -> set[str]:
    """Remove every plugin-loaded gate from :data:`GATE_REGISTRY`; keep built-ins.

    Returns the set of gate types removed, so the caller can warn about any that
    fail to come back (a plugin that was deleted or no longer imports cleanly).
    """
    removed: set[str] = set()
    for gate_type, cls in list(GATE_REGISTRY.items()):
        if getattr(cls, "__module__", "").startswith(_PLUGIN_MODULE_PREFIX):
            del GATE_REGISTRY[gate_type]
            removed.add(gate_type)
    return removed


def _warn_vanished(removed_types: set[str]) -> None:
    """Warn about gate types that were unregistered and did not re-register.

    A gate that disappears on reload means its plugin file was deleted or now
    fails to import. A job whose ``run_if`` names it no longer builds at all —
    ``build_gates`` raises on a type nothing registered — which refuses that
    reload before it can commit, so such a reload never reaches this warning. Its
    400 is the signal instead, and it names the job.

    What reaches here is the other case: the plugin went away and no job
    referenced it, so the reload committed. That is harmless right now, and still
    worth a line, because the next job to name that type will be refused and the
    cause will be this pull rather than that edit.
    """
    for gate_type in sorted(removed_types - set(GATE_REGISTRY)):
        logger.warning(
            "Cron gate %r is no longer registered after reload — its plugin file "
            "was removed or failed to re-import. No job uses it, or this reload "
            "would have been refused; one that names it will be refused until "
            "the plugin is back",
            gate_type,
        )


def _load_file(path: Path) -> int:
    """Import one plugin file and register its gate classes. Returns the count."""
    module = _import_module(path)
    if module is None:
        return 0

    count = 0
    for name, obj in inspect.getmembers(module, inspect.isclass):
        # Only classes *defined in this file* — skip imported symbols such as
        # the CronGate base itself or any built-in gate the plugin imported.
        if obj.__module__ != module.__name__:
            continue
        if not issubclass(obj, CronGate) or obj is CronGate:
            continue
        gate_type = getattr(obj, "type", "") or ""
        if not gate_type:
            logger.warning(
                "Cron gate plugin %s: class %s has an empty 'type'; skipping",
                path.name, name,
            )
            continue
        if inspect.isabstract(obj):
            # A typed but still-abstract gate imports cleanly, yet raises
            # TypeError when instantiated (in from_config). build_gates only
            # catches GateConfigError, so that TypeError would propagate out of
            # CronJob construction and crash daemon startup. Refuse it here,
            # where the failure is contained, instead.
            logger.warning(
                "Cron gate plugin %s: gate %s (type %r) is abstract — "
                "missing %s; skipping",
                path.name, name, gate_type,
                ", ".join(sorted(obj.__abstractmethods__)),
            )
            continue
        if _register(gate_type, obj, path):
            count += 1
    return count


def _import_module(path: Path):
    """Load a ``.py`` file as an isolated module. Returns ``None`` on any error.

    The module is intentionally not inserted into ``sys.modules`` — it is a
    throwaway namespace whose only purpose is to surface the gate classes it
    defines, so it never pollutes the global module table.
    """
    # Derived from the shared constant, never spelled out again: the prefix is
    # the only link between a module loaded here and the registry entries
    # ``_unregister_plugin_gates`` is allowed to drop, so two copies drifting
    # apart would silently turn hot-reload into "plugin gates never unregister".
    mod_name = f"{_PLUGIN_MODULE_PREFIX}{path.stem}"
    try:
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:
            logger.warning(
                "Cron gate plugin %s: could not create an import spec; skipping",
                path.name,
            )
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (Exception, SystemExit) as e:  # noqa: BLE001
        # A bad plugin must never crash startup. SystemExit (e.g. a stray
        # sys.exit() at import) subclasses BaseException, not Exception, so it
        # is caught explicitly. KeyboardInterrupt is deliberately NOT caught,
        # so an operator can still Ctrl-C a hung import.
        logger.warning("Cron gate plugin %s failed to load: %s", path.name, e)
        return None


def _register(gate_type: str, cls: type[CronGate], path: Path) -> bool:
    """Register *cls* under *gate_type* unless it collides. Returns True if added.

    A collision keeps the incumbent (a built-in, or the first plugin loaded by
    filename order) and warns. Re-importing the *same* plugin class — a fresh
    object with an identical module+qualname, e.g. if the loader is ever run a
    second time in one process — is an idempotent no-op rather than a (noisy,
    spurious) collision, since each import produces a brand-new class object.
    """
    existing = GATE_REGISTRY.get(gate_type)
    if existing is not None:
        is_same_plugin_class = (
            existing.__module__ == cls.__module__
            and existing.__qualname__ == cls.__qualname__
        )
        if not is_same_plugin_class:
            logger.warning(
                "Cron gate plugin %s: type %r already registered by %s; "
                "keeping the existing gate and skipping the plugin",
                path.name, gate_type, existing.__module__,
            )
        return False
    GATE_REGISTRY[gate_type] = cls
    logger.info(
        "Registered custom cron gate %r (%s) from %s",
        gate_type, cls.__name__, path.name,
    )
    return True
