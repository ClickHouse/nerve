"""Descendant containment + reaping for Codex **workflow** runs.

Why this exists
---------------
A Codex workflow run spawns ``codex app-server``, which spawns Ultracode
workers, which spawn ``codex-linux-sandbox`` processes. The sandbox
``setsid``\\ s into its own session/process-group and may double-fork, so a
descendant ends up in a process group that is *not* the app-server's.
:func:`CodexAppServerClient._signal_process_tree` signals a single process
group (``os.killpg(proc.pid, sig)``); it structurally cannot reach those
escaped descendants. When a run completes, is cancelled, hits its budget, or
the daemon crashes, the escaped processes survive as orphans (reparented to
PID 1). This module contains a run's whole descendant tree in a dedicated
**cgroup v2 scope** so a single ``systemctl --user stop`` reaps all of it —
setsid/double-fork and all — and records a durable, validated receipt so the
cleanup is repeatable and survives a daemon restart.

Scope of this first implementation (deliberately bounded)
---------------------------------------------------------
* **Codex workflow runs only.** Interactive/cron Codex sessions and Claude
  runs never engage this module.
* **Opt-in, three modes** (``codex.lifecycle.mode``):

  - ``disabled`` (default): this module is inert. Nothing changes.
  - ``observe``: **UNENFORCED.** The launch is *not* wrapped (today's
    behaviour is preserved) — a durable record is written for staged-rollout
    visibility, and teardown writes an explicitly *unenforced* observation.
    It never claims a cleanup it did not perform.
  - ``strict``: the app-server is launched **inside a cgroup scope created
    before the command execs** (no late ``cgroup.procs`` migration, no
    threaded ``preexec_fn``). If containment cannot be established the launch
    **fails before the command executes** — there is no silent downgrade to
    ``killpg``/subreaper.

* **Not a security boundary.** cgroup inheritance contains *trusted* tool
  processes that fork/setsid/double-fork. A cooperating or malicious same-UID
  process can still migrate out via the user manager or a writable cgroup, or
  tamper with receipts. Obtaining that stronger property is explicitly out of
  scope; no privilege is broadened to get it.

Honest contracts (no false total-containment)
----------------------------------------------
* Graceful terminal reap = bounded ``TERM`` → grace → ``KILL`` via
  ``systemctl --user stop`` (``KillMode=control-group``, ``SendSIGKILL=yes``,
  bounded ``TimeoutStopSec``), then emptiness is **verified** (the scope's
  recursive ``cgroup.events`` ``populated=0``, or the scope cgroup gone after a
  *successful* stop). A stop request is not itself proof of exit.
* A daemon ``SIGKILL`` leaves contained children alive until restart
  reconciliation — no in-process ``finally`` survives ``SIGKILL``.
* A *missing* scope is only success when we successfully stopped it (or the
  host rebooted, which no PID survives). After an access error, a unit
  replacement, or an unproven recovery it stays ``pending_retry``.

The durable store is per-run ``<runs_dir>/<run_id>/lifecycle.json`` (atomic
write + file & directory fsync). No raw argv, credentials, or environment ever
enter a record or an error string — only unit name, systemd ``InvocationID``,
resolved cgroup path, and identity metadata.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

# -- modes ----------------------------------------------------------------- #

MODE_DISABLED = "disabled"
MODE_OBSERVE = "observe"
MODE_STRICT = "strict"
VALID_MODES = (MODE_DISABLED, MODE_OBSERVE, MODE_STRICT)

# -- durable store filenames ----------------------------------------------- #

RECORD_NAME = "lifecycle.json"
LOCK_NAME = "lifecycle.lock"
RECONCILE_LOCK_NAME = ".lifecycle-reconcile.lock"

# Fixed, argv-independent scope description. systemd-run derives a description
# from the command line by default (a leak risk — argv could carry config
# values); we always pass this literal instead.
SCOPE_DESCRIPTION = "Nerve Codex workflow run containment"

# Record schema version (bump on incompatible shape changes).
RECORD_VERSION = 1

# systemd unit name charset (a conservative subset of what systemd accepts).
_UNIT_RE = re.compile(r"^nerve-wf-[A-Za-z0-9:_.-]+\.scope$")
# A resolved delegated user scope always lives under the user slice.
_CGROUP_ROOT = "/sys/fs/cgroup"
_USER_SLICE_PREFIX = "/user.slice/"

# Terminal receipt outcomes (cleanup is finished, do not retry).
_TERMINAL_OUTCOMES = frozenset({
    "complete",          # we stopped the scope and verified emptiness
    "resolved_absent",   # same-boot: the scope no longer exists, no signal sent
    "resolved_prior_boot",  # cross-boot: no process could have survived a reboot
    "failed_replaced",   # the unit was replaced by a foreign instance; refused
    "unenforced_observe",   # observe mode: no containment was ever established
    "no_scope",          # nothing was ever contained (disabled / non-strict)
})
# Outcomes that still owe a retry.
_RETRY_OUTCOMES = frozenset({"pending_retry"})


class LifecycleError(Exception):
    """A containment/reaping operation failed."""


class ContainmentUnavailable(LifecycleError):
    """Strict mode requested but the host cannot establish containment.

    Raised *before* the command is launched, so nothing runs un-contained.
    """


# -- host capability + instance identity ----------------------------------- #


@dataclass(frozen=True)
class Capabilities:
    ok: bool
    systemd_run: bool
    systemctl: bool
    user_manager: bool
    cgroup_v2: bool
    reason: str


_caps_cache: Capabilities | None = None


def detect_capabilities(*, refresh: bool = False) -> Capabilities:
    """Probe whether a per-run delegated user scope can be created here.

    Read-only and cached per process (the host does not change under us). A
    strict launch consults this and fails before exec when ``ok`` is False.
    """
    global _caps_cache
    if _caps_cache is not None and not refresh:
        return _caps_cache

    systemd_run = shutil.which("systemd-run") is not None
    systemctl = shutil.which("systemctl") is not None
    cgroup_v2 = Path(_CGROUP_ROOT, "cgroup.controllers").exists()
    user_manager = False
    if systemctl:
        try:
            proc = subprocess.run(
                ["systemctl", "--user", "show-environment"],
                capture_output=True, timeout=5, check=False,
            )
            user_manager = proc.returncode == 0
        except (OSError, subprocess.SubprocessError):
            user_manager = False

    reasons = []
    if not systemd_run:
        reasons.append("systemd-run not found")
    if not systemctl:
        reasons.append("systemctl not found")
    if not cgroup_v2:
        reasons.append("cgroup v2 unified hierarchy not mounted")
    if not user_manager:
        reasons.append("systemd --user manager not reachable")
    caps = Capabilities(
        ok=not reasons,
        systemd_run=systemd_run,
        systemctl=systemctl,
        user_manager=user_manager,
        cgroup_v2=cgroup_v2,
        reason="; ".join(reasons) or "ok",
    )
    _caps_cache = caps
    return caps


@dataclass(frozen=True)
class Instance:
    """Identity of the running daemon process, for concurrency fencing.

    ``generation`` changes across daemon restarts (pid + start_ticks differ)
    and across reboots (boot_id differs), so a record written by a previous
    daemon incarnation is always distinguishable from one this process owns.
    """

    boot_id: str
    pid: int
    start_ticks: int

    @property
    def generation(self) -> str:
        return f"{self.boot_id}:{self.pid}:{self.start_ticks}"


def read_boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return ""


def _proc_start_ticks(pid: int) -> int:
    """Field 22 of ``/proc/<pid>/stat`` (jiffies since boot at process start).

    ``comm`` (field 2) may contain spaces and parentheses, so split after the
    final ``)``.
    """
    try:
        raw = Path(f"/proc/{pid}/stat").read_bytes()
    except OSError:
        return 0
    try:
        after = raw.rsplit(b")", 1)[1].split()
        # after[0] is field 3 (state); field 22 is index 19 here.
        return int(after[19])
    except (IndexError, ValueError):
        return 0


def current_instance() -> Instance:
    pid = os.getpid()
    return Instance(boot_id=read_boot_id(), pid=pid, start_ticks=_proc_start_ticks(pid))


# -- durable record --------------------------------------------------------- #


@dataclass
class Receipt:
    """The outcome of a reap/reconcile attempt."""

    outcome: str
    enforced: bool = False
    signalled: bool = False
    verified_empty: bool = False
    method: str = ""
    attempts: int = 0
    error: str = ""
    ts: str = ""

    def is_terminal(self) -> bool:
        """No further reap attempt is owed for this outcome."""
        return self.outcome in _TERMINAL_OUTCOMES

    def is_complete(self) -> bool:
        """True only when no live owned descendant remains (or none could).

        Deliberately excludes ``unenforced_observe`` (observe mode never
        contained anything, so it makes no completion claim) and
        ``failed_replaced`` (a foreign unit we refused to touch — escalate,
        do not claim the tree was reaped)."""
        return self.outcome in {
            "complete", "resolved_absent", "resolved_prior_boot", "no_scope",
        }

    def needs_retry(self) -> bool:
        return self.outcome in _RETRY_OUTCOMES

    def needs_attention(self) -> bool:
        """A terminal outcome that resolved neither to complete nor retry —
        surfaced for a human/operator rather than silently accepted."""
        return self.outcome == "failed_replaced"


@dataclass
class LifecycleRecord:
    run_id: str
    session_id: str
    mode: str
    attempt_nonce: str
    unit: str
    workspace_id: str
    boot_id: str
    instance_generation: str
    phase: str = "intent"           # intent -> launched -> registered -> terminal
    owner_kind: str = "workflow"
    invocation_id: str = ""         # systemd InvocationID, recorded at 'launched'
    control_group: str = ""         # resolved cgroup path, recorded at 'launched'
    version: int = RECORD_VERSION
    created_at: str = ""
    updated_at: str = ""
    receipt: dict | None = None

    # -- serialization ------------------------------------------------- #

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LifecycleRecord":
        if not isinstance(d, dict):
            raise LifecycleError("lifecycle record is not an object")
        try:
            rec = cls(
                run_id=str(d["run_id"]),
                session_id=str(d["session_id"]),
                mode=str(d["mode"]),
                attempt_nonce=str(d["attempt_nonce"]),
                unit=str(d["unit"]),
                workspace_id=str(d.get("workspace_id", "")),
                boot_id=str(d.get("boot_id", "")),
                instance_generation=str(d.get("instance_generation", "")),
                phase=str(d.get("phase", "intent")),
                owner_kind=str(d.get("owner_kind", "workflow")),
                invocation_id=str(d.get("invocation_id", "")),
                control_group=str(d.get("control_group", "")),
                version=int(d.get("version", RECORD_VERSION)),
                created_at=str(d.get("created_at", "")),
                updated_at=str(d.get("updated_at", "")),
                receipt=d.get("receipt") if isinstance(d.get("receipt"), dict) else None,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise LifecycleError(f"malformed lifecycle record: {e}") from e
        rec.validate()
        return rec

    def validate(self) -> None:
        """Reject anything that could steer a signal at a foreign target.

        Never trusts a cgroup path that isn't a leaf under the user slice
        matching this record's unit, and never accepts an out-of-charset unit.
        """
        if self.owner_kind != "workflow":
            raise LifecycleError(f"unexpected owner_kind {self.owner_kind!r}")
        if self.mode not in VALID_MODES:
            raise LifecycleError(f"invalid mode {self.mode!r}")
        if not _UNIT_RE.match(self.unit):
            raise LifecycleError(f"unsafe unit name {self.unit!r}")
        if not self.attempt_nonce:
            raise LifecycleError("empty attempt_nonce")
        if self.control_group:
            _validate_cgroup_path(self.control_group, self.unit)


def _validate_cgroup_path(control_group: str, unit: str) -> None:
    """A resolved scope cgroup must be a leaf under the user slice named for
    its unit — never a root/parent, never a symlink escape, never ``..``."""
    if not control_group.startswith(_USER_SLICE_PREFIX):
        raise LifecycleError(f"cgroup path not under user slice: {control_group!r}")
    if ".." in control_group.split("/"):
        raise LifecycleError(f"cgroup path traverses parent: {control_group!r}")
    if control_group.rstrip("/").split("/")[-1] != unit:
        raise LifecycleError(
            f"cgroup leaf {control_group!r} does not match unit {unit!r}"
        )


# -- atomic durable IO ------------------------------------------------------ #


def _atomic_write_json(path: Path, obj: dict) -> None:
    """Write ``obj`` as JSON atomically with file + directory fsync.

    Raises :class:`OSError` on any failure — a caller that cannot persist
    launch identity must not launch, and one that cannot persist a completion
    receipt must not report completion.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    data = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    # fsync the directory so the rename is durable across a crash.
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def record_path(run_dir: Path) -> Path:
    return Path(run_dir) / RECORD_NAME


def read_record(run_dir: Path) -> LifecycleRecord | None:
    path = record_path(run_dir)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as e:
        raise LifecycleError(f"cannot read lifecycle record: {e}") from e
    try:
        return LifecycleRecord.from_dict(json.loads(raw))
    except ValueError as e:
        raise LifecycleError(f"corrupt lifecycle record JSON: {e}") from e


def write_record(run_dir: Path, record: LifecycleRecord) -> None:
    record.validate()
    record.updated_at = _now()
    if not record.created_at:
        record.created_at = record.updated_at
    _atomic_write_json(record_path(run_dir), record.to_dict())


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@contextlib.contextmanager
def _flock(lock_path: Path):
    """Exclusive advisory lock; serialises reap/retry/reconcile per owner."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# -- unit naming + launch wrapper ------------------------------------------- #


def scope_unit_name(run_id: str, nonce: str) -> str:
    """Unique, charset-safe scope unit name embedding the attempt nonce.

    The nonce makes the name globally unique per attempt, so a name match is a
    strong identity signal even in the tiny window before an InvocationID has
    been recorded (the create/register gap).
    """
    safe_run = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[:80]
    safe_nonce = re.sub(r"[^A-Za-z0-9]", "", nonce)[:16]
    return f"nerve-wf-{safe_run}-{safe_nonce}.scope"


def build_scope_argv(
    unit: str, inner_argv: list[str], *, term_grace_seconds: int,
) -> list[str]:
    """Wrap ``inner_argv`` so it runs inside a delegated transient user scope.

    Containment is established by systemd *before* the command execs: no late
    ``cgroup.procs`` migration and no threaded-Python ``preexec_fn``. The whole
    fork/setsid/double-fork subtree stays in this scope's cgroup.
    """
    if not _UNIT_RE.match(unit):
        raise LifecycleError(f"refusing to launch with unsafe unit {unit!r}")
    grace = max(1, int(term_grace_seconds))
    return [
        "systemd-run", "--user", "--scope", "--quiet", "--collect",
        "--expand-environment=no",
        "-p", "Delegate=yes",
        "-p", "KillMode=control-group",
        "-p", "SendSIGKILL=yes",
        "-p", f"TimeoutStopSec={grace}",
        "--description", SCOPE_DESCRIPTION,
        "--unit", unit,
        "--", *inner_argv,
    ]


# -- systemd / cgroup queries ----------------------------------------------- #


def _systemctl_show(unit: str, props: Iterable[str]) -> dict[str, str]:
    """``systemctl --user show`` a unit; parse ``KEY=VALUE`` lines.

    Returns an empty dict if systemctl is unavailable or the call fails.
    """
    prop_arg = ",".join(props)
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", unit, "--property", prop_arg],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.debug("systemctl show failed for %s: %s", unit, e)
        return {}
    out: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            out[key.strip()] = value.strip()
    return out


def _unit_identity(unit: str) -> dict[str, str]:
    return _systemctl_show(
        unit, ("LoadState", "ActiveState", "InvocationID", "ControlGroup", "Description"),
    )


def _await_unit_identity(
    unit: str, *, timeout: float = 4.0,
) -> tuple[str, str]:
    """Poll until the scope's InvocationID + ControlGroup are populated.

    Returns ``(invocation_id, control_group)`` — both empty on timeout.
    """
    deadline = time.monotonic() + timeout
    inv = cg = ""
    while time.monotonic() < deadline:
        ident = _unit_identity(unit)
        inv = ident.get("InvocationID", "")
        cg = ident.get("ControlGroup", "")
        if inv and cg:
            return inv, cg
        time.sleep(0.1)
    return inv, cg


def _cgroup_events(control_group: str) -> tuple[str, bool | None]:
    """Inspect a scope cgroup's ``cgroup.events`` ``populated`` state.

    ``populated`` is recursive per the kernel (set if *any* descendant cgroup
    holds a process), so it is the correct emptiness signal even when the
    delegated subtree created child cgroups.

    Returns ``(state, populated)`` where ``state`` is one of ``"gone"`` (the
    scope cgroup directory no longer exists) or ``"present"``. ``populated`` is
    None when the directory is gone or unreadable.
    """
    if not control_group:
        return "gone", None
    path = Path(_CGROUP_ROOT + control_group) / "cgroup.events"
    try:
        text = path.read_text()
    except FileNotFoundError:
        return "gone", None
    except OSError as e:
        logger.debug("cgroup.events unreadable for %s: %s", control_group, e)
        return "present", None
    populated = None
    for line in text.splitlines():
        if line.startswith("populated"):
            populated = line.split()[-1].strip() == "1"
            break
    return "present", populated


def _scope_stop(unit: str, *, timeout: float) -> tuple[bool, str]:
    """``systemctl --user stop`` a scope, bounded by ``timeout`` seconds.

    ``stop`` performs the configured ``KillMode=control-group`` TERM → grace
    (``TimeoutStopSec``) → SIGKILL sequence over the whole cgroup. Returns
    ``(ok, error)``.
    """
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "stop", unit],
            capture_output=True, text=True, timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "systemctl stop timed out"
    except (OSError, subprocess.SubprocessError) as e:
        return False, f"systemctl stop failed: {e}"
    if proc.returncode != 0:
        return False, (proc.stderr or f"systemctl stop rc={proc.returncode}").strip()[:200]
    return True, ""


def proc_cgroup(pid: int) -> str | None:
    """The cgroup v2 path of ``pid`` (``0::PATH`` line), or None."""
    try:
        text = Path(f"/proc/{pid}/cgroup").read_text()
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("0::"):
            return line[3:].strip()
    return None


def verify_membership(control_group: str, pid: int) -> bool:
    """True iff ``pid`` is contained in ``control_group`` (or a descendant)
    and the scope is populated — a concrete membership proof at handshake."""
    if not control_group:
        return False
    member = proc_cgroup(pid)
    if member is None:
        return False
    if member != control_group and not member.startswith(control_group.rstrip("/") + "/"):
        return False
    _state, populated = _cgroup_events(control_group)
    return populated is not False


# -- launch-side operations (used by the app-server client) ----------------- #


def persist_intent(
    run_dir: Path, *, run_id: str, session_id: str, mode: str, workspace_id: str,
) -> LifecycleRecord:
    """Write the validated owner intent BEFORE anything can fork the command.

    Raises :class:`OSError`/:class:`LifecycleError` on a persistence failure —
    a strict launch must not proceed if its identity cannot be recorded.
    """
    nonce = uuid.uuid4().hex
    unit = scope_unit_name(run_id, nonce)
    inst = current_instance()
    record = LifecycleRecord(
        run_id=run_id,
        session_id=session_id,
        mode=mode,
        attempt_nonce=nonce,
        unit=unit,
        workspace_id=workspace_id,
        boot_id=inst.boot_id,
        instance_generation=inst.generation,
        phase="intent",
    )
    write_record(Path(run_dir), record)
    return record


def record_launched(run_dir: Path, record: LifecycleRecord) -> LifecycleRecord:
    """After systemd-run started the scope, record its immutable identity.

    Reads the scope's systemd ``InvocationID`` and resolved ``ControlGroup``
    (the two together are the reuse-proof instance identity). Raises
    :class:`LifecycleError` if they cannot be resolved — in strict mode the
    caller then closes and fails the launch (the scope, already created, is
    still reapable from the persisted intent).
    """
    inv, cg = _await_unit_identity(record.unit)
    if not inv or not cg:
        raise LifecycleError(
            f"scope {record.unit} did not report an InvocationID/ControlGroup"
        )
    record.invocation_id = inv
    record.control_group = cg
    record.phase = "launched"
    write_record(Path(run_dir), record)
    return record


def record_registered(run_dir: Path, record: LifecycleRecord) -> LifecycleRecord:
    record.phase = "registered"
    write_record(Path(run_dir), record)
    return record


# -- reap + reconcile ------------------------------------------------------- #


def reap(
    run_dir: Path, *, term_grace_seconds: int = 5, stop_timeout_seconds: int = 20,
) -> Receipt:
    """Authoritatively reap a terminal run's contained descendants.

    Independent of any in-memory client: it works purely from the durable
    record. Idempotent — a record already carrying a terminal receipt is
    returned unchanged; a ``pending_retry`` record is re-attempted. Serialised
    per owner by a file lock so concurrent terminal/retry paths cannot race.
    """
    run_dir = Path(run_dir)
    # Cheap pre-check so run dirs without a lifecycle record (Claude runs,
    # disabled mode) are never even given a lock file. The authoritative read
    # happens under the lock below.
    if not record_path(run_dir).exists():
        return Receipt(outcome="no_scope", ts=_now())
    with _flock(run_dir / LOCK_NAME):
        try:
            record = read_record(run_dir)
        except LifecycleError as e:
            # A corrupt/unreadable record cannot be validated, so it cannot be
            # signalled from and cannot be safely rewritten — escalate
            # conservatively (never claim completion). R4.
            logger.warning("refusing to reap from corrupt record in %s: %s",
                           run_dir, e)
            return Receipt(
                outcome="pending_retry", method="corrupt-record",
                error="corrupt lifecycle record; manual attention required",
                ts=_now(),
            )
        if record is None:
            return Receipt(outcome="no_scope", ts=_now())
        if record.mode == MODE_OBSERVE:
            return _write_receipt(run_dir, record, Receipt(
                outcome="unenforced_observe", enforced=False, signalled=False,
                method="observe", ts=_now(),
            ))
        if record.mode != MODE_STRICT:
            return _write_receipt(run_dir, record, Receipt(
                outcome="no_scope", enforced=False, ts=_now(),
            ))
        existing = _receipt_from(record)
        if existing is not None and existing.is_terminal():
            return existing
        receipt = _reap_strict(
            record,
            term_grace_seconds=term_grace_seconds,
            stop_timeout_seconds=stop_timeout_seconds,
            prev=existing,
        )
        return _write_receipt(run_dir, record, receipt)


def _reap_strict(
    record: LifecycleRecord, *, term_grace_seconds: int,
    stop_timeout_seconds: int, prev: Receipt | None,
) -> Receipt:
    attempts = (prev.attempts if prev else 0) + 1
    current_boot = read_boot_id()

    # Cross-boot: nothing from a previous boot can still be running, and the
    # unit name may now belong to a foreign process this boot — never signal.
    if record.boot_id and current_boot and record.boot_id != current_boot:
        return Receipt(
            outcome="resolved_prior_boot", enforced=True, signalled=False,
            verified_empty=True, method="reboot", attempts=attempts, ts=_now(),
        )

    ident = _unit_identity(record.unit)
    load_state = ident.get("LoadState", "")
    active_state = ident.get("ActiveState", "")
    live_inv = ident.get("InvocationID", "")
    live_cg = ident.get("ControlGroup", "") or record.control_group
    description = ident.get("Description", "")

    unit_present = load_state not in ("", "not-found")

    if not unit_present:
        # The scope no longer exists. Same boot, so no reboot-cleanup claim —
        # but a transient scope's cgroup is removed only after it is emptied,
        # so absence means the contained tree is gone. No signal was sent.
        return Receipt(
            outcome="resolved_absent", enforced=True, signalled=False,
            verified_empty=True, method="absent", attempts=attempts, ts=_now(),
        )

    # Identity gate: refuse to signal anything that is not provably our scope.
    if record.invocation_id:
        if live_inv and live_inv != record.invocation_id:
            return Receipt(
                outcome="failed_replaced", enforced=True, signalled=False,
                method="identity", attempts=attempts, ts=_now(),
                error="unit InvocationID differs from record (replaced by a "
                      "foreign instance); refusing to signal",
            )
    else:
        # Create/register gap: no InvocationID was ever recorded. The unit name
        # embeds our unique nonce, so a same-name unit is ours — but demand the
        # fixed description too before signalling.
        if description and description != SCOPE_DESCRIPTION:
            return Receipt(
                outcome="failed_replaced", enforced=True, signalled=False,
                method="identity", attempts=attempts, ts=_now(),
                error="unit description does not match; refusing to signal",
            )

    if live_cg:
        with contextlib.suppress(LifecycleError):
            _validate_cgroup_path(live_cg, record.unit)

    ok, err = _scope_stop(record.unit, timeout=stop_timeout_seconds)
    if not ok:
        return Receipt(
            outcome="pending_retry", enforced=True, signalled=True,
            method="systemctl-stop", attempts=attempts, ts=_now(),
            error=err or "stop failed", verified_empty=False,
        )

    # A successful stop is a request, not proof of exit — verify emptiness.
    state, populated = _cgroup_events(live_cg)
    if state == "gone" or populated is False:
        return Receipt(
            outcome="complete", enforced=True, signalled=True,
            verified_empty=True, method="systemctl-stop",
            attempts=attempts, ts=_now(),
        )
    return Receipt(
        outcome="pending_retry", enforced=True, signalled=True,
        method="systemctl-stop", attempts=attempts, ts=_now(),
        error="scope still populated after stop", verified_empty=False,
    )


def reconcile_run(
    run_dir: Path, *, is_terminal: bool, active_generation: str,
    term_grace_seconds: int = 5, stop_timeout_seconds: int = 20,
) -> Receipt | None:
    """Reconcile one run's containment at startup / on demand.

    Cleans only a **terminal** run (or a **provably abandoned** launch: a
    record still at ``intent``/``launched`` whose launching daemon generation
    is not the current one). A run owned by the *current* generation, or a
    still-active run, is left alone. Returns the receipt, or None when nothing
    was due.
    """
    run_dir = Path(run_dir)
    record = read_record(run_dir)
    if record is None:
        return None
    if record.mode != MODE_STRICT:
        return None
    # Never touch a run this very daemon generation still owns.
    if record.instance_generation == active_generation:
        return None
    abandoned_launch = record.phase in ("intent", "launched", "registered")
    if not (is_terminal or abandoned_launch):
        return None
    return reap(
        run_dir,
        term_grace_seconds=term_grace_seconds,
        stop_timeout_seconds=stop_timeout_seconds,
    )


def list_records(runs_dir: Path) -> list[tuple[str, LifecycleRecord]]:
    """Every run dir under ``runs_dir`` that carries a lifecycle record.

    A corrupt record is surfaced as a synthetic ``pending_retry`` sentinel via
    the exception path in callers; here we skip unreadable ones and log, so a
    single bad file cannot abort a whole reconciliation sweep.
    """
    out: list[tuple[str, LifecycleRecord]] = []
    root = Path(runs_dir)
    if not root.is_dir():
        return out
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        try:
            record = read_record(child)
        except LifecycleError:
            logger.warning("skipping corrupt lifecycle record in %s", child.name)
            continue
        if record is not None:
            out.append((child.name, record))
    return out


def reconcile_all(
    runs_dir: Path, is_terminal: Callable[[str], bool], *,
    term_grace_seconds: int = 5, stop_timeout_seconds: int = 20,
) -> list[tuple[str, Receipt]]:
    """Sweep every run with a record, reaping terminal/abandoned containment.

    Holds a global reconcile lock so only one sweep runs at a time. ``is_terminal``
    maps a run id to whether its business status is terminal.
    """
    root = Path(runs_dir)
    results: list[tuple[str, Receipt]] = []
    gen = current_instance().generation
    with _flock(root / RECONCILE_LOCK_NAME):
        for run_id, _record in list_records(root):
            try:
                receipt = reconcile_run(
                    root / run_id,
                    is_terminal=is_terminal(run_id),
                    active_generation=gen,
                    term_grace_seconds=term_grace_seconds,
                    stop_timeout_seconds=stop_timeout_seconds,
                )
            except LifecycleError as e:
                logger.warning("reconcile failed for %s: %s", run_id, e)
                continue
            if receipt is not None:
                results.append((run_id, receipt))
    return results


def retry(
    run_dir: Path, *, term_grace_seconds: int = 5, stop_timeout_seconds: int = 20,
) -> Receipt:
    """The one supported, narrowly-scoped retry entry.

    Re-attempts a ``pending_retry`` (or not-yet-run) reap. Idempotent: a record
    already carrying a terminal receipt is returned unchanged. Never fabricates
    success — a still-failing stop stays ``pending_retry`` with the error.
    """
    return reap(
        run_dir,
        term_grace_seconds=term_grace_seconds,
        stop_timeout_seconds=stop_timeout_seconds,
    )


# -- launch config handed to the app-server client -------------------------- #


@dataclass(frozen=True)
class WorkflowContainment:
    """Immutable per-launch containment config for one workflow-run client.

    Built by the Codex backend only for a ``workflow:``-prefixed session when
    ``codex.lifecycle.mode`` is ``observe``/``strict``; ``None`` (and therefore
    today's behaviour) for every other session and for ``disabled`` mode.
    """

    mode: str
    run_dir: Path
    run_id: str
    session_id: str
    workspace_id: str
    term_grace_seconds: int = 5
    stop_timeout_seconds: int = 20

    @property
    def enabled(self) -> bool:
        return self.mode in (MODE_OBSERVE, MODE_STRICT)


def prepare_launch(
    containment: WorkflowContainment, inner_argv: list[str],
) -> tuple[list[str], LifecycleRecord]:
    """Persist intent and return the argv to actually spawn.

    * strict: validate host capability (raise :class:`ContainmentUnavailable`
      *before* anything execs), persist intent, return the scope-wrapped argv.
    * observe: persist an unenforced-mode intent, return ``inner_argv``
      unchanged (today's behaviour preserved).

    Raises before the command can execute if capability or persistence fails,
    so a strict run never launches un-contained.
    """
    if containment.mode == MODE_STRICT:
        caps = detect_capabilities()
        if not caps.ok:
            raise ContainmentUnavailable(
                f"strict cgroup containment unavailable: {caps.reason}"
            )
    record = persist_intent(
        containment.run_dir,
        run_id=containment.run_id,
        session_id=containment.session_id,
        mode=containment.mode,
        workspace_id=containment.workspace_id,
    )
    if containment.mode == MODE_STRICT:
        wrapped = build_scope_argv(
            record.unit, inner_argv,
            term_grace_seconds=containment.term_grace_seconds,
        )
        return wrapped, record
    return list(inner_argv), record


# -- receipt helpers -------------------------------------------------------- #


def _receipt_from(record: LifecycleRecord) -> Receipt | None:
    if not record.receipt:
        return None
    try:
        return Receipt(**{k: v for k, v in record.receipt.items() if k in Receipt.__dataclass_fields__})
    except TypeError:
        return None


def _write_receipt(
    run_dir: Path, record: LifecycleRecord, receipt: Receipt,
) -> Receipt:
    record.receipt = asdict(receipt)
    if receipt.is_terminal():
        record.phase = "terminal"
    write_record(Path(run_dir), record)
    return receipt


__all__ = [
    "MODE_DISABLED", "MODE_OBSERVE", "MODE_STRICT", "VALID_MODES",
    "Capabilities", "detect_capabilities", "Instance", "current_instance",
    "read_boot_id", "LifecycleError", "ContainmentUnavailable",
    "LifecycleRecord", "Receipt", "record_path", "read_record", "write_record",
    "scope_unit_name", "build_scope_argv", "verify_membership", "proc_cgroup",
    "persist_intent", "record_launched", "record_registered",
    "reap", "reconcile_run", "reconcile_all", "list_records", "retry",
    "SCOPE_DESCRIPTION", "WorkflowContainment", "prepare_launch",
]
