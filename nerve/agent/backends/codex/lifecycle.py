"""Cgroup containment + reaping for Codex workflow-run descendants.

A Codex workflow run's ``codex-linux-sandbox`` descendants ``setsid`` into their
own process groups, so the app-server teardown's single ``killpg`` cannot reach
them and they survive as orphans. When ``codex.lifecycle.mode`` is ``strict``,
a workflow run's app-server is launched inside a delegated systemd user scope so
the whole descendant tree lives in one cgroup and a single ``systemctl --user
stop`` reaps it; a durable per-run record lets the reap run at terminal or from
startup reconciliation after a crash. Default ``disabled`` leaves launch
unchanged. Requires Linux + a reachable systemd ``--user`` manager + cgroup v2;
strict fails before exec where that is absent (it never launches un-contained).
This contains trusted tool processes; it is not a security sandbox.
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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

MODE_DISABLED = "disabled"
MODE_STRICT = "strict"
VALID_MODES = (MODE_DISABLED, MODE_STRICT)

RECORD_NAME = "lifecycle.json"
LOCK_NAME = "lifecycle.lock"
RECONCILE_LOCK_NAME = ".lifecycle-reconcile.lock"
SCOPE_DESCRIPTION = "Nerve Codex workflow run containment"

# systemd-run enforces TERM, then SIGKILL after this grace, over the cgroup.
TERM_GRACE_SECONDS = 5
STOP_TIMEOUT_SECONDS = 20

_UNIT_RE = re.compile(r"^nerve-wf-[A-Za-z0-9:_.-]+\.scope$")
_CGROUP_ROOT = "/sys/fs/cgroup"
_USER_SLICE_PREFIX = "/user.slice/"

# Receipt outcomes. `complete` and `refused` are terminal (never retried);
# `pending_retry` is re-attempted by the next startup reconciliation.
_TERMINAL_OUTCOMES = frozenset({"complete", "refused", "no_scope"})


class LifecycleError(Exception):
    """A containment/reap operation failed."""


class ContainmentUnavailable(LifecycleError):
    """Strict mode requested but the host cannot create a scope (raised before exec)."""


# -- capabilities + instance identity --------------------------------------- #


@dataclass(frozen=True)
class Capabilities:
    ok: bool
    reason: str


_caps_cache: Capabilities | None = None


def detect_capabilities(*, refresh: bool = False) -> Capabilities:
    """Whether a delegated user scope can be created here. Cached per process."""
    global _caps_cache
    if _caps_cache is not None and not refresh:
        return _caps_cache
    reasons = []
    if shutil.which("systemd-run") is None:
        reasons.append("systemd-run not found")
    if shutil.which("systemctl") is None:
        reasons.append("systemctl not found")
    if not Path(_CGROUP_ROOT, "cgroup.controllers").exists():
        reasons.append("cgroup v2 not mounted")
    else:
        try:
            proc = subprocess.run(
                ["systemctl", "--user", "show-environment"],
                capture_output=True, timeout=5, check=False,
            )
            if proc.returncode != 0:
                reasons.append("systemd --user manager not reachable")
        except (OSError, subprocess.SubprocessError):
            reasons.append("systemd --user manager not reachable")
    _caps_cache = Capabilities(ok=not reasons, reason="; ".join(reasons) or "ok")
    return _caps_cache


def _boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return ""


def _proc_start_ticks(pid: int) -> int:
    """Field 22 of ``/proc/<pid>/stat`` (read after the final ``)`` — ``comm``
    may contain spaces/parens)."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_bytes()
    except OSError:
        return 0
    try:
        return int(raw.rsplit(b")", 1)[1].split()[19])
    except (IndexError, ValueError):
        return 0


def current_generation() -> str:
    """Identity of this daemon incarnation; changes on restart and reboot."""
    pid = os.getpid()
    return f"{_boot_id()}:{pid}:{_proc_start_ticks(pid)}"


# -- durable record ---------------------------------------------------------- #


@dataclass
class Receipt:
    outcome: str
    signalled: bool = False
    verified_empty: bool = False
    error: str = ""
    ts: str = ""

    def is_complete(self) -> bool:
        return self.outcome == "complete"

    def is_terminal(self) -> bool:
        return self.outcome in _TERMINAL_OUTCOMES


@dataclass
class LifecycleRecord:
    run_id: str
    session_id: str
    unit: str
    boot_id: str
    generation: str
    invocation_id: str = ""
    control_group: str = ""
    created_at: str = ""
    updated_at: str = ""
    receipt: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LifecycleRecord":
        if not isinstance(d, dict):
            raise LifecycleError("record is not an object")
        try:
            rec = cls(
                run_id=str(d["run_id"]), session_id=str(d["session_id"]),
                unit=str(d["unit"]), boot_id=str(d.get("boot_id", "")),
                generation=str(d.get("generation", "")),
                invocation_id=str(d.get("invocation_id", "")),
                control_group=str(d.get("control_group", "")),
                created_at=str(d.get("created_at", "")),
                updated_at=str(d.get("updated_at", "")),
                receipt=d.get("receipt") if isinstance(d.get("receipt"), dict) else None,
            )
        except (KeyError, TypeError) as e:
            raise LifecycleError(f"malformed record: {e}") from e
        rec.validate()
        return rec

    def validate(self) -> None:
        if not _UNIT_RE.match(self.unit):
            raise LifecycleError(f"unsafe unit name {self.unit!r}")
        if self.control_group:
            _validate_cgroup_path(self.control_group, self.unit)


def _validate_cgroup_path(control_group: str, unit: str) -> None:
    """A resolved scope cgroup must be a leaf under the user slice named for its
    unit — reject a root/parent, a ``..`` traversal, or a foreign path."""
    if not control_group.startswith(_USER_SLICE_PREFIX):
        raise LifecycleError(f"cgroup not under user slice: {control_group!r}")
    if ".." in control_group.split("/"):
        raise LifecycleError(f"cgroup traverses parent: {control_group!r}")
    if control_group.rstrip("/").split("/")[-1] != unit:
        raise LifecycleError(f"cgroup leaf != unit: {control_group!r} / {unit!r}")


def record_path(run_dir: Path) -> Path:
    return Path(run_dir) / RECORD_NAME


def read_record(run_dir: Path) -> LifecycleRecord | None:
    try:
        raw = record_path(run_dir).read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as e:
        raise LifecycleError(f"cannot read record: {e}") from e
    try:
        return LifecycleRecord.from_dict(json.loads(raw))
    except ValueError as e:
        raise LifecycleError(f"corrupt record JSON: {e}") from e


def write_record(run_dir: Path, record: LifecycleRecord) -> None:
    record.validate()
    record.updated_at = _now()
    if not record.created_at:
        record.created_at = record.updated_at
    _atomic_write_json(record_path(run_dir), record.to_dict())


def _atomic_write_json(path: Path, obj: dict) -> None:
    """Write JSON via a temp file, then rename; fsync the file and its directory
    so the record survives a crash. Raises OSError on any failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, json.dumps(obj, indent=2, sort_keys=True).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@contextlib.contextmanager
def _flock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# -- unit naming + launch wrapper -------------------------------------------- #


def scope_unit_name(run_id: str, nonce: str) -> str:
    """Unique, charset-safe scope unit name embedding the attempt nonce."""
    safe_run = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[:80]
    safe_nonce = re.sub(r"[^A-Za-z0-9]", "", nonce)[:16]
    return f"nerve-wf-{safe_run}-{safe_nonce}.scope"


def build_scope_argv(unit: str, inner_argv: list[str]) -> list[str]:
    """Wrap ``inner_argv`` to run inside a delegated transient user scope.
    systemd places the process in the scope's cgroup before it execs, so the
    whole fork/setsid/double-fork subtree stays contained."""
    if not _UNIT_RE.match(unit):
        raise LifecycleError(f"unsafe unit {unit!r}")
    return [
        "systemd-run", "--user", "--scope", "--quiet", "--collect",
        "--expand-environment=no",
        "-p", "Delegate=yes",
        "-p", "KillMode=control-group",
        "-p", "SendSIGKILL=yes",
        "-p", f"TimeoutStopSec={TERM_GRACE_SECONDS}",
        "--description", SCOPE_DESCRIPTION,
        "--unit", unit,
        "--", *inner_argv,
    ]


# -- systemd / cgroup queries ------------------------------------------------ #


def _systemctl_show(unit: str, props: Iterable[str]) -> dict[str, str]:
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", unit, "--property", ",".join(props)],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.debug("systemctl show %s failed: %s", unit, e)
        return {}
    out: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            out[key.strip()] = value.strip()
    return out


def _unit_identity(unit: str) -> dict[str, str]:
    return _systemctl_show(unit, ("LoadState", "InvocationID", "ControlGroup", "Description"))


def _await_unit_identity(unit: str, *, timeout: float = 4.0) -> tuple[str, str]:
    """Poll until the scope reports its InvocationID + ControlGroup."""
    deadline = time.monotonic() + timeout
    inv = cg = ""
    while time.monotonic() < deadline:
        ident = _unit_identity(unit)
        inv, cg = ident.get("InvocationID", ""), ident.get("ControlGroup", "")
        if inv and cg:
            return inv, cg
        time.sleep(0.1)
    return inv, cg


def _cgroup_gone_or_empty(control_group: str) -> bool | None:
    """True if the scope cgroup is gone or recursively unpopulated, False if it
    still holds a process, None if it can't be read. ``cgroup.events``
    ``populated`` is recursive per the kernel, so it covers delegated children."""
    if not control_group:
        return None
    path = Path(_CGROUP_ROOT + control_group) / "cgroup.events"
    try:
        text = path.read_text()
    except FileNotFoundError:
        return True
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("populated"):
            return line.split()[-1].strip() == "0"
    return None


def _scope_stop(unit: str) -> tuple[bool, str]:
    """``systemctl --user stop`` the scope (TERM → grace → SIGKILL over the
    control group). Returns (ok, error)."""
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "stop", unit],
            capture_output=True, text=True, timeout=STOP_TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "stop timed out"
    except (OSError, subprocess.SubprocessError) as e:
        return False, f"stop failed: {e}"
    if proc.returncode != 0:
        return False, (proc.stderr or f"rc={proc.returncode}").strip()[:200]
    return True, ""


def proc_cgroup(pid: int) -> str | None:
    try:
        text = Path(f"/proc/{pid}/cgroup").read_text()
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("0::"):
            return line[3:].strip()
    return None


def verify_membership(control_group: str, pid: int) -> bool:
    """True if ``pid`` is in ``control_group`` (or a descendant of it)."""
    if not control_group:
        return False
    member = proc_cgroup(pid)
    if member is None:
        return False
    return member == control_group or member.startswith(control_group.rstrip("/") + "/")


# -- launch config handed to the app-server client -------------------------- #


@dataclass(frozen=True)
class WorkflowContainment:
    mode: str
    run_dir: Path
    run_id: str
    session_id: str

    @property
    def enabled(self) -> bool:
        return self.mode == MODE_STRICT


def prepare_launch(
    containment: WorkflowContainment, inner_argv: list[str],
) -> tuple[list[str], LifecycleRecord]:
    """Persist the owner record, then return the scope-wrapped argv. Raises
    before anything execs if the host can't contain or the record can't be
    written, so a strict run never launches un-contained."""
    caps = detect_capabilities()
    if not caps.ok:
        raise ContainmentUnavailable(f"strict cgroup containment unavailable: {caps.reason}")
    nonce = uuid.uuid4().hex
    unit = scope_unit_name(containment.run_id, nonce)
    record = LifecycleRecord(
        run_id=containment.run_id, session_id=containment.session_id, unit=unit,
        boot_id=_boot_id(), generation=current_generation(),
    )
    write_record(containment.run_dir, record)
    return build_scope_argv(unit, inner_argv), record


def record_launched(run_dir: Path, record: LifecycleRecord) -> LifecycleRecord:
    """Record the scope's InvocationID + ControlGroup once systemd-run created
    it. Raises if they don't resolve — the caller then fails the launch."""
    inv, cg = _await_unit_identity(record.unit)
    if not inv or not cg:
        raise LifecycleError(f"scope {record.unit} reported no InvocationID/ControlGroup")
    record.invocation_id, record.control_group = inv, cg
    write_record(Path(run_dir), record)
    return record


# -- reap + reconcile -------------------------------------------------------- #


def reap(run_dir: Path) -> Receipt:
    """Reap a run's contained descendants from its durable record. Idempotent
    (a terminal receipt is returned unchanged); serialised per owner."""
    run_dir = Path(run_dir)
    if not record_path(run_dir).exists():
        return Receipt(outcome="no_scope", ts=_now())
    with _flock(run_dir / LOCK_NAME):
        try:
            record = read_record(run_dir)
        except LifecycleError as e:
            logger.warning("refusing to reap from corrupt record in %s: %s", run_dir, e)
            return Receipt(outcome="pending_retry", error="corrupt record", ts=_now())
        if record is None:
            return Receipt(outcome="no_scope", ts=_now())
        prev = _receipt_from(record)
        if prev is not None and prev.is_terminal():
            return prev
        receipt = _do_reap(record)
        record.receipt = asdict(receipt)
        write_record(run_dir, record)
        return receipt


def _do_reap(record: LifecycleRecord) -> Receipt:
    # A reboot leaves no process alive, and the unit name may now be foreign —
    # resolve without signalling.
    if record.boot_id and _boot_id() and record.boot_id != _boot_id():
        return Receipt(outcome="complete", verified_empty=True, ts=_now())

    ident = _unit_identity(record.unit)
    if ident.get("LoadState", "") in ("", "not-found"):
        # Same boot, unit gone: systemd removes a scope's cgroup only once it is
        # empty, so the tree is gone. No signal was sent.
        return Receipt(outcome="complete", verified_empty=True, ts=_now())

    live_inv = ident.get("InvocationID", "")
    if record.invocation_id:
        if live_inv and live_inv != record.invocation_id:
            return Receipt(outcome="refused", error="unit replaced by a foreign instance", ts=_now())
    elif ident.get("Description", "") not in ("", SCOPE_DESCRIPTION):
        return Receipt(outcome="refused", error="unit description mismatch", ts=_now())

    ok, err = _scope_stop(record.unit)
    if not ok:
        return Receipt(outcome="pending_retry", signalled=True, error=err, ts=_now())
    if _cgroup_gone_or_empty(record.control_group) is True:
        return Receipt(outcome="complete", signalled=True, verified_empty=True, ts=_now())
    return Receipt(outcome="pending_retry", signalled=True,
                   error="scope still populated after stop", ts=_now())


def reconcile(runs_dir: Path) -> list[tuple[str, Receipt]]:
    """Reap scopes left by a previous daemon incarnation. A record whose
    generation differs from the current one was owned by a dead daemon, so its
    run cannot be active; a record of the current generation is left alone."""
    root = Path(runs_dir)
    if not root.is_dir():
        return []
    gen = current_generation()
    results: list[tuple[str, Receipt]] = []
    with _flock(root / RECONCILE_LOCK_NAME):
        for child in sorted(p for p in root.iterdir() if p.is_dir()):
            try:
                record = read_record(child)
            except LifecycleError:
                logger.warning("skipping corrupt record in %s", child.name)
                continue
            if record is None or record.generation == gen:
                continue
            results.append((child.name, reap(child)))
    return results


def _receipt_from(record: LifecycleRecord) -> Receipt | None:
    if not record.receipt:
        return None
    try:
        return Receipt(**{k: v for k, v in record.receipt.items()
                          if k in Receipt.__dataclass_fields__})
    except TypeError:
        return None
