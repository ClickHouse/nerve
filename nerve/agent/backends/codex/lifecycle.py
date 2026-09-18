"""Cgroup containment + reaping for Codex workflow-run descendants.

In ``strict`` mode a workflow run's ``codex app-server`` is launched inside a
delegated systemd user scope, so its whole descendant tree — including
``codex-linux-sandbox`` children that ``setsid`` into their own process groups —
stays in one cgroup and a single ``systemctl --user stop`` reaps it. A durable
per-run record lets the reap run at terminal cleanup or from startup
reconciliation after a daemon crash. Requires Linux + a reachable systemd
``--user`` manager + cgroup v2; strict fails before exec where that is absent.
"""

from __future__ import annotations

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
from typing import Iterable

from nerve.utils.fs import atomic_write_text

logger = logging.getLogger(__name__)

MODE_STRICT = "strict"

RECORD_NAME = "lifecycle.json"
SCOPE_DESCRIPTION = "Nerve Codex workflow run containment"

# systemd-run sends TERM, then SIGKILL after this grace, over the whole cgroup.
TERM_GRACE_SECONDS = 5
STOP_TIMEOUT_SECONDS = 20

_UNIT_RE = re.compile(r"^nerve-wf-[A-Za-z0-9:_.-]+\.scope$")
_RUN_ID_RE = re.compile(r"^wfr-[A-Za-z0-9_]+$")
_CGROUP_ROOT = "/sys/fs/cgroup"
_USER_SLICE_PREFIX = "/user.slice/"

# `complete` and `refused` are final; `pending_retry` is re-attempted by the
# next startup reconciliation.
_TERMINAL_OUTCOMES = frozenset({"complete", "refused"})


class LifecycleError(Exception):
    """A containment/reap operation failed."""


class ContainmentUnavailable(LifecycleError):
    """Strict mode requested but the host cannot create a scope (raised before exec)."""


# -- capabilities ------------------------------------------------------------ #


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
            proc = subprocess.run(["systemctl", "--user", "show-environment"],
                                  capture_output=True, timeout=5, check=False)
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


# -- durable record ---------------------------------------------------------- #


@dataclass
class Receipt:
    outcome: str          # complete | pending_retry | refused | no_scope
    error: str = ""
    ts: str = ""

    def is_complete(self) -> bool:
        return self.outcome == "complete"

    def is_terminal(self) -> bool:
        return self.outcome in _TERMINAL_OUTCOMES


@dataclass
class LifecycleRecord:
    run_id: str
    unit: str
    boot_id: str
    invocation_id: str = ""
    control_group: str = ""
    receipt: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LifecycleRecord":
        if not isinstance(d, dict):
            raise LifecycleError("record is not an object")
        try:
            rec = cls(
                run_id=str(d["run_id"]), unit=str(d["unit"]),
                boot_id=str(d.get("boot_id", "")),
                invocation_id=str(d.get("invocation_id", "")),
                control_group=str(d.get("control_group", "")),
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
    atomic_write_text(record_path(run_dir),
                      json.dumps(record.to_dict(), indent=2, sort_keys=True), mode=0o600)


# -- unit naming + launch wrapper -------------------------------------------- #


def scope_unit_name(run_id: str, nonce: str) -> str:
    safe_run = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[:80]
    safe_nonce = re.sub(r"[^A-Za-z0-9]", "", nonce)[:16]
    return f"nerve-wf-{safe_run}-{safe_nonce}.scope"


def build_scope_argv(unit: str, inner_argv: list[str]) -> list[str]:
    """Wrap ``inner_argv`` to run inside a delegated transient user scope.
    systemd puts the process in the scope's cgroup before it execs, so the whole
    fork/setsid subtree stays contained."""
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


def _unit_identity(unit: str) -> dict[str, str] | None:
    """``systemctl --user show`` the unit's identity props, or None if the query
    itself failed (which must never be read as 'the scope is gone')."""
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", unit,
             "--property", "LoadState,InvocationID,ControlGroup,Description"],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.debug("systemctl show %s failed: %s", unit, e)
        return None
    if proc.returncode != 0:
        return None
    out: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            out[key.strip()] = value.strip()
    return out


def _await_unit_identity(unit: str, *, timeout: float = 4.0) -> tuple[str, str]:
    """Poll until the scope reports its InvocationID + ControlGroup."""
    deadline = time.monotonic() + timeout
    inv = cg = ""
    while time.monotonic() < deadline:
        ident = _unit_identity(unit) or {}
        inv, cg = ident.get("InvocationID", ""), ident.get("ControlGroup", "")
        if inv and cg:
            return inv, cg
        time.sleep(0.1)
    return inv, cg


def _cgroup_gone_or_empty(control_group: str) -> bool | None:
    """True if the scope cgroup is gone or recursively unpopulated, False if it
    still holds a process, None if it can't be read. ``cgroup.events``
    ``populated`` is recursive, so it covers delegated child cgroups."""
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
    try:
        proc = subprocess.run(["systemctl", "--user", "stop", unit],
                              capture_output=True, text=True,
                              timeout=STOP_TIMEOUT_SECONDS, check=False)
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
    run_dir: Path
    run_id: str


def prepare_launch(
    containment: WorkflowContainment, inner_argv: list[str],
) -> tuple[list[str], LifecycleRecord]:
    """Record the owner, then return the scope-wrapped argv. Raises before
    anything execs if the host can't contain the run, if the record can't be
    written, or if a previous attempt's scope is not confirmed reaped — the
    engine can recreate a crashed client for the same run, and a fresh launch
    must not overwrite (and orphan) a still-live prior scope."""
    caps = detect_capabilities()
    if not caps.ok:
        raise ContainmentUnavailable(f"strict cgroup containment unavailable: {caps.reason}")
    if record_path(containment.run_dir).exists():
        prior = reap(containment.run_dir)
        if not prior.is_complete():
            raise LifecycleError(
                f"previous scope not confirmed reaped ({prior.outcome}: {prior.error}); "
                "refusing to relaunch"
            )
    unit = scope_unit_name(containment.run_id, uuid.uuid4().hex)
    record = LifecycleRecord(run_id=containment.run_id, unit=unit, boot_id=_boot_id())
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
    """Reap a run's contained descendants from its durable record. Idempotent:
    a record already carrying a terminal receipt is returned unchanged."""
    run_dir = Path(run_dir)
    if not record_path(run_dir).exists():
        return Receipt(outcome="no_scope")
    try:
        record = read_record(run_dir)
    except LifecycleError as e:
        logger.warning("refusing to reap from corrupt record in %s: %s", run_dir, e)
        return Receipt(outcome="pending_retry", error="corrupt record")
    if record is None:
        return Receipt(outcome="no_scope")
    prev = _receipt_from(record)
    if prev is not None and prev.is_terminal():
        return prev
    receipt = _do_reap(record)
    record.receipt = asdict(receipt)
    try:
        write_record(run_dir, record)
    except OSError as e:
        # A stop we can't durably record cannot be reported complete.
        return Receipt(outcome="pending_retry", error=f"receipt not persisted: {e}")
    return receipt


def _do_reap(record: LifecycleRecord) -> Receipt:
    # A reboot leaves no process alive, and the unit name may now be foreign —
    # resolve without signalling.
    if record.boot_id and _boot_id() and record.boot_id != _boot_id():
        return Receipt(outcome="complete")

    ident = _unit_identity(record.unit)
    if ident is None:
        return Receipt(outcome="pending_retry", error="unit query failed")
    if ident.get("LoadState", "") == "not-found":
        # A transient scope's cgroup is removed only once empty, so a
        # not-found unit means the tree is gone. No signal sent.
        return Receipt(outcome="complete")

    # Verify identity before signalling: an exact InvocationID when recorded, or
    # the exact scope description for a pre-registration record.
    live_inv = ident.get("InvocationID", "")
    if record.invocation_id:
        if not live_inv:
            return Receipt(outcome="pending_retry", error="unit InvocationID unavailable")
        if live_inv != record.invocation_id:
            return Receipt(outcome="refused", error="unit replaced by a foreign instance")
    elif ident.get("Description", "") != SCOPE_DESCRIPTION:
        return Receipt(outcome="refused", error="unit description mismatch")

    ok, err = _scope_stop(record.unit)
    if not ok:
        return Receipt(outcome="pending_retry", error=err)
    if _cgroup_gone_or_empty(record.control_group) is True:
        return Receipt(outcome="complete")
    return Receipt(outcome="pending_retry", error="scope still populated after stop")


def reconcile(runs_dir: Path) -> list[tuple[str, Receipt]]:
    """Reap every run's scope from its record. Called at startup, after active
    runs have been marked failed, so no record belongs to a live run; reap is
    idempotent, so already-complete records are no-ops."""
    root = Path(runs_dir)
    if not root.is_dir():
        return []
    results: list[tuple[str, Receipt]] = []
    for child in sorted(p for p in root.iterdir() if p.is_dir()):
        try:
            record = read_record(child)
        except LifecycleError:
            logger.warning("skipping corrupt record in %s", child.name)
            continue
        if record is None:
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
