"""R6 acceptance suite for Codex workflow-run descendant containment.

These are assertion-bearing tests, not smoke checks. Where the review demands
it, they use **real** systemd user scopes running **real** setsid/double-fork
escaper processes (``tests/fixtures/lifecycle_escaper.py``) and the **real**
fake app-server (``tests/fixtures/fake_codex_appserver.py``) through the actual
launch wrapper — no paid model, no network. Every scope/process a test creates
is torn down by the ``scopes`` fixture (``systemctl --user stop`` + an
identity-checked pidfd sweep), and nothing ever touches the host user manager,
the daemon, the real ``~/.nerve/workflow-runs``, or any pre-existing process.

Cases map to the review's R6.1–R6.8. Tests needing a scope skip cleanly on a
host without a reachable systemd ``--user`` manager.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import uuid
from pathlib import Path

import pytest

from nerve.agent.backends import BackendDeps, SessionSpec, TransportDiedError
from nerve.agent.backends import events as ev
from nerve.agent.backends.base import TurnInput
from nerve.agent.backends.codex import CodexBackend, lifecycle
from nerve.agent.backends.codex.appserver import CodexAppServerClient
from nerve.agent.interactive import InteractiveToolHandler
from nerve.config import NerveConfig

FIX = Path(__file__).parent / "fixtures"
ESCAPER = str(FIX / "lifecycle_escaper.py")
FAKE = str(FIX / "fake_codex_appserver.py")

CAPS = lifecycle.detect_capabilities()
requires_scope = pytest.mark.skipif(
    not CAPS.ok, reason=f"systemd user scope unavailable: {CAPS.reason}",
)


# ----------------------------- helpers/fixtures ---------------------------- #


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


@pytest.fixture
def scopes():
    """Guaranteed teardown of every scope/process a test creates."""
    reg: dict[str, set] = {"units": set(), "tokens": set(), "run_dirs": set()}
    try:
        yield reg
    finally:
        for rd in reg["run_dirs"]:
            try:
                rec = lifecycle.read_record(Path(rd))
                if rec is not None:
                    reg["units"].add(rec.unit)
            except Exception:  # noqa: BLE001
                pass
        for unit in reg["units"]:
            subprocess.run(
                ["systemctl", "--user", "stop", unit],
                capture_output=True, timeout=15,
            )
        for tok in reg["tokens"]:
            subprocess.run(
                [sys.executable, ESCAPER, "sweep", tok],
                env={**os.environ, "ESCAPER_TOKEN": tok},
                capture_output=True, timeout=15,
            )


def _launch_escaper(
    scopes, run_dir: Path, run_id: str, *, variant: str = "",
    term_grace: int = 2, mode: str = "strict",
    session_id: str | None = None, invocation_id: str | None = None,
) -> tuple[lifecycle.LifecycleRecord, int, str]:
    """Launch a real escaper inside a real scope; return (record, gc_pid, unit).

    The escaper's main process exits after the setsid/double-fork grandchild is
    up, so the grandchild is left orphaned inside the scope — exactly the
    incident shape. A strict lifecycle record is written and (unless overridden)
    its live InvocationID/ControlGroup recorded, so ``reap`` can act on it.
    """
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    token = "lc" + uuid.uuid4().hex[:12]
    scopes["tokens"].add(token)
    nonce = uuid.uuid4().hex
    unit = lifecycle.scope_unit_name(run_id, nonce)
    scopes["units"].add(unit)
    cmd = "spawn-escapee" + (f"-{variant}" if variant else "")
    inner = [sys.executable, ESCAPER, cmd, str(run_dir)]
    argv = lifecycle.build_scope_argv(unit, inner, term_grace_seconds=term_grace)
    subprocess.run(
        argv, env={**os.environ, "ESCAPER_TOKEN": token},
        capture_output=True, timeout=30, check=True,
    )
    gc_pid = int((run_dir / "grandchild.pid").read_text())
    inst = lifecycle.current_instance()
    rec = lifecycle.LifecycleRecord(
        run_id=run_id, session_id=session_id or f"workflow:{run_id}", mode=mode,
        attempt_nonce=nonce, unit=unit, workspace_id="ws", boot_id=inst.boot_id,
        instance_generation=inst.generation, phase="intent",
    )
    lifecycle.write_record(run_dir, rec)
    if invocation_id is not None:
        # Caller wants a deliberately mismatched identity (replacement test).
        _inv, cg = lifecycle._await_unit_identity(unit)
        rec.invocation_id = invocation_id
        rec.control_group = cg
        rec.phase = "launched"
        lifecycle.write_record(run_dir, rec)
    else:
        rec = lifecycle.record_launched(run_dir, rec)
    return rec, gc_pid, unit


# ---- backend/turn helpers (mirroring tests/test_codex_appserver.py) ------- #


def _wf_config(tmp_path: Path, *, mode: str = "strict", **codex) -> NerveConfig:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "workflows": {"runs_dir": str(tmp_path / "workflow-runs")},
        "codex": {
            "bin_path": FAKE,
            "home_dir": str(tmp_path / "codex-home"),
            "model": "gpt-5.6-sol",
            "lifecycle": {
                "mode": mode, "term_grace_seconds": 2, "stop_timeout_seconds": 8,
            },
            **codex,
        },
    })
    (tmp_path / "ws").mkdir(parents=True, exist_ok=True)
    (tmp_path / "workflow-runs").mkdir(parents=True, exist_ok=True)
    return cfg


def _deps(cfg: NerveConfig) -> BackendDeps:
    return BackendDeps(
        config=lambda: cfg, db=None, registry=None,
        tool_ctx_factory=lambda sid: None, external_mcp_servers=lambda: [],
        gateway_port=lambda: 8900, mint_session_token=lambda sid: f"tok-{sid}",
    )


def _spec(cfg: NerveConfig, *, session_id: str, source: str) -> SessionSpec:
    hub = InteractiveToolHandler(
        session_id=session_id, broadcast_fn=_noop_broadcast,
        interactive_capable=False,
    )
    return SessionSpec(
        session_id=session_id, source=source, model=None, effort="high",
        system_prompt="You are Nerve.", cwd=str(cfg.workspace),
        resume_native_id=None, fork=False, interactive=hub,
        snapshot=None, record_wakeup=None, idle_timeout=15.0,
    )


async def _noop_broadcast(session_id: str, message: dict) -> None:
    return None


async def _collect_turn(client) -> list:
    return [event async for event in client.receive_turn()]


# =========================================================================== #
# R6.1 — adapter + fake app-server through the real containment wrapper
# =========================================================================== #


@requires_scope
@pytest.mark.asyncio
async def test_r61_strict_launch_roundtrip_preserves_argv_env_cwd(tmp_path, scopes):
    """initialize round-trips through systemd-run --scope with clean JSON
    stdout; literal ``${...}`` argv, env, and cwd are preserved; the app-server
    is verifiably contained and registration is durable."""
    run_dir = tmp_path / "workflow-runs" / "wfr-r61"
    scopes["run_dirs"].add(run_dir)
    containment = lifecycle.WorkflowContainment(
        mode="strict", run_dir=run_dir, run_id="wfr-r61",
        session_id="workflow:wfr-r61", workspace_id="ws",
        term_grace_seconds=2, stop_timeout_seconds=8,
    )
    literal = "keep-${NERVE_SENTINEL_DO_NOT_EXPAND}-literal"
    cwd = tmp_path / "ws"
    cwd.mkdir(parents=True, exist_ok=True)

    async def _handler(method, params):  # pragma: no cover - unused here
        return {}

    client = CodexAppServerClient(
        bin_path=FAKE, cwd=str(cwd),
        env={**os.environ, "CODEX_HOME": str(tmp_path / "home"),
             "FAKE_CODEX_MODE": "basic"},
        server_request_handler=_handler,
        config_overrides=[f"sentinel={literal}"],
        containment=containment,
    )
    resp = await client.start()
    try:
        # Clean JSON on stdout (no systemd-run control text) — it parsed.
        assert resp.get("userAgent")
        # Literal argv preserved verbatim through --expand-environment=no.
        assert any(literal in v for v in resp["_fake"]["configOverrides"])
        # Environment forwarded into the scope.
        assert resp["_fake"]["env"]["CODEX_HOME"] == str(tmp_path / "home")

        rec = lifecycle.read_record(run_dir)
        assert rec is not None and rec.phase == "registered"
        assert rec.invocation_id and rec.control_group
        # The scope is populated (a live contained app-server).
        _state, populated = lifecycle._cgroup_events(rec.control_group)
        assert populated is True
        # cwd preserved: a scope member reports our cwd.
        procs = Path("/sys/fs/cgroup" + rec.control_group + "/cgroup.procs")
        pids = [int(p) for p in procs.read_text().split()]
        assert pids
        assert any(
            os.readlink(f"/proc/{p}/cwd") == str(cwd)
            for p in pids if Path(f"/proc/{p}").exists()
        )
    finally:
        await client.close()


@requires_scope
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["basic", "big_line"])
async def test_r61_contained_turn_streams_including_large_records(
    tmp_path, monkeypatch, scopes, mode,
):
    """A full turn (incremental notifications; a ~2 MiB single-line tool result
    for backpressure) completes through the containment wrapper unchanged."""
    monkeypatch.setenv("FAKE_CODEX_MODE", mode)
    cfg = _wf_config(tmp_path)
    scopes["run_dirs"].add(tmp_path / "workflow-runs" / "wfr-turn")
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(
        _spec(cfg, session_id="workflow:wfr-turn", source="workflow"),
    )
    try:
        rec = lifecycle.read_record(tmp_path / "workflow-runs" / "wfr-turn")
        assert rec is not None and rec.phase == "registered"
        await client.start_turn(TurnInput(text="hi"))
        events = await _collect_turn(client)
        assert any(isinstance(e, ev.TurnCompleted) for e in events)
        assert events[-1].status == "completed"  # transport survived the turn
    finally:
        await client.disconnect()


@requires_scope
@pytest.mark.asyncio
async def test_r61_contained_appserver_death_propagates_exit(tmp_path, monkeypatch, scopes):
    """die_mid_turn: the app-server exits mid-turn; the exit propagates through
    the wrapper (transport dies) rather than hanging."""
    monkeypatch.setenv("FAKE_CODEX_MODE", "die_mid_turn")
    cfg = _wf_config(tmp_path)
    scopes["run_dirs"].add(tmp_path / "workflow-runs" / "wfr-die")
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(
        _spec(cfg, session_id="workflow:wfr-die", source="workflow"),
    )
    try:
        await client.start_turn(TurnInput(text="hi"))
        with pytest.raises(TransportDiedError):
            await _collect_turn(client)
    finally:
        await client.disconnect()


# =========================================================================== #
# R6.2 — success/error/cancel/timeout/budget converge on one reap; foreign runs survive
# =========================================================================== #


@requires_scope
def test_r62_reap_kills_contained_escapee_and_spares_foreign(tmp_path, scopes):
    target, gc_t, _u = _launch_escaper(scopes, tmp_path / "t", "wfr-target")
    foreign, gc_f, _uf = _launch_escaper(scopes, tmp_path / "f", "wfr-foreign")

    # Both escaped grandchildren are alive and each is contained in its scope.
    assert _alive(gc_t) and _alive(gc_f)
    assert lifecycle.proc_cgroup(gc_t) == target.control_group
    assert lifecycle.proc_cgroup(gc_f) == foreign.control_group

    receipt = lifecycle.reap(tmp_path / "t", term_grace_seconds=2, stop_timeout_seconds=8)
    assert receipt.outcome == "complete"
    assert receipt.signalled and receipt.verified_empty
    assert not _alive(gc_t)          # target's escapee is reaped
    assert _alive(gc_f)              # the foreign run is untouched

    # The receipt is persisted terminal on the record.
    rec = lifecycle.read_record(tmp_path / "t")
    assert rec.phase == "terminal" and rec.receipt["outcome"] == "complete"


# =========================================================================== #
# R6.3 — nested cgroups, TERM-ignoring, fork-during-teardown; recursive populated
# =========================================================================== #


@requires_scope
def test_r63_nested_cgroup_uses_recursive_populated(tmp_path, scopes):
    rec, gc, _u = _launch_escaper(scopes, tmp_path / "n", "wfr-nested", variant="nested")
    # The grandchild migrated into a child cgroup: the scope ROOT holds no
    # procs, yet recursive cgroup.events populated is 1 (the emptiness signal
    # root cgroup.procs alone would miss).
    root_procs = Path("/sys/fs/cgroup" + rec.control_group + "/cgroup.procs").read_text().split()
    assert root_procs == []
    _state, populated = lifecycle._cgroup_events(rec.control_group)
    assert populated is True
    assert lifecycle.proc_cgroup(gc) == rec.control_group + "/nested"

    receipt = lifecycle.reap(tmp_path / "n", term_grace_seconds=2, stop_timeout_seconds=8)
    assert receipt.outcome == "complete" and receipt.verified_empty
    assert not _alive(gc)


@requires_scope
def test_r63_term_ignoring_descendant_escalated_to_kill(tmp_path, scopes):
    rec, gc, _u = _launch_escaper(
        scopes, tmp_path / "ig", "wfr-igterm", variant="igterm", term_grace=2,
    )
    assert _alive(gc)
    receipt = lifecycle.reap(tmp_path / "ig", term_grace_seconds=2, stop_timeout_seconds=10)
    # A descendant that ignores TERM is still killed by the bounded
    # TERM→grace→SIGKILL sequence (SendSIGKILL=yes, KillMode=control-group).
    assert receipt.outcome == "complete" and receipt.verified_empty
    assert not _alive(gc)


@requires_scope
def test_r63_fork_during_teardown_all_reaped(tmp_path, scopes):
    rec, gc, _u = _launch_escaper(
        scopes, tmp_path / "fk", "wfr-forker", variant="forker", term_grace=2,
    )
    token_before = _scope_proc_count(rec.control_group)
    assert token_before >= 1
    receipt = lifecycle.reap(tmp_path / "fk", term_grace_seconds=2, stop_timeout_seconds=10)
    # Children forked *during* teardown are in the cgroup too, so the
    # control-group kill takes them all — verified by recursive emptiness.
    assert receipt.outcome == "complete" and receipt.verified_empty
    assert not _alive(gc)


def _scope_proc_count(control_group: str) -> int:
    total = 0
    root = Path("/sys/fs/cgroup" + control_group)
    for procs in root.rglob("cgroup.procs"):
        try:
            total += len(procs.read_text().split())
        except OSError:
            pass
    return total


# =========================================================================== #
# R6.4 — crash windows + fresh-process reconcile
# =========================================================================== #


@requires_scope
def test_r64_reconcile_reaps_abandoned_launch_from_fresh_generation(tmp_path, scopes):
    """A launch whose controller 'crashed' (record left at 'launched', a
    different daemon generation) is reaped by startup reconciliation reading
    only the durable record."""
    rec, gc, _u = _launch_escaper(scopes, tmp_path / "c", "wfr-crash")
    assert rec.phase == "launched" and _alive(gc)
    # Reconcile as a *fresh* daemon generation (the crashed one is gone).
    receipt = lifecycle.reconcile_run(
        tmp_path / "c", is_terminal=True,
        active_generation="fresh-other-generation",
        term_grace_seconds=2, stop_timeout_seconds=8,
    )
    assert receipt is not None and receipt.outcome == "complete"
    assert not _alive(gc)


def test_r64_reconcile_skips_current_generation_owner(tmp_path):
    """Reconcile never touches a run this very generation still owns."""
    run_dir = tmp_path / "own"
    run_dir.mkdir()
    inst = lifecycle.current_instance()
    rec = lifecycle.LifecycleRecord(
        run_id="wfr-own", session_id="workflow:wfr-own", mode="strict",
        attempt_nonce="n1", unit="nerve-wf-wfr-own-n1.scope", workspace_id="ws",
        boot_id=inst.boot_id, instance_generation=inst.generation, phase="launched",
    )
    lifecycle.write_record(run_dir, rec)
    out = lifecycle.reconcile_run(
        run_dir, is_terminal=False, active_generation=inst.generation,
    )
    assert out is None  # active owner, same generation → left alone


def test_r64_intent_only_abandoned_launch_resolves_absent(tmp_path):
    """A pre-launch crash (intent recorded, scope never created) reconciles to
    resolved_absent — nothing to signal, no false completion of a live tree."""
    run_dir = tmp_path / "int"
    run_dir.mkdir()
    inst = lifecycle.current_instance()
    rec = lifecycle.LifecycleRecord(
        run_id="wfr-int", session_id="workflow:wfr-int", mode="strict",
        attempt_nonce="n2", unit="nerve-wf-wfr-int-n2.scope", workspace_id="ws",
        boot_id=inst.boot_id, instance_generation="old-generation", phase="intent",
    )
    lifecycle.write_record(run_dir, rec)
    receipt = lifecycle.reconcile_run(
        run_dir, is_terminal=False, active_generation=inst.generation,
    )
    assert receipt is not None
    assert receipt.outcome == "resolved_absent" and not receipt.signalled


# =========================================================================== #
# R6.5 — identity: replacement, cross-boot, corrupt, unwritable/fsync, manager, no PID signal
# =========================================================================== #


@requires_scope
def test_r65_refuses_replaced_unit_and_spares_it(tmp_path, scopes):
    """When the recorded InvocationID no longer matches the live unit (a
    same-name replacement), reap refuses to signal — the foreign process that
    now owns the name survives."""
    rec, gc, _u = _launch_escaper(
        scopes, tmp_path / "rep", "wfr-rep",
        invocation_id="00000000000000000000000000000000",  # deliberately wrong
    )
    assert _alive(gc)
    receipt = lifecycle.reap(tmp_path / "rep", term_grace_seconds=2, stop_timeout_seconds=8)
    assert receipt.outcome == "failed_replaced"
    assert not receipt.signalled and not receipt.is_complete()
    assert receipt.needs_attention()
    assert _alive(gc)  # the (foreign) unit was NOT signalled


def test_r65_cross_boot_record_never_signals(tmp_path, monkeypatch):
    run_dir = tmp_path / "boot"
    run_dir.mkdir()
    rec = lifecycle.LifecycleRecord(
        run_id="wfr-boot", session_id="workflow:wfr-boot", mode="strict",
        attempt_nonce="n3", unit="nerve-wf-wfr-boot-n3.scope", workspace_id="ws",
        boot_id="0000-prior-boot", instance_generation="old", phase="registered",
        invocation_id="abc", control_group="/user.slice/nerve-wf-wfr-boot-n3.scope",
    )
    lifecycle.write_record(run_dir, rec)
    # No unit lookup / no signal should occur for a prior-boot record.
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    receipt = lifecycle.reap(run_dir)
    assert receipt.outcome == "resolved_prior_boot" and not receipt.signalled


def test_r65_corrupt_record_escalates_without_signal(tmp_path, monkeypatch):
    run_dir = tmp_path / "corrupt"
    run_dir.mkdir()
    (run_dir / lifecycle.RECORD_NAME).write_text("{ this is not valid json ")
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    receipt = lifecycle.reap(run_dir)
    assert receipt.outcome == "pending_retry" and not receipt.is_complete()


def test_r65_unpersistable_receipt_never_reports_complete(tmp_path, monkeypatch):
    """If the completion receipt cannot be fsynced/written, reap must not
    report complete (R2)."""
    run_dir = tmp_path / "fsync"
    run_dir.mkdir()
    inst = lifecycle.current_instance()
    rec = lifecycle.LifecycleRecord(
        run_id="wfr-fs", session_id="workflow:wfr-fs", mode="strict",
        attempt_nonce="n4", unit="nerve-wf-wfr-fs-n4.scope", workspace_id="ws",
        boot_id=inst.boot_id, instance_generation="old", phase="launched",
        invocation_id="inv", control_group="/user.slice/nerve-wf-wfr-fs-n4.scope",
    )
    lifecycle.write_record(run_dir, rec)
    # Unit resolves + stop succeeds + scope verified empty …
    monkeypatch.setattr(lifecycle, "_unit_identity", lambda u: {
        "LoadState": "loaded", "ActiveState": "active",
        "InvocationID": "inv", "ControlGroup": rec.control_group,
        "Description": lifecycle.SCOPE_DESCRIPTION,
    })
    monkeypatch.setattr(lifecycle, "_scope_stop", lambda u, timeout: (True, ""))
    monkeypatch.setattr(lifecycle, "_cgroup_events", lambda cg: ("gone", None))
    # … but persisting the receipt fails.
    monkeypatch.setattr(os, "fsync", _raise_oserror)
    with pytest.raises((OSError, lifecycle.LifecycleError)):
        lifecycle.reap(run_dir)
    # On-disk record must NOT claim completion.
    on_disk = json.loads((run_dir / lifecycle.RECORD_NAME).read_text())
    assert (on_disk.get("receipt") or {}).get("outcome") != "complete"


def test_r65_manager_unavailable_stays_pending_without_pid_signal(tmp_path, monkeypatch):
    run_dir = tmp_path / "mgr"
    run_dir.mkdir()
    inst = lifecycle.current_instance()
    rec = lifecycle.LifecycleRecord(
        run_id="wfr-mgr", session_id="workflow:wfr-mgr", mode="strict",
        attempt_nonce="n5", unit="nerve-wf-wfr-mgr-n5.scope", workspace_id="ws",
        boot_id=inst.boot_id, instance_generation="old", phase="launched",
        invocation_id="inv", control_group="/user.slice/nerve-wf-wfr-mgr-n5.scope",
    )
    lifecycle.write_record(run_dir, rec)
    monkeypatch.setattr(lifecycle, "_unit_identity", lambda u: {
        "LoadState": "loaded", "ActiveState": "active", "InvocationID": "inv",
        "ControlGroup": rec.control_group, "Description": lifecycle.SCOPE_DESCRIPTION,
    })
    monkeypatch.setattr(lifecycle, "_scope_stop", lambda u, timeout: (False, "manager down"))
    monkeypatch.setattr(lifecycle, "_cgroup_events", lambda cg: ("present", True))
    # The reap must never fall back to signalling a raw PID / process group.
    monkeypatch.setattr(os, "kill", _raise_if_called)
    monkeypatch.setattr(os, "killpg", _raise_if_called)
    receipt = lifecycle.reap(run_dir)
    assert receipt.outcome == "pending_retry" and not receipt.is_complete()
    assert receipt.needs_retry()


def _fail_if_called(*a, **k):
    raise AssertionError("scope stop must not be called for this case")


def _raise_if_called(*a, **k):
    raise AssertionError("a raw PID/process-group signal must never be sent")


def _raise_oserror(*a, **k):
    raise OSError("simulated fsync failure")


# =========================================================================== #
# R6.6 — concurrency + repeated terminal cleanup + real durable retry
# =========================================================================== #


@requires_scope
def test_r66_repeated_reap_is_idempotent(tmp_path, scopes, monkeypatch):
    rec, gc, _u = _launch_escaper(scopes, tmp_path / "idem", "wfr-idem")
    r1 = lifecycle.reap(tmp_path / "idem", term_grace_seconds=2, stop_timeout_seconds=8)
    assert r1.outcome == "complete"
    # A second reap must not stop anything again — the terminal receipt stands.
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    r2 = lifecycle.reap(tmp_path / "idem")
    assert r2.outcome == "complete" and r2.attempts == r1.attempts


@requires_scope
def test_r66_initial_failure_then_real_durable_retry(tmp_path, scopes, monkeypatch):
    rec, gc, _u = _launch_escaper(scopes, tmp_path / "retry", "wfr-retry")
    real_stop = lifecycle._scope_stop
    monkeypatch.setattr(lifecycle, "_scope_stop", lambda u, timeout: (False, "transient manager glitch"))
    first = lifecycle.reap(tmp_path / "retry", term_grace_seconds=2, stop_timeout_seconds=8)
    assert first.outcome == "pending_retry" and _alive(gc)
    # Restore the manager and use the supported retry entry — a REAL reap.
    monkeypatch.setattr(lifecycle, "_scope_stop", real_stop)
    second = lifecycle.retry(tmp_path / "retry", term_grace_seconds=2, stop_timeout_seconds=8)
    assert second.outcome == "complete" and second.verified_empty
    assert second.attempts == 2  # durable attempt count advanced
    assert not _alive(gc)


@requires_scope
def test_r66_concurrent_reaps_serialize(tmp_path, scopes):
    rec, gc, _u = _launch_escaper(scopes, tmp_path / "conc", "wfr-conc")
    results: list[lifecycle.Receipt] = []
    barrier = threading.Barrier(3)

    def _worker():
        barrier.wait()
        results.append(lifecycle.reap(tmp_path / "conc", term_grace_seconds=2, stop_timeout_seconds=8))

    threads = [threading.Thread(target=_worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    # The per-owner lock serialises them: all agree the tree is gone, and the
    # scope was stopped exactly once (attempts never double-counts).
    assert results and all(r.is_complete() for r in results)
    assert not _alive(gc)
    assert lifecycle.read_record(tmp_path / "conc").receipt["outcome"] == "complete"


# =========================================================================== #
# R6.7 — interactive + a second workflow survive while the target is cleaned
# =========================================================================== #


@requires_scope
def test_r67_only_target_is_reaped_no_broad_kill(tmp_path, scopes, monkeypatch):
    target, gc_t, _ = _launch_escaper(scopes, tmp_path / "wt", "wfr-victim")
    other_wf, gc_o, _ = _launch_escaper(scopes, tmp_path / "wo", "wfr-other")
    interactive, gc_i, _ = _launch_escaper(scopes, tmp_path / "wi", "wfr-interactive")

    monkeypatch.setattr(os, "killpg", _raise_if_called)  # no broad process-group kill
    receipt = lifecycle.reap(tmp_path / "wt", term_grace_seconds=2, stop_timeout_seconds=8)
    assert receipt.outcome == "complete"
    assert not _alive(gc_t)
    assert _alive(gc_o) and _alive(gc_i)  # concurrent workflow + interactive survive


# =========================================================================== #
# R6.8 — disabled / observe / strict-unavailable declared behaviour
# =========================================================================== #


def test_r68_disabled_mode_builds_no_containment(tmp_path):
    cfg = _wf_config(tmp_path, mode="disabled")
    backend = CodexBackend(_deps(cfg))
    from nerve.agent.backends.codex.backend import _workflow_containment
    spec = _spec(cfg, session_id="workflow:wfr-x", source="workflow")
    assert _workflow_containment(backend, spec) is None


def test_r68_non_workflow_session_builds_no_containment(tmp_path):
    cfg = _wf_config(tmp_path, mode="strict")
    backend = CodexBackend(_deps(cfg))
    from nerve.agent.backends.codex.backend import _workflow_containment
    # An interactive/cron session is never contained, even in strict mode.
    spec = _spec(cfg, session_id="s-interactive", source="web")
    assert _workflow_containment(backend, spec) is None


def test_r68_observe_mode_does_not_wrap_and_makes_no_completion_claim(tmp_path):
    run_dir = tmp_path / "obs"
    run_dir.mkdir()
    containment = lifecycle.WorkflowContainment(
        mode="observe", run_dir=run_dir, run_id="wfr-obs",
        session_id="workflow:wfr-obs", workspace_id="ws",
    )
    inner = ["/usr/bin/codex", "app-server", "--listen", "stdio://"]
    argv, rec = lifecycle.prepare_launch(containment, inner)
    assert argv == inner  # NOT wrapped — today's behaviour preserved
    assert rec.mode == "observe"
    receipt = lifecycle.reap(run_dir)
    assert receipt.outcome == "unenforced_observe"
    assert not receipt.enforced and not receipt.signalled
    assert not receipt.is_complete()  # observe is not a resolution


def test_r68_strict_unavailable_fails_before_exec(tmp_path, monkeypatch):
    run_dir = tmp_path / "unavail"
    run_dir.mkdir()
    monkeypatch.setattr(lifecycle, "detect_capabilities", lambda **k: lifecycle.Capabilities(
        ok=False, systemd_run=False, systemctl=False, user_manager=False,
        cgroup_v2=True, reason="systemd-run not found",
    ))
    containment = lifecycle.WorkflowContainment(
        mode="strict", run_dir=run_dir, run_id="wfr-un",
        session_id="workflow:wfr-un", workspace_id="ws",
    )
    with pytest.raises(lifecycle.ContainmentUnavailable):
        lifecycle.prepare_launch(containment, ["/usr/bin/codex", "app-server"])
    # Nothing was launched, and no half-written record claims containment.
    assert not (run_dir / lifecycle.RECORD_NAME).exists()


# =========================================================================== #
# Record validation / identity hardening (R2 unit-level)
# =========================================================================== #


def test_record_rejects_unsafe_unit_and_foreign_cgroup(tmp_path):
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.LifecycleRecord(
            run_id="r", session_id="workflow:r", mode="strict", attempt_nonce="n",
            unit="evil; rm -rf /.scope", workspace_id="ws", boot_id="b",
            instance_generation="g",
        ).validate()
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.LifecycleRecord(
            run_id="r", session_id="workflow:r", mode="strict", attempt_nonce="n",
            unit="nerve-wf-r-n.scope", workspace_id="ws", boot_id="b",
            instance_generation="g",
            control_group="/system.slice/other.scope",  # not our unit / not user slice
        ).validate()


def test_atomic_write_is_durable_and_survives_bad_tmp(tmp_path):
    p = tmp_path / "rec.json"
    lifecycle._atomic_write_json(p, {"a": 1})
    assert json.loads(p.read_text())["a"] == 1
    lifecycle._atomic_write_json(p, {"a": 2})   # overwrite
    assert json.loads(p.read_text())["a"] == 2
    # no leftover temp files
    assert list(tmp_path.glob(".rec.json*")) == []
