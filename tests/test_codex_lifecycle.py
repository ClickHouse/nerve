"""Tests for Codex workflow-run cgroup containment (codex/lifecycle.py).

Containment/reap cases use real systemd user scopes running real setsid escapers
(tests/fixtures/lifecycle_escaper.py) and the real fake app-server — no model,
no network. Every scope/process is torn down by the ``scopes`` fixture. Scope
cases skip where a systemd ``--user`` manager isn't reachable.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from nerve.agent.backends import BackendDeps, SessionSpec
from nerve.agent.backends.codex import CodexBackend, lifecycle
from nerve.agent.backends.codex.appserver import CodexAppServerClient
from nerve.agent.backends.codex.backend import _workflow_containment
from nerve.agent.interactive import InteractiveToolHandler
from nerve.config import NerveConfig

FIX = Path(__file__).parent / "fixtures"
ESCAPER = str(FIX / "lifecycle_escaper.py")
FAKE = str(FIX / "fake_codex_appserver.py")

CAPS = lifecycle.detect_capabilities()
requires_scope = pytest.mark.skipif(
    not CAPS.ok, reason=f"systemd user scope unavailable: {CAPS.reason}",
)


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
            subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, timeout=15)
        for tok in reg["tokens"]:
            subprocess.run([sys.executable, ESCAPER, "sweep", tok],
                           env={**os.environ, "ESCAPER_TOKEN": tok}, capture_output=True, timeout=15)


def _launch_escaper(scopes, run_dir: Path, run_id: str, *, variant: str = "",
                    invocation_id: str | None = None):
    """Launch a real escaper in a real scope; return (record, gc_pid)."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    token = "lc" + uuid.uuid4().hex[:12]
    scopes["tokens"].add(token)
    unit = lifecycle.scope_unit_name(run_id, uuid.uuid4().hex)
    scopes["units"].add(unit)
    cmd = "spawn-escapee" + (f"-{variant}" if variant else "")
    argv = lifecycle.build_scope_argv(unit, [sys.executable, ESCAPER, cmd, str(run_dir)])
    subprocess.run(argv, env={**os.environ, "ESCAPER_TOKEN": token},
                   capture_output=True, timeout=30, check=True)
    gc_pid = int((run_dir / "grandchild.pid").read_text())
    rec = lifecycle.LifecycleRecord(run_id=run_id, unit=unit, boot_id=lifecycle._boot_id())
    lifecycle.write_record(run_dir, rec)
    if invocation_id is not None:
        _inv, cg = lifecycle._await_unit_identity(unit)
        rec.invocation_id, rec.control_group = invocation_id, cg
        lifecycle.write_record(run_dir, rec)
    else:
        rec = lifecycle.record_launched(run_dir, rec)
    return rec, gc_pid


def _deps(cfg: NerveConfig) -> BackendDeps:
    return BackendDeps(
        config=lambda: cfg, db=None, registry=None, tool_ctx_factory=lambda sid: None,
        external_mcp_servers=lambda: [], gateway_port=lambda: 8900,
        mint_session_token=lambda sid: f"tok-{sid}",
    )


def _spec(cfg: NerveConfig, *, session_id: str, source: str) -> SessionSpec:
    hub = InteractiveToolHandler(session_id=session_id, broadcast_fn=_noop, interactive_capable=False)
    return SessionSpec(
        session_id=session_id, source=source, model=None, effort="high",
        system_prompt="You are Nerve.", cwd=str(cfg.workspace), resume_native_id=None,
        fork=False, interactive=hub, snapshot=None, record_wakeup=None, idle_timeout=15.0,
    )


async def _noop(session_id: str, message: dict) -> None:
    return None


def _wf_config(tmp_path: Path, *, mode: str = "strict") -> NerveConfig:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "workflows": {"runs_dir": str(tmp_path / "workflow-runs")},
        "codex": {"bin_path": FAKE, "home_dir": str(tmp_path / "home"),
                  "model": "gpt-5.6-sol", "lifecycle": {"mode": mode}},
    })
    (tmp_path / "ws").mkdir(parents=True, exist_ok=True)
    (tmp_path / "workflow-runs").mkdir(parents=True, exist_ok=True)
    return cfg


# -- launch path ------------------------------------------------------------ #


@requires_scope
@pytest.mark.asyncio
async def test_strict_launch_contains_and_roundtrips(tmp_path, scopes):
    run_dir = tmp_path / "workflow-runs" / "wfr-l1"
    scopes["run_dirs"].add(run_dir)
    cwd = tmp_path / "ws"
    cwd.mkdir(parents=True, exist_ok=True)
    containment = lifecycle.WorkflowContainment(run_dir=run_dir, run_id="wfr-l1")

    async def _handler(method, params):
        return {}

    client = CodexAppServerClient(
        bin_path=FAKE, cwd=str(cwd),
        env={**os.environ, "CODEX_HOME": str(tmp_path / "home"), "FAKE_CODEX_MODE": "basic"},
        server_request_handler=_handler, config_overrides=[], containment=containment,
    )
    resp = await client.start()
    try:
        assert resp.get("userAgent")                       # clean JSON through the wrapper
        rec = lifecycle.read_record(run_dir)
        assert rec.invocation_id and rec.control_group     # scope identity recorded
        assert lifecycle._cgroup_gone_or_empty(rec.control_group) is False  # contained + populated
    finally:
        await client.close()


def test_scope_argv_has_bounded_kill_directives():
    argv = lifecycle.build_scope_argv("nerve-wf-x-n.scope", ["/bin/true"])
    assert "Delegate=yes" in argv
    assert "KillMode=control-group" in argv
    assert "SendSIGKILL=yes" in argv
    assert any(a.startswith("TimeoutStopSec=") for a in argv)
    assert "--expand-environment=no" in argv


def test_strict_unavailable_fails_before_exec(tmp_path, monkeypatch):
    run_dir = tmp_path / "u"
    run_dir.mkdir()
    monkeypatch.setattr(lifecycle, "detect_capabilities",
                        lambda **k: lifecycle.Capabilities(ok=False, reason="no systemd-run"))
    c = lifecycle.WorkflowContainment(run_dir=run_dir, run_id="wfr-u")
    with pytest.raises(lifecycle.ContainmentUnavailable):
        lifecycle.prepare_launch(c, ["/usr/bin/codex", "app-server"])
    assert not lifecycle.record_path(run_dir).exists()   # nothing launched/recorded


def test_containment_gating_rejects_disabled_nonworkflow_and_traversal(tmp_path):
    cfg = _wf_config(tmp_path, mode="strict")
    backend = CodexBackend(_deps(cfg))
    # real workflow session in strict mode → contained
    c = _workflow_containment(backend, _spec(cfg, session_id="workflow:wfr-abc123", source="workflow"))
    assert c is not None and c.run_id == "wfr-abc123"
    # interactive session → never contained, even in strict mode
    assert _workflow_containment(backend, _spec(cfg, session_id="s1", source="web")) is None
    # path-traversal / spoofed run id → refused (no containment, no escape)
    assert _workflow_containment(backend, _spec(cfg, session_id="workflow:../../etc", source="workflow")) is None
    assert _workflow_containment(backend, _spec(cfg, session_id="workflow:wfr-a/b", source="workflow")) is None
    # disabled mode → not contained
    cfg2 = _wf_config(tmp_path, mode="disabled")
    b2 = CodexBackend(_deps(cfg2))
    assert _workflow_containment(b2, _spec(cfg2, session_id="workflow:wfr-x", source="workflow")) is None


# -- reap -------------------------------------------------------------------- #


@requires_scope
def test_reap_kills_contained_spares_foreign_and_is_idempotent(tmp_path, scopes, monkeypatch):
    target, gc_t = _launch_escaper(scopes, tmp_path / "t", "wfr-target")
    foreign, gc_f = _launch_escaper(scopes, tmp_path / "f", "wfr-foreign")
    assert _alive(gc_t) and _alive(gc_f)
    assert lifecycle.proc_cgroup(gc_t) == target.control_group

    r = lifecycle.reap(tmp_path / "t")
    assert r.outcome == "complete"
    assert not _alive(gc_t)          # target reaped
    assert _alive(gc_f)              # foreign run untouched

    # Idempotent: a second reap returns the stored terminal receipt, no re-stop.
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    assert lifecycle.reap(tmp_path / "t").outcome == "complete"


@requires_scope
def test_reap_nested_cgroup_uses_recursive_populated(tmp_path, scopes):
    rec, gc = _launch_escaper(scopes, tmp_path / "n", "wfr-nested", variant="nested")
    # grandchild migrated into a child cgroup: scope root cgroup.procs is empty,
    # but recursive cgroup.events populated is 1 (what reap must check).
    assert Path("/sys/fs/cgroup" + rec.control_group + "/cgroup.procs").read_text().split() == []
    assert lifecycle._cgroup_gone_or_empty(rec.control_group) is False
    assert lifecycle.proc_cgroup(gc) == rec.control_group + "/nested"
    assert lifecycle.reap(tmp_path / "n").outcome == "complete"
    assert not _alive(gc)


@requires_scope
def test_reap_refuses_replaced_unit(tmp_path, scopes):
    # Record a wrong InvocationID: the live unit is a foreign instance of the
    # same name; reap must refuse to signal it.
    rec, gc = _launch_escaper(scopes, tmp_path / "r", "wfr-rep",
                              invocation_id="00000000000000000000000000000000")
    r = lifecycle.reap(tmp_path / "r")
    assert r.outcome == "refused" and not r.is_complete()
    assert _alive(gc)


@requires_scope
def test_relaunch_reaps_prior_scope_before_overwriting_record(tmp_path, scopes):
    # A crashed client is recreated for the same run: prepare_launch must reap
    # the prior scope before overwriting the sole record, or attempt 1's escapee
    # would become unreachable.
    run_dir = tmp_path / "workflow-runs" / "wfr-relaunch"
    scopes["run_dirs"].add(run_dir)
    rec1, gc1 = _launch_escaper(scopes, run_dir, "wfr-relaunch")
    assert _alive(gc1)
    c = lifecycle.WorkflowContainment(run_dir=run_dir, run_id="wfr-relaunch")
    _argv, rec2 = lifecycle.prepare_launch(c, ["/bin/true"])
    assert not _alive(gc1)                       # prior scope reaped
    assert rec2.unit != rec1.unit                # new attempt gets a new unit
    assert lifecycle.read_record(run_dir).unit == rec2.unit


def test_relaunch_refused_when_prior_scope_not_confirmed(tmp_path, monkeypatch):
    run_dir = tmp_path / "workflow-runs" / "wfr-stuck"
    run_dir.mkdir(parents=True)
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-stuck", unit="nerve-wf-wfr-stuck-n.scope", boot_id=lifecycle._boot_id()))
    monkeypatch.setattr(lifecycle, "reap", lambda rd: lifecycle.Receipt(outcome="pending_retry", error="x"))
    c = lifecycle.WorkflowContainment(run_dir=run_dir, run_id="wfr-stuck")
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.prepare_launch(c, ["/bin/true"])


def test_reap_cross_boot_record_never_signals(tmp_path, monkeypatch):
    run_dir = tmp_path / "b"
    run_dir.mkdir()
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-b", unit="nerve-wf-wfr-b-n.scope", boot_id="prior-boot",
        invocation_id="i", control_group="/user.slice/nerve-wf-wfr-b-n.scope"))
    monkeypatch.setattr(lifecycle, "_unit_identity", _fail_if_called)
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    assert lifecycle.reap(run_dir).outcome == "complete"   # nothing survives a reboot


def test_reap_query_failure_is_pending_not_complete(tmp_path, monkeypatch):
    run_dir = tmp_path / "q"
    run_dir.mkdir()
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-q", unit="nerve-wf-wfr-q-n.scope", boot_id=lifecycle._boot_id(),
        invocation_id="inv", control_group="/user.slice/nerve-wf-wfr-q-n.scope"))
    # systemctl query failed → must NOT be read as "scope gone"
    monkeypatch.setattr(lifecycle, "_unit_identity", lambda u: None)
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    assert lifecycle.reap(run_dir).outcome == "pending_retry"
    # unit present but InvocationID blank (recorded one exists) → don't signal
    monkeypatch.setattr(lifecycle, "_unit_identity",
                        lambda u: {"LoadState": "loaded", "InvocationID": "", "ControlGroup": "", "Description": ""})
    # reset the stored receipt so reap re-evaluates
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-q", unit="nerve-wf-wfr-q-n.scope", boot_id=lifecycle._boot_id(),
        invocation_id="inv", control_group="/user.slice/nerve-wf-wfr-q-n.scope"))
    assert lifecycle.reap(run_dir).outcome == "pending_retry"


@pytest.mark.parametrize("body", ["{ not valid json ", '{"run_id":"r","unit":"evil; rm.scope","boot_id":"b"}'])
def test_reap_corrupt_or_invalid_record_never_signals(tmp_path, monkeypatch, body):
    run_dir = tmp_path / "c"
    run_dir.mkdir()
    lifecycle.record_path(run_dir).write_text(body)
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    assert lifecycle.reap(run_dir).outcome == "pending_retry"


def test_record_rejects_foreign_cgroup_path():
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.LifecycleRecord(run_id="r", unit="nerve-wf-r-n.scope", boot_id="b",
                                  control_group="/system.slice/other.scope").validate()


def _fail_if_called(*a, **k):
    raise AssertionError("must not be called for this case")


# -- reconcile --------------------------------------------------------------- #


@requires_scope
def test_reconcile_reaps_recorded_scope(tmp_path, scopes):
    rec, gc = _launch_escaper(scopes, tmp_path / "wf" / "wfr-old", "wfr-old")
    results = lifecycle.reconcile(tmp_path / "wf")
    assert ("wfr-old", ) == tuple(rid for rid, _ in results)
    assert results[0][1].outcome == "complete"
    assert not _alive(gc)


def test_reconcile_skips_dirs_without_a_record(tmp_path):
    (tmp_path / "wf" / "plain-run").mkdir(parents=True)   # e.g. a Claude run dir
    assert lifecycle.reconcile(tmp_path / "wf") == []
