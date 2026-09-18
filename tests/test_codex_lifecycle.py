"""Tests for Codex workflow-run cgroup containment (nerve/agent/backends/codex/lifecycle.py).

The containment/reap cases use real systemd user scopes running real
setsid/double-fork escapers (tests/fixtures/lifecycle_escaper.py) and the real
fake app-server through the actual launch wrapper — no model, no network. Every
scope/process is torn down by the ``scopes`` fixture. Scope cases skip on a host
without a reachable systemd ``--user`` manager.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from nerve.agent.backends import BackendDeps, SessionSpec
from nerve.agent.backends.base import TransportDiedError
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
            subprocess.run(["systemctl", "--user", "stop", unit],
                           capture_output=True, timeout=15)
        for tok in reg["tokens"]:
            subprocess.run([sys.executable, ESCAPER, "sweep", tok],
                           env={**os.environ, "ESCAPER_TOKEN": tok},
                           capture_output=True, timeout=15)


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
    rec = lifecycle.LifecycleRecord(
        run_id=run_id, session_id=f"workflow:{run_id}", unit=unit,
        boot_id=lifecycle._boot_id(), generation=lifecycle.current_generation(),
    )
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
        config=lambda: cfg, db=None, registry=None,
        tool_ctx_factory=lambda sid: None, external_mcp_servers=lambda: [],
        gateway_port=lambda: 8900, mint_session_token=lambda sid: f"tok-{sid}",
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
    containment = lifecycle.WorkflowContainment(
        mode="strict", run_dir=run_dir, run_id="wfr-l1", session_id="workflow:wfr-l1",
    )
    literal = "keep-${NERVE_SENTINEL_DO_NOT_EXPAND}"
    cwd = tmp_path / "ws"
    cwd.mkdir(parents=True, exist_ok=True)

    async def _handler(method, params):
        return {}

    client = CodexAppServerClient(
        bin_path=FAKE, cwd=str(cwd),
        env={**os.environ, "CODEX_HOME": str(tmp_path / "home"), "FAKE_CODEX_MODE": "basic"},
        server_request_handler=_handler, config_overrides=[f"sentinel={literal}"],
        containment=containment,
    )
    resp = await client.start()
    try:
        assert resp.get("userAgent")                       # clean JSON stdout
        assert any(literal in v for v in resp["_fake"]["configOverrides"])  # literal argv preserved
        assert resp["_fake"]["env"]["CODEX_HOME"] == str(tmp_path / "home")  # env forwarded
        rec = lifecycle.read_record(run_dir)
        assert rec.invocation_id and rec.control_group
        assert lifecycle._cgroup_gone_or_empty(rec.control_group) is False   # populated
        procs = Path("/sys/fs/cgroup" + rec.control_group + "/cgroup.procs").read_text().split()
        assert procs and any(
            os.readlink(f"/proc/{p}/cwd") == str(cwd)
            for p in map(int, procs) if Path(f"/proc/{p}").exists()
        )
    finally:
        await client.close()


def test_disabled_and_non_workflow_build_no_containment(tmp_path):
    cfg = _wf_config(tmp_path, mode="strict")
    backend = CodexBackend(_deps(cfg))
    # interactive/cron session: never contained, even in strict mode
    assert _workflow_containment(backend, _spec(cfg, session_id="s1", source="web")) is None
    # disabled mode: workflow session not contained
    cfg2 = _wf_config(tmp_path, mode="disabled")
    backend2 = CodexBackend(_deps(cfg2))
    assert _workflow_containment(backend2, _spec(cfg2, session_id="workflow:wfr-x", source="workflow")) is None


def test_strict_unavailable_fails_before_exec(tmp_path, monkeypatch):
    run_dir = tmp_path / "u"
    run_dir.mkdir()
    monkeypatch.setattr(lifecycle, "detect_capabilities",
                        lambda **k: lifecycle.Capabilities(ok=False, reason="no systemd-run"))
    c = lifecycle.WorkflowContainment(mode="strict", run_dir=run_dir, run_id="wfr-u",
                                      session_id="workflow:wfr-u")
    with pytest.raises(lifecycle.ContainmentUnavailable):
        lifecycle.prepare_launch(c, ["/usr/bin/codex", "app-server"])
    assert not (run_dir / lifecycle.RECORD_NAME).exists()  # nothing launched/recorded


# -- reap -------------------------------------------------------------------- #


@requires_scope
def test_reap_kills_contained_and_spares_foreign(tmp_path, scopes):
    target, gc_t = _launch_escaper(scopes, tmp_path / "t", "wfr-target")
    foreign, gc_f = _launch_escaper(scopes, tmp_path / "f", "wfr-foreign")
    assert _alive(gc_t) and _alive(gc_f)
    assert lifecycle.proc_cgroup(gc_t) == target.control_group

    r = lifecycle.reap(tmp_path / "t")
    assert r.outcome == "complete" and r.signalled and r.verified_empty
    assert not _alive(gc_t)   # target reaped
    assert _alive(gc_f)       # foreign run untouched
    assert lifecycle.read_record(tmp_path / "t").receipt["outcome"] == "complete"


@requires_scope
def test_reap_nested_cgroup_uses_recursive_populated(tmp_path, scopes):
    rec, gc = _launch_escaper(scopes, tmp_path / "n", "wfr-nested", variant="nested")
    # grandchild migrated into a child cgroup: root cgroup.procs is empty, but
    # recursive cgroup.events populated is 1 (what reap must check).
    assert Path("/sys/fs/cgroup" + rec.control_group + "/cgroup.procs").read_text().split() == []
    assert lifecycle._cgroup_gone_or_empty(rec.control_group) is False
    assert lifecycle.proc_cgroup(gc) == rec.control_group + "/nested"
    assert lifecycle.reap(tmp_path / "n").outcome == "complete"
    assert not _alive(gc)


@requires_scope
def test_reap_escalates_term_ignoring_descendant_to_kill(tmp_path, scopes):
    rec, gc = _launch_escaper(scopes, tmp_path / "ig", "wfr-igterm", variant="igterm")
    assert _alive(gc)
    r = lifecycle.reap(tmp_path / "ig")
    assert r.outcome == "complete" and r.verified_empty   # SIGKILL escalation
    assert not _alive(gc)


@requires_scope
def test_reap_is_idempotent(tmp_path, scopes, monkeypatch):
    rec, gc = _launch_escaper(scopes, tmp_path / "id", "wfr-idem")
    assert lifecycle.reap(tmp_path / "id").outcome == "complete"
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)  # must not re-stop
    assert lifecycle.reap(tmp_path / "id").outcome == "complete"


@requires_scope
def test_reap_refuses_replaced_unit(tmp_path, scopes):
    # Record a wrong InvocationID: the live unit is a foreign instance of the
    # same name; reap must refuse to signal it.
    rec, gc = _launch_escaper(scopes, tmp_path / "r", "wfr-rep",
                              invocation_id="00000000000000000000000000000000")
    r = lifecycle.reap(tmp_path / "r")
    assert r.outcome == "refused" and not r.signalled and not r.is_complete()
    assert _alive(gc)


def test_reap_cross_boot_record_never_signals(tmp_path, monkeypatch):
    run_dir = tmp_path / "b"
    run_dir.mkdir()
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-b", session_id="workflow:wfr-b", unit="nerve-wf-wfr-b-n.scope",
        boot_id="prior-boot", generation="old", invocation_id="i",
        control_group="/user.slice/nerve-wf-wfr-b-n.scope",
    ))
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    r = lifecycle.reap(run_dir)
    assert r.outcome == "complete" and not r.signalled   # nothing survives a reboot


def test_reap_corrupt_record_does_not_signal(tmp_path, monkeypatch):
    run_dir = tmp_path / "c"
    run_dir.mkdir()
    (run_dir / lifecycle.RECORD_NAME).write_text("{ not valid json ")
    monkeypatch.setattr(lifecycle, "_scope_stop", _fail_if_called)
    assert lifecycle.reap(run_dir).outcome == "pending_retry"


def _fail_if_called(*a, **k):
    raise AssertionError("scope stop must not be called here")


# -- reconcile --------------------------------------------------------------- #


@requires_scope
def test_reconcile_reaps_prior_generation(tmp_path, scopes):
    rec, gc = _launch_escaper(scopes, tmp_path / "wf" / "wfr-old", "wfr-old")
    # Rewrite the record's generation to a prior incarnation's.
    rec.generation = "prior-daemon-generation"
    lifecycle.write_record(tmp_path / "wf" / "wfr-old", rec)
    results = lifecycle.reconcile(tmp_path / "wf")
    assert [rid for rid, _ in results] == ["wfr-old"]
    assert not _alive(gc)


def test_reconcile_skips_current_generation(tmp_path):
    run_dir = tmp_path / "wf" / "wfr-live"
    run_dir.mkdir(parents=True)
    lifecycle.write_record(run_dir, lifecycle.LifecycleRecord(
        run_id="wfr-live", session_id="workflow:wfr-live",
        unit="nerve-wf-wfr-live-n.scope", boot_id=lifecycle._boot_id(),
        generation=lifecycle.current_generation(),
    ))
    assert lifecycle.reconcile(tmp_path / "wf") == []   # active owner, untouched


# -- record validation ------------------------------------------------------- #


def test_record_rejects_unsafe_unit_and_foreign_cgroup():
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.LifecycleRecord(run_id="r", session_id="s", unit="evil; rm -rf.scope",
                                  boot_id="b", generation="g").validate()
    with pytest.raises(lifecycle.LifecycleError):
        lifecycle.LifecycleRecord(run_id="r", session_id="s", unit="nerve-wf-r-n.scope",
                                  boot_id="b", generation="g",
                                  control_group="/system.slice/other.scope").validate()
