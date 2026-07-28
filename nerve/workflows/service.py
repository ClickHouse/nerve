"""WorkflowRunService — lifecycle, dollar-budget enforcement, journals.

Execution model: a run is a dedicated Nerve session (``workflow:<run-id>``,
source ``"workflow"``) on the engine backend the run kind selects:

- ``claude-workflow`` → Claude backend; the run prompt instructs the agent
  to orchestrate via the harness's built-in ``Workflow`` tool.
- ``codex-ultracode`` → Codex backend; the prompt instructs Ultracode
  multi-agent orchestration.

Budget enforcement is *metered dollars*, not advisory tokens: a single
monitor loop re-computes each running run's spend every poll interval as

    recorded  = session_usage effective cost (billed cost_usd, falling
                back to estimated_cost_usd for subscription-auth Codex
                turns — see UsageStore.get_session_effective_cost)
    live      = in-flight estimate for the current turn (Claude: live
                Workflow snapshot tokens priced as output tokens; Codex:
                Ultracode run journals attributed to this run, minus the
                already-folded base). Live estimates apply only while the
                session's turn is actually running — once a turn ends its
                real cost is recorded and live must drop to zero.

At ``warn_fraction`` a one-time notification fires; at 100% the run is
terminated: graceful ``engine.stop_session`` (interrupt), a grace window,
then a force client discard — which kills the session's own CLI
subprocess / process group and nothing else. Accuracy caveat: ``recorded``
lands at turn end, so the overshoot bound is roughly one turn's cost
beyond what the live estimates catch.

Stop-vs-done disambiguation: ``engine.run`` swallows both graceful
interrupts and hard cancels (it returns partial text), so this service
registers an engine *stop listener* that CASes the run terminal BEFORE
any stop touches the session — exactly the ordering the kill/budget paths
already use. ``_execute`` additionally re-checks the row and the session
status after ``engine.run`` returns, so a stopped/errored turn can never
be recorded as ``done``.

Runs do not survive a daemon restart: the startup recovery pass marks
orphaned active runs ``failed`` and notifies, so a paid job can never
burn unmetered.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nerve.agent.streaming import broadcaster
from nerve.db.usage import estimate_turn_cost
from nerve.db.workflow_runs import ACTIVE_STATUSES
from nerve.utils.time import utc_now_iso

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)

ENGINE_CLAUDE = "claude-workflow"
ENGINE_CODEX = "codex-ultracode"
# run engine kind -> session backend
ENGINE_BACKENDS = {ENGINE_CLAUDE: "claude", ENGINE_CODEX: "codex"}

# Spec keys accepted by start_run; everything else is dropped.
_SPEC_KEYS = {"prompt", "model", "effort", "cwd", "sandbox"}

# Valid per-run sandbox overrides (codex sessions only; claude ignores it).
_SANDBOX_MODES = ("read-only", "workspace-write", "danger-full-access")

# Cap stored result/error text.
_RESULT_MAX = 4000
_ERROR_MAX = 2000

_SESSION_PREFIX = "workflow:"


class WorkflowRunError(ValueError):
    """Raised for invalid start/kill requests (caller error, not a bug)."""


def _iso_to_epoch(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return 0.0


class WorkflowRunService:
    """Owns dispatch, budget metering, termination, and journals."""

    def __init__(self, config: NerveConfig, db: Database, engine: AgentEngine):
        self.config = config
        self.db = db
        self.engine = engine
        self._monitor_task: asyncio.Task | None = None
        # run_id -> the asyncio task driving engine.run for that run
        self._exec_tasks: dict[str, asyncio.Task] = {}
        # run_id -> codex journal dollars already folded into recorded cost
        self._codex_live_base: dict[str, float] = {}
        # Detached kill/teardown/dispatch tasks (referenced until done).
        self._kill_tasks: set[asyncio.Task] = set()
        self._dispatch_lock = asyncio.Lock()
        # Gate: no new dispatches once the daemon is shutting down —
        # otherwise queued runs get promoted (and paid CLI sessions
        # spawned) during teardown, only to die with the process.
        self._stopping = False
        self._stop_listener_registered = False
        # Run-completion listeners, invoked (awaited, exception-isolated)
        # from _finalize_terminal with the fresh terminal run row. Listeners
        # must hand off quickly (enqueue) — never start work inline.
        self._completion_listeners: list[Any] = []

    # ------------------------------------------------------------------ #
    #  Lifespan                                                           #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Recovery pass + monitor loop startup."""
        self._stopping = False
        if not self._stop_listener_registered:
            # CAS runs terminal BEFORE any session stop lands (engine.run
            # swallows interrupts/cancels — see module docstring).
            self.engine.add_stop_listener(self._on_session_stop)
            self._stop_listener_registered = True
        orphaned = await self.db.get_active_workflow_runs()
        for run in orphaned:
            flipped = await self.db.transition_workflow_run(
                run["id"], "failed", expect=ACTIVE_STATUSES,
                error="interrupted by nerve restart",
            )
            if flipped:
                fresh = await self.db.get_workflow_run(run["id"])
                if fresh:
                    self._journal_event(fresh, "interrupted", {
                        "reason": "nerve restart",
                    })
                    self._write_run_json(fresh)
                    await self._broadcast(fresh)
        loud = [r for r in orphaned if not self._is_quiet(r)]
        if loud:
            # Loop-owned legs are excluded: the review-loop recovery pass
            # owns their restart story (auto re-issue / escalation).
            ids = ", ".join(r["id"] for r in loud)
            await self._notify(
                "Workflow runs interrupted by restart",
                f"{len(loud)} active workflow run(s) were marked failed "
                f"after a Nerve restart: {ids}. Their sessions did not "
                "survive the restart; restart them explicitly if still needed.",
                priority="high",
            )
        interval = max(5, int(self.config.workflows.poll_interval_seconds))
        self._monitor_task = asyncio.create_task(self._monitor_loop(interval))
        logger.info(
            "WorkflowRunService started (poll=%ss, max_concurrent=%s)",
            interval, self.config.workflows.max_concurrent_runs,
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):
                pass
            self._monitor_task = None
        # Running _execute tasks die with the daemon; the next start()'s
        # recovery pass marks their rows failed.

    # ------------------------------------------------------------------ #
    #  Public API                                                         #
    # ------------------------------------------------------------------ #

    def runs_dir(self) -> Path:
        return Path(self.config.workflows.runs_dir).expanduser()

    def add_completion_listener(self, callback: Any) -> None:
        """Register an async callable invoked with the fresh run row every
        time a run reaches a terminal status through the live service
        (_finalize_terminal). NOT invoked by the restart-recovery pass —
        consumers own their own recovery. Idempotent per callback."""
        if callback not in self._completion_listeners:
            self._completion_listeners.append(callback)

    def remove_completion_listener(self, callback: Any) -> None:
        """Unregister a completion listener (consumer failed to start)."""
        try:
            self._completion_listeners.remove(callback)
        except ValueError:
            pass

    async def start_run(
        self,
        engine_kind: str,
        spec: dict,
        budget_usd: float | None,
        title: str = "",
        created_by: str = "user",
        run_id: str | None = None,
    ) -> dict:
        """Validate, persist, journal, and (slots permitting) dispatch.

        ``run_id`` lets a controller pre-generate the id and persist its own
        intent row before dispatch (the review-loop outbox pattern). A reused
        id is a caller error and raises."""
        if engine_kind not in ENGINE_BACKENDS:
            raise WorkflowRunError(
                f"unknown engine {engine_kind!r} — expected one of: "
                + ", ".join(sorted(ENGINE_BACKENDS))
            )
        spec = {k: v for k, v in (spec or {}).items() if k in _SPEC_KEYS and v}
        prompt = str(spec.get("prompt") or "").strip()
        if not prompt:
            raise WorkflowRunError("spec.prompt is required and must be non-empty")
        spec["prompt"] = prompt
        if spec.get("cwd"):
            cwd = Path(str(spec["cwd"])).expanduser()
            try:
                cwd = cwd.resolve()
            except OSError as e:
                raise WorkflowRunError(f"invalid cwd: {e}") from e
            if not cwd.is_dir():
                raise WorkflowRunError(f"cwd is not a directory: {cwd}")
            spec["cwd"] = str(cwd)
        if spec.get("sandbox"):
            sandbox = str(spec["sandbox"])
            if sandbox not in _SANDBOX_MODES:
                raise WorkflowRunError(
                    f"spec.sandbox must be one of {_SANDBOX_MODES}, got {sandbox!r}"
                )
            if engine_kind != ENGINE_CODEX:
                # Claude sessions have no sandbox knob — drop silently would
                # hide a config mistake; fail loud instead.
                raise WorkflowRunError(
                    "spec.sandbox is only supported for codex-ultracode runs"
                )

        try:
            budget = float(budget_usd) if budget_usd is not None else None
        except (TypeError, ValueError) as e:
            raise WorkflowRunError(f"budget_usd is not a number: {budget_usd!r}") from e
        # NaN/Infinity would defeat every subsequent comparison — coerce to
        # unbudgeted and let the allow_unbudgeted gate decide.
        if budget is not None and (budget <= 0 or not math.isfinite(budget)):
            budget = None
        if budget is None and not self.config.workflows.allow_unbudgeted:
            raise WorkflowRunError(
                "budget_usd is required (a finite number > 0). Unbudgeted "
                "runs are disabled; set workflows.allow_unbudgeted: true to "
                "permit them."
            )

        if engine_kind == ENGINE_CODEX and budget is not None:
            # Fail closed: a codex model absent from codex.pricing meters $0
            # everywhere (journals price to zero, recorded cost is NULL), so
            # a "budgeted" run would silently never be enforced.
            from nerve.agent.backends.codex.pricing import match_pricing

            model = str(spec.get("model") or "") or self.config.codex.model
            if match_pricing(model, self.config.codex.pricing) is None:
                raise WorkflowRunError(
                    f"model {model!r} has no codex.pricing entry — the budget "
                    "cannot be metered. Add a codex.pricing entry for it or "
                    "pick a priced model."
                )

        run_id = str(run_id or "").strip() or f"wfr-{uuid.uuid4().hex[:8]}"
        journal_dir = self.runs_dir() / run_id
        try:
            journal_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as e:
            raise WorkflowRunError(f"cannot create journal dir: {e}") from e

        try:
            run = await self.db.create_workflow_run(
                run_id, engine_kind, spec, budget,
                title=title.strip(), created_by=created_by,
                journal_dir=str(journal_dir),
            )
        except Exception as e:
            if "UNIQUE" in str(e) or "unique" in str(e):
                raise WorkflowRunError(f"run id already exists: {run_id}") from e
            raise
        self._journal_event(run, "created", {
            "engine": engine_kind, "budget_usd": budget,
            "created_by": created_by,
        })
        self._write_run_json(run)
        await self._broadcast(run)
        await self._maybe_dispatch()
        return await self.db.get_workflow_run(run_id) or run

    async def kill_run(self, run_id: str, reason: str = "", killed_by: str = "") -> dict:
        """Terminate a run. Scoped strictly to the run's own session.

        CAS-loop against dispatch races: a run read as ``pending`` may be
        promoted to ``running`` before our transition lands — retry on the
        running side rather than silently reporting success on a live run.
        """
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            raise WorkflowRunError(f"no such workflow run: {run_id}")
        detail = reason.strip() or "killed"
        if killed_by:
            detail = f"{detail} (by {killed_by})"

        if await self.db.transition_workflow_run(
            run_id, "killed", expect=("pending",), error=detail,
        ):
            await self._finalize_terminal(run_id, "killed", {"reason": detail})
            return await self.db.get_workflow_run(run_id) or run

        if await self.db.transition_workflow_run(
            run_id, "killed", expect=("running",), error=detail,
        ):
            await self._finalize_terminal(run_id, "killed", {"reason": detail})
            self._spawn_enforcement(run_id)
            return await self.db.get_workflow_run(run_id) or run

        # Already terminal — idempotent.
        return await self.db.get_workflow_run(run_id) or run

    async def get_run(self, run_id: str) -> dict | None:
        return await self.db.get_workflow_run(run_id)

    async def list_runs(
        self, status: str | None = None, limit: int = 50, offset: int = 0,
    ) -> list[dict]:
        return await self.db.list_workflow_runs(status=status, limit=limit, offset=offset)

    def public_run(self, run: dict) -> dict:
        """Wire-format view: spec prompt trimmed, everything else as-is."""
        out = dict(run)
        spec = dict(out.get("spec") or {})
        prompt = str(spec.get("prompt") or "")
        if len(prompt) > 500:
            spec["prompt"] = prompt[:500] + "…"
        out["spec"] = spec
        return out

    # ------------------------------------------------------------------ #
    #  Dispatch + execution                                               #
    # ------------------------------------------------------------------ #

    async def _maybe_dispatch(self) -> None:
        """Promote queued pending runs into free concurrency slots (FIFO)."""
        if self._stopping:
            return
        async with self._dispatch_lock:
            limit = max(1, int(self.config.workflows.max_concurrent_runs))
            running = await self.db.count_workflow_runs("running")
            if running >= limit:
                return
            pending = await self.db.next_pending_workflow_runs(limit - running)
            for run in pending:
                if self._stopping:
                    return
                # Shielded so a cancelled caller (e.g. an _execute finally)
                # can never commit the pending->running CAS without also
                # creating the executor task (zombie 'running' row).
                await asyncio.shield(self._promote(run["id"]))

    async def _promote(self, run_id: str) -> None:
        flipped = await self.db.transition_workflow_run(
            run_id, "running", expect=("pending",),
        )
        if not flipped:
            return
        task = asyncio.create_task(
            self._execute(run_id), name=f"workflow-run-{run_id}",
        )
        self._exec_tasks[run_id] = task

    def _session_id(self, run_id: str) -> str:
        return f"{_SESSION_PREFIX}{run_id}"

    def _default_model(self, backend: str) -> str:
        if backend == "codex":
            return self.config.codex.model
        return self.config.agent.model

    def _effective_cwd(self, run: dict) -> str:
        return str(run["spec"].get("cwd") or self.config.workspace)

    def _build_prompt(self, run: dict) -> str:
        spec = run["spec"]
        budget = run.get("budget_usd")
        interval = self.config.workflows.poll_interval_seconds
        if budget:
            budget_line = (
                f"- Hard budget: ${budget:.2f}. Nerve meters real spend every "
                f"~{interval}s and STOPS this run at 100% — pace fan-out "
                "accordingly and checkpoint partial results early."
            )
        else:
            budget_line = "- No budget cap is set for this run."
        if run["engine"] == ENGINE_CODEX:
            orchestrate = (
                "- Orchestrate with Ultracode multi-agent runs when the task "
                "benefits from parallel workers."
            )
        else:
            orchestrate = (
                "- Orchestrate with the Workflow tool when the task benefits "
                "from parallel agents (you are pre-authorized to use it)."
            )
        return (
            f"[Workflow run {run['id']}] {run.get('title') or ''}\n"
            "You are executing a tracked, budget-capped workflow run.\n"
            f"{budget_line}\n"
            f"{orchestrate}\n"
            "- You run autonomously: interactive questions are auto-denied — "
            "never wait for user input.\n"
            "- End your final message with a concise result summary; it is "
            "recorded as this run's result.\n\n"
            "TASK:\n"
            f"{spec['prompt']}"
        )

    async def _execute(self, run_id: str) -> None:
        """Drive one run: session, prompt, terminal transition, journal."""
        session_id = self._session_id(run_id)
        current = asyncio.current_task()
        if current is not None:
            # Registered before the first await so engine.stop_session
            # (manual kill, budget kill, or a user stop on the session)
            # can cancel this exact task from its first suspension point.
            self.engine.register_task(session_id, current)
        run = await self.db.get_workflow_run(run_id)
        if run is None or run["status"] != "running":
            # A kill landed between the dispatch CAS and here — never
            # execute a payload for a terminal row.
            return
        backend = ENGINE_BACKENDS[run["engine"]]
        spec = run["spec"]
        model = str(spec.get("model") or "") or self._default_model(backend)
        metadata = None
        if backend == "codex" and spec.get("sandbox"):
            # Per-leg sandbox override, read at codex client build
            # (SessionSpec.extra["sandbox"] beats the global codex.sandbox).
            metadata = {"codex_sandbox": str(spec["sandbox"])}
        try:
            await self.engine.sessions.get_or_create(
                session_id,
                title=f"Workflow: {run.get('title') or run_id}",
                source="workflow",
                metadata=metadata,
                backend=backend,
                model=model,
                cwd=spec.get("cwd") or None,
            )
            await self.db.update_workflow_run(run_id, {"session_id": session_id})
            run = await self.db.get_workflow_run(run_id) or run
            self._journal_event(run, "started", {
                "session_id": session_id, "backend": backend, "model": model,
            })
            self._write_run_json(run)
            await self._broadcast(run)

            response = await self.engine.run(
                session_id=session_id,
                user_message=self._build_prompt(run),
                source="workflow",
                model=model,
                effort_override=str(spec.get("effort") or "") or None,
            )

            # engine.run returns normally even for interrupted, cancelled,
            # or errored turns — classify the outcome before recording it.
            fresh = await self.db.get_workflow_run(run_id)
            if fresh is None or fresh["status"] != "running":
                # A terminator (kill/budget/stop-listener) owns the row:
                # leave status AND spent_usd alone (a final re-meter here
                # would clobber the live-estimate figure with the ~$0 of a
                # never-finalized turn).
                return

            # Background tasks inside the CLI (run_in_background Bash /
            # Agent / Workflow) keep spending after the turn returns, and
            # their completion re-invokes the session with a follow-up
            # auto-turn that carries the REAL final text. Keep the run
            # 'running' — metered and budget-killable — until the session is
            # fully quiescent: background tasks drained, auto-turns finished,
            # plus a short settle grace for the bg-done → auto-turn-spawn gap.
            waited_bg = False
            grace_left = 2
            while True:
                busy = (
                    self.engine.has_live_background_tasks(session_id)
                    or self.engine.is_session_running(session_id)
                )
                if busy:
                    if not waited_bg:
                        waited_bg = True
                        self._journal_event(run, "waiting_background", {})
                    grace_left = 2
                elif not waited_bg:
                    break  # plain single-turn run — nothing pending
                elif grace_left > 0:
                    grace_left -= 1
                else:
                    break
                await asyncio.sleep(
                    min(3, max(1, int(self.config.workflows.poll_interval_seconds)))
                    if not busy else
                    min(15, max(2, int(self.config.workflows.poll_interval_seconds)))
                )
                fresh = await self.db.get_workflow_run(run_id)
                if fresh is None or fresh["status"] != "running":
                    return

            fresh = await self.db.get_workflow_run(run_id)
            if fresh is None or fresh["status"] != "running":
                return
            session = await self.db.get_session(session_id)
            session_status = str((session or {}).get("status") or "")
            if session_status in ("stopped", "error"):
                # The turn was stopped or died and engine.run swallowed it.
                await self._refresh_spend(run_id, final=True)
                to_status = "killed" if session_status == "stopped" else "failed"
                flipped = await self.db.transition_workflow_run(
                    run_id, to_status, expect=("running",),
                    error=f"session {session_status}",
                )
                if flipped:
                    await self._finalize_terminal(
                        run_id, to_status, {"reason": f"session {session_status}"},
                    )
                    if to_status == "failed" and not self._is_quiet(run):
                        await self._notify(
                            f"Workflow run {run_id} failed",
                            f"{run.get('title') or run['engine']}: session "
                            "errored mid-run; partial output is in the journal.",
                            priority="high",
                        )
                return

            await self._refresh_spend(run_id, final=True)
            result_text = response or ""
            if waited_bg:
                # A background-orchestrating run's final text lands in the
                # follow-up auto-turn, NOT in engine.run's return (which
                # holds only the pre-background text). The session's newest
                # assistant message is the authoritative final output.
                try:
                    messages = await self.db.get_messages(session_id, limit=50)
                    for msg in reversed(messages):
                        if msg.get("role") == "assistant" and str(msg.get("content") or "").strip():
                            result_text = str(msg["content"])
                            break
                except Exception:  # noqa: BLE001 — fall back to turn text
                    logger.exception("post-background result read failed for %s", run_id)
            flipped = await self.db.transition_workflow_run(
                run_id, "done", expect=("running",),
                result=result_text[-_RESULT_MAX:],
            )
            if flipped:
                self._write_result(run, result_text)
                await self._finalize_terminal(run_id, "done", {})
                fresh = await self.db.get_workflow_run(run_id)
                spent = (fresh or {}).get("spent_usd") or 0.0
                budget = (fresh or {}).get("budget_usd")
                budget_txt = f" of ${budget:.2f} budget" if budget else ""
                snippet = " ".join(result_text.split())
                if len(snippet) > 300:
                    snippet = snippet[:300] + "…"
                if not self._is_quiet(run):
                    await self._notify(
                        f"Workflow run {run_id} finished",
                        f"{run.get('title') or run['engine']} — spent "
                        f"${spent:.2f}{budget_txt}. Session: {session_id}"
                        + (f"\n\n{snippet}" if snippet else ""),
                    )
        except asyncio.CancelledError:
            # Backstop only: the stop listener / kill paths CAS the row
            # terminal before cancelling us, so this usually no-ops.
            with_status = await self.db.transition_workflow_run(
                run_id, "killed", expect=("running",),
                error="session stopped",
            )
            if with_status:
                await self._finalize_terminal(
                    run_id, "killed", {"reason": "session stopped"},
                )
        except Exception as e:  # noqa: BLE001 — terminal state must land
            logger.exception("Workflow run %s failed", run_id)
            try:
                await self._refresh_spend(run_id, final=True)
            except Exception:  # noqa: BLE001
                pass
            flipped = await self.db.transition_workflow_run(
                run_id, "failed", expect=("running",),
                error=str(e)[:_ERROR_MAX],
            )
            if flipped:
                await self._finalize_terminal(run_id, "failed", {"error": str(e)[:500]})
                if not self._is_quiet(run):
                    await self._notify(
                        f"Workflow run {run_id} failed",
                        f"{run.get('title') or run['engine']}: {str(e)[:500]}",
                        priority="high",
                    )
        finally:
            self._exec_tasks.pop(run_id, None)
            self._codex_live_base.pop(run_id, None)
            fresh = await self.db.get_workflow_run(run_id)
            if fresh:
                self._write_run_json(fresh)
            # Always tear down the run's client: a lingering CLI process
            # (kept alive for background tasks) would spend outside the
            # budget meter after the run is terminal. Detached so a
            # cancelled _execute still triggers it.
            self._spawn_detached(
                self._teardown_client(run_id), f"workflow-teardown-{run_id}",
            )
            # Free slot -> promote the queue. Detached: _execute may be
            # running its finally under cancellation, and dispatch must not
            # be interruptible mid-promotion.
            self._spawn_detached(self._maybe_dispatch(), f"workflow-dispatch-{run_id}")

    async def _on_session_stop(self, session_id: str) -> None:
        """Engine stop listener: a direct stop on a workflow session is a
        kill. CASes the run terminal BEFORE the stop lands so the swallowed
        interrupt/cancel can never be recorded as success. No-ops for
        non-workflow sessions and for runs a terminator already owns."""
        if not session_id.startswith(_SESSION_PREFIX):
            return
        run_id = session_id[len(_SESSION_PREFIX):]
        flipped = await self.db.transition_workflow_run(
            run_id, "killed", expect=("running",), error="session stopped",
        )
        if flipped:
            await self._finalize_terminal(run_id, "killed", {"reason": "session stopped"})

    async def _finalize_terminal(self, run_id: str, status: str, detail: dict) -> None:
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            return
        # Belt-and-braces vs budget escape: any wakeup a leg managed to
        # schedule must die with the run (a cron-fired wakeup would execute
        # after terminal, unmetered). schedule_wakeup also rejects workflow
        # sessions outright; this covers rows created before that guard.
        try:
            await self.db.cancel_wakeups_for_session(self._session_id(run_id))
        except Exception:  # noqa: BLE001
            logger.debug("wakeup cancellation failed for %s", run_id, exc_info=True)
        self._journal_event(run, status, detail)
        self._write_run_json(run)
        await self._broadcast(run)
        for listener in list(self._completion_listeners):
            try:
                await listener(run)
            except Exception:  # noqa: BLE001 — listeners never break run paths
                logger.exception(
                    "workflow run completion listener failed for %s", run_id,
                )

    @staticmethod
    def _is_quiet(run: dict | None) -> bool:
        """Loop-owned legs are reported by their controller, not per-run
        notifications (a 3-iteration loop would otherwise ping 6-8 times)."""
        return str((run or {}).get("created_by") or "").startswith("review-loop:")

    # ------------------------------------------------------------------ #
    #  Budget monitor                                                     #
    # ------------------------------------------------------------------ #

    async def _monitor_loop(self, interval: int) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("workflow run monitor tick failed")

    async def _tick(self) -> None:
        runs = await self.db.list_workflow_runs(status="running", limit=200)
        for stale in runs:
            spent = await self._refresh_spend(stale["id"])
            # Re-read: the run may have finished/been killed while (or
            # since) we metered it — budget actions only apply to rows
            # still running.
            run = await self.db.get_workflow_run(stale["id"])
            if run is None or run["status"] != "running":
                continue
            budget = run.get("budget_usd")
            if not budget or budget <= 0:
                continue
            frac = spent / budget
            if frac >= 1.0:
                await self._budget_kill(run, spent)
            elif frac >= self.config.workflows.warn_fraction and not run.get("warned_at"):
                await self.db.update_workflow_run(
                    run["id"], {"warned_at": utc_now_iso()},
                )
                self._journal_event(run, "budget_warning", {
                    "spent_usd": spent, "budget_usd": budget,
                })
                if not self._is_quiet(run):
                    await self._notify(
                        f"Workflow run {run['id']} at "
                        f"{int(frac * 100)}% of budget",
                        f"{run.get('title') or run['engine']} — ${spent:.2f} of "
                        f"${budget:.2f}. It will be stopped at 100%.",
                        priority="high",
                    )
        await self._maybe_dispatch()

    async def _refresh_spend(self, run_id: str, final: bool = False) -> float:
        """Re-meter a run; persists + broadcasts when the number moved.

        Terminal rows are never touched: their spend was settled by
        whichever path finished them, and a straggling monitor tick must
        not clobber it (or resurrect codex live-base state).
        """
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            return 0.0
        if run["status"] != "running":
            return float(run.get("spent_usd") or 0.0)
        session_id = run.get("session_id")
        if not session_id:
            return float(run.get("spent_usd") or 0.0)
        recorded = await self.db.get_session_effective_cost(session_id)
        live = 0.0 if final else self._live_estimate(run)
        spent = round(recorded + live, 6)
        if abs(spent - float(run.get("spent_usd") or 0.0)) > 1e-9:
            await self.db.update_workflow_run(run_id, {"spent_usd": spent})
            fresh = await self.db.get_workflow_run(run_id)
            if fresh:
                await self._broadcast(fresh)
        return spent

    def _live_estimate(self, run: dict) -> float:
        """In-flight (mid-turn) spend estimate. Best effort, never raises."""
        try:
            if run["engine"] == ENGINE_CLAUDE:
                return self._live_estimate_claude(run)
            return self._live_estimate_codex(run)
        except Exception:  # noqa: BLE001
            logger.debug("live estimate failed for %s", run["id"], exc_info=True)
            return 0.0

    def _live_estimate_claude(self, run: dict) -> float:
        session_id = run.get("session_id") or ""
        if not self.engine.is_session_running(session_id):
            # Between turns the recorded cost is authoritative; counting
            # snapshots here would double-bill the just-recorded turn.
            return 0.0
        tokens = self.engine.get_live_workflow_tokens(session_id)
        if tokens <= 0:
            return 0.0
        spec_model = str(run["spec"].get("model") or "") or self._default_model("claude")
        return estimate_turn_cost({"output_tokens": tokens}, spec_model)

    def _live_estimate_codex(self, run: dict) -> float:
        """Price Ultracode journals attributed to this run.

        Attribution prefers the exact run-id set the session's live Codex
        client tracked for its current turn; the fallback is heuristic
        (created since this run started + same effective cwd), which can
        cross-attribute concurrent same-cwd runs mid-turn. Journals fold
        into recorded turn cost at each turn end, so the between-turns
        total becomes the subtraction base and only the current turn's
        delta counts as live.
        """
        from nerve.agent.backends.codex import ultracode

        session_id = run.get("session_id") or ""
        run_dirpath = Path(self.config.codex.home_dir).expanduser() / "ultracode" / "runs"
        if not run_dirpath.is_dir():
            return 0.0
        exact_ids = self.engine.get_codex_ultracode_run_ids(session_id)
        started = _iso_to_epoch(run.get("started_at"))
        run_cwd = self._effective_cwd(run)
        total = 0.0
        for path in run_dirpath.glob("ultra-*.json"):
            if exact_ids is not None:
                if path.stem not in exact_ids:
                    continue
            else:
                # started_at is stamped at dispatch, before the session's
                # CLI exists — this run's journals can never predate it.
                try:
                    if path.stat().st_mtime < started:
                        continue
                except OSError:
                    continue
            journal = ultracode.read_verified_run_journal(self.config, path.stem)
            if not journal:
                continue
            if exact_ids is None:
                jcwd = str(journal.get("cwd") or "")
                if jcwd and run_cwd and jcwd != run_cwd:
                    continue
            total += self._journal_cost(journal, run)
        if not self.engine.is_session_running(session_id):
            # All settled work is (about to be) folded into recorded cost.
            self._codex_live_base[run["id"]] = total
            return 0.0
        base = self._codex_live_base.get(run["id"], 0.0)
        return max(0.0, total - base)

    def _journal_cost(self, journal: dict, run: dict) -> float:
        """Dollar-price one Ultracode journal (per-worker models when known,
        aggregate as fallback when worker rows carry no usable usage)."""
        from nerve.agent.backends.codex.pricing import match_pricing

        parent_model = str(run["spec"].get("model") or "") or self._default_model("codex")
        table = self.config.codex.pricing

        def price(usage: dict, model: str | None) -> float:
            prices = match_pricing(model or parent_model, table)
            if not prices:
                return 0.0
            input_t = int(usage.get("input_tokens") or 0)
            cached = int(usage.get("cached_input_tokens") or 0)
            output_t = int(usage.get("output_tokens") or 0)
            fresh = max(0, input_t - cached)
            return (
                fresh * float(prices.get("input") or 0.0)
                + cached * float(prices.get("cached_input") or 0.0)
                + output_t * float(prices.get("output") or 0.0)
            ) / 1_000_000

        workers = journal.get("workers")
        if isinstance(workers, list) and workers:
            total = 0.0
            for w in workers:
                if not isinstance(w, dict):
                    continue
                total += price(w.get("usage") or {}, w.get("model"))
            if total > 0:
                return total
            # Worker rows without usable usage/pricing — don't silently
            # meter $0 when the aggregate is available.
        return price(journal.get("aggregate_usage") or {}, None)

    # ------------------------------------------------------------------ #
    #  Termination                                                        #
    # ------------------------------------------------------------------ #

    async def _budget_kill(self, run: dict, spent: float) -> None:
        budget = float(run.get("budget_usd") or 0.0)
        flipped = await self.db.transition_workflow_run(
            run["id"], "budget_exhausted", expect=("running",),
            error=f"budget exhausted: ${spent:.2f} of ${budget:.2f}",
        )
        if not flipped:
            return
        await self._finalize_terminal(run["id"], "budget_exhausted", {
            "spent_usd": spent, "budget_usd": budget,
        })
        if not self._is_quiet(run):
            await self._notify(
                f"Workflow run {run['id']} stopped: budget exhausted",
                f"{run.get('title') or run['engine']} hit its ${budget:.2f} "
                f"budget (metered ${spent:.2f}). The run's session was stopped; "
                f"partial results are in its journal: {run.get('journal_dir')}",
                priority="high",
            )
        self._spawn_enforcement(run["id"])

    def _spawn_detached(self, coro: Any, name: str) -> None:
        """Fire-and-forget a coroutine, keeping it referenced until done."""
        task = asyncio.create_task(coro, name=name)
        self._kill_tasks.add(task)
        task.add_done_callback(self._kill_tasks.discard)

    def _spawn_enforcement(self, run_id: str) -> None:
        """Fire-and-forget the stop sequence so callers return immediately."""
        self._spawn_detached(
            self._stop_session_hard(run_id), f"workflow-kill-{run_id}",
        )

    async def _teardown_client(self, run_id: str) -> None:
        """Discard the run session's client (kills its CLI subprocess /
        process group, including lingering background tasks). Safe when no
        client exists. Does NOT go through engine.stop_session — that would
        cancel a still-finalizing _execute task."""
        session_id = self._session_id(run_id)
        try:
            await self.engine._discard_client(  # noqa: SLF001 — engine-internal by design
                session_id, background_memorize=True,
            )
        except Exception:  # noqa: BLE001
            logger.exception("client teardown failed for %s", session_id)

    async def _stop_session_hard(self, run_id: str) -> None:
        """Graceful interrupt → grace window → force client discard.

        Scoped strictly to the run's own session: the Claude backend kills
        its own CLI subprocess, the Codex backend killpg's its own
        app-server group. The force discard also covers the keepalive path
        where a live background task would otherwise pin the client (and
        its subprocess) alive indefinitely.
        """
        run = await self.db.get_workflow_run(run_id)
        # The session id is deterministic — enforce even when the row never
        # recorded it (kill racing the earliest phase of _execute).
        session_id = (run or {}).get("session_id") or self._session_id(run_id)
        try:
            await self.engine.stop_session(session_id)
        except Exception:  # noqa: BLE001
            logger.exception("graceful stop failed for %s", session_id)
        grace = max(0, int(self.config.workflows.kill_grace_seconds))
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if not self.engine.is_session_running(session_id):
                break
            await asyncio.sleep(1)
        try:
            # Force-discard even when not "running": kills a parked client
            # kept alive for background tasks.
            await self.engine._discard_client(  # noqa: SLF001 — engine-internal by design
                session_id, background_memorize=True,
            )
        except Exception:  # noqa: BLE001
            logger.exception("force discard failed for %s", session_id)
        if run:
            self._journal_event(run, "enforced_stop", {"session_id": session_id})

    # ------------------------------------------------------------------ #
    #  Journals, notifications, broadcast                                 #
    # ------------------------------------------------------------------ #

    def _journal_event(self, run: dict, event: str, detail: dict | None) -> None:
        journal_dir = run.get("journal_dir")
        if not journal_dir:
            return
        try:
            path = Path(journal_dir) / "events.ndjson"
            line = json.dumps({
                "ts": utc_now_iso(), "run_id": run["id"], "event": event,
                **(detail or {}),
            })
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            logger.warning("journal write failed for %s", run.get("id"))

    def _write_run_json(self, run: dict) -> None:
        journal_dir = run.get("journal_dir")
        if not journal_dir:
            return
        try:
            path = Path(journal_dir) / "run.json"
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(run, indent=2, default=str), encoding="utf-8",
            )
            tmp.replace(path)
        except OSError:
            logger.warning("run.json write failed for %s", run.get("id"))

    def _write_result(self, run: dict, text: str) -> None:
        journal_dir = run.get("journal_dir")
        if not journal_dir or not text:
            return
        try:
            (Path(journal_dir) / "result.md").write_text(text, encoding="utf-8")
        except OSError:
            logger.warning("result.md write failed for %s", run.get("id"))

    async def _notify(self, title: str, body: str, priority: str = "normal") -> None:
        svc = getattr(self.engine, "notification_service", None)
        if svc is None:
            return
        try:
            await svc.send_notification(
                session_id="system", title=title, body=body, priority=priority,
            )
        except Exception:  # noqa: BLE001
            logger.exception("workflow run notification failed")

    async def _broadcast(self, run: dict) -> None:
        try:
            await broadcaster.broadcast("__global__", {
                "type": "workflow_run_update",
                "session_id": run.get("session_id"),
                "run": self.public_run(run),
            })
        except Exception:  # noqa: BLE001
            logger.debug("workflow run broadcast failed", exc_info=True)
