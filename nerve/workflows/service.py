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
                Ultracode run journals under the codex home, minus the
                already-folded base)

At ``warn_fraction`` a one-time notification fires; at 100% the run is
terminated: graceful ``engine.stop_session`` (interrupt), a grace window,
then a force client discard — which kills the session's own CLI
subprocess / process group and nothing else. Accuracy caveat: ``recorded``
lands at turn end, so the overshoot bound is roughly one turn's cost
beyond what the live estimates catch.

Runs do not survive a daemon restart: the startup recovery pass marks
orphaned active runs ``failed`` and notifies, so a paid job can never
burn unmetered.
"""

from __future__ import annotations

import asyncio
import json
import logging
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
_SPEC_KEYS = {"prompt", "model", "effort", "cwd"}

# Cap stored result/error text.
_RESULT_MAX = 4000
_ERROR_MAX = 2000


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
        # Detached kill/enforcement tasks (kept referenced until done).
        self._kill_tasks: set[asyncio.Task] = set()
        self._dispatch_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    #  Lifespan                                                           #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Recovery pass + monitor loop startup."""
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
        if orphaned:
            ids = ", ".join(r["id"] for r in orphaned)
            await self._notify(
                "Workflow runs interrupted by restart",
                f"{len(orphaned)} active workflow run(s) were marked failed "
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

    async def start_run(
        self,
        engine_kind: str,
        spec: dict,
        budget_usd: float | None,
        title: str = "",
        created_by: str = "user",
    ) -> dict:
        """Validate, persist, journal, and (slots permitting) dispatch."""
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

        budget = float(budget_usd) if budget_usd is not None else None
        if budget is not None and budget <= 0:
            budget = None
        if budget is None and not self.config.workflows.allow_unbudgeted:
            raise WorkflowRunError(
                "budget_usd is required (> 0). Unbudgeted runs are disabled; "
                "set workflows.allow_unbudgeted: true to permit them."
            )

        run_id = f"wfr-{uuid.uuid4().hex[:8]}"
        journal_dir = self.runs_dir() / run_id
        try:
            journal_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as e:
            raise WorkflowRunError(f"cannot create journal dir: {e}") from e

        run = await self.db.create_workflow_run(
            run_id, engine_kind, spec, budget,
            title=title.strip(), created_by=created_by,
            journal_dir=str(journal_dir),
        )
        self._journal_event(run, "created", {
            "engine": engine_kind, "budget_usd": budget,
            "created_by": created_by,
        })
        self._write_run_json(run)
        await self._broadcast(run)
        await self._maybe_dispatch()
        return await self.db.get_workflow_run(run_id) or run

    async def kill_run(self, run_id: str, reason: str = "", killed_by: str = "") -> dict:
        """Terminate a run. Scoped strictly to the run's own session."""
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            raise WorkflowRunError(f"no such workflow run: {run_id}")
        detail = reason.strip() or "killed"
        if killed_by:
            detail = f"{detail} (by {killed_by})"

        if run["status"] == "pending":
            flipped = await self.db.transition_workflow_run(
                run_id, "killed", expect=("pending",), error=detail,
            )
            if flipped:
                await self._finalize_terminal(run_id, "killed", {"reason": detail})
            return await self.db.get_workflow_run(run_id) or run

        flipped = await self.db.transition_workflow_run(
            run_id, "killed", expect=("running",), error=detail,
        )
        if not flipped:
            # Already terminal — idempotent.
            return await self.db.get_workflow_run(run_id) or run
        await self._finalize_terminal(run_id, "killed", {"reason": detail})
        self._spawn_enforcement(run_id)
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
        """Promote queued pending runs into free concurrency slots."""
        async with self._dispatch_lock:
            limit = max(1, int(self.config.workflows.max_concurrent_runs))
            running = await self.db.count_workflow_runs("running")
            if running >= limit:
                return
            pending = await self.db.list_workflow_runs(
                status="pending", limit=limit - running,
            )
            # list is newest-first; dispatch oldest-first
            for run in reversed(pending):
                flipped = await self.db.transition_workflow_run(
                    run["id"], "running", expect=("pending",),
                )
                if not flipped:
                    continue
                task = asyncio.create_task(
                    self._execute(run["id"]), name=f"workflow-run-{run['id']}",
                )
                self._exec_tasks[run["id"]] = task

    def _session_id(self, run_id: str) -> str:
        return f"workflow:{run_id}"

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
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            return
        session_id = self._session_id(run_id)
        backend = ENGINE_BACKENDS[run["engine"]]
        spec = run["spec"]
        model = str(spec.get("model") or "") or self._default_model(backend)
        try:
            await self.engine.sessions.get_or_create(
                session_id,
                title=f"Workflow: {run.get('title') or run_id}",
                source="workflow",
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

            current = asyncio.current_task()
            if current is not None:
                # Lets engine.stop_session (manual kill, budget kill, or a
                # user stop on the session) cancel this exact task.
                self.engine.register_task(session_id, current)

            response = await self.engine.run(
                session_id=session_id,
                user_message=self._build_prompt(run),
                source="workflow",
                model=model,
                effort_override=str(spec.get("effort") or "") or None,
            )

            await self._refresh_spend(run_id, final=True)
            flipped = await self.db.transition_workflow_run(
                run_id, "done", expect=("running",),
                result=(response or "")[-_RESULT_MAX:],
            )
            if flipped:
                self._write_result(run, response or "")
                await self._finalize_terminal(run_id, "done", {})
                fresh = await self.db.get_workflow_run(run_id)
                spent = (fresh or {}).get("spent_usd") or 0.0
                budget = (fresh or {}).get("budget_usd")
                budget_txt = f" of ${budget:.2f} budget" if budget else ""
                await self._notify(
                    f"Workflow run {run_id} finished",
                    f"{run.get('title') or run['engine']} — spent "
                    f"${spent:.2f}{budget_txt}. Session: {session_id}",
                )
        except asyncio.CancelledError:
            # Terminator (kill/budget) flipped status before cancelling us;
            # a user stop on the session lands here with status still
            # 'running' — record it as killed.
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
            # Free slot → promote the queue.
            try:
                await self._maybe_dispatch()
            except Exception:  # noqa: BLE001
                logger.exception("workflow dispatch after %s failed", run_id)

    async def _finalize_terminal(self, run_id: str, status: str, detail: dict) -> None:
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            return
        self._journal_event(run, status, detail)
        self._write_run_json(run)
        await self._broadcast(run)

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
        for run in runs:
            spent = await self._refresh_spend(run["id"])
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
                await self._notify(
                    f"Workflow run {run['id']} at "
                    f"{int(frac * 100)}% of budget",
                    f"{run.get('title') or run['engine']} — ${spent:.2f} of "
                    f"${budget:.2f}. It will be stopped at 100%.",
                    priority="high",
                )
        await self._maybe_dispatch()

    async def _refresh_spend(self, run_id: str, final: bool = False) -> float:
        """Re-meter a run; persists + broadcasts when the number moved."""
        run = await self.db.get_workflow_run(run_id)
        if run is None:
            return 0.0
        session_id = run.get("session_id")
        if not session_id:
            return float(run.get("spent_usd") or 0.0)
        recorded = await self.db.get_session_effective_cost(session_id)
        live = 0.0 if final else self._live_estimate(run)
        spent = round(recorded + live, 6)
        if abs(spent - float(run.get("spent_usd") or 0.0)) > 1e-9:
            await self.db.update_workflow_run(run_id, {"spent_usd": spent})
            run["spent_usd"] = spent
            await self._broadcast(run)
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
        tokens = self.engine.get_live_workflow_tokens(session_id)
        if tokens <= 0:
            return 0.0
        spec_model = str(run["spec"].get("model") or "") or self._default_model("claude")
        return estimate_turn_cost({"output_tokens": tokens}, spec_model)

    def _live_estimate_codex(self, run: dict) -> float:
        """Price Ultracode journals created since this run started.

        Journals fold into recorded turn cost at each turn end, so the
        between-turns total becomes the subtraction base and only the
        current turn's delta counts as live. Attribution is heuristic
        (start-time + cwd match): concurrent codex runs sharing one cwd
        may cross-attribute mid-turn; the turn-end fold is exact.
        """
        from nerve.agent.backends.codex import ultracode

        session_id = run.get("session_id") or ""
        run_dirpath = Path(self.config.codex.home_dir).expanduser() / "ultracode" / "runs"
        if not run_dirpath.is_dir():
            return 0.0
        started = _iso_to_epoch(run.get("started_at"))
        run_cwd = self._effective_cwd(run)
        total = 0.0
        for path in run_dirpath.glob("ultra-*.json"):
            try:
                if path.stat().st_mtime < started - 60:
                    continue
            except OSError:
                continue
            journal = ultracode.read_verified_run_journal(self.config, path.stem)
            if not journal:
                continue
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
        """Dollar-price one Ultracode journal (per-worker models when known)."""
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
            return total
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
        await self._notify(
            f"Workflow run {run['id']} stopped: budget exhausted",
            f"{run.get('title') or run['engine']} hit its ${budget:.2f} "
            f"budget (metered ${spent:.2f}). The run's session was stopped; "
            f"partial results are in its journal: {run.get('journal_dir')}",
            priority="high",
        )
        self._spawn_enforcement(run["id"])

    def _spawn_enforcement(self, run_id: str) -> None:
        """Fire-and-forget the stop sequence so callers return immediately."""
        task = asyncio.create_task(
            self._stop_session_hard(run_id), name=f"workflow-kill-{run_id}",
        )
        self._kill_tasks.add(task)
        task.add_done_callback(self._kill_tasks.discard)

    async def _stop_session_hard(self, run_id: str) -> None:
        """Graceful interrupt → grace window → force client discard.

        Scoped strictly to the run's own session: the Claude backend kills
        its own CLI subprocess, the Codex backend killpg's its own
        app-server group. The force discard also covers the keepalive path
        where a live background task would otherwise pin the client (and
        its subprocess) alive indefinitely.
        """
        run = await self.db.get_workflow_run(run_id)
        session_id = (run or {}).get("session_id")
        if not session_id:
            return
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
