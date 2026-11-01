"""Deterministic polling, durable outbox dispatch and result reconciliation."""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from nerve.cron.service import _crontab_to_trigger

from nerve import paths
from nerve.code_review.artifacts import freeze, read_output, validate
from nerve.code_review.github import GitHub, ObservationError
from nerve.code_review.prompt import review_prompt
from nerve.db.code_review import digest, encode
from nerve.identity import attribution_for
from nerve.tasks.files import move_task_file
from nerve.tasks.locks import task_mutation
from nerve.utils.fs import atomic_write_text
from nerve.utils.time import utc_now_iso

logger = logging.getLogger(__name__)
ACTIVE = {"pending", "running"}


class CodeReviewService:
    def __init__(self, config, db, runs, notifications=None, github=None):
        self.config = config
        self.db = db
        self.runs = runs
        self.notifications = notifications
        self.github = github or GitHub()
        self.scheduler = AsyncIOScheduler()
        self.lock = asyncio.Lock()
        self.observation_locks = {}
        self.started = False
        self.last_tick_at = None
        self.worker_error = ""
        self.worker_error_count = 0
        self._tasks = set()
        self._lease = None
        self.root = Path(config.workspace) / "memory" / "code-reviews"

    def profile(self, profile_id):
        profile = next((p for p in self.config.code_review.profiles if p.id == profile_id), None)
        if profile is None:
            raise KeyError(profile_id)
        return profile

    def day(self, profile):
        return datetime.now(ZoneInfo(profile.timezone)).date().isoformat()

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def start(self):
        # A second daemon must never consume an intent while the first is
        # between durable reservation and workflow creation.
        lock_path = paths.nerve_path("code-review.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lease = lock_path.open("a")
        try:
            fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._lease.close()
            self._lease = None
            raise RuntimeError("another Code Review controller holds the lease") from None
        await self.db.cr_profiles(p.id for p in self.config.code_review.profiles)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        for profile in self.config.code_review.profiles:
            if profile.enabled:
                self.scheduler.add_job(
                    self.scheduled, _crontab_to_trigger(profile.schedule, timezone=ZoneInfo(profile.timezone)),
                    args=[self.poll, profile.id], id=f"code-review-{profile.id}", max_instances=1,
                    coalesce=True, misfire_grace_time=7200,
                )
        self.scheduler.add_job(self.scheduled, "interval", args=[self.tick], seconds=20, id="code-review-worker", max_instances=1, coalesce=True)
        if self.runs:
            self.runs.add_dispatch_guard(self.guard_dispatch)
        self.started = True
        # Exactly one startup catch-up. Dispatch waits for this observation.
        self.spawn(self._startup())

    async def scheduled(self, function, *args):
        if self.started:
            await self.spawn(function(*args))

    async def _startup(self):
        try:
            await self.reconcile()
            for profile in self.config.code_review.profiles:
                if profile.enabled:
                    await self.poll(profile.id)
        finally:
            if self.started:
                self.scheduler.start()

    async def stop(self):
        self.started = False
        # Keep the guard installed until the workflow service stops. It fails
        # closed while this controller is stopped, including pending runs.
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._lease:
            self._lease.close()
            self._lease = None

    async def allowed(self, profile):
        state = await self.db.cr_one("SELECT * FROM code_review_profiles WHERE id=?", (profile.id,))
        return profile.enabled and state and not state["paused"]

    async def pause(self, profile_id):
        self.profile(profile_id)  # validate before writing
        await self.db._write("UPDATE code_review_profiles SET paused=1 WHERE id=?", (profile_id,))
        # Only runs linked by our durable ledger belong to this controller.
        attempts = await self.db.cr_rows("SELECT * FROM code_review_attempts WHERE profile=? AND settled=0", (profile_id,))
        if self.runs:
            for attempt in attempts:
                run = await self.runs.get_run(attempt["run_id"])
                if run and run["status"] in ACTIVE:
                    await self.runs.kill_run(run["id"], reason="Code Review profile paused")

    async def resume(self, profile_id):
        profile = self.profile(profile_id)
        if not profile.enabled:
            raise ValueError("Enable the profile in reviewed configuration first")
        if not self.started:
            raise ValueError("Code Review controller is stopped")
        await self.db._write("UPDATE code_review_profiles SET paused=0 WHERE id=?", (profile_id,))
        self.spawn(self.poll(profile_id))

    async def observe(self, profile, number):
        # Dispatch guards also observe while the polling/queue lock is held.
        # A separate per-PR lock serializes acquisition through persistence,
        # without deadlocking start_run's inline guard.
        lock = self.observation_locks.setdefault((profile.id, number), asyncio.Lock())
        async with lock:
            return await self._observe(profile, number)

    async def _observe(self, profile, number):
        pr = await self.github.pr(profile.repository, number)
        if pr["base"]["repo"]["full_name"].lower() != profile.repository.lower():
            raise ObservationError("repository identity mismatch")
        snapshot = None
        if pr["state"] == "open" and not pr.get("draft"):
            snapshot = await self.github.snapshot(profile.repository, pr)
        row = await self.db.cr_observe(profile.id, pr, snapshot)
        if row:
            if snapshot and row["current_version"]:
                version = await self.db.cr_one("SELECT * FROM code_review_versions WHERE id=?", (row["current_version"],))
                # Original CI evidence stays immutable; fresh evidence is in events.
                freeze(self.root / row["id"] / version["id"] / "snapshot.json", json.loads(version["snapshot"]))
            await self.sync_task(row)
        return row

    async def poll(self, profile_id):
        profile = self.profile(profile_id)
        if not await self.allowed(profile):
            return
        async with self.lock:
            errors = []
            numbers = set()
            try:
                numbers.update(await self.github.search(profile))
            except Exception as error:
                errors.append(str(error))
            # Includes closed PRs: reopen detection must not depend on team search.
            known = await self.db.cr_rows("SELECT * FROM code_review_prs WHERE profile=?", (profile_id,))
            numbers.update(row["number"] for row in known)
            for number in sorted(numbers):
                try:
                    await self.observe(profile, number)
                except Exception as error:
                    message = str(error)[:500]
                    errors.append(f"#{number}: {message}")
                    await self.db._write(
                        "UPDATE code_review_prs SET error=?,last_polled_at=? WHERE profile=? AND number=?",
                        (message, utc_now_iso(), profile_id, number),
                    )
            now = utc_now_iso()
            await self.db._write(
                """UPDATE code_review_profiles SET last_polled_at=?,
                last_success_at=CASE WHEN ?='' THEN ? ELSE last_success_at END,error=?,
                error_count=CASE WHEN ?='' THEN 0 ELSE error_count+1 END WHERE id=?""",
                (now, "\n".join(errors), now, "\n".join(errors), "\n".join(errors), profile_id),
            )
        await self.notify()

    async def tick(self):
        if self.lock.locked() or not self.started:
            return
        async with self.lock:
            try:
                await self.reconcile()
                for profile in self.config.code_review.profiles:
                    if await self.allowed(profile):
                        await self.dispatch(profile)
                await self.notify()
                self.last_tick_at = utc_now_iso()
                self.worker_error = ""
                self.worker_error_count = 0
            except Exception as error:
                self.worker_error = str(error)[:500]
                self.worker_error_count += 1
                logger.exception("Code Review worker tick failed")
                if self.worker_error_count == 3 and self.notifications:
                    await self.notifications.send_notification(
                        session_id="system", title="Code Review worker failed",
                        body=self.worker_error, priority="high",
                    )

    async def guard_dispatch(self, run):
        if not run.get("created_by", "").startswith("code-review:"):
            return ""
        if not self.started:
            return "Code Review controller is stopped"
        try:
            return await self.spawn(self._guard_dispatch(run))
        except asyncio.CancelledError:
            return "Code Review controller stopped during validation"

    async def _guard_dispatch(self, run):
        if not self.started:
            return "Code Review controller is stopped"
        attempt = await self.db.cr_one("SELECT * FROM code_review_attempts WHERE run_id=?", (run["id"],))
        if not attempt or attempt["settled"]:
            return "Code Review reservation is unavailable"
        profile = self.profile(attempt["profile"])
        if not await self.allowed(profile):
            return "Code Review profile is paused or disabled"
        version = await self.db.cr_one("SELECT * FROM code_review_versions WHERE id=?", (attempt["version_id"],))
        snapshot = json.loads(version["snapshot"])
        row = await self.observe(profile, snapshot["number"])
        if not self.started or not await self.allowed(profile):
            return "Code Review controller is stopped or profile paused"
        if not row or row["draft"] or row["state"] != "open" or row["current_version"] != version["id"]:
            return "PR changed before workflow execution"
        return ""

    async def dispatch(self, profile):
        if not self.runs:
            return
        if await self.db.cr_one("SELECT run_id FROM code_review_attempts WHERE profile=? AND settled=0", (profile.id,)):
            return
        if await self.db.cr_day_spend(self.day(profile)) >= profile.daily_budget_usd - 0.001:
            await self.db._write(
                "UPDATE code_review_versions SET status='waiting_budget' WHERE status IN ('queued','retry') "
                "AND pr_id IN (SELECT id FROM code_review_prs WHERE profile=? AND state='open' AND draft=0)", (profile.id,),
            )
            await self.db._write(
                "UPDATE code_review_prs SET review_state='waiting_budget' WHERE profile=? AND state='open' AND draft=0 "
                "AND current_version IN (SELECT id FROM code_review_versions WHERE status='waiting_budget')", (profile.id,),
            )
            return
        candidate = await self.db.cr_one(
            """SELECT p.* FROM code_review_prs p JOIN code_review_versions v ON v.id=p.current_version
            WHERE p.profile=? AND p.state='open' AND p.draft=0 AND p.error=''
            AND v.status IN ('queued','waiting_budget','retry') AND v.retry_at<=?
            ORDER BY p.queue_since,p.id LIMIT 1""", (profile.id, utc_now_iso()),
        )
        if not candidate:
            return
        prior_spend = await self.db.cr_one(
            "SELECT COUNT(*) n,COALESCE(SUM(spent+uncertain),0) used FROM code_review_attempts WHERE version_id=?", (candidate["current_version"],),
        )
        if prior_spend["n"] >= profile.max_attempts or prior_spend["used"] >= profile.version_budget_usd - 0.001:
            await self.db.cr_reserve(profile.id, self.day(profile), profile.version_budget_usd,
                                     profile.daily_budget_usd, profile.max_attempts, candidate["current_version"])
            return
        # Refresh full context immediately before reserving; draft transition or
        # new code can never knowingly launch against a stale queued version.
        try:
            refreshed = await self.observe(profile, candidate["number"])
        except Exception as error:
            await self.db._write("UPDATE code_review_prs SET error=? WHERE id=?", (str(error)[:500], candidate["id"]))
            return
        # Claim only after refreshing this FIFO candidate; the store is atomic.
        if not refreshed or refreshed["draft"] or refreshed["state"] != "open":
            return
        claim = await self.db.cr_reserve(profile.id, self.day(profile), profile.version_budget_usd,
                                         profile.daily_budget_usd, profile.max_attempts,
                                         version_id=refreshed["current_version"])
        if not claim:
            return
        work = self.root / claim["pr_id"] / claim["id"] / "attempts" / claim["run_id"] / "work"
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        work.mkdir(parents=True, exist_ok=True, mode=0o700)
        work.chmod(0o700)
        snapshot = json.loads(claim["snapshot"])
        try:
            atomic_write_text(work / "snapshot.json", encode(snapshot))
            atomic_write_text(work / "sources.json", encode(await self.github.sources(snapshot)))
            previous = await self.db.cr_rows(
                "SELECT artifact_path FROM code_review_versions WHERE pr_id=? AND status='complete' ORDER BY created_at DESC LIMIT 1",
                (claim["pr_id"],),
            )
            prior = "none"
            if previous and previous[0]["artifact_path"]:
                prior = "previous.json (untrusted historical review; recheck findings)"
                atomic_write_text(work / "previous.json", (self.root / previous[0]["artifact_path"]).read_text())
            latest = await self.observe(profile, claim["number"])
            if not latest or latest["draft"] or latest["state"] != "open" or latest["current_version"] != claim["id"] or not await self.allowed(profile):
                await self.db.cr_settle(claim["run_id"], 0, self.day(profile), "superseded", "", "changed before dispatch")
                return
            backend = self.config.agent.backend
            spec = {"prompt": review_prompt(snapshot, prior), "cwd": str(work)}
            if backend == "codex":
                spec.update(model=self.config.codex.model, sandbox="workspace-write")
            else:
                spec["model"] = self.config.agent.model
            await self.runs.start_run(
                "codex-ultracode" if backend == "codex" else "claude-workflow", spec, claim["budget"],
                title=f"Code Review #{claim['number']} · {claim['head'][:12]}",
                created_by=f"code-review:{profile.id}", run_id=claim["run_id"],
                attribution=attribution_for(self.db.system_actor),
            )
        except Exception as error:
            # A start call can fail after persisting its run; never release the
            # reservation until the durable run has actually settled.
            if await self.runs.get_run(claim["run_id"]):
                return
            await self.db.cr_settle(
                claim["run_id"], 0, self.day(profile), "retry", "", str(error)[:1000],
                retry_at=(datetime.now(timezone.utc) + timedelta(minutes=10 * claim["attempt"])).isoformat(),
            )

    async def reconcile(self):
        await self.revalidate()
        attempts = await self.db.cr_rows("SELECT * FROM code_review_attempts WHERE settled=0 ORDER BY created_at")
        for attempt in attempts:
            profile = self.profile(attempt["profile"])
            run = await self.runs.get_run(attempt["run_id"]) if self.runs else None
            if run and run["status"] in ACTIVE:
                await self.db._write("UPDATE code_review_attempts SET spent=MAX(spent,?) WHERE run_id=?",
                                     (run.get("spent_usd") or 0, run["id"]))
                pr = await self.db.cr_one(
                    "SELECT p.* FROM code_review_prs p JOIN code_review_versions v ON v.pr_id=p.id WHERE v.id=?",
                    (attempt["version_id"],),
                )
                if pr["draft"] or pr["state"] != "open" or not await self.allowed(profile):
                    await self.runs.kill_run(run["id"], reason="Code Review PR unavailable or profile disabled")
                continue
            final_spend = await self.runs.settled_spend(attempt["run_id"]) if self.runs else attempt["reserved"]
            if final_spend is None:
                continue
            version = await self.db.cr_one("SELECT * FROM code_review_versions WHERE id=?", (attempt["version_id"],))
            snapshot = json.loads(version["snapshot"])
            directory = self.root / version["pr_id"] / version["id"] / "attempts" / attempt["run_id"]
            artifact = directory / "result.json"
            error, output, verdict = "", {}, {}
            status = "retry"
            try:
                if artifact.exists():
                    saved = json.loads(artifact.read_text())
                    output = saved["output"]
                else:
                    output, output_errors = read_output(directory / "work")
                    if output_errors:
                        raise ValueError("; ".join(output_errors))
                if not run or run["status"] != "done":
                    raise ValueError((run or {}).get("error") or "run interrupted before completion")
                verdict = validate(output, snapshot)
                # Admission retries are read-only: a temporary GitHub failure
                # must not pay for a second audit of an already finished report.
                try:
                    fresh = await self.observe(profile, snapshot["number"])
                except Exception as observation_error:
                    if not artifact.exists():
                        freeze(artifact, {"version": version["id"], "run": attempt["run_id"],
                                          "output": output, "admission": "awaiting_validation",
                                          "error": "", "spent_usd": final_spend})
                    await self.db.cr_settle(
                        attempt["run_id"], max(attempt["spent"], final_spend), self.day(profile),
                        "awaiting_validation", str(artifact.relative_to(self.root)), str(observation_error)[:1000],
                        retry_at=(datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
                    )
                    await self.db._write(
                        "UPDATE code_review_prs SET error=? WHERE current_version=?",
                        (str(observation_error)[:500], version["id"]),
                    )
                    continue
                status = "complete" if (fresh and fresh["current_version"] == version["id"] and
                                         not fresh["draft"] and fresh["state"] == "open") else "stale"
            except Exception as exc:
                error = str(exc)[:1000]
            spent = max(attempt["spent"], final_spend)
            uncertain = 0
            if run and run.get("started_at") and "interrupted by nerve restart" in (run.get("error") or ""):
                # Keep unreported cost reserved, separately from measured spend.
                uncertain = max(0, attempt["reserved"] - spent)
            if run and run["status"] == "budget_exhausted":
                previous_spend = await self.db.cr_one(
                    "SELECT COALESCE(SUM(spent),0) total FROM code_review_attempts WHERE version_id=? AND run_id!=?",
                    (version["id"], attempt["run_id"]),
                )
                status = "review_limit" if previous_spend["total"] + spent >= profile.version_budget_usd else "waiting_budget"
            if not artifact.exists():
                freeze(artifact, {"version": version["id"], "run": attempt["run_id"], "output": output,
                                  "admission": status, "error": error, "spent_usd": spent})
            else:
                json.loads(artifact.read_text())  # read back before indexing
            relative = str(artifact.relative_to(self.root))
            await self.db.cr_settle(
                attempt["run_id"], spent, self.day(profile), status, relative, error,
                verdict.get("severity", "none"),
                (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat() if status == "retry" else "",
                serious_keys=[digest([f["path"], f["condition"], f["impact"]]) for f in verdict.get("findings", [])
                              if f["severity"] in ("high", "critical")], uncertain=uncertain,
            )
            row = await self.db.cr_one("SELECT * FROM code_review_prs WHERE id=?", (version["pr_id"],))
            await self.sync_task(row)

    async def revalidate(self):
        pending = await self.db.cr_rows(
            "SELECT v.*,p.profile FROM code_review_versions v JOIN code_review_prs p ON p.id=v.pr_id "
            "WHERE v.status='awaiting_validation' AND v.retry_at<=? ORDER BY v.created_at", (utc_now_iso(),),
        )
        for version in pending:
            profile = self.profile(version["profile"])
            if not await self.allowed(profile):
                continue
            try:
                snapshot = json.loads(version["snapshot"])
                output = json.loads((self.root / version["artifact_path"]).read_text())["output"]
                verdict = validate(output, snapshot)
                await self.observe(profile, snapshot["number"])
                await self.db.cr_finish_validation(
                    version["id"], "complete", verdict["severity"],
                    [digest([f["path"], f["condition"], f["impact"]]) for f in verdict["findings"]
                     if f["severity"] in ("high", "critical")],
                )
                row = await self.db.cr_one("SELECT * FROM code_review_prs WHERE id=?", (version["pr_id"],))
                await self.sync_task(row)
            except Exception as error:
                await self.db._write(
                    "UPDATE code_review_versions SET error=?,retry_at=? WHERE id=?",
                    (str(error)[:1000], (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(), version["id"]),
                )

    async def sync_task(self, row):
        """Reconcile the task projection; preserve the user's editable text."""
        task_id = row["task_id"]
        status = "in_progress" if row["state"] == "open" else "done"
        managed = (f"<!-- code-review:start -->\nPR: [{row['repository']} #{row['number']}]({row['url']})\n\n"
                   f"Head: `{row['head']}` · PR: {row['state']} · Review: {row['review_state']}\n\n"
                   f"[Review and immutable history](/tasks-code-review/{row['id']})\n<!-- code-review:end -->")
        async with task_mutation(task_id):
            task = await self.db.get_task(task_id)
            if task and task["source"] != "code-review":
                raise RuntimeError("Code Review task ID belongs to another source")
            title = f"{row['repository']} #{row['number']}: {row['title']}"
            if task:
                source = Path(self.config.workspace) / task["file_path"]
                content = source.read_text() if source.exists() else ""
                if content.startswith("# "):
                    _, separator, rest = content.partition("\n")
                    content = f"# {title}" + separator + rest
                start, end = content.find("<!-- code-review:start -->"), content.find("<!-- code-review:end -->")
                if start >= 0 and end >= start:
                    content = content[:start] + managed + content[end + len("<!-- code-review:end -->"):]
                else:
                    content += "\n\n" + managed
            else:
                content = f"# {title}\n\n**Source:** {row['url']}\n**Tags:** code-review,{row['profile']}\n\n{managed}\n"
            relative = f"memory/tasks/{'done' if status == 'done' else 'active'}/{task_id}.md"
            destination = Path(self.config.workspace) / relative
            if task and task["status"] == status and task["title"] == title and destination.exists() and destination.read_text() == content:
                return
            if task and source != destination and source.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                move_task_file(source, destination, content)
            else:
                atomic_write_text(destination, content)
            await self.db.upsert_task(
                task_id, relative, title, status=status, source="code-review", source_url=row["url"],
                tags=f"code-review,{row['profile']}", content=content,
                attribution=attribution_for(self.db.system_actor),
            )

    async def notify(self):
        if not self.notifications:
            return
        events = await self.db.cr_rows("SELECT * FROM code_review_events WHERE notified=0 ORDER BY id LIMIT 1000")
        visible = [e for e in events if e["kind"] in ("discovered", "lifecycle", "result", "review_stopped")]
        high = any((e["kind"] == "result" and json.loads(e["detail"]).get("new_serious")) or
                   e["kind"] == "review_stopped" for e in visible)
        last = await self.db.cr_one("SELECT MAX(last_digest_at) sent FROM code_review_profiles")
        due = not last["sent"] or (datetime.now(timezone.utc) - datetime.fromisoformat(last["sent"])).total_seconds() >= 600
        immediate = high or any(e["kind"] in ("discovered", "lifecycle") for e in visible)
        if visible and (due or immediate):
            counts = {kind: sum(e["kind"] == kind for e in visible) for kind in ("discovered", "lifecycle", "result", "review_stopped")}
            await self.notifications.send_notification(
                session_id="system", title=self.config.code_review.board_name,
                body=f"New PRs: {counts['discovered']}; state changes: {counts['lifecycle']}; review results: {counts['result']}; stopped reviews: {counts['review_stopped']}.\n[Open board](/tasks-code-review)",
                priority="high" if high else "normal",
            )
            await self.db._write("UPDATE code_review_profiles SET last_digest_at=?", (utc_now_iso(),))
        elif visible:
            # Keep result events for the next digest; heartbeat/CI events are silent.
            visible = []
        acknowledged = [e["id"] for e in events if e["kind"] not in ("discovered", "lifecycle", "result", "review_stopped")] + [e["id"] for e in visible]
        if acknowledged:
            placeholders = ",".join("?" for _ in acknowledged)
            await self.db._write(f"UPDATE code_review_events SET notified=1 WHERE id IN ({placeholders})", acknowledged)
        for profile in self.config.code_review.profiles:
            state = await self.db.cr_one("SELECT * FROM code_review_profiles WHERE id=?", (profile.id,))
            if state["error_count"] >= 3 and state["error"] != state["notified_error"]:
                await self.notifications.send_notification(session_id="system", title="Code Review observation failed",
                                                           body=f"{profile.id}: {state['error']}", priority="high")
                await self.db._write("UPDATE code_review_profiles SET notified_error=? WHERE id=?", (state["error"], profile.id))
            day = self.day(profile)
            if await self.db.cr_day_spend(day) >= profile.daily_budget_usd and state["notified_budget_day"] != day:
                await self.notifications.send_notification(session_id="system", title="Code Review daily budget reserved or exhausted",
                                                           body="Reviews remain queued; state polling continues. [Open board](/tasks-code-review)", priority="normal")
                await self.db._write("UPDATE code_review_profiles SET notified_budget_day=? WHERE id=?", (day, profile.id))

    async def status(self):
        profiles = []
        for profile in self.config.code_review.profiles:
            stored = await self.db.cr_one("SELECT * FROM code_review_profiles WHERE id=?", (profile.id,)) or {}
            job = self.scheduler.get_job(f"code-review-{profile.id}") if self.started else None
            next_run = getattr(job, "next_run_time", None)
            last_success = stored.get("last_success_at")
            expected = _crontab_to_trigger(profile.schedule, timezone=ZoneInfo(profile.timezone)).get_next_fire_time(
                None, datetime.fromisoformat(last_success) + timedelta(seconds=1),
            ) if last_success else None
            overdue = not expected or datetime.now(timezone.utc) > expected + timedelta(minutes=10)
            profiles.append({**stored, "id": profile.id, "enabled": profile.enabled,
                             "schedule": profile.schedule, "timezone": profile.timezone,
                             "job_id": f"code-review-{profile.id}", "registered": job is not None,
                             "next_run_at": next_run.isoformat() if next_run else None, "overdue": overdue,
                             "version_budget_usd": profile.version_budget_usd,
                             "daily_budget_usd": profile.daily_budget_usd,
                             "day": self.day(profile), "day_committed_usd": await self.db.cr_day_spend(self.day(profile))})
        return {"board_name": self.config.code_review.board_name, "running": self.started,
                "worker_available": self.runs is not None, "last_tick_at": self.last_tick_at,
                "worker_error": self.worker_error, "profiles": profiles}
