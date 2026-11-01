"""Code review persistence. All state/budget claims use BEGIN IMMEDIATE."""

from __future__ import annotations

import hashlib
import json
import uuid

from nerve.utils.time import utc_now_iso


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class CodeReviewStore:
    async def cr_rows(self, sql, args=()):
        async with self.db.execute(sql, args) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    async def cr_one(self, sql, args=()):
        rows = await self.cr_rows(sql, args)
        return rows[0] if rows else None

    async def cr_profiles(self, ids):
        async with self._atomic():
            for profile in ids:
                await self.db.execute("INSERT OR IGNORE INTO code_review_profiles(id) VALUES (?)", (profile,))

    async def cr_event(self, pr_id, kind, detail):
        # Internal transaction helper; callers own the write lock.
        await self.db.execute(
            "INSERT INTO code_review_events(pr_id,kind,detail,created_at) VALUES (?,?,?,?)",
            (pr_id, kind, detail, utc_now_iso()),
        )

    async def cr_observe(self, profile, pr, snapshot=None):
        """Direct GET is authoritative for lifecycle; a heartbeat never moves cards."""
        now = utc_now_iso()
        repo_id, number = pr["base"]["repo"]["id"], pr["number"]
        pr_id = "cr-" + digest([repo_id, number, profile])[:24]
        head, base = pr["head"]["sha"], pr["base"]["sha"]
        state = "merged" if pr.get("merged") else pr["state"]
        draft = bool(pr.get("draft"))
        async with self._atomic():
            old = await self.cr_one("SELECT * FROM code_review_prs WHERE id=?", (pr_id,))
            if not old and (draft or state != "open" or snapshot is None):
                return None
            if not old:
                await self.db.execute(
                    """INSERT INTO code_review_prs
                    (id,profile,repository_id,repository,number,task_id,title,url,state,draft,
                     head,base,base_ref,last_polled_at,pr_updated_at,content_updated_at,queue_since)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pr_id, profile, repo_id, pr["base"]["repo"]["full_name"], number,
                     pr_id, pr["title"], pr["html_url"], state, draft, head, base,
                     pr["base"]["ref"], now, pr["updated_at"], now, now),
                )
                await self.cr_event(pr_id, "discovered", f"PR #{number}")
            changed = old is None or any(old[k] != v for k, v in {
                "state": state, "draft": draft, "title": pr["title"], "head": head,
                "base": base, "base_ref": pr["base"]["ref"],
            }.items())
            review_state = (old or {}).get("review_state", "queued")
            current = (old or {}).get("current_version")
            ci = (old or {}).get("ci", "{}")
            if state != "open" or draft:
                review_state = "paused" if draft and state == "open" else "closed"
                await self.db.execute(
                    "UPDATE code_review_versions SET status='superseded',updated_at=? "
                    "WHERE pr_id=? AND status IN ('queued','waiting_budget','retry')", (now, pr_id),
                )
            elif snapshot is not None:
                # CI-only changes update evidence, without pretending to audit again.
                snapshot = dict(snapshot)
                # Retain already seen failure context for identical code/requirements.
                # A recovery/success or a new CI run ID is evidence, not a new audit.
                if current:
                    previous = await self.cr_one("SELECT snapshot FROM code_review_versions WHERE id=?", (current,))
                    prior = json.loads(previous["snapshot"]) if previous else {}
                    core = lambda value: {k: v for k, v in value.items() if k not in ("ci", "failures")}
                    if core(prior) == core(snapshot):
                        failures = {encode(f): f for f in [*prior.get("failures", []), *snapshot.get("failures", [])]}
                        snapshot["failures"] = [failures[key] for key in sorted(failures)]
                if snapshot.get("failures") == []:
                    snapshot.pop("failures", None)
                fingerprint = digest({k: v for k, v in snapshot.items() if k != "ci"})
                version = await self.cr_one(
                    "SELECT * FROM code_review_versions WHERE pr_id=? AND fingerprint=?", (pr_id, fingerprint),
                )
                if not version:
                    version_id = "crv-" + uuid.uuid4().hex
                    await self.db.execute(
                        """INSERT INTO code_review_versions
                        (id,pr_id,fingerprint,head,base,snapshot,created_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        (version_id, pr_id, fingerprint, head, base, encode(snapshot), now, now),
                    )
                    await self.cr_event(pr_id, "version", f"{version_id} {head}")
                    review_state = "queued"
                else:
                    version_id = version["id"]
                    reactivated = old and (old["draft"] or old["state"] != "open")
                    if version["status"] == "superseded" or (version["status"] == "stale" and (version_id != current or reactivated)):
                        await self.db.execute("UPDATE code_review_versions SET status='queued' WHERE id=?", (version_id,))
                        review_state = "queued"
                    elif version_id != current or (old and (old["draft"] or old["state"] != "open")):
                        review_state = "current" if version["status"] == "complete" else version["status"]
                if current != version_id:
                    await self.db.execute(
                        "UPDATE code_review_versions SET status='superseded',updated_at=? "
                        "WHERE pr_id=? AND id!=? AND status IN ('queued','waiting_budget','retry')",
                        (now, pr_id, version_id),
                    )
                    current = version_id
                    changed = True
                new_ci = encode(snapshot.get("ci", {}))
                if ci != new_ci:
                    ci = new_ci
                    changed = True
                    await self.cr_event(pr_id, "ci", ci)
            if old and (old["state"] != state or old["draft"] != draft):
                await self.cr_event(pr_id, "lifecycle", f"{state}, draft={draft}")
            await self.db.execute(
                """UPDATE code_review_prs SET title=?,state=?,draft=?,head=?,base=?,base_ref=?,
                   current_version=?,review_state=?,ci=?,last_polled_at=?,pr_updated_at=?,error='',
                   content_updated_at=CASE WHEN ? THEN ? ELSE content_updated_at END WHERE id=?""",
                (pr["title"], state, draft, head, base, pr["base"]["ref"], current,
                 review_state, ci, now, pr["updated_at"], changed, now, pr_id),
            )
        return await self.cr_one("SELECT * FROM code_review_prs WHERE id=?", (pr_id,))

    async def cr_day_spend(self, day):
        # Unsettled reservations survive midnight. A crossing run is charged
        # conservatively to each touched day; no reset can create free capacity.
        row = await self.cr_one(
            """SELECT COALESCE(SUM(CASE WHEN settled THEN spent+uncertain ELSE MAX(reserved,spent) END),0) AS used
            FROM code_review_attempts WHERE day=? OR (day<? AND (settled=0 OR end_day>=?))""",
            (day, day, day),
        )
        return row["used"]

    async def cr_reserve(self, profile, day, version_cap, daily_cap, max_attempts, version_id=None):
        now = utc_now_iso()
        async with self._atomic():
            settings = await self.cr_one("SELECT * FROM code_review_profiles WHERE id=?", (profile,))
            if not settings or settings["paused"]:
                return None
            if await self.cr_one("SELECT run_id FROM code_review_attempts WHERE profile=? AND settled=0", (profile,)):
                return None
            row = await self.cr_one(
                """SELECT v.*,p.task_id,p.number,p.repository,p.severity FROM code_review_versions v
                   JOIN code_review_prs p ON p.id=v.pr_id WHERE p.profile=? AND p.state='open'
                   AND p.draft=0 AND p.error='' AND p.current_version=v.id
                   AND v.status IN ('queued','waiting_budget','retry') AND v.retry_at<=?
                   AND (? IS NULL OR v.id=?)
                   ORDER BY p.queue_since,p.id LIMIT 1""", (profile, now, version_id, version_id),
            )
            if not row:
                return None
            attempts = await self.cr_one(
                "SELECT COUNT(*) n,COALESCE(SUM(MAX(spent+uncertain,reserved*(1-settled))),0) used "
                "FROM code_review_attempts WHERE version_id=?", (row["id"],),
            )
            remaining = version_cap - attempts["used"]
            daily = daily_cap - await self.cr_day_spend(day)
            if attempts["n"] >= max_attempts or remaining <= 0.001:
                state = "review_limit" if remaining <= 0.001 else "error"
                await self.db.execute("UPDATE code_review_versions SET status=?,updated_at=? WHERE id=?", (state, now, row["id"]))
                await self.db.execute("UPDATE code_review_prs SET review_state=? WHERE id=?", (state, row["pr_id"]))
                await self.cr_event(row["pr_id"], "review_stopped", encode({"status": state, "version": row["id"]}))
                return None
            if daily <= 0.001:
                await self.db.execute("UPDATE code_review_versions SET status='waiting_budget' WHERE id=?", (row["id"],))
                await self.db.execute("UPDATE code_review_prs SET review_state='waiting_budget' WHERE id=?", (row["pr_id"],))
                return None
            amount = min(remaining, daily)
            run_id = "wfr-cr-" + uuid.uuid4().hex
            await self.db.execute(
                "INSERT INTO code_review_attempts(run_id,version_id,profile,day,reserved,created_at) VALUES (?,?,?,?,?,?)",
                (run_id, row["id"], profile, day, amount, now),
            )
            await self.db.execute("UPDATE code_review_versions SET status='running',updated_at=? WHERE id=?", (now, row["id"]))
            await self.db.execute("UPDATE code_review_prs SET review_state='running',queue_since=? WHERE id=?", (now, row["pr_id"]))
            return {**row, "run_id": run_id, "budget": amount, "attempt": attempts["n"] + 1}

    async def cr_settle(self, run_id, spent, day, status, artifact_path, error="", severity="none", retry_at="", serious_keys=(), uncertain=0):
        now = utc_now_iso()
        async with self._atomic():
            attempt = await self.cr_one("SELECT * FROM code_review_attempts WHERE run_id=?", (run_id,))
            if not attempt or attempt["settled"]:
                return False
            await self.db.execute(
                "UPDATE code_review_attempts SET settled=1,spent=MAX(spent,?),uncertain=?,end_day=?,artifact_path=? WHERE run_id=?",
                (spent, uncertain, day, artifact_path, run_id),
            )
            version = await self.cr_one("SELECT * FROM code_review_versions WHERE id=?", (attempt["version_id"],))
            await self.db.execute(
                "UPDATE code_review_versions SET status=?,artifact_path=?,error=?,retry_at=?,updated_at=? WHERE id=?",
                (status, artifact_path, error, retry_at, now, version["id"]),
            )
            pr = await self.cr_one("SELECT * FROM code_review_prs WHERE id=?", (version["pr_id"],))
            current = pr["current_version"] == version["id"] and not pr["draft"] and pr["state"] == "open"
            new_serious = []
            if current and status == "complete":
                new_serious = sorted(set(serious_keys) - set(json.loads(pr["serious_keys"])))
                await self.db.execute("UPDATE code_review_prs SET serious_keys=? WHERE id=?", (encode(sorted(serious_keys)), pr["id"]))
            if current:
                await self.db.execute(
                    """UPDATE code_review_prs SET review_state=?,severity=?,content_updated_at=?,
                    reviewed_version=CASE WHEN ?='complete' THEN ? ELSE reviewed_version END WHERE id=?""",
                    ("findings" if status == "complete" and severity != "none" else
                     "current" if status == "complete" else status, severity, now, status, version["id"], pr["id"]),
                )
            await self.cr_event(pr["id"], "result", encode({"status": status, "version": version["id"], "severity": severity, "current": current, "new_serious": new_serious}))
        return True

    async def cr_finish_validation(self, version_id, status, severity="none", serious_keys=()):
        """Admit saved output without launching or charging another workflow."""
        now = utc_now_iso()
        async with self._atomic():
            version = await self.cr_one("SELECT * FROM code_review_versions WHERE id=?", (version_id,))
            if not version or version["status"] != "awaiting_validation":
                return False
            pr = await self.cr_one("SELECT * FROM code_review_prs WHERE id=?", (version["pr_id"],))
            current = pr["current_version"] == version_id and not pr["draft"] and pr["state"] == "open"
            if not current:
                status = "stale"
            await self.db.execute("UPDATE code_review_versions SET status=?,error='',updated_at=? WHERE id=?", (status, now, version_id))
            new_serious = []
            if current and status == "complete":
                new_serious = sorted(set(serious_keys) - set(json.loads(pr["serious_keys"])))
                await self.db.execute(
                    "UPDATE code_review_prs SET reviewed_version=?,review_state=?,severity=?,serious_keys=?,content_updated_at=?,error='' WHERE id=?",
                    (version_id, "findings" if severity != "none" else "current", severity, encode(sorted(serious_keys)), now, pr["id"]),
                )
            await self.cr_event(pr["id"], "result", encode({"status": status, "version": version_id, "severity": severity,
                                                           "current": current, "new_serious": new_serious}))
        return True
