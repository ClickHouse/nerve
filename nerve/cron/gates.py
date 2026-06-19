"""Cron run gates — preconditions evaluated before a job fires.

A *gate* answers a single question: should this cron run **right now**?
Jobs declare zero or more gates via the ``run_if`` config key. All gates
must be satisfied (logical AND) for the job to run; if any gate is
unsatisfied the run is skipped — logged, with no agent invocation.

Design:

* A gate is a pure declaration built from config (:meth:`CronGate.from_config`).
  It reaches the database only at evaluation time, through :class:`GateContext`,
  so gate objects are cheap to build and hold no live resources.
* Gate types are looked up in :data:`GATE_REGISTRY` by their ``type`` key,
  which mirrors the ``type:`` field in the YAML spec.
* Adding a new gate = subclass :class:`CronGate`, set ``type``, implement the
  three abstract methods, and register the class in :data:`GATE_REGISTRY`.

Example config::

    run_if:
      - type: tasks            # run only when there is something to plan
        status: pending
      - type: messages         # ...and only when sources have new messages
        sources: [gmail, github]

Evaluation is AND across the list, so the job above runs only when *both*
hold.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)


class GateConfigError(ValueError):
    """Raised when a gate spec in config is malformed."""


@dataclass
class GateContext:
    """Runtime dependencies handed to a gate at evaluation time."""

    job_id: str
    db: "Database"


@dataclass
class GateDecision:
    """Outcome of evaluating a job's gates.

    ``reason`` is a short human-readable explanation, populated only when
    ``should_run`` is False, for logging the skip.
    """

    should_run: bool
    reason: str = ""


class CronGate(ABC):
    """A precondition checked before a cron job runs."""

    #: Registry key — must match the ``type`` field in the YAML spec.
    type: ClassVar[str] = ""

    @abstractmethod
    async def is_satisfied(self, ctx: GateContext) -> bool:
        """Return True if the job is allowed to run under this condition."""

    @abstractmethod
    def describe(self) -> str:
        """Short human-readable description of the condition (UI + logs)."""

    @classmethod
    @abstractmethod
    def from_config(cls, spec: dict) -> "CronGate":
        """Build an instance from its config dict (``type`` key included)."""


class MessagesGate(CronGate):
    """Satisfied when monitored sync sources have unread messages.

    Generalises the legacy ``skip_when_idle`` / ``idle_consumer`` fields:
    a job that only wants to run when an inbox has new mail.

    Config::

        type: messages
        sources: [gmail, github]   # source names to check (required)
        consumer: inbox            # consumer cursor name (default "inbox")

    The check compares each source's max ingested rowid against the
    consumer's cursor position; it never advances any cursor.
    """

    type = "messages"

    def __init__(self, sources: list[str], consumer: str = "inbox") -> None:
        if not sources:
            raise GateConfigError("'messages' gate requires at least one source")
        self.sources = sources
        self.consumer = consumer

    async def is_satisfied(self, ctx: GateContext) -> bool:
        for source in self.sources:
            cursor_seq = await ctx.db.get_consumer_cursor(self.consumer, source)
            max_seq = await ctx.db.get_source_max_rowid(source)
            if max_seq > cursor_seq:
                return True
        return False

    def describe(self) -> str:
        return (
            f"new messages in {', '.join(self.sources)} "
            f"(consumer={self.consumer})"
        )

    @classmethod
    def from_config(cls, spec: dict) -> "MessagesGate":
        # Accept the new "sources" key, plus the legacy "skip_when_idle"
        # spelling so an old shorthand maps cleanly onto this gate.
        sources = spec.get("sources")
        if sources is None:
            sources = spec.get("skip_when_idle", [])
        if isinstance(sources, str):
            sources = [sources]
        consumer = spec.get("consumer") or spec.get("idle_consumer") or "inbox"
        return cls(sources=list(sources), consumer=consumer)


class TasksGate(CronGate):
    """Satisfied when enough tasks match a status/tag filter.

    The motivating case: only run the task-planner when there is actually
    something to plan.

    Config::

        type: tasks
        status: pending            # status name, list of names, "all",
                                   # or omitted (= any non-done task)
        tag: backend               # optional tag filter; a list ORs the tags
        min_count: 1               # minimum matching tasks (default 1)

    ``status`` semantics:

    * omitted / null  → any *open* task (non-done), the common case
    * ``"all"``       → any task regardless of status
    * a single name   → tasks in exactly that status (e.g. ``pending``)
    * a list of names → tasks in any of those statuses (counts summed)
    """

    type = "tasks"

    def __init__(
        self,
        targets: list[str | None],
        tag: str | list[str] | None = None,
        min_count: int = 1,
    ) -> None:
        # Each element is passed straight to ``count_tasks(status=...)``:
        #   None    → non-done    ("all" handled by count_tasks directly)
        #   "all"   → every task
        #   "<name>"→ that status
        self.targets: list[str | None] = targets or [None]
        self.tag = tag
        self.min_count = max(1, int(min_count))

    async def is_satisfied(self, ctx: GateContext) -> bool:
        count = 0
        for status in self.targets:
            count += await ctx.db.count_tasks(status=status, tag=self.tag)
            if count >= self.min_count:
                return True
        return count >= self.min_count

    def describe(self) -> str:
        labels = ["open" if t is None else t for t in self.targets]
        status_str = "/".join(labels)
        if self.tag:
            tags = [self.tag] if isinstance(self.tag, str) else list(self.tag)
            tag_str = f" tagged '{'/'.join(tags)}'"
        else:
            tag_str = ""
        thresh = "" if self.min_count == 1 else f" (>= {self.min_count})"
        return f"{status_str} tasks{tag_str}{thresh} exist"

    @classmethod
    def from_config(cls, spec: dict) -> "TasksGate":
        raw = spec.get("status")
        if raw is None:
            targets: list[str | None] = [None]
        elif isinstance(raw, str):
            targets = [raw]  # includes the "all" sentinel
        elif isinstance(raw, (list, tuple)):
            targets = [str(s) for s in raw] or [None]
        else:
            raise GateConfigError(
                f"'tasks' gate 'status' must be a string or list, "
                f"got {type(raw).__name__}"
            )

        min_count = spec.get("min_count", 1)
        try:
            min_count = int(min_count)
        except (TypeError, ValueError):
            raise GateConfigError(
                f"'tasks' gate 'min_count' must be an integer, got {min_count!r}"
            )

        tag = spec.get("tag")
        if tag is not None and not isinstance(tag, (str, list, tuple)):
            raise GateConfigError(
                f"'tasks' gate 'tag' must be a string or list, "
                f"got {type(tag).__name__}"
            )
        if isinstance(tag, (list, tuple)):
            tag = [str(t) for t in tag]
        return cls(targets=targets, tag=tag, min_count=min_count)


class GitHubPrActivityGate(CronGate):
    """Satisfied only when an author's open PRs show *new* activity.

    The motivating case: wake an expensive (LLM) PR-monitor cron only when
    something it actually acts on has changed on one of the author's open
    PRs — merge/close (``state``), a new commit (``headRefOid``), a review
    verdict (``reviewDecision``), or a CI transition (``statusCheckRollup``)
    — instead of every N minutes for the entire lifetime of an open PR.

    Why a dedicated gate (not the ``messages``/``tasks`` gates): comments and
    @-mentions arrive via the ``github`` sync source, but **CI status changes
    and silent merges do not** (a check flipping red posts no comment), so a
    poll-based signal is required. A PR's ``updatedAt`` is *not* enough — check
    runs attach to the head commit, not the PR, so CI changes often don't bump
    it; we therefore fingerprint the CI rollup directly.

    Mechanics (cheap, non-LLM): shell out to ``gh`` (same async-subprocess
    pattern as ``nerve.sources.github``), hash the fields above across all of
    the author's open PRs, and compare to the fingerprint stored at the last
    fire (a sentinel file under ``~/.nerve/cache``). Fire iff the fingerprint
    changed. A ``force_run_after_hours`` safety net guarantees a periodic wake
    even if a signal is somehow missed, and any ``gh`` failure fails *open*
    (fires) so a transient error can never strand a PR.

    Compose it with a cheap ``tasks`` gate (evaluated first) so the network
    calls only happen when there's actually a PR task to monitor::

        run_if:
          - type: tasks                 # DB-only: any open-PR task at all?
            status: in_progress
            tag: pr-open
          - type: github_pr_activity    # network: did any open PR change?
            author: my-bot
            force_run_after_hours: 8

    Config:

    * ``author`` (required) — GitHub login whose open PRs to watch.
    * ``force_run_after_hours`` (default 8) — force a run if this long has
      elapsed since the last fire; ``0`` disables the safety net.
    """

    type = "github_pr_activity"

    def __init__(self, author: str, force_run_after_hours: float = 8.0) -> None:
        if not author:
            raise GateConfigError(
                "'github_pr_activity' gate requires a non-empty 'author'"
            )
        self.author = author
        self.force_run_after_hours = force_run_after_hours

    async def is_satisfied(self, ctx: GateContext) -> bool:
        fp = await self._fingerprint()
        if fp is None:
            # gh failed entirely — fail open (run) rather than risk stranding a PR.
            logger.warning(
                "github_pr_activity: gh query failed; allowing run (fail-open)"
            )
            return True

        prev_fp, last_fire = self._load_state(ctx.job_id)
        now = datetime.now(timezone.utc)

        changed = prev_fp is None or fp != prev_fp
        stale = (
            self.force_run_after_hours > 0
            and last_fire is not None
            and (now - last_fire) >= timedelta(hours=self.force_run_after_hours)
        )
        if changed or stale:
            # Record the state we're firing on. If the monitor crashes mid-run
            # the worst case is one missed change, bounded by force_run_after_hours.
            self._save_state(ctx.job_id, fp, now)
            return True
        return False

    def describe(self) -> str:
        return (
            f"new activity on {self.author}'s open PRs "
            f"(state/CI/review change; force every {self.force_run_after_hours}h)"
        )

    @classmethod
    def from_config(cls, spec: dict) -> "GitHubPrActivityGate":
        author = spec.get("author")
        hours = spec.get("force_run_after_hours", 8)
        try:
            hours = float(hours)
        except (TypeError, ValueError):
            raise GateConfigError(
                "'github_pr_activity' gate 'force_run_after_hours' must be a "
                f"number, got {hours!r}"
            )
        return cls(author=author, force_run_after_hours=hours)

    # --- internals ---------------------------------------------------------

    async def _fingerprint(self) -> str | None:
        """Hash the monitor-relevant state of every open PR. None on gh failure."""
        prs = await self._list_open_prs()
        if prs is None:
            return None
        entries: list[dict] = []
        for repo, number in sorted(prs):
            detail = await self._pr_detail(repo, number)
            if detail is None:
                return None  # partial data -> treat as failure (fail-open upstream)
            entries.append(detail)
        blob = json.dumps(entries, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    async def _list_open_prs(self) -> list[tuple[str, int]] | None:
        out = await self._gh(
            "search", "prs", "--author", self.author, "--state", "open",
            "--json", "repository,number", "--limit", "100",
        )
        if out is None:
            return None
        data = json.loads(out) if out.strip() else []
        return [(d["repository"]["nameWithOwner"], int(d["number"])) for d in data]

    async def _pr_detail(self, repo: str, number: int) -> dict | None:
        out = await self._gh(
            "pr", "view", str(number), "--repo", repo,
            "--json", "state,reviewDecision,headRefOid,statusCheckRollup",
        )
        if out is None:
            return None
        d = json.loads(out)
        checks = sorted(
            (c.get("name", ""), c.get("status", ""), c.get("conclusion", ""))
            for c in (d.get("statusCheckRollup") or [])
        )
        return {
            "repo": repo,
            "number": number,
            "state": d.get("state"),
            "review": d.get("reviewDecision"),
            "head": d.get("headRefOid"),
            "checks": checks,
        }

    @staticmethod
    async def _gh(*args: str, timeout: float = 30.0) -> str | None:
        """Run a ``gh`` command (inherits the daemon's gh auth). None on failure.

        Mirrors ``nerve.sources.github`` — the proven pattern for calling gh
        from inside the daemon's event loop.
        """
        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            if proc.returncode != 0:
                logger.debug("gh %s failed: %s", args, stderr.decode()[:200])
                return None
            return stdout.decode()
        except Exception as e:  # noqa: BLE001 — caller treats None as fail-open
            logger.debug("gh %s error: %s", args, e)
            return None

    def _state_path(self, job_id: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in job_id)
        d = Path.home() / ".nerve" / "cache"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"pr_activity_{safe}.json"

    def _load_state(self, job_id: str) -> tuple[str | None, datetime | None]:
        try:
            raw = json.loads(self._state_path(job_id).read_text(encoding="utf-8"))
            fp = raw.get("fingerprint")
            lf = raw.get("last_fire")
            return fp, (datetime.fromisoformat(lf) if lf else None)
        except Exception:
            return None, None

    def _save_state(self, job_id: str, fingerprint: str, now: datetime) -> None:
        try:
            self._state_path(job_id).write_text(
                json.dumps({"fingerprint": fingerprint, "last_fire": now.isoformat()}),
                encoding="utf-8",
            )
        except Exception as e:  # noqa: BLE001 — non-fatal; worst case is an extra run
            logger.warning("github_pr_activity: could not persist state: %s", e)


#: Maps a gate's ``type`` key to its implementing class. Register new gate
#: types here to make them usable from config.
GATE_REGISTRY: dict[str, type[CronGate]] = {
    MessagesGate.type: MessagesGate,
    TasksGate.type: TasksGate,
    GitHubPrActivityGate.type: GitHubPrActivityGate,
}


def build_gate(spec: dict) -> CronGate:
    """Build a single gate from a config dict.

    Raises :class:`GateConfigError` if the spec is malformed or names an
    unknown gate type.
    """
    if not isinstance(spec, dict):
        raise GateConfigError(
            f"gate spec must be a mapping, got {type(spec).__name__}"
        )
    gate_type = spec.get("type")
    if not gate_type:
        raise GateConfigError("gate spec missing required 'type' key")
    cls = GATE_REGISTRY.get(gate_type)
    if cls is None:
        known = ", ".join(sorted(GATE_REGISTRY)) or "(none)"
        raise GateConfigError(
            f"unknown gate type {gate_type!r} (known types: {known})"
        )
    return cls.from_config(spec)


def build_gates(specs: list[dict]) -> list[CronGate]:
    """Build gates from a list of config specs.

    Invalid specs are logged and skipped rather than raising, so one bad
    gate can't take down the whole cron service at load time. A job whose
    gates all fail to build behaves as if it has no gates (runs normally).
    """
    gates: list[CronGate] = []
    for spec in specs or []:
        try:
            gates.append(build_gate(spec))
        except GateConfigError as e:
            logger.warning("Ignoring invalid cron gate %s: %s", spec, e)
    return gates


async def evaluate_gates(
    gates: list[CronGate], ctx: GateContext,
) -> GateDecision:
    """Evaluate gates with AND semantics — every gate must be satisfied.

    Fail-open: if a gate raises while checking, it's treated as satisfied
    (the run proceeds) and a warning is logged. Silently skipping forever
    on a transient DB error would be worse than an occasional wasted run —
    and a wasted run is just the pre-gate behaviour.
    """
    for gate in gates:
        try:
            ok = await gate.is_satisfied(ctx)
        except Exception as e:  # noqa: BLE001 — fail open, never block forever
            logger.warning(
                "Cron gate %r for job %s errored; allowing run: %s",
                gate.type, ctx.job_id, e,
            )
            continue
        if not ok:
            return GateDecision(
                should_run=False,
                reason=f"gate {gate.type!r} not satisfied ({gate.describe()})",
            )
    return GateDecision(should_run=True)
