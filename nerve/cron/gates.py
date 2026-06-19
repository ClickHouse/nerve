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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
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


#: Maps a gate's ``type`` key to its implementing class. Register new gate
#: types here to make them usable from config.
GATE_REGISTRY: dict[str, type[CronGate]] = {
    MessagesGate.type: MessagesGate,
    TasksGate.type: TasksGate,
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
