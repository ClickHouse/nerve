"""Cron job definitions and persistence.

Jobs are defined in a YAML file and loaded at startup.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from nerve.cron.gates import CronGate

logger = logging.getLogger(__name__)


# Scheduler ids the daemon owns; a job defined in system.yaml/jobs.yaml may not
# claim one. `source:` is reserved as a whole namespace rather than as the set of
# runners that happen to be live: source runners register as `source:<name>`, and
# which ones exist depends on the sync config, so reserving only the live ones
# would let a job take `source:telegram` while telegram is switched off and then
# be silently replaced the moment it is switched back on.
RESERVED_JOB_IDS = frozenset({"cleanup", "wakeup_sweep"})
RESERVED_JOB_ID_PREFIX = "source:"


def is_reserved_job_id(job_id: str) -> bool:
    """True if *job_id* belongs to the daemon and can't be used by a cron job."""
    return job_id in RESERVED_JOB_IDS or job_id.startswith(RESERVED_JOB_ID_PREFIX)


def describe_reserved_job_ids() -> str:
    """Human-readable summary of the reservation, for logs and CLI output."""
    return (
        f"{', '.join(sorted(RESERVED_JOB_IDS))}, "
        f"and anything starting with '{RESERVED_JOB_ID_PREFIX}'"
    )


def _none_to_empty(value: Any) -> Any:
    """``None`` becomes ``[]``; every other value is returned verbatim.

    A bare ``run_if:`` / ``skip_when_idle:`` key parses to None and means "no
    gates". Nothing else does — see :meth:`CronJob.from_dict`.
    """
    return [] if value is None else value


@dataclass
class CronJob:
    """A cron job definition."""
    id: str
    schedule: str  # crontab expression or interval (e.g., "*/30 * * * *", "2h")
    prompt: str = ""  # The message/instruction sent to the agent (inline)
    # Path to a file containing the prompt. Relative paths resolve against
    # the directory of the YAML file the job was loaded from. When set, the
    # file is read fresh on every run (edits apply without a restart) and
    # multiple jobs may share the same prompt file. Takes precedence over
    # the inline prompt; the inline prompt acts as a fallback if the file
    # is unreadable.
    prompt_file: str = ""
    # Workflow-run declaration: instead of prompting a cron session, the
    # job launches a budget-capped workflow run (see nerve/workflows) and
    # returns immediately — the run notifies on its own. Required keys:
    # 'engine', 'prompt', and a positive numeric 'budget_usd'. Optional:
    # title, model, effort, cwd. Takes precedence over prompt/prompt_file.
    workflow: dict | None = None
    description: str = ""
    model: str = ""  # Override model; empty = use config default
    effort: str = ""  # Override reasoning effort (low/medium/high/xhigh/max); empty = source default (cron_effort)
    # Per-job prompt-cache TTL override: "5m", "1h" or "auto"; empty = use
    # agent.cache_ttl from config. See nerve/agent/cache_policy.py.
    cache_ttl: str = ""
    session_mode: str = "isolated"  # "isolated" (new session per run) or "persistent" (reuse context)
    context_rotate_hours: int = 24  # Hours before persistent context is rotated (0 = never)
    context_rotate_at: str = ""  # Time of day to rotate (e.g. "04:00"); overrides hours-based rotation
    reminder_mode: bool = False  # Persistent only: send short reminder instead of full prompt on subsequent runs
    catchup: bool = True  # Fire once on startup if missed while server was down
    enabled: bool = True
    lock: bool = False  # When True, prevent concurrent runs of this job (next run waits for previous)
    # Run gates — preconditions evaluated before each fire. Each entry is a
    # spec dict like {"type": "tasks", "status": "pending"}. All gates must
    # pass (AND) for the job to run. See nerve/cron/gates.py.
    run_if: list[dict] = field(default_factory=list)
    # Legacy shorthand for a "messages" gate, kept for backward compatibility.
    skip_when_idle: list[str] = field(default_factory=list)  # Source names to check; skip run if no new messages
    idle_consumer: str = "inbox"  # Consumer cursor name for the idle check
    show_session_label: bool = True  # Show "Session: ..." in notification messages
    metadata: dict = field(default_factory=dict)
    # Built run gates (derived from run_if + legacy fields in __post_init__).
    # Not serialized; excluded from equality/repr.
    gates: list["CronGate"] = field(
        default_factory=list, init=False, repr=False, compare=False,
    )
    # Resolved absolute path for prompt_file (set by from_dict/load_jobs).
    # Not serialized; excluded from equality/repr.
    prompt_path: Path | None = field(
        default=None, init=False, repr=False, compare=False,
    )

    def __post_init__(self) -> None:
        self.gates = self._build_gates()
        if self.workflow is not None:
            self._validate_workflow()
        elif not self.prompt and not self.prompt_file:
            raise ValueError(
                f"Cron job {self.id!r} needs a 'prompt', 'prompt_file', "
                "or 'workflow'"
            )
        if self.prompt_file and self.prompt_path is None:
            # Direct construction (tests, programmatic) — resolve against cwd.
            self.prompt_path = Path(self.prompt_file).expanduser()

    def _validate_workflow(self) -> None:
        """Validate the workflow declaration at construction time.

        Raises ValueError with a precise message so invalid jobs are
        dropped at load with a log, same as jobs missing a prompt.
        """
        w = self.workflow
        if not isinstance(w, dict):
            raise ValueError(
                f"Cron job {self.id!r}: 'workflow' must be a mapping, "
                f"got {type(w).__name__}"
            )
        if not str(w.get("engine") or "").strip():
            raise ValueError(
                f"Cron job {self.id!r}: workflow needs a non-empty 'engine'"
            )
        if not str(w.get("prompt") or "").strip():
            raise ValueError(
                f"Cron job {self.id!r}: workflow needs a non-empty 'prompt'"
            )
        budget = w.get("budget_usd")
        if (
            isinstance(budget, bool)
            or not isinstance(budget, (int, float))
            or budget <= 0
            or not math.isfinite(float(budget))
        ):
            raise ValueError(
                f"Cron job {self.id!r}: workflow needs a positive finite "
                f"numeric 'budget_usd', got {budget!r}"
            )

    def resolve_prompt(self) -> str:
        """Return the effective prompt for a run.

        Reads prompt_file fresh on every call so edits apply without a
        restart. Falls back to the inline prompt if the file is unreadable;
        raises if neither is usable.
        """
        if self.prompt_path is not None:
            try:
                return self.prompt_path.read_text(encoding="utf-8")
            except OSError as e:
                if self.prompt:
                    logger.error(
                        "Cron job %s: cannot read prompt_file %s (%s) — "
                        "falling back to inline prompt",
                        self.id, self.prompt_path, e,
                    )
                    return self.prompt
                raise RuntimeError(
                    f"Cron job {self.id!r}: cannot read prompt_file "
                    f"{self.prompt_path} and no inline prompt fallback: {e}"
                ) from e
        return self.prompt

    def _build_gates(self) -> list["CronGate"]:
        """Construct gate objects from run_if plus the legacy shorthand.

        A spec that cannot be built raises out of __post_init__ and takes the
        whole job with it — see :func:`nerve.cron.gates.build_gates` for why that
        beats quietly dropping the gate. The same goes for the two fields
        themselves: both hold whatever the config said (from_dict normalizes only
        a bare key), so either may be the wrong shape here, and a shape nobody
        can read is not permission to run the job unguarded.

        Messages name the job and the offending value, because that is what the
        operator has to go and fix — and for a spec, what reaches the reload's
        400.
        """
        from nerve.cron.gates import GateConfigError, build_gates

        if not isinstance(self.run_if, list):
            raise GateConfigError(
                f"Cron job {self.id!r}: 'run_if' must be a list of gate specs, "
                f"got {self.run_if!r}"
            )
        specs: list[dict] = list(self.run_if)
        # Translate the legacy skip_when_idle shorthand into a messages gate
        # so old configs keep working without rewrites.
        if not isinstance(self.skip_when_idle, list):
            raise GateConfigError(
                f"Cron job {self.id!r}: 'skip_when_idle' must be a list of "
                f"source names, got {self.skip_when_idle!r}"
            )
        if self.skip_when_idle:
            specs.append({
                "type": "messages",
                "sources": list(self.skip_when_idle),
                "consumer": self.idle_consumer,
            })
        try:
            return build_gates(specs)
        except GateConfigError as e:
            raise GateConfigError(f"Cron job {self.id!r}: {e}") from e

    @classmethod
    def from_dict(
        cls, d: dict, base_dir: Path | None = None, *, gates: bool = True,
    ) -> CronJob:
        """Build a job from its YAML mapping.

        ``gates=False`` attaches the gate specs without building them. Only
        ``nerve config validate`` wants that: it never loads the bundle's gate
        plugins, so every plugin-provided type looks unregistered to it, and
        building would refuse jobs that are in fact fine. It checks the specs
        itself instead — per type and per field — and reports an unrecognized
        type as unverified rather than wrong.
        """
        job = cls(
            id=d["id"],
            schedule=d["schedule"],
            prompt=d.get("prompt", ""),
            # Blank means unset: it is truthy, and it outranks ``prompt`` below,
            # so a stray space would beat a perfectly good inline prompt and
            # then fail every run trying to read a file named for a space.
            prompt_file=str(d.get("prompt_file") or "").strip(),
            workflow=d.get("workflow"),
            description=d.get("description", ""),
            model=d.get("model", ""),
            effort=d.get("effort", ""),
            cache_ttl=d.get("cache_ttl", ""),
            session_mode=d.get("session_mode", "isolated"),
            context_rotate_hours=int(d.get("context_rotate_hours", 24)),
            context_rotate_at=d.get("context_rotate_at", ""),
            reminder_mode=bool(d.get("reminder_mode", False)),
            catchup=d.get("catchup", True),
            enabled=d.get("enabled", True),
            lock=bool(d.get("lock", False)),
            # A bare `run_if:` parses to None, which means "no gates" and must
            # not reach gate construction. Only None is normalized: any other
            # wrong shape (`run_if: {}`) is kept verbatim so validation can
            # reject it, rather than silently reading as an ungated job.
            #
            # Held back when gates aren't being built, so __post_init__ has
            # nothing to construct; the real values are attached below.
            run_if=_none_to_empty(d.get("run_if")) if gates else [],
            skip_when_idle=(
                _none_to_empty(d.get("skip_when_idle")) if gates else []
            ),
            idle_consumer=d.get("idle_consumer", "inbox"),
            show_session_label=d.get("show_session_label", True),
            metadata=d.get("metadata", {}),
        )
        if not gates:
            # Attached after construction, so ``gates`` stays empty while the
            # specs are still there to be inspected. The pair is deliberately
            # inconsistent and only validation ever sees it.
            job.run_if = _none_to_empty(d.get("run_if"))
            job.skip_when_idle = _none_to_empty(d.get("skip_when_idle"))
        if job.prompt_file:
            p = Path(job.prompt_file).expanduser()
            if not p.is_absolute() and base_dir is not None:
                p = (base_dir / p).resolve()
            job.prompt_path = p
        return job


def load_jobs(
    jobs_file: Path,
    strict: bool = False,
    errors: list[str] | None = None,
    *,
    build_gates: bool = True,
) -> list[CronJob]:
    """Load cron jobs from a YAML file.

    By default this is *tolerant*: a YAML parse failure or an invalid job entry
    is logged and skipped (so a typo never takes down daemon startup). With
    ``strict=True`` any such failure raises :class:`nerve.config.ConfigError`
    instead — used by hot-reload so a malformed file is refused rather than
    silently unscheduling every job. A missing file is never an error (returns
    ``[]``) in either mode.

    Pass ``errors`` to collect per-job failures instead of raising or logging
    them: each bad entry appends one message and is skipped, and the jobs that
    did build are still returned. ``nerve config validate`` needs that, because
    raising on the first bad job would hide every problem after it in the same
    file, and its whole job is to list them all in one pass. File-level failures
    still follow ``strict``.

    ``build_gates=False`` is passed straight to :meth:`CronJob.from_dict`; see
    there for who wants it and why.
    """
    if not jobs_file.exists():
        logger.info("No cron jobs file at %s", jobs_file)
        return []

    try:
        with open(jobs_file) as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error("Failed to load cron jobs from %s: %s", jobs_file, e)
        if strict:
            from nerve.config import ConfigError
            raise ConfigError(f"Failed to parse cron file {jobs_file}: {e}") from e
        return []

    jobs_data = data.get("jobs", []) if isinstance(data, dict) else data
    if not isinstance(jobs_data, list):
        if strict:
            from nerve.config import ConfigError
            raise ConfigError(
                f"Cron file {jobs_file} must contain a 'jobs:' list"
            )
        logger.error("Cron file %s has no valid 'jobs' list — ignoring", jobs_file)
        return []

    jobs = []
    for item in jobs_data:
        try:
            jobs.append(CronJob.from_dict(
                item, base_dir=jobs_file.parent, gates=build_gates,
            ))
        except (KeyError, TypeError, ValueError) as e:
            message = f"Invalid cron job in {jobs_file}: {item!r} — {e}"
            if errors is not None:
                errors.append(message)
                continue
            logger.warning("Invalid cron job definition: %s — %s", item, e)
            if strict:
                from nerve.config import ConfigError
                raise ConfigError(message) from e

    logger.info("Loaded %d cron jobs from %s", len(jobs), jobs_file)
    return jobs


class JobEditError(Exception):
    """A cron file could not be edited in place, with the reason why.

    Carries an operator-facing message: every raise site names the file and what
    about it defeated the edit, because the answer is always "go and change that
    line by hand" and the message is the only thing that says which line.
    """


def _find_jobs_sequence(root: yaml.Node, jobs_file: Path) -> yaml.SequenceNode:
    """The node holding the job list, for either shape :func:`load_jobs` accepts.

    ``jobs:`` under a mapping is the documented form; a bare top-level list is
    the one old installs still have (see :func:`load_jobs`). Both are read at
    startup, so both have to be editable — supporting only the first would give
    those installs a toggle that returns success and changes nothing.
    """
    if isinstance(root, yaml.SequenceNode):
        return root
    if isinstance(root, yaml.MappingNode):
        for key, value in root.value:
            if isinstance(key, yaml.ScalarNode) and key.value == "jobs":
                if not isinstance(value, yaml.SequenceNode):
                    raise JobEditError(
                        f"{jobs_file}: 'jobs' is not a list"
                    )
                return value
    raise JobEditError(f"{jobs_file}: no 'jobs' list to edit")


def _mapping_entry(
    mapping: yaml.MappingNode, name: str,
) -> tuple[yaml.ScalarNode, yaml.Node] | None:
    """The ``(key, value)`` node pair for *name*, or ``None`` if absent."""
    for key, value in mapping.value:
        if isinstance(key, yaml.ScalarNode) and key.value == name:
            return key, value
    return None


def set_job_enabled_in_file(
    jobs_file: Path, job_id: str, enabled: bool,
) -> bool:
    """Flip one job's ``enabled`` flag in *jobs_file*. Returns whether it changed.

    Rewrites the smallest span of text that carries the answer — the scalar after
    ``enabled:``, or a single inserted line — rather than re-dumping the
    document. :func:`save_jobs` is the other option and the wrong one for a
    toggle: ``safe_dump`` cannot round-trip comments, so pausing one job would
    silently delete every note in a hand-written cron file and materialize each
    omitted default as an explicit key. The same reasoning, and the same choice,
    as the settings writer in :mod:`nerve.bootstrap`.

    Locating the span is left to the parser. ``yaml.compose`` gives every node
    the offsets it occupied in the source, so the job is found by structure and
    the edit lands at a real position — where matching ``enabled:`` by text or
    indentation would be guessing, and would eventually guess on a job whose
    prompt happens to contain the word.

    The result is parsed and compared against the original before anything is
    written: the requested flag must have moved and nothing else may have. A
    surgical text edit that type-checks is still a text edit, and the cheap
    structural check is what separates "the file now says what I meant" from
    "the old value is gone".

    Raises :class:`JobEditError` if the file can't be parsed, holds no such job,
    or is shaped so the flag can't be placed.
    """
    try:
        text = jobs_file.read_text(encoding="utf-8")
    except OSError as e:
        raise JobEditError(f"Cannot read {jobs_file}: {e}") from e

    try:
        root = yaml.compose(text)
    except yaml.YAMLError as e:
        raise JobEditError(f"Cannot parse {jobs_file}: {e}") from e
    if root is None:
        raise JobEditError(f"{jobs_file} is empty")

    sequence = _find_jobs_sequence(root, jobs_file)

    target: yaml.MappingNode | None = None
    for item in sequence.value:
        if not isinstance(item, yaml.MappingNode):
            continue
        found = _mapping_entry(item, "id")
        if (
            found is not None
            and isinstance(found[1], yaml.ScalarNode)
            and found[1].value == job_id
        ):
            target = item
            break
    if target is None:
        raise JobEditError(f"{jobs_file} defines no job {job_id!r}")

    literal = "true" if enabled else "false"
    existing = _mapping_entry(target, "enabled")

    if existing is not None:
        _, value_node = existing
        if not isinstance(value_node, yaml.ScalarNode):
            raise JobEditError(
                f"{jobs_file}: job {job_id!r} has a non-scalar 'enabled' value "
                "— edit it by hand"
            )
        start, end = value_node.start_mark.index, value_node.end_mark.index
        # Anything after `enabled:` on that line — a trailing comment most
        # often — sits outside the scalar's span and survives untouched.
        new_text = text[:start] + literal + text[end:]
    else:
        # No flag to overwrite, so one is added. It goes directly beneath `id:`,
        # at the column the mapping's keys already use: the first key of a
        # sequence item shares its line with the `- `, so taking the indent from
        # `id` rather than from that line is what keeps the insert aligned with
        # its siblings instead of with the dash.
        if target.flow_style:
            raise JobEditError(
                f"{jobs_file}: job {job_id!r} is written in flow style "
                "({...}) and has no 'enabled' key — add one by hand"
            )
        id_key, id_value = _mapping_entry(target, "id")  # type: ignore[misc]
        indent = " " * id_key.start_mark.column
        lines = text.splitlines(keepends=True)
        at = id_value.end_mark.line + 1
        # A file whose last line has no newline would otherwise get the new key
        # appended to it.
        if at > 0 and lines[at - 1] and not lines[at - 1].endswith("\n"):
            lines[at - 1] += "\n"
        lines.insert(at, f"{indent}enabled: {literal}\n")
        new_text = "".join(lines)

    if new_text == text:
        return False

    _verify_only_enabled_changed(text, new_text, job_id, enabled, jobs_file)

    try:
        jobs_file.write_text(new_text, encoding="utf-8")
    except OSError as e:
        raise JobEditError(f"Cannot write {jobs_file}: {e}") from e
    return True


def _verify_only_enabled_changed(
    before: str, after: str, job_id: str, enabled: bool, jobs_file: Path,
) -> None:
    """Refuse an edit that did anything besides set *job_id*'s flag.

    Compares the two documents as data: the target job's ``enabled`` must now be
    *enabled*, and every other key of every job — including the rest of the
    target's — must be identical. A span computed from stale offsets, a second
    job sharing the id, a truncating write: each shows up here as a diff nobody
    asked for, before the file is touched.
    """
    def jobs_of(text: str) -> list[dict]:
        data = yaml.safe_load(text) or {}
        raw = data.get("jobs", []) if isinstance(data, dict) else data
        return [j for j in raw if isinstance(j, dict)] if isinstance(raw, list) else []

    try:
        old_jobs, new_jobs = jobs_of(before), jobs_of(after)
    except yaml.YAMLError as e:
        raise JobEditError(
            f"{jobs_file}: editing job {job_id!r} would leave unparseable "
            f"YAML ({e}) — file not written"
        ) from e

    if len(old_jobs) != len(new_jobs):
        raise JobEditError(
            f"{jobs_file}: editing job {job_id!r} changed the job count "
            f"({len(old_jobs)} → {len(new_jobs)}) — file not written"
        )

    for old, new in zip(old_jobs, new_jobs):
        is_target = old.get("id") == job_id
        if is_target and new.get("enabled") is not enabled:
            raise JobEditError(
                f"{jobs_file}: setting job {job_id!r} to enabled={enabled} "
                f"produced {new.get('enabled')!r} — file not written"
            )
        stripped_old = {k: v for k, v in old.items() if k != "enabled"}
        stripped_new = {k: v for k, v in new.items() if k != "enabled"}
        if stripped_old != stripped_new or (
            not is_target and old.get("enabled") != new.get("enabled")
        ):
            raise JobEditError(
                f"{jobs_file}: editing job {job_id!r} would also change job "
                f"{old.get('id')!r} — file not written"
            )


def save_jobs(jobs: list[CronJob], jobs_file: Path) -> None:
    """Save cron jobs to a YAML file.

    Guarded even though nothing calls it today: the file it writes is
    ``<workspace>/config/cron/jobs.yaml`` on a migrated install, which is tracked
    config, and a guard added when the first caller appears is a guard that was
    missing for however long the caller took to notice.
    """
    from nerve.config import ensure_path_not_tracked_config

    ensure_path_not_tracked_config(jobs_file, "save cron jobs to")
    jobs_file.parent.mkdir(parents=True, exist_ok=True)
    data = {"jobs": []}
    for job in jobs:
        data["jobs"].append({
            "id": job.id,
            "schedule": job.schedule,
            "prompt": job.prompt,
            "prompt_file": job.prompt_file,
            "workflow": job.workflow,
            "description": job.description,
            "model": job.model,
            "effort": job.effort,
            "cache_ttl": job.cache_ttl,
            "session_mode": job.session_mode,
            "context_rotate_hours": job.context_rotate_hours,
            "context_rotate_at": job.context_rotate_at,
            "reminder_mode": job.reminder_mode,
            "catchup": job.catchup,
            "enabled": job.enabled,
            "lock": job.lock,
            "run_if": job.run_if,
            "skip_when_idle": job.skip_when_idle,
            "idle_consumer": job.idle_consumer,
            "show_session_label": job.show_session_label,
            "metadata": job.metadata,
        })

    with open(jobs_file, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False)
