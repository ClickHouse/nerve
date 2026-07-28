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
        beats quietly dropping the gate. The job id is added to the message here
        because the spec alone doesn't say which YAML entry to go and fix, and
        this text is what reaches the reload's 400.
        """
        from nerve.cron.gates import GateConfigError, build_gates

        specs: list[dict] = list(self.run_if)
        # Translate the legacy skip_when_idle shorthand into a messages gate
        # so old configs keep working without rewrites.
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
    def from_dict(cls, d: dict, base_dir: Path | None = None) -> CronJob:
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
            run_if=d.get("run_if", []),
            skip_when_idle=d.get("skip_when_idle", []),
            idle_consumer=d.get("idle_consumer", "inbox"),
            show_session_label=d.get("show_session_label", True),
            metadata=d.get("metadata", {}),
        )
        if job.prompt_file:
            p = Path(job.prompt_file).expanduser()
            if not p.is_absolute() and base_dir is not None:
                p = (base_dir / p).resolve()
            job.prompt_path = p
        return job


def load_jobs(jobs_file: Path, strict: bool = False) -> list[CronJob]:
    """Load cron jobs from a YAML file.

    By default this is *tolerant*: a YAML parse failure or an invalid job entry
    is logged and skipped (so a typo never takes down daemon startup). With
    ``strict=True`` any such failure raises :class:`nerve.config.ConfigError`
    instead — used by hot-reload so a malformed file is refused rather than
    silently unscheduling every job. A missing file is never an error (returns
    ``[]``) in either mode.
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
            jobs.append(CronJob.from_dict(item, base_dir=jobs_file.parent))
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Invalid cron job definition: %s — %s", item, e)
            if strict:
                from nerve.config import ConfigError
                raise ConfigError(
                    f"Invalid cron job in {jobs_file}: {item!r} — {e}"
                ) from e

    logger.info("Loaded %d cron jobs from %s", len(jobs), jobs_file)
    return jobs


def save_jobs(jobs: list[CronJob], jobs_file: Path) -> None:
    """Save cron jobs to a YAML file."""
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
