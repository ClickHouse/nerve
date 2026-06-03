"""Cron job definitions and persistence.

Jobs are defined in a YAML file and loaded at startup.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from nerve.cron.gates import CronGate

logger = logging.getLogger(__name__)


@dataclass
class CronJob:
    """A cron job definition."""
    id: str
    schedule: str  # crontab expression or interval (e.g., "*/30 * * * *", "2h")
    prompt: str  # The message/instruction sent to the agent
    description: str = ""
    model: str = ""  # Override model; empty = use config default
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

    def __post_init__(self) -> None:
        self.gates = self._build_gates()

    def _build_gates(self) -> list["CronGate"]:
        """Construct gate objects from run_if plus the legacy shorthand."""
        from nerve.cron.gates import build_gates

        specs: list[dict] = list(self.run_if)
        # Translate the legacy skip_when_idle shorthand into a messages gate
        # so old configs keep working without rewrites.
        if self.skip_when_idle:
            specs.append({
                "type": "messages",
                "sources": list(self.skip_when_idle),
                "consumer": self.idle_consumer,
            })
        return build_gates(specs)

    @classmethod
    def from_dict(cls, d: dict) -> CronJob:
        return cls(
            id=d["id"],
            schedule=d["schedule"],
            prompt=d["prompt"],
            description=d.get("description", ""),
            model=d.get("model", ""),
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


def load_jobs(jobs_file: Path) -> list[CronJob]:
    """Load cron jobs from a YAML file."""
    if not jobs_file.exists():
        logger.info("No cron jobs file at %s", jobs_file)
        return []

    try:
        with open(jobs_file) as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error("Failed to load cron jobs from %s: %s", jobs_file, e)
        return []

    jobs_data = data.get("jobs", [])
    if isinstance(data, list):
        jobs_data = data

    jobs = []
    for item in jobs_data:
        try:
            jobs.append(CronJob.from_dict(item))
        except (KeyError, TypeError) as e:
            logger.warning("Invalid cron job definition: %s — %s", item, e)

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
            "description": job.description,
            "model": job.model,
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
