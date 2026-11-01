"""Validated, repository-independent review profiles."""

import math
import re
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo



@dataclass
class ReviewProfile:
    id: str
    repository: str
    query: str
    enabled: bool = False
    schedule: str = "0 */2 * * *"
    timezone: str = "UTC"
    version_budget_usd: float = 5.0
    daily_budget_usd: float = 30.0
    max_attempts: int = 3

    def __post_init__(self):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", self.id):
            raise ValueError("code_review profile id must be a lowercase slug")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repository):
            raise ValueError("code_review repository must be owner/repository")
        if not self.query.strip():
            raise ValueError("code_review query is required")
        if not isinstance(self.enabled, bool):
            raise ValueError("code_review enabled must be a boolean")
        ZoneInfo(self.timezone)
        from nerve.cron.service import _crontab_to_trigger
        _crontab_to_trigger(self.schedule, timezone=ZoneInfo(self.timezone))
        for value in (self.version_budget_usd, self.daily_budget_usd):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("code_review budgets must be finite and positive")
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("code_review max_attempts must be between 1 and 10")


@dataclass
class CodeReviewConfig:
    board_name: str = "Tasks Code Review"
    profiles: list[ReviewProfile] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data):
        data = data or {}
        profiles = [ReviewProfile(**p) for p in data.get("profiles", [])]
        if len({p.id for p in profiles}) != len(profiles):
            raise ValueError("duplicate code_review profile id")
        # Daily accounting is shared across this automation, not per profile.
        if len({(p.timezone, p.daily_budget_usd) for p in profiles}) > 1:
            raise ValueError("code_review profiles must share timezone and daily budget")
        return cls(board_name=str(data.get("board_name") or "Tasks Code Review"), profiles=profiles)
