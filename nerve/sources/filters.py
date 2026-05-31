"""Inbox guardrails — programmatic filtering of source records before they
reach the agent's inbox.

A guardrail is the choke point between a source's raw fetch output and the
inbox (``source_messages``). It limits what an autonomous agent can ever
see, shrinking the prompt-injection attack surface — especially important in
worker mode, where the agent acts on inbox content without a human in the
loop.

Filters are declarative. Each :class:`FieldRule` matches one field of a
:class:`~nerve.sources.models.SourceRecord` — a ``metadata`` key, or the
special ``source`` / ``record_type`` attributes — against an *allow* list
and a *deny* list.

Per-rule semantics:

* **deny wins** — if the value matches any deny pattern, the record is
  dropped, regardless of the allow list.
* **allow is a gate** — if the allow list is non-empty, the value MUST match
  one of its patterns or the record is dropped. An absent field cannot
  satisfy a non-empty allow list (fail-closed).
* an empty allow list means "allow anything not denied".

A record is kept only if it passes **every** rule (logical AND).

Matching is case-insensitive and supports shell-style globs
(``ClickHouse/*``). List-valued metadata (e.g. Gmail ``labels``) matches if
*any* element matches. Non-string scalars (e.g. Telegram ``chat_id``) are
coerced to ``str`` before matching.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.sources.models import SourceRecord

logger = logging.getLogger(__name__)


def _norm(value: object) -> str:
    """Normalize a value for case-insensitive matching."""
    return str(value).strip().lower()


def _matches_any(value: str, patterns: list[str]) -> bool:
    """Case-insensitive shell-glob match of *value* against any pattern."""
    norm_value = _norm(value)
    return any(fnmatch.fnmatchcase(norm_value, _norm(p)) for p in patterns)


@dataclass
class FieldRule:
    """Allow/deny matching on a single record field.

    Args:
        field: A :class:`SourceRecord` ``metadata`` key, or the special
            attributes ``"source"`` / ``"record_type"``.
        allow: If non-empty, the field value must match at least one pattern.
        deny: If the field value matches any pattern, the record is dropped
            (takes precedence over ``allow``).
    """

    field: str
    allow: list[str] = dataclasses.field(default_factory=list)
    deny: list[str] = dataclasses.field(default_factory=list)

    @property
    def active(self) -> bool:
        """Whether this rule actually constrains anything."""
        return bool(self.allow or self.deny)

    def _values(self, record: SourceRecord) -> list[str]:
        """Extract candidate string value(s) for this field from *record*."""
        if self.field == "source":
            raw: object = record.source
        elif self.field == "record_type":
            raw = record.record_type
        else:
            raw = (record.metadata or {}).get(self.field)

        if raw is None:
            return []
        if isinstance(raw, (list, tuple, set)):
            return [str(v) for v in raw if v is not None]
        return [str(raw)]

    def passes(self, record: SourceRecord) -> bool:
        """Return True if *record* satisfies this rule (should be kept)."""
        values = self._values(record)

        # deny wins — drop on any deny match.
        if self.deny and any(_matches_any(v, self.deny) for v in values):
            return False

        # allow gate — when set, require a match. An absent field fails closed.
        if self.allow:
            if not values:
                return False
            return any(_matches_any(v, self.allow) for v in values)

        return True


@dataclass
class InboxFilter:
    """A set of :class:`FieldRule` applied to source records (logical AND).

    An inactive filter (no rules, or all rules empty) is a pass-through —
    :meth:`partition` returns every record as kept with nothing dropped.
    """

    rules: list[FieldRule] = dataclasses.field(default_factory=list)

    @property
    def active(self) -> bool:
        """Whether any rule actually constrains anything."""
        return any(r.active for r in self.rules)

    def passes(self, record: SourceRecord) -> bool:
        """Return True if *record* passes all rules (should be kept)."""
        return all(r.passes(record) for r in self.rules)

    def partition(
        self, records: list[SourceRecord],
    ) -> tuple[list[SourceRecord], list[SourceRecord]]:
        """Split *records* into ``(kept, dropped)``.

        Order within each list is preserved. When the filter is inactive,
        all records are kept and nothing is dropped (no per-record work).
        """
        if not self.active:
            return list(records), []
        kept: list[SourceRecord] = []
        dropped: list[SourceRecord] = []
        for r in records:
            (kept if self.passes(r) else dropped).append(r)
        return kept, dropped

    @classmethod
    def from_field(
        cls, field: str, allow: list[str], deny: list[str],
    ) -> InboxFilter:
        """Build a single-rule filter for one field (the common case)."""
        return cls(rules=[FieldRule(field=field, allow=allow or [], deny=deny or [])])
