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
* a rule may exempt record types: a record of such a type passes the rule
  whatever its value.

A record is kept only if it passes **every** rule (logical AND).

Matching is case-insensitive and supports shell-style globs
(``ClickHouse/*``). List-valued metadata (e.g. Gmail ``labels``) matches if
*any* element matches. Non-string scalars (e.g. Telegram ``chat_id``) are
coerced to ``str`` before matching.

A generic hosted source (Linear, for example) uses :class:`DataRule`: the
record's metadata is the JSON data that the provider sent, and each rule
names a value in it with a JSON Pointer. The channel gateway applies the same
rules before it stores a record, so these semantics are the channel
contract's, and the two sides drop the same records.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from nerve.sources.source_json import SourceJSONError, canonical_number, pointer_tokens, resolve_pointer

if TYPE_CHECKING:
    from nerve.sources.models import SourceRecord

logger = logging.getLogger(__name__)


def _norm(value: object) -> str:
    """Normalize a value for case-insensitive matching."""
    return str(value).strip().lower()


def matches_any(value: object, patterns: Iterable[str]) -> bool:
    """Case-insensitive shell-glob match of *value* against any pattern.

    The guardrail rules, for a source that applies them itself (the GitHub
    Repos source's ``deny_authors``): both sides are stripped and lowered,
    then matched with :func:`fnmatch.fnmatchcase`, so ``[...]`` is a
    character class.
    """
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
        exempt_record_types: Record types the rule does not apply to; a
            record of one of them passes the rule without being matched.
    """

    field: str
    allow: list[str] = dataclasses.field(default_factory=list)
    deny: list[str] = dataclasses.field(default_factory=list)
    exempt_record_types: tuple[str, ...] = ()

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

    def value(self, record: SourceRecord) -> object:
        """The metadata value that the rule judges, for a log line."""
        return (record.metadata or {}).get(self.field)

    def passes(self, record: SourceRecord) -> bool:
        """Return True if *record* satisfies this rule (should be kept)."""
        if record.record_type in self.exempt_record_types:
            return True
        return _passes(self._values(record), self.allow, self.deny)


def _passes(values: list[str], allow: Sequence[str], deny: Sequence[str]) -> bool:
    """The decision of one rule on its values, with the semantics at the top of this module."""
    # deny wins — drop on any deny match.
    if deny and any(matches_any(v, deny) for v in values):
        return False

    # allow gate — when set, require a match. An absent field fails closed.
    if allow:
        if not values:
            return False
        return any(matches_any(v, allow) for v in values)

    return True


@dataclass(frozen=True)
class DataRule:
    """Allow/deny matching on a value in the data of a generic hosted record.

    ``path`` is a JSON Pointer (RFC 6901) into the record's metadata, which
    holds the provider's data, such as ``/actor/kind``. The values at the
    path are:

    * a string, as it is;
    * a number, as its canonical JSON text: ``2`` for ``2.0``, ``1.5``;
    * ``true``, ``false`` and ``null``, as those words;
    * each element of an array that is one of the above.

    An object, an array or object in an array, an empty array, and a path
    that names nothing in the data give no value. ``allow`` and ``deny``
    match those values as :class:`FieldRule` matches: a deny match drops the
    record, and a non-empty allow list drops a record without a match, also
    one without a value. A path that is not a valid JSON Pointer names
    nothing.
    """

    path: str
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()

    @property
    def field(self) -> str:
        """The path, as the runner's drop log and the drop hook name a rule."""
        return self.path

    @property
    def active(self) -> bool:
        return bool(self.allow or self.deny)

    def _resolved(self, record: SourceRecord) -> tuple[bool, Any]:
        tokens = pointer_tokens(self.path)
        if tokens is None:
            return False, None
        return resolve_pointer(record.metadata or {}, tokens)

    def values(self, record: SourceRecord) -> list[str]:
        """The values that the rule judges in *record*."""
        found, value = self._resolved(record)
        if not found:
            return []
        values: list[str] = []
        for element in value if isinstance(value, (list, tuple)) else [value]:
            if isinstance(element, str):
                values.append(element)
            elif element is None:
                values.append("null")
            elif isinstance(element, bool):
                values.append("true" if element else "false")
            elif isinstance(element, (int, float)):
                try:
                    values.append(canonical_number(element))
                except SourceJSONError:  # a number that no JSON data can hold
                    continue
        return values

    def value(self, record: SourceRecord) -> object:
        """The value at the path, for a log line; ``None`` when there is none."""
        return self._resolved(record)[1]

    def passes(self, record: SourceRecord) -> bool:
        """Return True if *record* satisfies this rule (should be kept)."""
        return _passes(self.values(record), self.allow, self.deny)


def data_rules(rules: Iterable[Mapping[str, Any]]) -> list[DataRule]:
    """The :class:`DataRule` of each rule of a generic source's settings.

    Each rule is a mapping with a ``path`` and an ``allow`` or ``deny`` list,
    as the config loads it (``sync.linear.rules``, for example).
    """
    return [
        DataRule(
            path=str(rule.get("path", "")),
            allow=tuple(rule.get("allow") or ()),
            deny=tuple(rule.get("deny") or ()),
        )
        for rule in rules
    ]


@dataclass
class InboxFilter:
    """A set of rules applied to source records (logical AND).

    Each rule is a :class:`FieldRule` or a :class:`DataRule`. An inactive
    filter (no rules, or all rules empty) is a pass-through —
    :meth:`partition` returns every record as kept with nothing dropped.
    """

    rules: list[FieldRule | DataRule] = dataclasses.field(default_factory=list)

    @property
    def active(self) -> bool:
        """Whether any rule actually constrains anything."""
        return any(r.active for r in self.rules)

    def passes(self, record: SourceRecord) -> bool:
        """Return True if *record* passes all rules (should be kept)."""
        return all(r.passes(record) for r in self.rules)

    def rejects(self, record: SourceRecord) -> FieldRule | DataRule | None:
        """The first rule that drops *record*, or None if it passes.

        Dropped records are never persisted, so this is the only way to tell
        *why* something vanished (see the runner's drop logging).
        """
        for rule in self.rules:
            if not rule.passes(record):
                return rule
        return None

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
