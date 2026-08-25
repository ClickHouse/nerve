"""Pattern-matching primitives for transport access policies.

A gate matches an identity's platform ID and resolved aliases using
case-insensitive globs. Deny wins, a non-empty allow list must match, and an
incomplete identity cannot clear a deny list.

Aliases are split by who controls them. A deny rule may match any of them.
An allow rule may match only the ones the subject cannot set for itself,
because a grant that rests on a self-set name lets the subject choose its
own access.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import Callable


def _norm(value: object) -> str:
    """Normalize a value for case-insensitive matching."""
    return str(value).strip().lower()


def _matches(value: str, pattern: str) -> bool:
    """Case-insensitive shell-glob match of one value against one pattern."""
    return fnmatch.fnmatchcase(_norm(value), _norm(pattern))


@dataclass(frozen=True)
class Identity:
    """A platform ID and resolved names used for policy matching.

    ``names`` hold identity the platform or its administrators control, so a
    grant may rest on them. ``self_set_names`` hold whatever the subject can
    set without approval; a deny rule may match those, but an allow rule may
    not, because the subject would then choose its own access.

    ``complete`` means the candidates cover every name relevant to the active
    patterns. ID-only policies therefore need no lookup, while deny lists reject
    an incomplete identity.
    """

    id: str = ""
    names: tuple[str, ...] = ()
    self_set_names: tuple[str, ...] = ()
    complete: bool = True

    @property
    def candidates(self) -> tuple[str, ...]:
        """Every string an allow rule may grant on."""
        return tuple(v for v in (self.id, *self.names) if v)

    @property
    def deny_candidates(self) -> tuple[str, ...]:
        """Every string a deny rule may refuse on, self-set names included."""
        return tuple(
            v for v in (self.id, *self.names, *self.self_set_names) if v
        )

    def __str__(self) -> str:
        label = next(iter((*self.names, *self.self_set_names)), "")
        if label and self.id:
            return f"{label} ({self.id})"
        return label or self.id or "unknown"


@dataclass(frozen=True)
class Decision:
    """The outcome of a policy check, with a reason fit for a log line."""

    allowed: bool
    reason: str = ""

    def __bool__(self) -> bool:
        return self.allowed


@dataclass
class PatternGate:
    """Allow/deny matching for a labeled identity."""

    label: str
    allow: list[str] = field(default_factory=list)
    deny: list[str] = field(default_factory=list)

    def any_deny_pattern(self, predicate: Callable[[str], bool]) -> bool:
        """Whether any non-empty deny pattern satisfies *predicate*."""
        return any(predicate(p) for p in self.deny if p)

    def check(self, who: Identity) -> Decision:
        """Decide whether *who* passes this gate."""
        candidates = who.candidates

        for pattern in self.deny:
            for value in who.deny_candidates:
                if _matches(value, pattern):
                    return Decision(
                        False,
                        f"{self.label} {who} matches deny pattern {pattern!r}",
                    )

        # A deny list is only meaningful against a candidate set known to
        # cover it. Refuse rather than let an unread name walk past the list
        # that names it.
        if self.deny and not who.complete:
            return Decision(
                False,
                f"{self.label} {who} could not be fully identified, so the "
                f"deny list cannot be checked",
            )

        if self.allow:
            if not candidates:
                return Decision(
                    False, f"{self.label} is unidentified and an allow list is set",
                )
            for pattern in self.allow:
                for value in candidates:
                    if _matches(value, pattern):
                        return Decision(
                            True,
                            f"{self.label} {who} matches allow pattern {pattern!r}",
                        )
            # Say so when the only match was on a name the subject sets,
            # otherwise the rule looks broken rather than declined.
            for pattern in self.allow:
                for value in who.self_set_names:
                    if _matches(value, pattern):
                        return Decision(
                            False,
                            f"{self.label} {who} matches allow pattern "
                            f"{pattern!r} only on a profile name it sets "
                            f"itself; grant on the id, handle, or email",
                        )
            return Decision(False, f"{self.label} {who} is not on the allow list")

        return Decision(True, "")


def needs_name_resolution(
    *gates: PatternGate, is_id: Callable[[str], bool] | None = None,
) -> bool:
    """Whether any gate pattern requires names beyond the platform ID.

    ``is_id`` recognizes literal IDs. Omitting it forces resolution, as does
    any glob; extra lookups are safer than skipping one needed by a deny rule.
    """
    for gate in gates:
        for pattern in (*gate.allow, *gate.deny):
            if not pattern:
                continue
            if any(c in pattern for c in "*?["):
                return True
            if is_id is None or not is_id(pattern):
                return True
    return False


__all__ = [
    "Decision",
    "Identity",
    "PatternGate",
    "needs_name_resolution",
]
