"""Scalar coercion at the config boundary.

YAML gives a real ``bool`` for ``enabled: false`` and a real ``int`` for
``port: 8900``. A ``${ENV_VAR}`` reference does not: interpolation is a string
substitution, so the value that reaches a builder is always ``str``.
``bool("false")`` is ``True``, and an int field left as ``"8900"`` raises at
its first arithmetic or ``socket.bind`` use.

That is not a corner case here. Under lockdown the machine-local config layers
are discarded, so every per-machine value *has* to be an env reference in the
tracked settings — precisely the shape that breaks. A quoted YAML scalar
(``enabled: "false"``) reaches the same code path with no env var involved at
all.

Coercion is applied at the ``from_dict`` boundary (see :func:`coerced`) rather
than in ``__init__``, so constructing a config object in code keeps exact-type
semantics and a test can still pass a deliberately odd value. The decorator
goes on *every* ``from_dict``, including the ones whose dataclass currently has
no numeric or boolean field, so that adding one later needs no ceremony.

Coercers are *lenient* about values they cannot parse: a bad value in a section
that isn't even active must not stop the daemon from starting. They are logged
with the owning ``Class.field`` so the line is actually actionable.

This module lives outside ``nerve.config`` so that dataclasses defined in other
packages (which ``nerve.config`` imports) can use it without an import cycle.
Keep it dependency-free.
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import types
import typing
from typing import Any

logger = logging.getLogger(__name__)

TRUTHY = frozenset({"1", "true", "yes", "on", "y", "t"})
FALSY = frozenset({"0", "false", "no", "off", "n", "f"})

# Sentinel for "this value could not be converted" — distinct from None, which
# is a legitimate value for an Optional field.
_UNCONVERTIBLE = object()


def _log_reject(value: Any, default: Any, kind: str, label: str) -> None:
    logger.warning(
        "%signoring non-%s config value %r, using %r",
        f"{label}: " if label else "",
        kind,
        value,
        default,
    )


def lenient_int(value: Any, default: int, *, label: str = "") -> int:
    """Best-effort ``int`` coercion, falling back to ``default``.

    Note that this is ``int()``, so ``"1.5"`` and ``"1e3"`` are rejected
    rather than truncated — they fall back to the default with a warning.
    """
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        _log_reject(value, default, "integer", label)
        return default


def lenient_float(value: Any, default: float, *, label: str = "") -> float:
    """Best-effort ``float`` coercion, falling back to ``default``."""
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        _log_reject(value, default, "numeric", label)
        return default


def as_bool(value: Any, default: bool, *, label: str = "") -> bool:
    """Best-effort ``bool`` coercion accepting the usual YAML/shell spellings.

    Three cases, and the distinction between them matters:

    * ``None`` (a bare ``enabled:`` in YAML) and ``""`` (a ``${VAR}`` whose
      variable is set but empty) are **false**. They were false before this
      module existed — ``bool(None)`` and ``bool("")`` — and an empty env var
      is the natural way to spell "off" for a value that must be an env
      reference under lockdown. Returning the field's default here would make
      a blanked-out flag silently keep whatever the tracked config said.
    * A recognized spelling is parsed, not tested for truthiness. This is the
      whole point: ``bool("false")`` is ``True``.
    * Anything else falls back to the field's declared default and is logged.
      Falling back rather than guessing means a typo can't flip a flag to the
      opposite of both what it says and what the config declares.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return False
        if text in TRUTHY:
            return True
        if text in FALSY:
            return False
    _log_reject(value, default, "boolean", label)
    return default


_COERCERS: dict[type, Any] = {bool: as_bool, int: lenient_int, float: lenient_float}


def _convert_element(base: type, value: Any) -> Any:
    """Strict conversion for a list element — no default to fall back to."""
    if type(value) is base:
        return value
    if value is None:
        # Already the answer for the numeric types (``int(None)`` raises), but
        # spelled out because ``str(None)`` would otherwise turn a YAML ``~``
        # entry into the literal text "None" instead of flagging it.
        return _UNCONVERTIBLE
    if base is bool:
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in TRUTHY:
                return True
            if text in FALSY:
                return False
        return _UNCONVERTIBLE
    try:
        return base(value)
    except (TypeError, ValueError):
        return _UNCONVERTIBLE


def _scalar_to_list(value: Any, base: type) -> list[Any] | None:
    """Normalize a non-list value on a ``list[X]`` field, or ``None`` to skip.

    A ``${VAR}`` reference on a list field interpolates to a plain string, and
    lockdown makes env references mandatory for per-machine values, which
    ``telegram.allowed_users`` and ``sync.telegram.exclude_chats`` both are.
    Leaving such a string alone moves the problem to the consumers rather than
    avoiding it: both call ``set()`` on the value, and ``set("123")`` is
    ``{"1", "2", "3"}``. An allowlist then fails closed, but an exclude list
    fails open — an int chat id never equals a one-character string, so an
    excluded chat is ingested.

    **How a string becomes a list depends on the element type**, because the two
    cases have different safe answers:

    * ``list[int]``/``list[float]``/``list[bool]`` split on comma, matching how
      the bootstrap wizard parses this same field. A comma cannot occur inside a
      valid number, so the separator is unambiguous.
    * ``list[str]`` is **wrapped as a single element**, never split. A comma is
      legal inside a string — three of the four default
      ``langfuse.redact_patterns`` are regexes containing ``{20,}`` — so
      splitting would corrupt values that are correct as written. One ``${VAR}``
      is one element; several values are spelled as a YAML list.

    A blank string yields the empty list rather than one empty entry, and a lone
    non-string scalar (``exclude_chats: 42``) is wrapped so it reaches the
    element coercion below.

    A tuple is copied element-wise. YAML never parses to one, so a tuple can
    only be a code-side default — a builder handing a module-level tuple
    constant to a ``list[X]`` field — where the elements are already the
    intended entries and only the container is wrong.
    """
    if value is None:
        return []
    if isinstance(value, str):
        if base is str:
            return [value] if value.strip() else []
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (bool, int, float)):
        return [value]
    if isinstance(value, tuple):
        return list(value)
    return None


def _classify(declared: Any) -> tuple[str, type] | None:
    """Map a declared annotation onto ``(kind, base_type)``.

    Handles the three shapes that actually appear in the config dataclasses:
    a bare scalar, ``X | None``, and ``list[X]``. Anything else is left alone.

    ``str`` is absent from ``_COERCERS`` — a bare ``str`` field needs no
    coercion, since interpolation already yields one — but ``list[str]`` does,
    because the container is the thing that gets lost. See
    :func:`_scalar_to_list`.
    """
    if declared in _COERCERS:
        return ("scalar", declared)
    origin = typing.get_origin(declared)
    args = typing.get_args(declared)
    if origin in (typing.Union, types.UnionType):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and non_none[0] in _COERCERS:
            return ("optional", non_none[0])
        return None
    if origin is list and len(args) == 1 and (args[0] in _COERCERS or args[0] is str):
        return ("list", args[0])
    return None


@functools.lru_cache(maxsize=None)
def scalar_fields(cls: type) -> tuple[tuple[str, str, type, Any], ...]:
    """``(name, kind, base_type, default)`` for each coercible field."""
    hints = typing.get_type_hints(cls)
    out: list[tuple[str, str, type, Any]] = []
    for f in dataclasses.fields(cls):
        classified = _classify(hints.get(f.name))
        if classified is None:
            continue
        kind, base = classified
        if f.default is not dataclasses.MISSING:
            default = f.default
        elif f.default_factory is not dataclasses.MISSING:
            default = f.default_factory()
        else:
            default = base()
        out.append((f.name, kind, base, default))
    return tuple(out)


def coerce_scalars(obj: Any) -> Any:
    """Force declared bool/int/float fields to their annotated type."""
    cls_name = type(obj).__name__
    for name, kind, base, default in scalar_fields(type(obj)):
        value = getattr(obj, name)
        label = f"{cls_name}.{name}"

        if kind == "list":
            widened = False
            if not isinstance(value, list):
                normalized = _scalar_to_list(value, base)
                if normalized is None:
                    logger.warning(
                        "%s: ignoring non-list config value %r, leaving it as-is",
                        label,
                        value,
                    )
                    continue
                value = normalized
                widened = True
            converted = [_convert_element(base, v) for v in value]
            if any(c is _UNCONVERTIBLE for c in converted):
                # Keep the offending entries verbatim rather than dropping
                # them: silently shrinking a list of allowed users or excluded
                # chats is worse than a type that stands out.
                for original, result in zip(value, converted):
                    if result is _UNCONVERTIBLE:
                        _log_reject(original, original, base.__name__, f"{label}[]")
                converted = [
                    original if result is _UNCONVERTIBLE else result
                    for original, result in zip(value, converted)
                ]
            if (
                widened
                or converted != value
                or any(type(a) is not type(b) for a, b in zip(converted, value))
            ):
                setattr(obj, name, converted)
            continue

        if kind == "optional" and value is None:
            continue

        # `type() is` rather than isinstance: a bool sitting in an int field,
        # or an int in a float field, should still be normalized.
        if type(value) is not base:
            setattr(obj, name, _COERCERS[base](value, default, label=label))
    return obj


def coerced(fn):
    """Decorator for ``from_dict``: coerce declared scalars on the way out.

    Goes *under* ``@classmethod``::

        @classmethod
        @coerced
        def from_dict(cls, d: dict) -> GatewayConfig:
            ...

    (The other order raises ``TypeError: 'classmethod' object is not
    callable`` at import, so it cannot silently no-op.)

    Builders should hand raw YAML values straight to the constructor. An eager
    ``bool(d.get("enabled", True))`` inside the builder defeats this entirely,
    because ``bool("false")`` is already ``True`` by the time it arrives.
    """

    @functools.wraps(fn)
    def wrapper(cls, *args, **kwargs):
        return coerce_scalars(fn(cls, *args, **kwargs))

    return wrapper
