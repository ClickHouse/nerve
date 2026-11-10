"""The JSON values of generic hosted sources.

A generic source of the hosted channel gateway, such as Linear, sends the data
of each record as a JSON object that the provider defines, and Nerve sends
the settings of a subscription to it as a JSON object too. The channel
contract calls both a *source JSON object*: a JSON object without a repeated
member name, of valid Unicode (no unpaired surrogate), with at most
:data:`MAX_SOURCE_JSON_DEPTH` levels of objects and arrays (the object itself
is the first level), and with numbers that IEEE 754 binary64 can hold. Its
byte bounds count the UTF-8 bytes of its canonical form (RFC 8785, the JSON
Canonicalization Scheme), which does not depend on how a sender encodes it.

The rules of a subscription keep or drop records by the value at a JSON
Pointer (RFC 6901) into their data.

This module holds what the channel contract (which checks these values on
the wire) and the inbox guardrail (which applies the rules again) share. It
works on the values that :func:`json.loads` returns, and it imports nothing
from Nerve.
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from decimal import Decimal
from typing import Any

# The levels of objects and arrays in a source JSON object, the object itself
# as the first. A JSON Pointer into one has at most this many tokens.
MAX_SOURCE_JSON_DEPTH = 8
# The UTF-8 bytes of a JSON Pointer in a subscription rule.
MAX_POINTER_BYTES = 256
# Plain decimal notation of a canonical number: from 1e-6 up to but not
# including 1e21 (ECMAScript Number.prototype.toString).
_PLAIN_EXPONENT_MIN = -6
_PLAIN_EXPONENT_MAX = 21
# A "~" that does not start the escape "~0" or "~1".
_BAD_ESCAPE = re.compile(r"~(?![01])")
# An array index in a JSON Pointer: decimal, without a leading zero.
_ARRAY_INDEX = re.compile(r"0|[1-9][0-9]*")


class SourceJSONError(ValueError):
    """A value that breaks a rule of a source JSON object.

    ``limit`` is true when the value is above a bound (its depth or its
    size), and false when it is not valid.
    """

    def __init__(self, detail: str, *, limit: bool = False) -> None:
        super().__init__(detail)
        self.limit = limit


def check_source_json_object(value: Any, maximum_bytes: int) -> None:
    """Check that *value* is a source JSON object within *maximum_bytes*.

    The bound counts the UTF-8 bytes of the canonical form. Raises
    :class:`SourceJSONError`. A repeated member name cannot be seen here, so
    the parser refuses it (see ``nerve.channels.hosted.contract.wire``).
    """
    if not isinstance(value, dict):
        raise SourceJSONError("is not a JSON object")
    _check(value, 1)
    if len(canonical_json(value).encode("utf-8")) > maximum_bytes:
        raise SourceJSONError("exceeds its byte limit", limit=True)


def _check(value: Any, depth: int) -> None:
    if isinstance(value, (dict, list, tuple)):
        if depth > MAX_SOURCE_JSON_DEPTH:
            raise SourceJSONError("exceeds its depth limit", limit=True)
        if isinstance(value, dict):
            for name, member in value.items():
                if not isinstance(name, str):
                    raise SourceJSONError("has a member name that is not a string")
                _check_text(name)
                _check(member, depth + 1)
        else:
            for element in value:
                _check(element, depth + 1)
    elif isinstance(value, str):
        _check_text(value)
    elif isinstance(value, bool) or value is None:
        pass
    elif isinstance(value, (int, float)):
        _binary64(value)
    else:
        raise SourceJSONError(f"holds a {type(value).__name__}, which is not a JSON value")


def _check_text(text: str) -> None:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        raise SourceJSONError("has a string with an unpaired surrogate") from None


def _binary64(number: int | float) -> float:
    """*number* as binary64, or :class:`SourceJSONError` when it does not fit.

    A number below the smallest binary64 value is zero, as the parsers of
    both sides read it.
    """
    try:
        value = float(number)
    except OverflowError:
        raise SourceJSONError("has a number outside the binary64 range") from None
    if not math.isfinite(value):
        raise SourceJSONError("has a number outside the binary64 range")
    return value


# ---------------------------------------------------------------------- #
#  Canonical form (RFC 8785)                                              #
# ---------------------------------------------------------------------- #


def canonical_json(value: Any) -> str:
    """The RFC 8785 canonical form of a value that passes the checks.

    Object members sorted by the UTF-16 code units of their names, no white
    space, the shortest ECMAScript form of each number, and the minimal
    escapes of each string: only the quotation mark, the reverse solidus and
    the characters below U+0020 are escaped.
    """
    parts: list[str] = []
    _write(parts, value)
    return "".join(parts)


def _write(parts: list[str], value: Any) -> None:
    if value is None:
        parts.append("null")
    elif value is True:
        parts.append("true")
    elif value is False:
        parts.append("false")
    elif isinstance(value, (int, float)):
        parts.append(canonical_number(value))
    elif isinstance(value, str):
        # The escapes of json.dumps without ensure_ascii are those of RFC 8785
        # section 3.2.2.2, with lowercase hexadecimal digits.
        parts.append(json.dumps(value, ensure_ascii=False))
    elif isinstance(value, dict):
        parts.append("{")
        names = sorted(value, key=lambda name: name.encode("utf-16-be", "surrogatepass"))
        for index, name in enumerate(names):
            if index:
                parts.append(",")
            parts.append(json.dumps(name, ensure_ascii=False))
            parts.append(":")
            _write(parts, value[name])
        parts.append("}")
    else:
        parts.append("[")
        for index, element in enumerate(value):
            if index:
                parts.append(",")
            _write(parts, element)
        parts.append("]")


def canonical_number(number: int | float) -> str:
    """The ECMAScript ``Number.prototype.toString`` form of a binary64 number.

    RFC 8785 section 3.2.2.3: the fewest digits that read back as the same
    number, plain decimal notation from 1e-6 up to but not including 1e21,
    exponent notation otherwise, and ``0`` for negative zero. An integer is
    read as binary64 first, so ``12345678901234567890`` is
    ``12345678901234567000`` and ``2.0`` is ``2``.
    """
    value = _binary64(number)
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    # repr gives the shortest digits that read back as the same binary64.
    _, digit_tuple, exponent = Decimal(repr(abs(value))).normalize().as_tuple()
    digits = "".join(map(str, digit_tuple))
    count = len(digits)
    point = int(exponent) + count  # the decimal point is after this many digits
    if count <= point <= _PLAIN_EXPONENT_MAX:
        return sign + digits + "0" * (point - count)
    if 0 < point <= _PLAIN_EXPONENT_MAX:
        return sign + digits[:point] + "." + digits[point:]
    if _PLAIN_EXPONENT_MIN < point <= 0:
        return sign + "0." + "0" * -point + digits
    power = point - 1
    mantissa = digits if count == 1 else f"{digits[0]}.{digits[1:]}"
    return f"{sign}{mantissa}e{'+' if power > 0 else '-'}{abs(power)}"


# ---------------------------------------------------------------------- #
#  JSON Pointer (RFC 6901)                                                #
# ---------------------------------------------------------------------- #


def pointer_tokens(path: str) -> list[str] | None:
    """The reference tokens of *path*, unescaped, or ``None`` when it is not valid.

    A valid path starts with ``/``, has 1 to :data:`MAX_SOURCE_JSON_DEPTH`
    tokens and at most :data:`MAX_POINTER_BYTES` bytes of UTF-8 without
    control characters, and writes ``~`` only in the escapes ``~0`` (``~``)
    and ``~1`` (``/``).
    """
    if not isinstance(path, str) or not path.startswith("/"):
        return None
    try:
        size = len(path.encode("utf-8"))
    except UnicodeEncodeError:
        return None
    if size > MAX_POINTER_BYTES or any(unicodedata.category(c) == "Cc" for c in path):
        return None
    tokens = path[1:].split("/")
    if len(tokens) > MAX_SOURCE_JSON_DEPTH or any(_BAD_ESCAPE.search(t) for t in tokens):
        return None
    # RFC 6901 section 4: ~1 becomes / before ~0 becomes ~.
    return [token.replace("~1", "/").replace("~0", "~") for token in tokens]


def resolve_pointer(value: Any, tokens: list[str]) -> tuple[bool, Any]:
    """Whether the *tokens* name a value in *value*, and that value.

    A token names the member of an object, or the element of an array at the
    index that it writes in decimal without a leading zero.
    """
    for token in tokens:
        if isinstance(value, dict):
            if token not in value:
                return False, None
            value = value[token]
        elif isinstance(value, (list, tuple)):
            if _ARRAY_INDEX.fullmatch(token) is None or int(token) >= len(value):
                return False, None
            value = value[int(token)]
        else:
            return False, None
    return True, value


__all__ = [
    "MAX_POINTER_BYTES",
    "MAX_SOURCE_JSON_DEPTH",
    "SourceJSONError",
    "canonical_json",
    "canonical_number",
    "check_source_json_object",
    "pointer_tokens",
    "resolve_pointer",
]
