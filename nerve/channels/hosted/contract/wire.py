"""JSON encoding and decoding of the channel contract records.

A record is a frozen dataclass whose field names are the JSON member names.
Decoding checks the JSON type of each member that a record declares. It
ignores members that the record does not declare, so the gateway can add
members without a change in Nerve. A missing or null member takes the
field's default. Encoding leaves out a field marked with :func:`omit_empty`
when it holds its zero value.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import json
import types
import typing
import uuid
from datetime import datetime, timezone
from typing import Any


class RejectionReason:
    """The contract's close reasons that Nerve uses."""

    VERSION_UNSUPPORTED = "version_unsupported"
    SCOPE_MISMATCH = "scope_mismatch"
    KIND_UNSUPPORTED = "kind_unsupported"
    FRAME_TOO_LARGE = "frame_too_large"
    MALFORMED_FRAME = "malformed_frame"
    LIMIT_EXCEEDED = "limit_exceeded"


class Rejected(Exception):
    """A frame or value that Nerve cannot use.

    ``reason`` is one of :class:`RejectionReason` and is safe to send as a
    WebSocket close reason. ``detail`` is for local logs only.
    """

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def reject(reason: str, detail: str) -> typing.NoReturn:
    raise Rejected(reason, detail)


def malformed(detail: str) -> typing.NoReturn:
    raise Rejected(RejectionReason.MALFORMED_FRAME, detail)


def omit_empty(default: Any = "") -> Any:
    """A field that is left out of the JSON when it holds its zero value."""
    return dataclasses.field(default=default, metadata={"omitempty": True})


def struct(factory: type) -> Any:
    """A nested record that defaults to a record of zero values."""
    return dataclasses.field(default_factory=factory)


def format_time(value: datetime) -> str:
    """An RFC 3339 UTC timestamp, as the contract sends it."""
    utc = value.astimezone(timezone.utc)
    text = utc.strftime("%Y-%m-%dT%H:%M:%S")
    if utc.microsecond:
        text += f".{utc.microsecond:06d}".rstrip("0")
    return text + "Z"


def parse_json(text: str) -> Any:
    """Parse one JSON value, or raise :class:`Rejected`."""
    try:
        return json.loads(text)
    except RecursionError:
        reject(RejectionReason.LIMIT_EXCEEDED, "JSON nesting exceeds the parser's depth")
    except ValueError as error:
        malformed(f"decode frame: {error}")


_HINTS: dict[type, dict[str, Any]] = {}


def decode_record(cls: type, raw: Any, path: str = "") -> Any:
    """Decode a JSON object into the record type *cls*."""
    if raw is None:
        return cls()
    if not isinstance(raw, dict):
        malformed(f"decode frame: {path or cls.__name__} is not a JSON object")
    hints = _HINTS.get(cls)
    if hints is None:
        hints = _HINTS[cls] = typing.get_type_hints(cls)
    values: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        value = raw.get(field.name)
        if value is not None:
            member_path = f"{path}.{field.name}" if path else field.name
            values[field.name] = _decode_value(hints[field.name], value, member_path)
    return cls(**values)


def _decode_value(kind: Any, raw: Any, path: str) -> Any:
    origin = typing.get_origin(kind)
    if origin in (typing.Union, types.UnionType):
        (inner,) = [arg for arg in typing.get_args(kind) if arg is not type(None)]
        return _decode_value(inner, raw, path)
    if origin is tuple:
        if not isinstance(raw, list):
            malformed(f"decode frame: {path} is not a JSON array")
        element = typing.get_args(kind)[0]
        return tuple(_decode_value(element, item, f"{path}[{index}]") for index, item in enumerate(raw))
    if dataclasses.is_dataclass(kind):
        return decode_record(kind, raw, path)
    if kind is bool:
        if not isinstance(raw, bool):
            malformed(f"decode frame: {path} is not a boolean")
        return raw
    if kind is int:
        if type(raw) is not int:
            malformed(f"decode frame: {path} is not an integer")
        return raw
    if not isinstance(raw, str):
        malformed(f"decode frame: {path} is not a string")
    if kind is str:
        return raw
    try:
        if kind is datetime:
            value = datetime.fromisoformat(raw)
            if value.tzinfo is None:
                raise ValueError("no time zone")
            return value
        if kind is uuid.UUID:
            return uuid.UUID(raw)
        if kind is bytes:
            return base64.b64decode(raw, validate=True)
    except (ValueError, binascii.Error):
        malformed(f"decode frame: {path} is not a valid {kind.__name__}")
    raise TypeError(f"unsupported record field type {kind!r}")


def encode_record(record: Any) -> dict[str, Any]:
    """The JSON object for *record*."""
    out: dict[str, Any] = {}
    for field in dataclasses.fields(record):
        value = getattr(record, field.name)
        if field.metadata.get("omitempty") and value in (None, False, 0, "", ()):
            continue
        out[field.name] = _encode_value(value)
    return out


def _encode_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_time(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, tuple):
        return [_encode_value(item) for item in value]
    if dataclasses.is_dataclass(value):
        return encode_record(value)
    return value


def dumps(record: Any) -> str:
    """Encode a record as the compact JSON text of one WebSocket message."""
    return json.dumps(encode_record(record), ensure_ascii=False, separators=(",", ":"))


__all__ = [
    "Rejected",
    "RejectionReason",
    "decode_record",
    "dumps",
    "encode_record",
    "format_time",
    "malformed",
    "omit_empty",
    "parse_json",
    "reject",
    "struct",
]
