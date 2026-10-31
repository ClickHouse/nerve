"""Verification of the gateway's actor context on a request.

In external mode the gateway names the person behind each request and each
WebSocket upgrade in the ``X-Nerve-Actor-Context`` header: a compact JWS that
the gateway signs with Ed25519 (``EdDSA``) and a ``kid``. With
``auth.external.gateway_jwks_file`` set, Nerve acts as that person only when
the signature verifies with a key from that file and the claims name this
tenant and agent, the method and target of this request, and a time within
the assertion's lifetime. Each assertion is accepted once. Each check here is
part of the trust boundary.

The public keys come from a local JWK Set file. Nerve never fetches keys over
the network. The file is read at startup and on a config reload, and again,
at most once per minute, when a token names an unknown ``kid``. When a later
read fails, the keys from the last good read stay in use.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import re
import time
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

logger = logging.getLogger(__name__)

ACTOR_CONTEXT_TYPE = "nerve-actor-context+jws"
ACTOR_CONTEXT_VERSION = "1"
CLOCK_SKEW_SECONDS = 30
MAXIMUM_LIFETIME_SECONDS = 30
UNKNOWN_KID_REREAD_SECONDS = 60.0
MAXIMUM_KEY_FILE_BYTES = 1024 * 1024
MAXIMUM_TOKEN_BYTES = 8 * 1024
MAXIMUM_KID_BYTES = 128
# Assertion IDs remembered at one time. A full cache forgets the oldest.
MAXIMUM_REMEMBERED_ASSERTIONS = 10_000

# JWK members that hold private or symmetric key material (RFC 7518, RFC 8037).
PRIVATE_KEY_MEMBERS = frozenset({"d", "p", "q", "dp", "dq", "qi", "oth", "k"})
# Header members that embed or reference a key. Keys come only from the file.
KEY_HEADER_MEMBERS = frozenset({"jwk", "jku", "x5c", "x5u"})

_BASE64URL = re.compile(r"[A-Za-z0-9_-]*")


class AssertionRejected(Exception):
    """An actor context that fails verification. The message is for local logs only."""


class KeysUnavailable(Exception):
    """No usable key set is loaded, so no actor context can be verified."""


class KeyFileError(ValueError):
    """A gateway key file that Nerve cannot use. The message names no key material."""


class ActorContextVerifier:
    """Verify actor contexts for one tenant and agent against the gateway's keys.

    The constructor reads the key file. When the file cannot be used, the
    verifier starts without keys and logs why, and :meth:`verify` raises
    :class:`KeysUnavailable` until a read succeeds. ``now`` is the clock for
    the claims, and ``monotonic`` drives the reread limit, for tests.
    """

    def __init__(
        self,
        key_file: Path,
        tenant_id: str,
        agent_id: str,
        *,
        now: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.key_file = key_file
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self._signer_id = f"urn:nerve:gateway:actor:{tenant_id}:{agent_id}"
        self._audience = f"urn:nerve:runtime:{tenant_id}:{agent_id}"
        self._now = now
        self._monotonic = monotonic
        self._keys: dict[str, Ed25519PublicKey] = {}
        self._read_at = 0.0
        # Accepted assertion IDs, oldest first, each with the time after which
        # the assertion can no longer pass the time check.
        self._seen: OrderedDict[str, float] = OrderedDict()
        try:
            self.reload_keys()
        except KeyFileError as error:
            logger.warning("The gateway's actor key file cannot be used: %s", error)

    @property
    def key_count(self) -> int:
        """The number of keys in use."""
        return len(self._keys)

    def reload_keys(self) -> int:
        """Read the key file again and return the number of keys.

        Raises :class:`KeyFileError` and keeps the current keys when the file
        cannot be used.
        """
        self._read_at = self._monotonic()
        keys = read_key_file(self.key_file)
        self._keys = keys
        return len(keys)

    def verify(self, value: str, *, method: str, target: str, websocket: bool) -> None:
        """Raise unless *value* is a valid actor context for this request.

        ``method`` and ``target`` are the method and the request-target of the
        request as Nerve received it, and ``websocket`` says that the request
        is a WebSocket upgrade. An accepted assertion is not accepted again.
        Raises :class:`KeysUnavailable` when no key set is loaded, and
        :class:`AssertionRejected` when a check fails.
        """
        if not self._keys:
            self._reread_if_due()
            if not self._keys:
                raise KeysUnavailable(f"no usable key set was read from {self.key_file}")
        if not value or len(value) > MAXIMUM_TOKEN_BYTES:
            raise AssertionRejected("assertion is empty or oversized")
        parts = value.split(".")
        if len(parts) != 3:
            raise AssertionRejected("assertion is not a compact JWS")
        header, claims = _json_object(parts[0]), _json_object(parts[1])
        if header is None or claims is None:
            raise AssertionRejected("assertion header or payload is not base64url JSON")
        try:
            self._check_signature(header, parts)
            self._check_claims(claims, method=method, target=target, websocket=websocket)
        except AssertionRejected as error:
            raise AssertionRejected(
                f"{error} (kid {_shown(header.get('kid'))}, "
                f"request {_shown(claims.get('request_id'))})"
            ) from None
        self._remember(claims["assertion_id"], claims["expires_at"])

    def _check_signature(self, header: dict[str, Any], parts: list[str]) -> None:
        if header.get("typ") != ACTOR_CONTEXT_TYPE:
            raise AssertionRejected(f"assertion typ is not {ACTOR_CONTEXT_TYPE}")
        if header.get("alg") != "EdDSA":
            raise AssertionRejected("assertion algorithm is not EdDSA")
        if "crit" in header:
            raise AssertionRejected("assertion has critical header extensions")
        if KEY_HEADER_MEMBERS & header.keys():
            raise AssertionRejected("assertion header carries or references a key")
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid or len(kid) > MAXIMUM_KID_BYTES:
            raise AssertionRejected("assertion has no usable kid")
        key = self._key_for(kid)
        if key is None:
            raise AssertionRejected("assertion kid is not in the key set")
        signature = _base64url(parts[2]) or b""
        try:
            key.verify(signature, f"{parts[0]}.{parts[1]}".encode("ascii"))
        except InvalidSignature:
            raise AssertionRejected("assertion signature does not verify") from None

    def _check_claims(
        self, claims: dict[str, Any], *, method: str, target: str, websocket: bool,
    ) -> None:
        if claims.get("version") != ACTOR_CONTEXT_VERSION:
            raise AssertionRejected(f"assertion version is not {ACTOR_CONTEXT_VERSION}")
        if claims.get("tenant_id") != self.tenant_id:
            raise AssertionRejected("assertion tenant_id names another tenant")
        if claims.get("agent_id") != self.agent_id:
            raise AssertionRejected("assertion agent_id names another agent")
        if claims.get("signer_id") != self._signer_id:
            raise AssertionRejected("assertion signer_id is not this agent's")
        if claims.get("audience") != self._audience:
            raise AssertionRejected("assertion audience is not this agent's")
        if claims.get("actor_kind") != "human":
            raise AssertionRejected("assertion actor_kind is not human")

        binding = claims.get("request_binding")
        if not isinstance(binding, dict):
            raise AssertionRejected("assertion has no request_binding")
        kind = "websocket" if websocket else "http"
        if binding.get("kind") != kind:
            raise AssertionRejected(
                f"assertion binds kind {_shown(binding.get('kind'))}, request is {kind}"
            )
        connection = claims.get("gateway_connection_id")
        if websocket and (not isinstance(connection, str) or not connection):
            raise AssertionRejected("assertion has no gateway_connection_id")
        if binding.get("method") != method:
            raise AssertionRejected(
                f"assertion binds method {_shown(binding.get('method'))}, request is {method}"
            )
        # The gateway keeps a bare trailing "?", and the server drops it.
        # The target is not logged: its query can hold a secret.
        bound = binding.get("target")
        if bound != target and not ("?" not in target and bound == f"{target}?"):
            raise AssertionRejected("assertion binds another request target")

        issued_at, expires_at = claims.get("issued_at"), claims.get("expires_at")
        if not _is_integer(issued_at) or not _is_integer(expires_at):
            raise AssertionRejected("assertion issued_at or expires_at is not an integer")
        if not 0 < expires_at - issued_at <= MAXIMUM_LIFETIME_SECONDS:
            raise AssertionRejected("assertion lifetime is out of range")
        now = self._now()
        if issued_at > now + CLOCK_SKEW_SECONDS:
            raise AssertionRejected("assertion is issued in the future")
        if expires_at < now - CLOCK_SKEW_SECONDS:
            raise AssertionRejected("assertion has expired")

        assertion_id = claims.get("assertion_id")
        if not isinstance(assertion_id, str) or not assertion_id:
            raise AssertionRejected("assertion has no assertion_id")
        if assertion_id in self._seen:
            raise AssertionRejected("assertion was already used")

    def _remember(self, assertion_id: str, expires_at: int) -> None:
        """Remember an accepted assertion until it can no longer pass the time check."""
        now = self._now()
        while self._seen and next(iter(self._seen.values())) < now:
            self._seen.popitem(last=False)
        if len(self._seen) >= MAXIMUM_REMEMBERED_ASSERTIONS:
            # IDs need not expire in the order they came: forget every expired
            # one before a live one.
            self._seen = OrderedDict(
                (seen, until) for seen, until in self._seen.items() if until >= now
            )
            if len(self._seen) >= MAXIMUM_REMEMBERED_ASSERTIONS:
                self._seen.popitem(last=False)
        self._seen[assertion_id] = expires_at + CLOCK_SKEW_SECONDS

    def _key_for(self, kid: str) -> Ed25519PublicKey | None:
        """The key for *kid*, after a reread when it is unknown and one is due."""
        key = self._keys.get(kid)
        if key is None:
            self._reread_if_due()
            key = self._keys.get(kid)
        return key

    def _reread_if_due(self) -> None:
        if self._monotonic() - self._read_at < UNKNOWN_KID_REREAD_SECONDS:
            return
        try:
            self.reload_keys()
        except KeyFileError as error:
            logger.warning("The gateway's actor key file reread failed: %s", error)


def _base64url(segment: str) -> bytes | None:
    """The bytes of an unpadded base64url segment, or ``None``."""
    if not _BASE64URL.fullmatch(segment):
        return None
    try:
        return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except (binascii.Error, ValueError):
        return None


def _json_object(segment: str) -> dict[str, Any] | None:
    """The JSON object in a base64url segment, or ``None``."""
    raw = _base64url(segment)
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _shown(value: Any) -> str:
    """A short header or claim value for a log line, or ``-``."""
    return repr(value) if isinstance(value, str) and len(value) <= MAXIMUM_KID_BYTES else "-"


def read_key_file(path: Path) -> dict[str, Ed25519PublicKey]:
    """The public keys of the gateway key file at *path*, by ``kid``.

    Raises :class:`KeyFileError` when the file cannot be read or does not
    hold only valid public Ed25519 signing keys.
    """
    try:
        with open(path, "rb") as file:
            body = file.read(MAXIMUM_KEY_FILE_BYTES + 1)
    except OSError as error:
        raise KeyFileError(f"cannot read {path}: {error.strerror or type(error).__name__}") from None
    if len(body) > MAXIMUM_KEY_FILE_BYTES:
        raise KeyFileError(f"{path} is larger than {MAXIMUM_KEY_FILE_BYTES} bytes")
    return parse_key_set(body, source=str(path))


def parse_key_set(body: bytes, *, source: str = "key file") -> dict[str, Ed25519PublicKey]:
    """The keys of a JWK Set of public Ed25519 signing keys, by ``kid``.

    Only ``keys`` is read; other members of the document are ignored. Every
    entry must be an OKP Ed25519 public key with ``alg`` ``EdDSA``, ``use``
    ``sig`` and a unique ``kid``. A set with any private key member is
    refused as a whole, because it shows that a signing key was copied to the
    wrong place.
    """
    try:
        document = json.loads(body)
    except (ValueError, RecursionError):
        raise KeyFileError(f"{source} is not valid JSON") from None
    entries = document.get("keys") if isinstance(document, dict) else None
    if not isinstance(entries, list) or not entries:
        raise KeyFileError(f"{source} has no keys")
    if any(isinstance(entry, dict) and PRIVATE_KEY_MEMBERS & entry.keys() for entry in entries):
        raise KeyFileError(f"{source} holds private key members; give Nerve the public keys only")
    keys: dict[str, Ed25519PublicKey] = {}
    for index, entry in enumerate(entries):
        where = f"{source} key {index}"
        if not isinstance(entry, dict):
            raise KeyFileError(f"{where} is not an object")
        kid = entry.get("kid")
        if not isinstance(kid, str) or not kid or len(kid) > MAXIMUM_KID_BYTES:
            raise KeyFileError(f"{where} has no usable kid")
        where = f"{source} key {kid!r}"
        if kid in keys:
            raise KeyFileError(f"{where} appears more than once")
        if entry.get("kty") != "OKP" or entry.get("crv") != "Ed25519":
            raise KeyFileError(f"{where} is not an Ed25519 key")
        if entry.get("alg") != "EdDSA" or entry.get("use") != "sig":
            raise KeyFileError(f"{where} is not an EdDSA signing key")
        x = entry.get("x")
        raw = _base64url(x) if isinstance(x, str) else None
        try:
            keys[kid] = Ed25519PublicKey.from_public_bytes(raw or b"")
        except ValueError:
            raise KeyFileError(f"{where} is not a valid Ed25519 public key") from None
    return keys


__all__ = [
    "ACTOR_CONTEXT_TYPE",
    "CLOCK_SKEW_SECONDS",
    "MAXIMUM_LIFETIME_SECONDS",
    "ActorContextVerifier",
    "AssertionRejected",
    "KeyFileError",
    "KeysUnavailable",
    "parse_key_set",
    "read_key_file",
]
