"""Verification of the gateway's stream token on a stream upgrade.

The gateway opens each stream with ``Authorization: Bearer <token>``. The
gateway signs the token itself: ES256 with a ``kid``, the configured issuer
and audience, and the subject ``tenants/<tenant>/agents/<agent>``. Nerve
accepts every valid holder as the gateway for its agent, so each check here
is part of the trust boundary.

The public keys come from a local JWK Set file. Nerve never fetches keys over
the network. The file is read at startup and on a config reload, and again,
at most once per minute, when a token names an unknown ``kid``. When a later
read fails, the keys from the last good read stay in use.
"""

from __future__ import annotations

import json
import logging
import math
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import jwt
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm

logger = logging.getLogger(__name__)

STREAM_ISSUER = "nerve-gateway"
STREAM_AUDIENCE = "nerve-channel"
CLOCK_SKEW_SECONDS = 60
MAXIMUM_LIFETIME_SECONDS = 300
UNKNOWN_KID_REREAD_SECONDS = 60.0
MAXIMUM_KEY_FILE_BYTES = 1024 * 1024
MAXIMUM_TOKEN_BYTES = 8 * 1024
MAXIMUM_KID_BYTES = 128
# The largest integer that a JSON number carries exactly. A date above it is
# refused before any arithmetic, so no date can overflow a float.
MAXIMUM_NUMERIC_DATE = 2**53

# JWK members that hold private or symmetric key material (RFC 7518).
PRIVATE_KEY_MEMBERS = frozenset({"d", "p", "q", "dp", "dq", "qi", "oth", "k"})


class TokenRejected(Exception):
    """A token that fails verification. The message is for local logs only."""


class KeyFileError(ValueError):
    """A gateway key file that Nerve cannot use. The message names no key material."""


@dataclass(frozen=True)
class StreamIdentity:
    """The verified claims of one stream token."""

    tenant_id: uuid.UUID
    agent_id: uuid.UUID
    token_id: str


class GatewayTokenVerifier:
    """Verify stream tokens against the gateway's public keys.

    The constructor reads the key file and raises :class:`KeyFileError` when
    it cannot be used. ``monotonic`` drives the reread limit, for tests.
    """

    def __init__(
        self,
        *,
        key_file: Path,
        issuer: str,
        audience: str,
        tenant_id: uuid.UUID,
        agent_id: uuid.UUID,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._key_file = key_file
        self._issuer = issuer
        self._audience = audience
        self._tenant_id = tenant_id
        self._agent_id = agent_id
        self._subject = f"tenants/{tenant_id}/agents/{agent_id}"
        self._monotonic = monotonic
        self._keys: dict[str, ec.EllipticCurvePublicKey] = {}
        self._read_at = 0.0
        self.read_count = 0
        self.reload_keys()

    def reload_keys(self) -> int:
        """Read the key file again and return the number of keys.

        Raises :class:`KeyFileError` and keeps the current keys when the file
        cannot be used.
        """
        self._read_at = self._monotonic()
        self.read_count += 1
        keys = read_key_file(self._key_file)
        self._keys = keys
        return len(keys)

    async def verify(self, token: str) -> StreamIdentity:
        """Return the stream identity of *token*, or raise :class:`TokenRejected`."""
        if not token or len(token) > MAXIMUM_TOKEN_BYTES:
            raise TokenRejected("token is empty or oversized")
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as error:
            raise TokenRejected(f"token header is malformed: {error}") from None
        if header.get("alg") != "ES256":
            raise TokenRejected("token algorithm is not ES256")
        if "crit" in header:
            raise TokenRejected("token has critical header extensions")
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid or len(kid) > MAXIMUM_KID_BYTES:
            raise TokenRejected("token has no usable kid")

        key = self._key_for(kid)
        if key is None:
            raise TokenRejected("token kid is not in the key set")
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=["ES256"],
                audience=self._audience,
                issuer=self._issuer,
                leeway=CLOCK_SKEW_SECONDS,
                options={
                    "require": ["iss", "sub", "aud", "exp", "nbf", "iat", "jti"],
                    "strict_aud": True,
                },
            )
        except jwt.PyJWTError as error:
            raise TokenRejected(f"token failed verification: {error}") from None
        except (OverflowError, ValueError, TypeError):
            raise TokenRejected("token has a claim out of range") from None
        _check_lifetime(claims)
        return self._check_subject(claims)

    def _key_for(self, kid: str) -> ec.EllipticCurvePublicKey | None:
        """The key for *kid*, after a reread when it is unknown and one is due."""
        key = self._keys.get(kid)
        if key is not None:
            return key
        if self._monotonic() - self._read_at < UNKNOWN_KID_REREAD_SECONDS:
            return None
        try:
            self.reload_keys()
        except KeyFileError as error:
            logger.warning("Channel stream key file reread failed: %s", error)
        return self._keys.get(kid)

    def _check_subject(self, claims: dict[str, Any]) -> StreamIdentity:
        if claims.get("sub") != self._subject:
            raise TokenRejected("token subject names another agent")
        if claims.get("tenant_id") != str(self._tenant_id):
            raise TokenRejected("token tenant_id names another tenant")
        if claims.get("agent_id") != str(self._agent_id):
            raise TokenRejected("token agent_id names another agent")
        return StreamIdentity(
            tenant_id=self._tenant_id,
            agent_id=self._agent_id,
            token_id=claims["jti"],
        )


def _check_lifetime(claims: dict[str, Any]) -> None:
    """Refuse a NumericDate that is not a JSON number, or a lifetime above the limit."""
    dates = {}
    for name in ("iat", "nbf", "exp"):
        value = claims.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TokenRejected(f"token {name} is not a number")
        if (isinstance(value, float) and not math.isfinite(value)) or abs(value) > MAXIMUM_NUMERIC_DATE:
            raise TokenRejected(f"token {name} is out of range")
        dates[name] = value
    lifetime = dates["exp"] - dates["iat"]
    if not 0 < lifetime <= MAXIMUM_LIFETIME_SECONDS:
        raise TokenRejected("token lifetime is out of range")


def read_key_file(path: Path) -> dict[str, ec.EllipticCurvePublicKey]:
    """The public keys of the gateway key file at *path*, by ``kid``.

    Raises :class:`KeyFileError` when the file cannot be read or does not
    hold only valid public P-256 signing keys.
    """
    try:
        with open(path, "rb") as file:
            body = file.read(MAXIMUM_KEY_FILE_BYTES + 1)
    except OSError as error:
        raise KeyFileError(f"cannot read {path}: {error.strerror or type(error).__name__}") from None
    if len(body) > MAXIMUM_KEY_FILE_BYTES:
        raise KeyFileError(f"{path} is larger than {MAXIMUM_KEY_FILE_BYTES} bytes")
    return parse_key_set(body, source=str(path))


def parse_key_set(body: bytes, *, source: str = "key file") -> dict[str, ec.EllipticCurvePublicKey]:
    """The keys of a JWK Set of public P-256 signing keys, by ``kid``.

    Every entry must be an EC P-256 public key with a unique ``kid``. An
    ``alg`` other than ``ES256``, a ``use`` other than ``sig``, or a
    ``key_ops`` without ``verify`` is refused.
    A set with any private key member is refused as a whole, because it
    shows that a signing key was copied to the wrong place.
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
    keys: dict[str, ec.EllipticCurvePublicKey] = {}
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
        if entry.get("kty") != "EC" or entry.get("crv") != "P-256":
            raise KeyFileError(f"{where} is not a P-256 key")
        key_ops = entry.get("key_ops", ["verify"])
        if (
            entry.get("alg", "ES256") != "ES256"
            or entry.get("use", "sig") != "sig"
            or not isinstance(key_ops, list)
            or "verify" not in key_ops
        ):
            raise KeyFileError(f"{where} is not an ES256 signing key")
        try:
            key = ECAlgorithm.from_jwk(json.dumps(entry))
        except (jwt.PyJWTError, ValueError, TypeError):
            raise KeyFileError(f"{where} is not a valid P-256 public key") from None
        if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(key.curve, ec.SECP256R1):
            raise KeyFileError(f"{where} is not a valid P-256 public key")
        keys[kid] = key
    return keys


__all__ = [
    "CLOCK_SKEW_SECONDS",
    "MAXIMUM_LIFETIME_SECONDS",
    "STREAM_AUDIENCE",
    "STREAM_ISSUER",
    "GatewayTokenVerifier",
    "KeyFileError",
    "StreamIdentity",
    "TokenRejected",
    "parse_key_set",
    "read_key_file",
]
