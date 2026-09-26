"""Verification of the workload identity token on a stream upgrade.

The gateway opens each stream with ``Authorization: Bearer <token>``. The
token is a control plane workload identity JWT: ES256 with a ``kid``, the
audience ``nerve-channel``, and the subject ``tenants/<tenant>/agents/<agent>``.
Nerve accepts every valid holder as the gateway for its agent, so each check
here is part of the trust boundary.

The JWK Set is fetched on first use and cached. It is fetched again on first
use after ten minutes, and for an unknown ``kid``, at most once per minute.
Until a first fetch succeeds, a failed fetch may be repeated after five
seconds. Cached keys stay in use for at most a day after the last successful
fetch. Concurrent upgrades share one fetch, and no lock is held while a
signature is checked. The audience must be the single string that the
control plane issues.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm

logger = logging.getLogger(__name__)

STREAM_AUDIENCE = "nerve-channel"
CLOCK_SKEW_SECONDS = 60
REFRESH_AFTER_SECONDS = 600.0
UNKNOWN_KID_REFETCH_SECONDS = 60.0
FAILED_FETCH_RETRY_SECONDS = 5.0
MAXIMUM_KEY_AGE_SECONDS = 24 * 3600.0
FETCH_TIMEOUT_SECONDS = 5.0
MAXIMUM_JWKS_BYTES = 1024 * 1024
MAXIMUM_TOKEN_BYTES = 8 * 1024
MAXIMUM_KID_BYTES = 128


class TokenRejected(Exception):
    """A token that fails verification. The message is for local logs only."""


@dataclass(frozen=True)
class StreamIdentity:
    """The verified claims of one stream token."""

    tenant_id: uuid.UUID
    agent_id: uuid.UUID
    token_id: str


class WorkloadTokenVerifier:
    """Verify stream tokens against the control plane's JWK Set.

    ``transport`` replaces the HTTP transport, and ``monotonic`` drives the
    cache timers. Both exist for tests.
    """

    def __init__(
        self,
        *,
        issuer: str,
        jwks_url: str,
        audience: str,
        tenant_id: uuid.UUID,
        agent_id: uuid.UUID,
        transport: httpx.AsyncBaseTransport | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._issuer = issuer
        self._jwks_url = jwks_url
        self._audience = audience
        self._tenant_id = tenant_id
        self._agent_id = agent_id
        self._subject = f"tenants/{tenant_id}/agents/{agent_id}"
        self._transport = transport
        self._monotonic = monotonic
        self._keys: dict[str, ec.EllipticCurvePublicKey] = {}
        self._fetched_at: float | None = None
        self._last_attempt: float | None = None
        self._last_attempt_failed = False
        self._fetch: asyncio.Task | None = None
        self.fetch_count = 0

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
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid or len(kid) > MAXIMUM_KID_BYTES:
            raise TokenRejected("token has no usable kid")

        key = await self._key_for(kid)
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
                    "require": ["iss", "sub", "aud", "exp", "nbf", "iat"],
                    "strict_aud": True,
                },
            )
        except jwt.PyJWTError as error:
            raise TokenRejected(f"token failed verification: {error}") from None
        return self._check_subject(claims)

    def _check_subject(self, claims: dict[str, Any]) -> StreamIdentity:
        if claims.get("sub") != self._subject:
            raise TokenRejected("token subject names another agent")
        if claims.get("tenant_id") != str(self._tenant_id):
            raise TokenRejected("token tenant_id names another tenant")
        if claims.get("agent_id") != str(self._agent_id):
            raise TokenRejected("token agent_id names another agent")
        token_id = claims.get("jti")
        return StreamIdentity(
            tenant_id=self._tenant_id,
            agent_id=self._agent_id,
            token_id=token_id if isinstance(token_id, str) else "",
        )

    # ------------------------------------------------------------------ #
    #  Key set                                                             #
    # ------------------------------------------------------------------ #

    async def _key_for(self, kid: str) -> ec.EllipticCurvePublicKey | None:
        now = self._monotonic()
        if self._fetched_at is not None and now - self._fetched_at > MAXIMUM_KEY_AGE_SECONDS:
            self._keys = {}
        stale = self._fetched_at is None or now - self._fetched_at >= REFRESH_AFTER_SECONDS
        unknown = kid not in self._keys
        if (stale or unknown) and self._may_fetch(now):
            await self._refresh()
        return self._keys.get(kid)

    def _may_fetch(self, now: float) -> bool:
        """Whether a fetch may start now.

        Until the first fetch succeeds, a failed one may be repeated after
        five seconds. After that, fetches start at most once per minute, also
        while the key set is stale or the endpoint fails.
        """
        if self._fetch is not None and not self._fetch.done():
            return True
        if self._last_attempt is None:
            return True
        elapsed = now - self._last_attempt
        if self._fetched_at is None and self._last_attempt_failed:
            return elapsed >= FAILED_FETCH_RETRY_SECONDS
        return elapsed >= UNKNOWN_KID_REFETCH_SECONDS

    async def _refresh(self) -> None:
        """Fetch the key set once, shared by every caller that waits for it."""
        if self._fetch is None or self._fetch.done():
            self._last_attempt = self._monotonic()
            self._fetch = asyncio.create_task(self._fetch_keys())
        await asyncio.shield(self._fetch)

    async def _fetch_keys(self) -> None:
        """Replace the cached keys, or keep them and log when the fetch fails."""
        self.fetch_count += 1
        try:
            keys = await self._download()
        except Exception as error:  # noqa: BLE001 - any failure keeps the old keys
            self._last_attempt_failed = True
            logger.warning("Channel stream key set fetch failed: %s", type(error).__name__)
            return
        self._last_attempt_failed = False
        self._keys = keys
        self._fetched_at = self._monotonic()

    async def _download(self) -> dict[str, ec.EllipticCurvePublicKey]:
        async with httpx.AsyncClient(
            transport=self._transport,
            timeout=FETCH_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as client:
            async with client.stream("GET", self._jwks_url) as response:
                if response.status_code != 200:
                    raise ValueError(f"key set fetch answered {response.status_code}")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAXIMUM_JWKS_BYTES:
                        raise ValueError("key set exceeds its size limit")
        return parse_jwks(bytes(body))


def parse_jwks(body: bytes) -> dict[str, ec.EllipticCurvePublicKey]:
    """The usable P-256 public keys of a JWK Set, by ``kid``.

    Other key types, private keys, keys for another algorithm or use, and
    every key whose ``kid`` repeats are left out.
    """
    document = json.loads(body)
    entries = document.get("keys") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise ValueError("key set has no keys array")
    keys: dict[str, ec.EllipticCurvePublicKey] = {}
    repeated: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        kid = entry.get("kid")
        if (
            not isinstance(kid, str) or not kid
            or entry.get("kty") != "EC" or entry.get("crv") != "P-256"
            or "d" in entry
            or entry.get("alg", "ES256") != "ES256"
            or entry.get("use", "sig") != "sig"
        ):
            continue
        try:
            key = ECAlgorithm.from_jwk(json.dumps(entry))
        except (jwt.PyJWTError, ValueError, TypeError):
            continue
        if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(key.curve, ec.SECP256R1):
            continue
        if kid in keys or kid in repeated:
            keys.pop(kid, None)
            repeated.add(kid)
            continue
        keys[kid] = key
    return keys


__all__ = [
    "CLOCK_SKEW_SECONDS",
    "STREAM_AUDIENCE",
    "StreamIdentity",
    "TokenRejected",
    "WorkloadTokenVerifier",
    "parse_jwks",
]
