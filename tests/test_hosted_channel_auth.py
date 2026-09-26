"""Workload identity verification for hosted channel streams.

The verifier is the trust boundary of hosted mode: Nerve accepts any valid
holder of a stream token as the gateway for its agent. These tests drive it
with tokens from the fake gateway's ES256 key and its JWK Set.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

from nerve.channels.hosted.auth import TokenRejected, WorkloadTokenVerifier, parse_jwks

from tests.fake_channel_gateway import (
    AGENT_ID,
    AUDIENCE,
    ISSUER,
    JWKS_URL,
    TENANT_ID,
    FakeChannelGateway,
)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def verifier(gateway: FakeChannelGateway, clock: Clock | None = None, **overrides) -> WorkloadTokenVerifier:
    settings = dict(
        issuer=ISSUER,
        jwks_url=JWKS_URL,
        audience=AUDIENCE,
        tenant_id=uuid.UUID(TENANT_ID),
        agent_id=uuid.UUID(AGENT_ID),
        transport=gateway.transport(),
    )
    if clock is not None:
        settings["monotonic"] = clock
    settings.update(overrides)
    return WorkloadTokenVerifier(**settings)


async def refused(check: WorkloadTokenVerifier, token: str) -> str:
    with pytest.raises(TokenRejected) as error:
        await check.verify(token)
    return str(error.value)


@pytest.mark.asyncio
class TestClaims:
    async def test_a_gateway_token_is_accepted(self):
        gateway = FakeChannelGateway()

        identity = await verifier(gateway).verify(gateway.token())

        assert identity.tenant_id == uuid.UUID(TENANT_ID)
        assert identity.agent_id == uuid.UUID(AGENT_ID)
        assert identity.token_id

    async def test_another_audience_is_refused(self):
        gateway = FakeChannelGateway()

        assert "verification" in await refused(verifier(gateway), gateway.token(aud="nerve-local-inference"))

    async def test_an_audience_list_is_refused(self):
        gateway = FakeChannelGateway()

        await refused(verifier(gateway), gateway.token(aud=[AUDIENCE]))

    async def test_another_issuer_is_refused(self):
        gateway = FakeChannelGateway()

        await refused(verifier(gateway), gateway.token(iss="https://other.test/workload-identity"))

    async def test_another_agent_in_the_subject_is_refused(self):
        gateway = FakeChannelGateway()
        other = str(uuid.uuid4())

        assert "subject" in await refused(
            verifier(gateway), gateway.token(sub=f"tenants/{TENANT_ID}/agents/{other}"),
        )

    @pytest.mark.parametrize("claim", ["tenant_id", "agent_id"])
    async def test_another_scope_claim_is_refused(self, claim):
        gateway = FakeChannelGateway()

        assert claim in await refused(verifier(gateway), gateway.token(**{claim: str(uuid.uuid4())}))

    @pytest.mark.parametrize("claim", ["exp", "nbf", "iat", "sub", "aud", "iss"])
    async def test_a_missing_claim_is_refused(self, claim):
        gateway = FakeChannelGateway()

        await refused(verifier(gateway), gateway.token(**{claim: None}))

    async def test_an_expired_token_is_refused(self):
        gateway = FakeChannelGateway()
        now = int(time.time())

        await refused(verifier(gateway), gateway.token(iat=now - 600, nbf=now - 600, exp=now - 61))

    async def test_expiry_allows_sixty_seconds_of_skew(self):
        gateway = FakeChannelGateway()
        now = int(time.time())

        await verifier(gateway).verify(gateway.token(iat=now - 600, nbf=now - 600, exp=now - 50))

    async def test_a_token_that_is_not_valid_yet_is_refused(self):
        gateway = FakeChannelGateway()
        now = int(time.time())

        await refused(verifier(gateway), gateway.token(iat=now + 120, nbf=now + 120, exp=now + 420))

    async def test_not_before_allows_sixty_seconds_of_skew(self):
        gateway = FakeChannelGateway()
        now = int(time.time())

        await verifier(gateway).verify(gateway.token(iat=now + 50, nbf=now + 50, exp=now + 350))


@pytest.mark.asyncio
class TestSignature:
    async def test_another_algorithm_is_refused_before_the_key_set_is_read(self):
        gateway = FakeChannelGateway()
        forged = jwt.encode(
            {"sub": "x"}, "not-a-key-but-long-enough-for-hmac-sha256", algorithm="HS256",
            headers={"kid": gateway.kid},
        )
        check = verifier(gateway)

        assert "ES256" in await refused(check, forged)
        assert gateway.jwks_fetches == 0

    async def test_an_unsigned_token_is_refused(self):
        gateway = FakeChannelGateway()
        unsigned = jwt.encode({"sub": "x"}, None, algorithm="none", headers={"kid": gateway.kid})

        await refused(verifier(gateway), unsigned)

    async def test_a_token_without_kid_is_refused(self):
        gateway = FakeChannelGateway()
        token = jwt.encode({"sub": "x"}, gateway.keys[gateway.kid], algorithm="ES256")

        assert "kid" in await refused(verifier(gateway), token)

    async def test_a_signature_from_another_key_with_a_known_kid_is_refused(self):
        gateway = FakeChannelGateway()
        impostor = FakeChannelGateway()
        token = impostor.token(kid=impostor.kid)
        forged = jwt.encode(
            jwt.decode(token, options={"verify_signature": False}),
            impostor.keys[impostor.kid], algorithm="ES256", headers={"kid": gateway.kid},
        )

        assert "verification" in await refused(verifier(gateway), forged)

    async def test_the_web_session_token_is_refused(self):
        from nerve.gateway.auth import create_session_token

        gateway = FakeChannelGateway()

        await refused(
            verifier(gateway),
            create_session_token("a-web-ui-secret-of-at-least-32-bytes", "an-account"),
        )

    async def test_garbage_is_refused(self):
        gateway = FakeChannelGateway()

        await refused(verifier(gateway), "not.a.jwt")
        await refused(verifier(gateway), "")
        await refused(verifier(gateway), "a" * 9000)


@pytest.mark.asyncio
class TestKeySet:
    async def test_the_key_set_is_fetched_once_and_cached(self):
        gateway = FakeChannelGateway()
        check = verifier(gateway)

        for _ in range(3):
            await check.verify(gateway.token())

        assert gateway.jwks_fetches == 1

    async def test_an_unknown_kid_fetches_again_at_most_once_a_minute(self):
        gateway = FakeChannelGateway()
        clock = Clock()
        check = verifier(gateway, clock)
        await check.verify(gateway.token())
        rotated = gateway.add_key("gateway-test-key-2", publish=False)

        clock.now += 61
        await refused(check, gateway.token(kid=rotated))
        clock.now += 30
        await refused(check, gateway.token(kid=rotated))
        await refused(check, gateway.token(kid="no-such-key"))

        assert gateway.jwks_fetches == 2

    async def test_a_rotated_key_is_found_by_the_refetch(self):
        gateway = FakeChannelGateway()
        clock = Clock()
        check = verifier(gateway, clock)
        await check.verify(gateway.token())
        rotated = gateway.add_key("gateway-test-key-2")

        clock.now += 61
        await check.verify(gateway.token(kid=rotated))

        assert gateway.jwks_fetches == 2

    async def test_the_key_set_is_refreshed_after_ten_minutes(self):
        gateway = FakeChannelGateway()
        clock = Clock()
        check = verifier(gateway, clock)
        await check.verify(gateway.token())
        gateway.published.remove(gateway.kid)

        clock.now += 601
        await refused(check, gateway.token())

        assert gateway.jwks_fetches == 2

    async def test_a_failed_fetch_is_retried_after_five_seconds(self):
        gateway = FakeChannelGateway()
        clock = Clock()
        failing = verifier(gateway, clock, jwks_url="https://cp.test/missing.json")

        await refused(failing, gateway.token())
        await refused(failing, gateway.token())
        clock.now += 6
        await refused(failing, gateway.token())

        assert failing.fetch_count == 2

    async def test_after_a_first_fetch_a_failing_endpoint_is_asked_once_a_minute(self):
        gateway = FakeChannelGateway()
        clock = Clock()
        check = verifier(gateway, clock)
        await check.verify(gateway.token())
        gateway.jwks_url = "https://cp.test/moved.json"

        for step in (61, 5, 5, 5):
            clock.now += step
            await refused(check, gateway.token(kid="unknown-kid"))

        assert check.fetch_count == 2
        await check.verify(gateway.token())

    async def test_concurrent_upgrades_share_one_fetch(self):
        gateway = FakeChannelGateway()
        check = verifier(gateway)

        await asyncio.gather(*(check.verify(gateway.token()) for _ in range(5)))

        assert gateway.jwks_fetches == 1


class TestParseJwks:
    def _entry(self, key, **extra):
        algorithm = ECAlgorithm if isinstance(key, ec.EllipticCurvePrivateKey) else RSAAlgorithm
        entry = json.loads(algorithm.to_jwk(key.public_key()))
        entry.update(extra)
        return entry

    def test_only_p256_public_signing_keys_are_kept(self):
        p256 = ec.generate_private_key(ec.SECP256R1())
        p384 = ec.generate_private_key(ec.SECP384R1())
        rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private = json.loads(ECAlgorithm.to_jwk(p256))
        body = json.dumps({"keys": [
            self._entry(p256, kid="good", alg="ES256", use="sig"),
            self._entry(p384, kid="p384"),
            self._entry(rsa_key, kid="rsa"),
            {**private, "kid": "private"},
            self._entry(p256, kid="encryption", use="enc"),
            self._entry(p256, kid="other-alg", alg="ES384"),
        ]}).encode()

        assert set(parse_jwks(body)) == {"good"}

    def test_a_repeated_kid_is_dropped(self):
        first = ec.generate_private_key(ec.SECP256R1())
        second = ec.generate_private_key(ec.SECP256R1())
        body = json.dumps({"keys": [
            self._entry(first, kid="same"), self._entry(second, kid="same"),
        ]}).encode()

        assert parse_jwks(body) == {}
