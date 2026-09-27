"""Verification of gateway-signed tokens for hosted channel streams.

The verifier is the trust boundary of hosted mode: Nerve accepts any valid
holder of a stream token as the gateway for its agent. These tests drive it
with tokens from the fake gateway's ES256 key and the public keys that the
fake gateway writes to a key file.
"""

from __future__ import annotations

import json
import time
import uuid

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

from nerve.channels.hosted.auth import (
    GatewayTokenVerifier,
    KeyFileError,
    TokenRejected,
    parse_key_set,
    read_key_file,
)

from tests.fake_channel_gateway import (
    AGENT_ID,
    AUDIENCE,
    ISSUER,
    TENANT_ID,
    FakeChannelGateway,
)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def gateway(tmp_path) -> FakeChannelGateway:
    return FakeChannelGateway(jwks_file=tmp_path / "gateway-jwks.json")


def verifier(gateway: FakeChannelGateway, clock: Clock | None = None, **overrides) -> GatewayTokenVerifier:
    settings = dict(
        key_file=gateway.jwks_file,
        issuer=ISSUER,
        audience=AUDIENCE,
        tenant_id=uuid.UUID(TENANT_ID),
        agent_id=uuid.UUID(AGENT_ID),
    )
    if clock is not None:
        settings["monotonic"] = clock
    settings.update(overrides)
    return GatewayTokenVerifier(**settings)


async def refused(check: GatewayTokenVerifier, token: str) -> str:
    with pytest.raises(TokenRejected) as error:
        await check.verify(token)
    return str(error.value)


@pytest.mark.asyncio
class TestClaims:
    async def test_a_gateway_token_is_accepted(self, gateway):
        identity = await verifier(gateway).verify(gateway.token())

        assert identity.tenant_id == uuid.UUID(TENANT_ID)
        assert identity.agent_id == uuid.UUID(AGENT_ID)
        assert identity.token_id

    async def test_another_audience_is_refused(self, gateway):
        assert "verification" in await refused(verifier(gateway), gateway.token(aud="nerve-local-inference"))

    async def test_an_audience_list_is_refused(self, gateway):
        assert "strict" in await refused(verifier(gateway), gateway.token(aud=[AUDIENCE]))

    async def test_another_issuer_is_refused(self, gateway):
        assert "issuer" in await refused(
            verifier(gateway), gateway.token(iss="https://cp.test/workload-identity"),
        )

    async def test_another_agent_in_the_subject_is_refused(self, gateway):
        other = str(uuid.uuid4())

        assert "subject" in await refused(
            verifier(gateway), gateway.token(sub=f"tenants/{TENANT_ID}/agents/{other}"),
        )

    @pytest.mark.parametrize("claim", ["tenant_id", "agent_id"])
    async def test_another_scope_claim_is_refused(self, gateway, claim):
        assert claim in await refused(verifier(gateway), gateway.token(**{claim: str(uuid.uuid4())}))

    @pytest.mark.parametrize("claim", ["exp", "nbf", "iat", "sub", "aud", "iss", "jti", "tenant_id", "agent_id"])
    async def test_a_missing_claim_is_refused(self, gateway, claim):
        await refused(verifier(gateway), gateway.token(**{claim: None}))

    async def test_an_expired_token_is_refused(self, gateway):
        now = int(time.time())

        await refused(verifier(gateway), gateway.token(iat=now - 361, nbf=now - 361, exp=now - 61))

    async def test_expiry_allows_sixty_seconds_of_skew(self, gateway):
        now = int(time.time())

        await verifier(gateway).verify(gateway.token(iat=now - 350, nbf=now - 350, exp=now - 50))

    async def test_a_token_that_is_not_valid_yet_is_refused(self, gateway):
        now = int(time.time())

        await refused(verifier(gateway), gateway.token(iat=now, nbf=now + 120, exp=now + 300))

    async def test_not_before_allows_sixty_seconds_of_skew(self, gateway):
        now = int(time.time())

        await verifier(gateway).verify(gateway.token(iat=now + 50, nbf=now + 50, exp=now + 350))

    async def test_a_lifetime_of_300_seconds_is_accepted(self, gateway):
        now = int(time.time())

        await verifier(gateway).verify(gateway.token(iat=now, nbf=now, exp=now + 300))

    async def test_a_lifetime_above_300_seconds_is_refused(self, gateway):
        now = int(time.time())

        assert "lifetime" in await refused(
            verifier(gateway), gateway.token(iat=now, nbf=now, exp=now + 301),
        )

    async def test_an_expiry_before_the_issue_time_is_refused(self, gateway):
        now = int(time.time())

        assert "lifetime" in await refused(
            verifier(gateway), gateway.token(iat=now + 30, nbf=now, exp=now + 20),
        )

    @pytest.mark.parametrize("claim", ["iat", "exp"])
    @pytest.mark.parametrize("value", [10**400, 1e308, float("inf")])
    async def test_a_date_out_of_range_is_refused(self, gateway, claim, value):
        await refused(verifier(gateway), gateway.token(**{claim: value}))

    @pytest.mark.parametrize(("claim", "value"), [("exp", 10**400), ("iat", -(10**400)), ("nbf", -(10**400))])
    async def test_a_huge_date_beside_a_fractional_date_is_refused(self, gateway, claim, value):
        now = time.time()
        dates = {"iat": now + 0.5, "nbf": now, "exp": now + 200.5}
        dates[claim] = value

        assert "out of range" in await refused(verifier(gateway), gateway.token(**dates))

    @pytest.mark.parametrize("claim", ["iat", "nbf", "exp"])
    async def test_a_date_that_is_not_a_number_is_refused(self, gateway, claim):
        now = int(time.time())
        dates = {"iat": now, "nbf": now, "exp": now + 300}
        dates[claim] = str(dates[claim])

        await refused(verifier(gateway), gateway.token(**dates))


@pytest.mark.asyncio
class TestSignature:
    async def test_another_algorithm_is_refused(self, gateway):
        forged = jwt.encode(
            {"sub": "x"}, "not-a-key-but-long-enough-for-hmac-sha256", algorithm="HS256",
            headers={"kid": gateway.kid},
        )

        assert "ES256" in await refused(verifier(gateway), forged)

    async def test_a_token_signed_with_the_public_key_as_an_hmac_secret_is_refused(self, gateway):
        public_json = json.dumps(gateway.jwks()["keys"][0])
        claims = jwt.decode(gateway.token(), options={"verify_signature": False})
        forged = _hmac_token(claims, public_json.encode(), gateway.kid)

        assert "ES256" in await refused(verifier(gateway), forged)

    async def test_another_elliptic_curve_algorithm_is_refused(self, gateway):
        p384 = ec.generate_private_key(ec.SECP384R1())
        claims = jwt.decode(gateway.token(), options={"verify_signature": False})
        token = jwt.encode(claims, p384, algorithm="ES384", headers={"kid": gateway.kid})

        assert "ES256" in await refused(verifier(gateway), token)

    async def test_an_unsigned_token_is_refused(self, gateway):
        unsigned = jwt.encode({"sub": "x"}, None, algorithm="none", headers={"kid": gateway.kid})

        await refused(verifier(gateway), unsigned)

    async def test_a_token_without_kid_is_refused(self, gateway):
        token = jwt.encode({"sub": "x"}, gateway.keys[gateway.kid], algorithm="ES256")

        assert "kid" in await refused(verifier(gateway), token)

    async def test_a_critical_header_extension_is_refused(self, gateway):
        claims = jwt.decode(gateway.token(), options={"verify_signature": False})
        token = jwt.encode(
            claims, gateway.keys[gateway.kid], algorithm="ES256",
            headers={"kid": gateway.kid, "crit": ["exp"]},
        )

        assert "critical" in await refused(verifier(gateway), token)

    async def test_a_signature_from_another_key_with_a_known_kid_is_refused(self, gateway):
        impostor = FakeChannelGateway()
        claims = jwt.decode(impostor.token(), options={"verify_signature": False})
        forged = jwt.encode(
            claims, impostor.keys[impostor.kid], algorithm="ES256", headers={"kid": gateway.kid},
        )

        assert "verification" in await refused(verifier(gateway), forged)

    async def test_the_web_session_token_is_refused(self, gateway):
        from nerve.gateway.auth import create_session_token

        await refused(
            verifier(gateway),
            create_session_token("a-web-ui-secret-of-at-least-32-bytes", "an-account"),
        )

    async def test_garbage_is_refused(self, gateway):
        check = verifier(gateway)

        await refused(check, "not.a.jwt")
        await refused(check, "")
        await refused(check, "a" * 9000)


def _hmac_token(claims: dict, secret: bytes, kid: str) -> str:
    """A token signed with HS256 by hand, since PyJWT refuses a PEM or JWK secret."""
    import base64
    import hashlib
    import hmac

    def segment(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode()

    header = segment(json.dumps({"alg": "HS256", "typ": "JWT", "kid": kid}).encode())
    payload = segment(json.dumps(claims).encode())
    signature = hmac.new(secret, f"{header}.{payload}".encode(), hashlib.sha256).digest()
    return f"{header}.{payload}.{segment(signature)}"


@pytest.mark.asyncio
class TestKeyFile:
    async def test_the_file_is_read_once_at_start(self, gateway):
        check = verifier(gateway)

        for _ in range(3):
            await check.verify(gateway.token())

        assert check.read_count == 1

    async def test_an_unknown_kid_rereads_the_file_at_most_once_a_minute(self, gateway):
        clock = Clock()
        check = verifier(gateway, clock)
        rotated = gateway.add_key("gateway-test-key-2", publish=False)

        await refused(check, gateway.token(kid=rotated))
        clock.now += 61
        await refused(check, gateway.token(kid=rotated))
        clock.now += 30
        await refused(check, gateway.token(kid=rotated))
        await refused(check, gateway.token(kid="no-such-key"))

        assert check.read_count == 2

    async def test_a_failed_reread_counts_toward_the_limit(self, gateway):
        clock = Clock()
        check = verifier(gateway, clock)
        good = gateway.jwks_file.read_text(encoding="utf-8")
        gateway.jwks_file.write_text("{not json", encoding="utf-8")

        clock.now += 61
        await refused(check, gateway.token(kid="unknown-kid"))
        gateway.jwks_file.write_text(good, encoding="utf-8")
        clock.now += 30
        await refused(check, gateway.token(kid="unknown-kid"))

        assert check.read_count == 2

    async def test_a_rotated_key_is_found_by_the_reread(self, gateway):
        clock = Clock()
        check = verifier(gateway, clock)
        rotated = gateway.add_key("gateway-test-key-2")

        await refused(check, gateway.token(kid=rotated))
        clock.now += 61
        await check.verify(gateway.token(kid=rotated))

        assert check.read_count == 2

    async def test_a_known_kid_does_not_reread_the_file(self, gateway):
        clock = Clock()
        check = verifier(gateway, clock)
        gateway.unpublish(gateway.kid)

        clock.now += 3600
        await check.verify(gateway.token())

        assert check.read_count == 1

    async def test_a_reload_removes_a_key(self, gateway):
        check = verifier(gateway)
        old = gateway.kid
        gateway.kid = gateway.add_key("gateway-test-key-2")
        gateway.unpublish(old)

        assert check.reload_keys() == 1
        await refused(check, gateway.token(kid=old))
        await check.verify(gateway.token())

    async def test_a_failed_reread_keeps_the_last_good_keys(self, gateway):
        clock = Clock()
        check = verifier(gateway, clock)
        gateway.jwks_file.write_text("{not json", encoding="utf-8")

        clock.now += 61
        await refused(check, gateway.token(kid="unknown-kid"))
        with pytest.raises(KeyFileError):
            check.reload_keys()

        assert check.read_count == 3
        await check.verify(gateway.token())

    async def test_an_unusable_file_at_start_is_an_error(self, tmp_path):
        with pytest.raises(KeyFileError, match="cannot read"):
            verifier(FakeChannelGateway(), key_file=tmp_path / "missing.json")

    async def test_a_file_with_a_private_key_is_refused(self, gateway):
        private = json.loads(ECAlgorithm.to_jwk(gateway.keys[gateway.kid]))
        document = gateway.jwks()
        document["keys"].append({**private, "kid": "signing-key"})
        gateway.jwks_file.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(KeyFileError, match="private key members") as error:
            verifier(gateway)

        assert private["d"] not in str(error.value)


class TestParseKeySet:
    def _entry(self, key, **extra):
        algorithm = ECAlgorithm if isinstance(key, ec.EllipticCurvePrivateKey) else RSAAlgorithm
        entry = json.loads(algorithm.to_jwk(key.public_key()))
        entry.update(extra)
        return entry

    def _body(self, *entries) -> bytes:
        return json.dumps({"keys": list(entries)}).encode()

    def test_public_p256_signing_keys_are_read_by_kid(self):
        first = ec.generate_private_key(ec.SECP256R1())
        second = ec.generate_private_key(ec.SECP256R1())

        keys = parse_key_set(self._body(
            self._entry(first, kid="one", alg="ES256", use="sig"), self._entry(second, kid="two"),
        ))

        assert set(keys) == {"one", "two"}

    @pytest.mark.parametrize(("make", "problem"), [
        (lambda e: e(ec.generate_private_key(ec.SECP384R1()), kid="p384"), "P-256"),
        (lambda e: e(rsa.generate_private_key(public_exponent=65537, key_size=2048), kid="rsa"), "P-256"),
        (lambda e: e(ec.generate_private_key(ec.SECP256R1()), kid="enc", use="enc"), "signing"),
        (lambda e: e(ec.generate_private_key(ec.SECP256R1()), kid="es384", alg="ES384"), "signing"),
        (lambda e: e(ec.generate_private_key(ec.SECP256R1()), kid="ops", key_ops=["encrypt"]), "signing"),
        (lambda e: e(ec.generate_private_key(ec.SECP256R1())), "kid"),
        (lambda e: {**e(ec.generate_private_key(ec.SECP256R1()), kid="bad"), "x": "AAAA"}, "valid"),
    ], ids=["p384", "rsa", "encryption", "other_alg", "other_key_ops", "no_kid", "bad_point"])
    def test_a_file_with_an_unusable_key_is_refused(self, make, problem):
        good = self._entry(ec.generate_private_key(ec.SECP256R1()), kid="good")

        with pytest.raises(KeyFileError, match=problem):
            parse_key_set(self._body(good, make(self._entry)))

    def test_a_repeated_kid_is_refused(self):
        first = ec.generate_private_key(ec.SECP256R1())
        second = ec.generate_private_key(ec.SECP256R1())

        with pytest.raises(KeyFileError, match="more than once"):
            parse_key_set(self._body(self._entry(first, kid="same"), self._entry(second, kid="same")))

    @pytest.mark.parametrize("member", ["d", "k", "p"])
    def test_any_private_member_refuses_the_whole_set(self, member):
        good = self._entry(ec.generate_private_key(ec.SECP256R1()), kid="good")
        other = self._entry(ec.generate_private_key(ec.SECP256R1()), kid="other")

        with pytest.raises(KeyFileError, match="private key members"):
            parse_key_set(self._body(good, {**other, member: "c2VjcmV0"}))

    @pytest.mark.parametrize("body", [
        b"", b"[]", b'{"keys": []}', b'{"keys": {}}', b"\xff", b"[" * 100_000 + b"]" * 100_000,
    ], ids=["empty", "array", "no_keys", "keys_object", "not_utf8", "deeply_nested"])
    def test_a_file_without_keys_is_refused(self, body):
        with pytest.raises(KeyFileError):
            parse_key_set(body)

    def test_an_oversized_file_is_refused(self, tmp_path):
        path = tmp_path / "big.json"
        path.write_bytes(b" " * (1024 * 1024 + 1))

        with pytest.raises(KeyFileError, match="larger"):
            read_key_file(path)
