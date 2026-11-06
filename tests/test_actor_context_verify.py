"""Verification of the gateway's actor context (``auth.external``).

With ``auth.external.gateway_jwks_file`` set, external mode accepts the
``X-Nerve-Actor-Context`` header only with the gateway's Ed25519 signature and
claims for this tenant, agent and request. The golden vectors in
``fixtures/actor_context_vectors.json`` come from the gateway's own signer at a
fixed clock, with throw-away keys: each valid vector must be accepted and each
invalid one refused, at the vector's clock. The other tests sign with a key
made here, in the same format.
"""

from __future__ import annotations

import base64
import json
import logging
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated

import httpx
import pytest
import pytest_asyncio
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

from nerve.cli import doctor_report
from nerve.config import (
    ExternalAuthConfig,
    NerveConfig,
    get_config,
    load_config,
    set_config,
    validate_config_keys,
)
from nerve.config_reload import reload_all
from nerve.db import Database
from nerve.gateway import actor_context, auth, server
from nerve.gateway.actor_context import (
    ActorContextVerifier,
    AssertionRejected,
    KeyFileError,
    KeysUnavailable,
    parse_key_set,
    read_key_file,
)
from nerve.gateway.auth import (
    ACTOR_CONTEXT_HEADER,
    AUTH_MODE_ENV,
    AUTH_MODE_EXTERNAL,
    decode_actor_context,
    pin_auth_mode,
    require_auth,
)
from nerve.identity import Actor
from nerve.migrate import bootstrap_identity
from tests.test_external_auth import (
    _SECRET,
    _close_code,
    _no_lifespan,
    _open_bootstrapped_db,
    _RecordingEngine,
)

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures" / "actor_context_vectors.json").read_text(encoding="utf-8"),
)
TENANT, AGENT = VECTORS["tenant_id"], VECTORS["agent_id"]
VALID = [vector for vector in VECTORS["vectors"] if vector["valid"]]
INVALID = [vector for vector in VECTORS["vectors"] if not vector["valid"]]
# The vectors' IDs have no letters, so a form without hyphens is the
# non-canonical spelling of the same UUID.
OTHER_SPELLING = {TENANT: TENANT.replace("-", ""), AGENT: AGENT.replace("-", "")}
ALICE = "6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b"
BOB = "0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d"
KID = "test-key"
REFUSED_DETAIL = "The actor context failed verification"


def _names(vectors: list[dict]) -> list[str]:
    return [vector["name"] for vector in vectors]


def _vector(name: str) -> dict:
    return next(vector for vector in VECTORS["vectors"] if vector["name"] == name)


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class Signer:
    """Signs actor contexts in the gateway's format, with keys made here."""

    def __init__(self, key_file: Path) -> None:
        self.key_file = key_file
        self.keys = {KID: Ed25519PrivateKey.generate()}
        self.publish()

    def jwk(self, kid: str) -> dict:
        public = self.keys[kid].public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        return {"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": _b64(public)}

    def publish(self, *kids: str) -> None:
        document = {"keys": [self.jwk(kid) for kid in kids or self.keys]}
        self.key_file.write_text(json.dumps(document), encoding="utf-8")

    def add_key(self, kid: str) -> str:
        self.keys[kid] = Ed25519PrivateKey.generate()
        return kid

    @staticmethod
    def claims(now: int, *, method="GET", target="/api/whoami", websocket=False, **changes) -> dict:
        claims = {
            "version": "1",
            "signer_id": f"urn:nerve:gateway:actor:{TENANT}:{AGENT}",
            "audience": f"urn:nerve:runtime:{TENANT}:{AGENT}",
            "tenant_id": TENANT,
            "agent_id": AGENT,
            "principal_id": ALICE,
            "actor_kind": "human",
            "profile": {"display_name": "Alice", "primary_email": None, "version": 1},
            "source": "web",
            "authentication_method": "gateway_session",
            "request_id": "req-1",
            "assertion_id": uuid.uuid4().hex,
            "admitted_at": now,
            "issued_at": now,
            "expires_at": now + 30,
            "request_binding": {
                "kind": "websocket" if websocket else "http", "method": method, "target": target,
            },
            "authorization": {"kind": "projection", "reference": 1},
        }
        if websocket:
            claims["gateway_connection_id"] = "connection-1"
        claims.update(changes)
        return claims

    def sign(self, claims: dict, *, kid: str | None = KID, key=None, **header) -> str:
        protected = {"alg": "EdDSA", "typ": "nerve-actor-context+jws", **header}
        if kid is not None:
            protected["kid"] = kid
        signing_input = f"{_b64(json.dumps(protected).encode())}.{_b64(json.dumps(claims).encode())}"
        signature = (key or self.keys[kid or KID]).sign(signing_input.encode())
        return f"{signing_input}.{_b64(signature)}"

    def assertion(self, now: int, **claims) -> str:
        return self.sign(self.claims(now, **claims))


NOW = 1_800_000_000


@pytest.fixture(autouse=True)
def _no_process_verifier(monkeypatch):
    """Each test starts without the process verifier, and leaves none."""
    monkeypatch.setattr(auth, "_actor_verifier", None)


@pytest.fixture
def signer(tmp_path) -> Signer:
    return Signer(tmp_path / "actor-keys.json")


def verifier(key_file: Path, clock: Clock | None = None, monotonic: Clock | None = None):
    return ActorContextVerifier(
        key_file, TENANT, AGENT,
        now=clock or Clock(NOW), monotonic=monotonic or Clock(1000.0),
    )


def refused(check: ActorContextVerifier, value: str, **request) -> str:
    request = {"method": "GET", "target": "/api/whoami", "websocket": False, **request}
    with pytest.raises(AssertionRejected) as error:
        check.verify(value, **request)
    return str(error.value)


def accepted(check: ActorContextVerifier, value: str, **request) -> None:
    request = {"method": "GET", "target": "/api/whoami", "websocket": False, **request}
    check.verify(value, **request)


@pytest.fixture
def reads(monkeypatch) -> list[Path]:
    """Every read of a key file by the verifier, in order."""
    paths: list[Path] = []
    read = actor_context.read_key_file

    def counted(path: Path):
        paths.append(path)
        return read(path)

    monkeypatch.setattr(actor_context, "read_key_file", counted)
    return paths


# --------------------------------------------------------------------------- #
#  The gateway's own assertions                                               #
# --------------------------------------------------------------------------- #


class TestGoldenVectors:
    @pytest.mark.parametrize("form", ["key_set", "ring"])
    @pytest.mark.parametrize("vector", VALID, ids=_names(VALID))
    def test_a_valid_vector_is_accepted(self, tmp_path, vector, form):
        key_file = tmp_path / "keys.json"
        key_file.write_text(json.dumps(VECTORS[form]), encoding="utf-8")
        check = verifier(key_file, Clock(vector["now"]))

        check.verify(
            vector["assertion"],
            method=vector["method"], target=vector["target"], websocket=vector["websocket"],
        )
        assert decode_actor_context(vector["assertion"]) == (
            vector["principal_id"], vector["display_name"],
        )

    @pytest.mark.parametrize("vector", INVALID, ids=_names(INVALID))
    def test_an_invalid_vector_is_refused(self, tmp_path, vector):
        key_file = tmp_path / "keys.json"
        key_file.write_text(json.dumps(VECTORS["key_set"]), encoding="utf-8")
        check = verifier(key_file, Clock(vector["now"]))

        with pytest.raises(AssertionRejected):
            check.verify(
                vector["assertion"],
                method=vector["method"], target=vector["target"], websocket=vector["websocket"],
            )

    def test_other_members_of_the_document_are_ignored(self):
        """The ``ring`` form holds the same keys beside other members."""
        keys = parse_key_set(json.dumps(VECTORS["ring"]).encode())

        assert set(keys) == set(parse_key_set(json.dumps(VECTORS["key_set"]).encode()))
        assert set(keys) == {"test-2026-10", "test-2026-11"}


# --------------------------------------------------------------------------- #
#  The checks                                                                  #
# --------------------------------------------------------------------------- #


class TestSignature:
    def test_a_changed_payload_is_refused(self, signer):
        value = signer.assertion(NOW)
        header, _payload, signature = value.split(".")
        forged = _b64(json.dumps(Signer.claims(NOW, principal_id=BOB)).encode())

        assert "signature" in refused(verifier(signer.key_file), f"{header}.{forged}.{signature}")

    def test_a_changed_signature_is_refused(self, signer):
        header, payload, signature = signer.assertion(NOW).split(".")
        raw = bytearray(base64.urlsafe_b64decode(signature + "=="))
        raw[0] ^= 1

        assert "signature" in refused(verifier(signer.key_file), f"{header}.{payload}.{_b64(bytes(raw))}")

    def test_a_signature_from_another_key_with_a_known_kid_is_refused(self, signer):
        value = signer.sign(Signer.claims(NOW), key=Ed25519PrivateKey.generate())

        assert "signature" in refused(verifier(signer.key_file), value)

    @pytest.mark.parametrize(("header", "problem"), [
        ({"typ": "JWT"}, "typ"),
        ({"alg": "none"}, "algorithm"),
        ({"alg": "HS256"}, "algorithm"),
        ({"jwk": {"kty": "OKP", "crv": "Ed25519", "x": "AAAA"}}, "key"),
        ({"jku": "https://keys.example/jwks.json"}, "key"),
        ({"x5c": ["MIIB"]}, "key"),
        ({"x5u": "https://keys.example/cert.pem"}, "key"),
        ({"crit": ["exp"]}, "critical"),
        ({"kid": None}, "kid"),
        ({"kid": "k" * 129}, "kid"),
    ], ids=["typ", "alg_none", "alg_hmac", "jwk", "jku", "x5c", "x5u", "crit", "no_kid", "long_kid"])
    def test_a_header_that_is_not_the_gateways_is_refused(self, signer, header, problem):
        kid = header.pop("kid", KID)
        value = signer.sign(Signer.claims(NOW), kid=kid, key=signer.keys[KID], **header)

        assert problem in refused(verifier(signer.key_file), value)

    @pytest.mark.parametrize("value", [
        "", "abc", "a.b", "a.b.c.d", "!!.e30.x", "e30.!!.x", "W10.e30.x", "a" * 9000,
    ], ids=["empty", "one_part", "two_parts", "four_parts", "header_not_base64url",
            "payload_not_base64url", "header_not_object", "oversized"])
    def test_garbage_is_refused(self, signer, value):
        refused(verifier(signer.key_file), value)


class TestClaims:
    @pytest.mark.parametrize(("changes", "problem"), [
        ({"version": "2"}, "version"),
        ({"version": 1}, "version"),
        ({"tenant_id": str(uuid.uuid4())}, "tenant_id"),
        ({"agent_id": OTHER_SPELLING[AGENT]}, "agent_id"),
        ({"signer_id": f"urn:nerve:gateway:actor:{TENANT}:{uuid.uuid4()}"}, "signer_id"),
        ({"audience": f"urn:nerve:runtime:{TENANT}:{uuid.uuid4()}"}, "audience"),
        ({"actor_kind": "service"}, "actor_kind"),
        ({"request_binding": None}, "request_binding"),
        ({"expires_at": NOW + 31}, "lifetime"),
        ({"expires_at": NOW}, "lifetime"),
        ({"issued_at": str(NOW)}, "integer"),
        ({"expires_at": float(NOW + 30)}, "integer"),
        ({"issued_at": True}, "integer"),
        ({"assertion_id": ""}, "assertion_id"),
    ], ids=["version", "version_number", "tenant", "agent_other_spelling", "signer", "audience",
            "actor_kind", "no_binding", "lifetime_31s", "lifetime_0s", "issued_at_string",
            "expires_at_float", "issued_at_bool", "no_assertion_id"])
    def test_a_claim_for_another_agent_or_shape_is_refused(self, signer, changes, problem):
        value = signer.sign(Signer.claims(NOW, **changes))

        assert problem in refused(verifier(signer.key_file), value)

    @pytest.mark.parametrize(("now", "ok"), [
        (NOW - 30, True), (NOW - 31, False), (NOW + 60, True), (NOW + 61, False),
    ], ids=["issued_30s_ahead", "issued_31s_ahead", "expired_30s_ago", "expired_31s_ago"])
    def test_the_clock_may_differ_by_30_seconds(self, signer, now, ok):
        check = verifier(signer.key_file, Clock(now))
        value = signer.assertion(NOW)

        if ok:
            accepted(check, value)
        else:
            refused(check, value)

    @pytest.mark.parametrize(("bound", "target", "ok"), [
        ("/api/plans", "/api/plans", True),
        ("/api/plans?", "/api/plans", True),
        ("/api/plans?a=1&b=2", "/api/plans?a=1&b=2", True),
        ("/api/plans?", "/api/plans?a=1", False),
        ("/api/plans", "/api/plans?a=1", False),
        ("/api/plans?b=2&a=1", "/api/plans?a=1&b=2", False),
        ("/api/plan", "/api/plans", False),
    ], ids=["exact", "bare_question_mark", "query", "query_added", "query_added_to_path",
            "query_reordered", "other_path"])
    def test_the_target_is_the_request_target(self, signer, bound, target, ok):
        check = verifier(signer.key_file)
        value = signer.assertion(NOW, target=bound)

        if ok:
            accepted(check, value, target=target)
        else:
            assert "target" in refused(check, value, target=target)

    def test_a_websocket_assertion_needs_a_connection_id(self, signer):
        check = verifier(signer.key_file)
        claims = Signer.claims(NOW, target="/ws", websocket=True)
        del claims["gateway_connection_id"]

        assert "gateway_connection_id" in refused(
            check, signer.sign(claims), target="/ws", websocket=True,
        )
        accepted(check, signer.assertion(NOW, target="/ws", websocket=True), target="/ws", websocket=True)

    def test_the_reason_names_the_kid_and_the_request_but_not_the_target(self, signer):
        value = signer.assertion(NOW, target="/api/files?token=s3cr3t-1")

        reason = refused(verifier(signer.key_file), value, target="/api/files?token=s3cr3t-2")

        assert f"kid {KID!r}" in reason and "request 'req-1'" in reason
        assert "s3cr3t" not in reason and "/api/files" not in reason


class TestReplay:
    def test_an_assertion_is_accepted_once(self, signer):
        check = verifier(signer.key_file)
        value = signer.assertion(NOW)

        accepted(check, value)
        assert "already used" in refused(check, value)

    def test_a_refused_assertion_is_not_remembered(self, signer):
        check = verifier(signer.key_file)
        value = signer.assertion(NOW, method="POST")

        refused(check, value, method="GET")
        accepted(check, value, method="POST")

    def test_a_full_cache_forgets_expired_ids_before_the_oldest_live_one(self, signer, monkeypatch):
        monkeypatch.setattr(actor_context, "MAXIMUM_REMEMBERED_ASSERTIONS", 2)
        clock = Clock(NOW)
        check = verifier(signer.key_file, clock)
        oldest = signer.assertion(NOW)
        accepted(check, oldest)
        accepted(check, signer.assertion(NOW - 25))  # expires at NOW + 5

        clock.now = NOW + 40  # the second one has expired, the oldest has not
        accepted(check, signer.assertion(NOW + 40))
        assert "already used" in refused(check, oldest)

        accepted(check, signer.assertion(NOW + 40))  # full of live IDs: the oldest goes
        accepted(check, oldest)


# --------------------------------------------------------------------------- #
#  The key file                                                                #
# --------------------------------------------------------------------------- #


class TestKeyFile:
    def test_an_unknown_kid_rereads_the_file_at_most_once_a_minute(self, signer, reads):
        monotonic = Clock(1000.0)
        check = verifier(signer.key_file, monotonic=monotonic)
        rotated = signer.add_key("test-key-2")
        value = signer.sign(Signer.claims(NOW), kid=rotated)

        refused(check, value)
        monotonic.now += 30
        refused(check, value)
        refused(check, signer.sign(Signer.claims(NOW), kid="no-such-key", key=signer.keys[KID]))

        assert len(reads) == 1
        signer.publish()
        monotonic.now += 31
        accepted(check, value)
        assert len(reads) == 2

    def test_a_known_kid_does_not_reread_the_file(self, signer, reads):
        monotonic = Clock(1000.0)
        check = verifier(signer.key_file, monotonic=monotonic)
        signer.key_file.write_text("{not json", encoding="utf-8")

        monotonic.now += 3600
        accepted(check, signer.assertion(NOW))

        assert len(reads) == 1

    def test_a_failed_reread_keeps_the_last_good_keys(self, signer, reads):
        monotonic = Clock(1000.0)
        check = verifier(signer.key_file, monotonic=monotonic)
        signer.key_file.write_text('{"keys": []}', encoding="utf-8")

        monotonic.now += 61
        refused(check, signer.sign(Signer.claims(NOW), kid="unknown-kid", key=signer.keys[KID]))
        with pytest.raises(KeyFileError, match="has no keys"):
            check.reload_keys()

        assert len(reads) == 3
        accepted(check, signer.assertion(NOW))

    def test_a_reload_removes_a_key(self, signer):
        check = verifier(signer.key_file)
        rotated = signer.add_key("test-key-2")
        signer.publish(rotated)

        assert check.reload_keys() == 1
        assert "not in the key set" in refused(check, signer.assertion(NOW))
        accepted(check, signer.sign(Signer.claims(NOW), kid=rotated))

    def test_without_a_usable_file_there_are_no_keys_until_a_read_succeeds(self, tmp_path, signer, caplog):
        missing = tmp_path / "later.json"
        monotonic = Clock(1000.0)
        with caplog.at_level(logging.WARNING, logger="nerve.gateway.actor_context"):
            check = verifier(missing, monotonic=monotonic)

        assert "cannot read" in caplog.text
        with pytest.raises(KeysUnavailable):
            accepted(check, signer.assertion(NOW))
        missing.write_text(signer.key_file.read_text(encoding="utf-8"), encoding="utf-8")
        with pytest.raises(KeysUnavailable):
            accepted(check, signer.assertion(NOW))
        monotonic.now += 61
        accepted(check, signer.assertion(NOW))

    def test_a_file_with_a_private_key_is_refused(self, signer):
        private = signer.keys[KID].private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
        document = {"keys": [{**signer.jwk(KID), "d": _b64(private)}]}
        signer.key_file.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(KeyFileError, match="private key members") as error:
            read_key_file(signer.key_file)
        assert _b64(private) not in str(error.value)
        with pytest.raises(KeysUnavailable):
            accepted(verifier(signer.key_file), signer.assertion(NOW))

    @pytest.mark.parametrize(("change", "problem"), [
        ({"kty": "EC", "crv": "P-256"}, "not an Ed25519 key"),
        ({"crv": "X25519"}, "not an Ed25519 key"),
        ({"alg": "ES256"}, "not an EdDSA signing key"),
        ({"alg": None}, "not an EdDSA signing key"),
        ({"use": "enc"}, "not an EdDSA signing key"),
        ({"use": None}, "not an EdDSA signing key"),
        ({"x": "AAAA"}, "not a valid Ed25519 public key"),
        ({"x": "not base64url!"}, "not a valid Ed25519 public key"),
        ({"kid": ""}, "no usable kid"),
    ], ids=["ec_key", "x25519", "other_alg", "no_alg", "encryption", "no_use", "short_x", "bad_x",
            "no_kid"])
    def test_a_file_with_an_unusable_key_is_refused(self, signer, change, problem):
        body = json.dumps({"keys": [signer.jwk(KID), {**signer.jwk(KID), "kid": "other", **change}]})

        with pytest.raises(KeyFileError, match=problem):
            parse_key_set(body.encode())

    @pytest.mark.parametrize("body", [
        b"", b"[]", b'{"keys": []}', b'{"keys": {}}', b"\xff", b"[" * 100_000 + b"]" * 100_000,
    ], ids=["empty", "array", "no_keys", "keys_object", "not_utf8", "deeply_nested"])
    def test_a_file_without_keys_is_refused(self, body):
        with pytest.raises(KeyFileError):
            parse_key_set(body)

    def test_a_repeated_kid_is_refused(self, signer):
        body = json.dumps({"keys": [signer.jwk(KID), signer.jwk(KID)]})

        with pytest.raises(KeyFileError, match="more than once"):
            parse_key_set(body.encode())


# --------------------------------------------------------------------------- #
#  Requests                                                                    #
# --------------------------------------------------------------------------- #


def _config(tmp_path: Path, **external) -> NerveConfig:
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {
            "jwt_secret": _SECRET,
            "external": {"tenant_id": TENANT, "agent_id": AGENT, **external},
        },
    })


def _external(monkeypatch) -> None:
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


def _vector_key_file(tmp_path: Path) -> Path:
    key_file = tmp_path / "actor-keys.json"
    key_file.write_text(json.dumps(VECTORS["key_set"]), encoding="utf-8")
    return key_file


def _app() -> FastAPI:
    """Every path, behind ``require_auth`` twice: on the router and on the route.

    FastAPI runs the dependency once per request, so an accepted assertion
    is not seen again as a replay.
    """
    app = FastAPI()
    router = APIRouter(dependencies=[Depends(require_auth)])

    @router.api_route("/{path:path}", methods=["GET", "POST"])
    async def whoami(path: str, actor: Annotated[Actor, Depends(require_auth)]):
        return {"actor_id": actor.actor_id, "display_name": actor.display_name}

    app.include_router(router)
    return app


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=_app()), base_url="http://nerve-test")


async def _send(vector: dict, value: str | None = None) -> httpx.Response:
    async with _client() as client:
        return await client.request(
            vector["method"], vector["target"],
            headers={ACTOR_CONTEXT_HEADER: value or vector["assertion"]},
        )


@pytest_asyncio.fixture
async def instance(tmp_path, monkeypatch, wire_identity_store):
    """External mode with the vectors' key set, at a clock that a test sets."""
    _external(monkeypatch)
    key_file = _vector_key_file(tmp_path)
    config = _config(tmp_path, gateway_jwks_file=str(key_file))
    set_config(config)
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    try:
        await bootstrap_identity(database, config)
        wire_identity_store(database)
        clock = Clock(VECTORS["now"])
        monkeypatch.setattr(auth, "_actor_verifier", verifier(key_file, clock))
        yield SimpleNamespace(db=database, config=config, clock=clock)
    finally:
        await database.close()
        set_config(NerveConfig())


def _own_keys(instance, tmp_path: Path, monkeypatch) -> tuple[Signer, Clock]:
    """Switch the instance to keys made here, at a clock that a test sets."""
    signer, clock = Signer(tmp_path / "own-keys.json"), Clock(NOW)
    instance.config.auth.external.gateway_jwks_file = signer.key_file
    monkeypatch.setattr(auth, "_actor_verifier", verifier(signer.key_file, clock))
    return signer, clock


HTTP_VALID = [vector for vector in VALID if not vector["websocket"]]
HTTP_INVALID = [vector for vector in INVALID if not vector["websocket"]]


@pytest.mark.asyncio
class TestHttpRequests:
    @pytest.mark.parametrize("vector", HTTP_VALID, ids=_names(HTTP_VALID))
    async def test_a_valid_vector_acts_as_its_principal(self, instance, vector):
        instance.clock.now = vector["now"]

        res = await _send(vector)

        assert res.status_code == 200
        assert res.json() == {"actor_id": vector["principal_id"], "display_name": vector["display_name"]}
        assert (await instance.db.get_actor_ref(vector["principal_id"]))["display_name"] == "Test Person"

    @pytest.mark.parametrize("vector", HTTP_INVALID, ids=_names(HTTP_INVALID))
    async def test_an_invalid_vector_is_refused_without_the_reason(self, instance, vector):
        instance.clock.now = vector["now"]

        res = await _send(vector)

        assert res.status_code == 401
        assert res.json()["detail"] == REFUSED_DETAIL
        assert await instance.db.get_actor_ref(vector["principal_id"]) is None

    async def test_a_replayed_assertion_is_refused(self, instance):
        vector = _vector("api_token_source")

        assert (await _send(vector)).status_code == 200
        assert (await _send(vector)).status_code == 401

    async def test_the_reason_goes_to_the_log_without_the_header(self, instance, caplog):
        vector = _vector("expired_beyond_skew")
        instance.clock.now = vector["now"]

        with caplog.at_level(logging.WARNING, logger="nerve.gateway.auth"):
            assert (await _send(vector)).status_code == 401

        assert "assertion has expired (kid 'test-2026-10', request 'req-GET')" in caplog.text
        assert vector["assertion"] not in caplog.text
        assert vector["assertion"].split(".")[2] not in caplog.text

    async def test_an_unsigned_header_is_refused(self, instance):
        payload = _b64(json.dumps({"principal_id": ALICE}).encode())

        res = await _send(_vector("api_token_source"), f"{_b64(b'{}')}.{payload}.x")

        assert res.status_code == 401

    async def test_no_usable_key_file_gives_503(self, instance, tmp_path):
        instance.config.auth.external.gateway_jwks_file = tmp_path / "missing.json"

        res = await _send(_vector("api_token_source"))

        assert res.status_code == 503
        assert res.json()["detail"] == auth.NO_ACTOR_KEYS_DETAIL

    @pytest.mark.parametrize(("key", "value"), [
        ("tenant_id", OTHER_SPELLING[TENANT]),
        ("agent_id", ""),
        ("verification", "warn"),
        ("gateway_jwks_file", Path("actor-keys.json")),
    ])
    async def test_invalid_settings_give_503(self, instance, key, value):
        setattr(instance.config.auth.external, key, value)

        assert (await _send(_vector("api_token_source"))).status_code == 503

    async def test_log_mode_logs_a_failed_check_and_uses_the_header(self, instance, caplog):
        instance.config.auth.external.verification = "log"
        vector = _vector("target_mismatch")

        with caplog.at_level(logging.WARNING, logger="nerve.gateway.auth"):
            res = await _send(vector)

        assert res.status_code == 200
        assert res.json()["actor_id"] == vector["principal_id"]
        assert "used anyway" in caplog.text and "another request target" in caplog.text
        assert vector["assertion"] not in caplog.text

    async def test_log_mode_continues_without_a_key_set(self, instance, tmp_path):
        instance.config.auth.external.verification = "log"
        instance.config.auth.external.gateway_jwks_file = tmp_path / "missing.json"
        vector = _vector("api_token_source")

        res = await _send(vector)

        assert res.status_code == 200
        assert res.json()["actor_id"] == vector["principal_id"]

    async def test_log_mode_still_refuses_a_header_it_cannot_read(self, instance):
        instance.config.auth.external.verification = "log"

        assert (await _send(_vector("api_token_source"), "abc")).status_code == 401

    async def test_without_the_key_file_setting_the_header_is_not_checked(self, instance):
        instance.config.auth.external.gateway_jwks_file = None
        vector = _vector("expired_beyond_skew")

        res = await _send(vector)

        assert res.status_code == 200
        assert res.json()["actor_id"] == vector["principal_id"]

    @pytest.mark.parametrize(("changes", "problem"), [
        ({"principal_id": "alice"}, "UUID"),
        ({"profile": {"display_name": 42}}, "display name"),
    ], ids=["principal_not_uuid", "display_name_not_string"])
    async def test_a_verified_header_that_cannot_be_read_is_refused(
        self, instance, tmp_path, monkeypatch, changes, problem,
    ):
        signer, _clock = _own_keys(instance, tmp_path, monkeypatch)

        async with _client() as client:
            res = await client.get(
                "/api/whoami", headers={ACTOR_CONTEXT_HEADER: signer.sign(Signer.claims(NOW, **changes))},
            )

        assert res.status_code == 401
        assert problem in res.json()["detail"]

    async def test_who_am_i_through_the_gateway_app(self, instance, tmp_path, monkeypatch):
        signer, _clock = _own_keys(instance, tmp_path, monkeypatch)
        value = signer.assertion(NOW, target="/api/auth/me")

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.create_app()), base_url="http://nerve-test",
        ) as client:
            res = await client.get("/api/auth/me", headers={ACTOR_CONTEXT_HEADER: value})
            again = await client.get("/api/auth/me", headers={ACTOR_CONTEXT_HEADER: value})

        assert res.status_code == 200
        assert res.json()["actor"] == {
            "id": ALICE, "kind": "human", "display_name": "Alice", "username": None,
        }
        assert again.status_code == 401


UPLOAD = "/api/files/upload"
BOUNDARY = "nerve-test-boundary"


def _upload_form(session_id: str) -> tuple[bytes, bytes]:
    """A multipart upload of one file, in two parts."""
    head = (
        f'--{BOUNDARY}\r\nContent-Disposition: form-data; name="session_id"\r\n\r\n{session_id}\r\n'
        f'--{BOUNDARY}\r\nContent-Disposition: form-data; name="files"; filename="notes.txt"\r\n'
        "Content-Type: text/plain\r\n\r\nthe first part, "
    ).encode()
    return head, f"the second part\r\n--{BOUNDARY}--\r\n".encode()


def _upload_headers(*values: str) -> httpx.Headers:
    return httpx.Headers(
        [(ACTOR_CONTEXT_HEADER, value) for value in values]
        + [("Content-Type", f"multipart/form-data; boundary={BOUNDARY}")],
    )


@pytest.mark.asyncio
class TestArrival:
    """The gateway app checks a request when it arrives, before it reads the body."""

    @pytest_asyncio.fixture
    async def uploads(self, instance, tmp_path, monkeypatch):
        """The real upload route, with keys made here and a session to upload to."""
        signer, clock = _own_keys(instance, tmp_path, monkeypatch)
        await instance.db.create_session("upload-session", source="web", actor=None)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.create_app()), base_url="http://nerve-test",
        ) as client:
            yield SimpleNamespace(client=client, signer=signer, clock=clock)

    async def test_a_slow_upload_is_checked_when_it_arrives(self, uploads, caplog):
        value = uploads.signer.assertion(NOW, method="POST", target=UPLOAD)
        head, tail = _upload_form("upload-session")

        async def slow_body():
            yield head
            uploads.clock.now = NOW + 120  # the transfer outlasts the assertion
            yield tail

        res = await uploads.client.post(UPLOAD, content=slow_body(), headers=_upload_headers(value))

        assert res.status_code == 200
        assert [file["filename"] for file in res.json()["files"]] == ["notes.txt"]

        uploads.clock.now = NOW + 10
        with caplog.at_level(logging.WARNING, logger="nerve.gateway.auth"):
            again = await uploads.client.post(UPLOAD, content=head + tail, headers=_upload_headers(value))
        assert again.status_code == 401
        assert "already used" in caplog.text

    async def test_two_headers_are_still_refused(self, uploads):
        first, second = (
            uploads.signer.assertion(NOW, method="POST", target=UPLOAD) for _ in range(2)
        )
        head, tail = _upload_form("upload-session")

        res = await uploads.client.post(UPLOAD, content=head + tail, headers=_upload_headers(first, second))

        assert res.status_code == 401
        assert ACTOR_CONTEXT_HEADER in res.json()["detail"]


@pytest.fixture
def ws_instance(tmp_path, monkeypatch, wire_identity_store):
    """The real ``/ws`` endpoint, verifying with the vectors' key set."""
    _external(monkeypatch)
    key_file = _vector_key_file(tmp_path)
    config = _config(tmp_path, gateway_jwks_file=str(key_file))
    set_config(config)
    clock = Clock(VECTORS["now"])
    monkeypatch.setattr(auth, "_actor_verifier", verifier(key_file, clock))
    app = server.create_app()
    app.router.lifespan_context = _no_lifespan
    with TestClient(app) as client:
        database = client.portal.call(_open_bootstrapped_db, tmp_path / "nerve.db", config)
        wire_identity_store(database)
        monkeypatch.setattr(server, "_engine", _RecordingEngine(), raising=False)
        try:
            yield SimpleNamespace(client=client, config=config, clock=clock)
        finally:
            client.portal.call(database.close)
    set_config(NerveConfig())


class TestWebSocket:
    def test_the_upgrade_vector_is_admitted_once(self, ws_instance):
        vector = _vector("websocket_upgrade")
        ws_instance.clock.now = vector["now"]
        headers = {ACTOR_CONTEXT_HEADER: vector["assertion"]}

        with ws_instance.client.websocket_connect("/ws", headers=headers) as socket:
            assert socket.receive_json()["type"] == "session_switched"
            (connection, _socket), = server._live_sockets.values()
            assert connection.actor == Actor(
                actor_id=vector["principal_id"], kind="human", account_id=None,
                display_name=vector["display_name"],
            )
        assert _close_code(ws_instance.client, "/ws", headers) == 4001

    def test_an_http_assertion_does_not_open_a_socket(self, ws_instance):
        vector = _vector("kind_mismatch_http_assertion_on_websocket")
        ws_instance.clock.now = vector["now"]

        assert _close_code(ws_instance.client, "/ws", {ACTOR_CONTEXT_HEADER: vector["assertion"]}) == 4001
        assert server._live_sockets == {}

    def test_no_usable_key_file_refuses_the_socket(self, ws_instance, tmp_path):
        ws_instance.config.auth.external.gateway_jwks_file = tmp_path / "missing.json"
        vector = _vector("websocket_upgrade")

        assert _close_code(ws_instance.client, "/ws", {ACTOR_CONTEXT_HEADER: vector["assertion"]}) == 4001


# --------------------------------------------------------------------------- #
#  Configuration, startup, reload and doctor                                   #
# --------------------------------------------------------------------------- #


class TestConfig:
    def test_the_settings_are_read(self):
        config = NerveConfig.from_dict({"auth": {"external": {
            "gateway_jwks_file": "/run/nerve/actor-keys.json",
            "tenant_id": TENANT, "agent_id": AGENT, "verification": " Log ",
        }}})

        assert config.auth.external == ExternalAuthConfig(
            gateway_jwks_file=Path("/run/nerve/actor-keys.json"),
            tenant_id=TENANT, agent_id=AGENT, verification="log",
        )
        assert config.auth.external.problems() == []

    @pytest.mark.parametrize("auth_section", [{}, {"external": None}, {"jwt_secret": "s"}])
    def test_without_the_section_nothing_is_checked(self, auth_section):
        external = NerveConfig.from_dict({"auth": auth_section}).auth.external

        assert external == ExternalAuthConfig()
        assert external.verification == "require"
        assert external.problems() == []

    @pytest.mark.parametrize(("key", "value", "problem"), [
        ("gateway_jwks_file", "actor-keys.json", "absolute path"),
        ("tenant_id", OTHER_SPELLING[TENANT], "tenant_id must be a lowercase UUID"),
        ("tenant_id", ALICE.upper(), "tenant_id must be a lowercase UUID"),
        ("agent_id", "", "agent_id must be a lowercase UUID"),
        ("agent_id", "agent-1", "agent_id must be a lowercase UUID"),
        ("verification", "warn", "verification must be require or log"),
    ])
    def test_unusable_settings_are_named(self, key, value, problem):
        settings = {
            "gateway_jwks_file": "/run/nerve/actor-keys.json", "tenant_id": TENANT, "agent_id": AGENT,
            key: value,
        }
        problems = NerveConfig.from_dict({"auth": {"external": settings}}).auth.external.problems()

        assert len(problems) == 1 and problem in problems[0]

    def test_the_keys_are_known(self):
        merged = {"auth": {"external": {
            "gateway_jwks_file": "/run/nerve/actor-keys.json",
            "tenant_id": TENANT, "agent_id": AGENT, "verification": "require",
        }}}

        assert validate_config_keys(merged) == []


class TestStartup:
    def test_startup_logs_the_check_and_reads_the_key_file(self, tmp_path, monkeypatch, caplog, reads):
        _external(monkeypatch)
        key_file = _vector_key_file(tmp_path)
        set_config(_config(tmp_path, gateway_jwks_file=str(key_file)))
        try:
            with caplog.at_level(logging.INFO, logger="nerve.gateway.server"):
                server.create_app()
        finally:
            set_config(NerveConfig())

        assert [
            record.levelname for record in caplog.records
            if f"verifies the {ACTOR_CONTEXT_HEADER} header (verification=require, "
            f"key file {key_file})" in record.getMessage()
        ] == ["INFO"]
        assert "without a signature check" not in caplog.text
        assert reads == [key_file]

    def test_log_mode_is_a_warning_at_startup(self, tmp_path, monkeypatch, caplog):
        _external(monkeypatch)
        set_config(_config(tmp_path, gateway_jwks_file=str(_vector_key_file(tmp_path)), verification="log"))
        try:
            with caplog.at_level(logging.INFO, logger="nerve.gateway.server"):
                server.create_app()
        finally:
            set_config(NerveConfig())

        assert [
            record.levelname for record in caplog.records
            if "uses it also when the check fails (verification=log" in record.getMessage()
        ] == ["WARNING"]

    def test_startup_warns_when_the_key_file_cannot_be_used(self, tmp_path, monkeypatch, caplog):
        _external(monkeypatch)
        set_config(_config(tmp_path, gateway_jwks_file=str(tmp_path / "missing.json")))
        try:
            with caplog.at_level(logging.WARNING):
                server.create_app()
        finally:
            set_config(NerveConfig())

        assert "actor key file cannot be used" in caplog.text

    def test_startup_warns_about_invalid_settings(self, tmp_path, monkeypatch, caplog):
        _external(monkeypatch)
        set_config(_config(tmp_path, gateway_jwks_file="actor-keys.json"))
        try:
            with caplog.at_level(logging.WARNING, logger="nerve.gateway.server"):
                server.create_app()
        finally:
            set_config(NerveConfig())

        assert "cannot be verified: auth.external.gateway_jwks_file must be an absolute path" in caplog.text

    def test_without_the_key_file_setting_startup_keeps_the_warning(self, tmp_path, monkeypatch, caplog):
        _external(monkeypatch)
        set_config(_config(tmp_path))
        try:
            with caplog.at_level(logging.WARNING, logger="nerve.gateway.server"):
                server.create_app()
        finally:
            set_config(NerveConfig())

        assert "without a signature check" in caplog.text
        assert auth._actor_verifier is None


def _write_config(config_dir: Path, workspace: Path, **external) -> None:
    (config_dir / "config.yaml").write_text(yaml.safe_dump({
        "workspace": str(workspace),
        "auth": {"external": {"tenant_id": TENANT, "agent_id": AGENT, **external}},
    }), encoding="utf-8")


@pytest.mark.asyncio
class TestReload:
    @pytest.fixture
    def config_dir(self, tmp_path):
        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (workspace / "config").mkdir(parents=True)
        yield config_dir
        set_config(NerveConfig())

    async def test_a_reload_reads_the_key_file_again(self, tmp_path, monkeypatch, config_dir, signer, reads):
        _external(monkeypatch)
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(signer.key_file))
        set_config(load_config(config_dir))
        check = auth.actor_context_verifier(get_config().auth.external)
        signer.add_key("test-key-2")
        signer.publish()

        summary = await reload_all(None, None, config_dir)

        assert summary["actor_context"] == "2 gateway key(s)"
        assert auth._actor_verifier is check and len(reads) == 2

    async def test_an_unusable_file_is_a_reload_error_and_keeps_the_keys(
        self, tmp_path, monkeypatch, config_dir, signer,
    ):
        _external(monkeypatch)
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(signer.key_file))
        set_config(load_config(config_dir))
        check = auth.actor_context_verifier(get_config().auth.external)
        signer.key_file.write_text('{"keys": []}', encoding="utf-8")

        summary = await reload_all(None, None, config_dir)

        assert summary["actor_context"].startswith("error: ")
        assert auth._actor_verifier is check
        check.verify(
            signer.assertion(int(time.time())), method="GET", target="/api/whoami", websocket=False,
        )

    async def test_a_reload_applies_a_new_key_file(self, tmp_path, monkeypatch, config_dir, signer, reads):
        _external(monkeypatch)
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(signer.key_file))
        set_config(load_config(config_dir))
        before = auth.actor_context_verifier(get_config().auth.external)
        moved = tmp_path / "moved.json"
        moved.write_text(signer.key_file.read_text(encoding="utf-8"), encoding="utf-8")
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(moved))

        summary = await reload_all(None, None, config_dir)

        assert summary["actor_context"] == "1 gateway key(s)"
        assert auth._actor_verifier is not before and auth._actor_verifier.key_file == moved
        assert reads == [signer.key_file, moved]

    async def test_a_new_key_file_that_cannot_be_used_is_a_reload_error(
        self, tmp_path, monkeypatch, config_dir, signer,
    ):
        _external(monkeypatch)
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(tmp_path / "missing.json"))

        summary = await reload_all(None, None, config_dir)

        assert summary["actor_context"].startswith("error: no usable key set")

    async def test_local_mode_and_no_key_file_report_nothing(self, tmp_path, monkeypatch, config_dir, signer):
        _write_config(config_dir, tmp_path / "ws", gateway_jwks_file=str(signer.key_file))
        assert "actor_context" not in await reload_all(None, None, config_dir)

        _external(monkeypatch)
        _write_config(config_dir, tmp_path / "ws")
        assert "actor_context" not in await reload_all(None, None, config_dir)


class TestDoctor:
    def test_the_key_file_is_read(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        key_file = _vector_key_file(tmp_path)

        report = doctor_report(_config(tmp_path, gateway_jwks_file=str(key_file)))

        assert (
            f"[OK] Actor context verified (verification: require): agent {AGENT}, "
            f"2 gateway key(s) from {key_file}"
        ) in report

    def test_log_mode_is_a_warning(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        key_file = _vector_key_file(tmp_path)

        report = doctor_report(_config(tmp_path, gateway_jwks_file=str(key_file), verification="log"))

        assert (
            "[WARN] Actor context: auth.external.verification is log, so a header that fails "
            f"the check is used anyway (agent {AGENT}, 2 gateway key(s) from {key_file})"
        ) in report
        assert "[OK] Actor context" not in report

    def test_a_missing_key_file_is_an_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)

        report = doctor_report(_config(tmp_path, gateway_jwks_file=str(tmp_path / "missing.json")))

        assert "[ERR] Actor context: auth.external.gateway_jwks_file: cannot read" in report

    def test_a_key_file_with_a_private_key_is_an_error(self, tmp_path, monkeypatch, signer):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        private = _b64(signer.keys[KID].private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()))
        signer.key_file.write_text(json.dumps({"keys": [{**signer.jwk(KID), "d": private}]}), encoding="utf-8")

        report = doctor_report(_config(tmp_path, gateway_jwks_file=str(signer.key_file)))

        assert "[ERR] Actor context: auth.external.gateway_jwks_file:" in report
        assert "private key members" in report
        assert private not in report

    def test_unusable_settings_are_errors(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        config = _config(tmp_path, gateway_jwks_file=str(_vector_key_file(tmp_path)), agent_id="agent-1")

        report = doctor_report(config)

        assert "[ERR] Actor context: auth.external.agent_id must be a lowercase UUID" in report

    def test_without_the_key_file_setting_it_warns(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)

        report = doctor_report(_config(tmp_path))

        assert "[WARN] Actor context: the X-Nerve-Actor-Context header is not verified" in report
        assert "[ERR] Actor context" not in report

    def test_local_mode_does_not_read_it(self, tmp_path):
        report = doctor_report(_config(tmp_path, gateway_jwks_file=str(tmp_path / "missing.json")))

        assert "Actor context" not in report
