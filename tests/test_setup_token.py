"""Who a setup token is for, and what makes one unnecessary.

A fresh install has one account with no password: every caller who reaches the
gateway is admitted as the owner until somebody claims it. The claim is guarded
by two things and this file is about both — the token itself (generated once,
logged, kept as state, dropped on a claim) and the locality rule that makes it
optional for a caller who is already on the machine.

The endpoint that uses them is tested in ``test_setup_wizard.py``; these are
the pieces, tested where a bug in one of them is cheapest to see.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve import setup_token
from nerve.config import AuthConfig, NerveConfig
from nerve.gateway.auth import hash_password

_PASSWORD = "correct-horse-battery-staple"


class TestLocality:
    @pytest.mark.parametrize("host", [
        "127.0.0.1",
        "127.0.0.53",          # all of 127/8 is the loopback interface
        "::1",
        "[::1]",
        "::ffff:127.0.0.1",    # what a dual-stack listener reports for v4 loopback
        "::ffff:7f00:1",       # the same address, written the other way
    ])
    def test_loopback_spellings_are_local(self, host):
        assert setup_token.is_loopback_peer(host) is True

    @pytest.mark.parametrize("host", [
        "203.0.113.7", "10.0.0.1", "192.168.1.10", "::ffff:10.0.0.1",
        "fe80::1%eth0", "0.0.0.0", "localhost", "not-an-address", "", None,
    ])
    def test_everything_else_is_not(self, host):
        assert setup_token.is_loopback_peer(host) is False

    def test_the_peer_comes_from_the_socket_and_nowhere_else(self):
        scope = {
            "client": ("203.0.113.7", 5555),
            "headers": [(b"x-forwarded-for", b"127.0.0.1")],
        }
        assert setup_token.peer_host(scope) == "203.0.113.7"
        assert setup_token.peer_host({"headers": scope["headers"]}) is None
        assert setup_token.peer_host(None) is None

    def test_no_header_is_read_anywhere_in_the_guard(self):
        """A grep, deliberately: the guard must never grow header handling.

        A caller-supplied header that can turn a remote request into a local
        one defeats the whole thing, so the absence is worth pinning rather
        than assuming. Comments and every string literal are removed first —
        the module explains at length *why* it ignores ``X-Forwarded-For``,
        and a check that could not tell prose from code would either fail on
        the explanation or have to stop looking for the word.
        """
        import ast

        tree = ast.parse(Path(setup_token.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                node.value = ""
        code = ast.unparse(tree)
        assert "headers" not in code
        assert "request" not in code, "the guard reads a scope, never a Request"

    def test_a_local_peer_needs_no_token_unless_configuration_says_so(self):
        relaxed = NerveConfig(auth=AuthConfig())
        strict = NerveConfig(auth=AuthConfig(setup_token_required=True))
        assert setup_token.token_is_required("127.0.0.1", relaxed) is False
        assert setup_token.token_is_required("127.0.0.1", strict) is True
        assert setup_token.token_is_required("203.0.113.7", relaxed) is True
        assert setup_token.token_is_required("203.0.113.7", strict) is True
        # No peer at all is not proof of anything: fail closed.
        assert setup_token.token_is_required(None, relaxed) is True

    def test_the_switch_is_off_by_default_and_needs_a_real_answer(self):
        from nerve.config import ConfigError

        assert AuthConfig.from_dict({}).setup_token_required is False
        assert AuthConfig.from_dict({"setup_token_required": "yes"}).setup_token_required
        with pytest.raises(ConfigError):
            AuthConfig.from_dict({"setup_token_required": "sometimes"})


class TestComparison:
    def test_nothing_matches_when_no_token_is_stored(self):
        assert setup_token.token_accepted("", "") is False
        assert setup_token.token_accepted(None, "") is False
        assert setup_token.token_accepted("anything", "") is False

    def test_only_the_exact_token_matches(self):
        assert setup_token.token_accepted("abc", "abc") is True
        assert setup_token.token_accepted("abc ", "abc") is False
        assert setup_token.token_accepted("ab", "abc") is False


@pytest.mark.asyncio
class TestTheTokenLifecycle:
    @pytest.fixture(autouse=True)
    def _config(self):
        self.config = NerveConfig(auth=AuthConfig())

    async def _db(self, open_identity_db, tmp_path):
        return await open_identity_db(tmp_path / "nerve.db")

    async def test_generated_once_and_kept_across_restarts(
        self, open_identity_db, tmp_path,
    ):
        db, _identity = await self._db(open_identity_db, tmp_path)
        try:
            first = await setup_token.ensure_setup_token(db, unclaimed=True)
            assert first and len(first) >= 20
            # A restart runs the same code against the same database. An
            # operator who wrote the token down must not have to go looking
            # for a new one.
            assert await setup_token.ensure_setup_token(db, unclaimed=True) == first
            assert await setup_token.stored_setup_token(db) == first
        finally:
            await db.close()

    async def test_a_claimed_instance_has_none_and_loses_any_it_had(
        self, open_identity_db, tmp_path,
    ):
        db, identity = await self._db(open_identity_db, tmp_path)
        try:
            await setup_token.ensure_setup_token(db, unclaimed=True)
            assert await setup_token.ensure_setup_token(db, unclaimed=False) is None
            assert await setup_token.stored_setup_token(db) == ""
        finally:
            await db.close()

    async def test_invalidating_is_idempotent(self, open_identity_db, tmp_path):
        db, _identity = await self._db(open_identity_db, tmp_path)
        try:
            await setup_token.ensure_setup_token(db, unclaimed=True)
            assert await setup_token.invalidate_setup_token(db) is True
            assert await setup_token.invalidate_setup_token(db) is False
        finally:
            await db.close()

    async def test_unclaimed_reads_the_accounts_and_the_configuration(
        self, open_identity_db, tmp_path,
    ):
        """Both halves, through the predicate the login route uses.

        A row with no credential is not enough on its own: PR 3's D3 window is
        a ``none`` row that authenticates through a hot-reloaded
        ``auth.password_hash``, and an instance with a working password is not
        there to be claimed.
        """
        db, identity = await self._db(open_identity_db, tmp_path)
        try:
            assert await setup_token.instance_is_unclaimed(db, self.config) is True

            configured = NerveConfig(
                auth=AuthConfig(password_hash=hash_password(_PASSWORD)),
            )
            assert await setup_token.instance_is_unclaimed(db, configured) is False

            await db.update_account_login(
                identity.owner_account_id, credential=hash_password(_PASSWORD),
            )
            assert await setup_token.instance_is_unclaimed(db, self.config) is False
        finally:
            await db.close()

    async def test_the_announcement_carries_the_token_and_says_where_to_go(
        self, caplog,
    ):
        with caplog.at_level("WARNING"):
            setup_token.announce("a-synthetic-token", host="0.0.0.0", port=8900)
        message = caplog.text
        assert "a-synthetic-token" in message
        assert "/setup" in message
        # 0.0.0.0 is a bind address, not somewhere to point a browser.
        assert "localhost:8900" in message

    async def test_nothing_is_announced_once_there_is_no_token(self, caplog):
        with caplog.at_level("WARNING"):
            setup_token.announce(None, host="127.0.0.1", port=8900)
        assert caplog.text == ""
