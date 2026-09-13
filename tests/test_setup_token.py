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
        than assuming. Docstrings are blanked first and comments are dropped by
        the parse — the module explains at length *why* it ignores
        ``X-Forwarded-For``, and a check that could not tell prose from code
        would fail on the explanation. String literals in *code* are kept,
        because ``scope.get("headers")`` is exactly the shape being refused.
        """
        import ast

        tree = ast.parse(Path(setup_token.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                continue
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                first.value.value = ""
        code = ast.unparse(tree).lower()
        for forbidden in ("header", "forwarded", "real-ip", "request."):
            assert forbidden not in code, forbidden

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

    def test_not_even_the_decoy_it_compares_against(self, monkeypatch):
        """The comparison still happens when nothing is stored, so a refusal
        costs the same either way — and the decoy it compares against must not
        become a password. Unguessable in practice (it is random per process);
        checked anyway, because "unguessable" is not "cannot match"."""
        monkeypatch.setattr(setup_token, "_DECOY_TOKEN", "a-synthetic-decoy")
        assert setup_token.token_accepted("a-synthetic-decoy", "") is False

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


@pytest.mark.asyncio
class TestWhereTheTokenIsPrinted:
    """It is a credential: the channels it appears on are chosen, not incidental."""

    async def test_doctor_names_the_state_but_never_the_token(
        self, open_identity_db, tmp_path, monkeypatch,
    ):
        """`nerve doctor` is also produced for the Telegram ``/doctor``
        command, so a live credential in it would be a credential in a chat
        log. It says where to find the token instead."""
        from nerve.cli import doctor_report

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        report = doctor_report(NerveConfig(workspace=tmp_path / "workspace"))
        assert token not in report
        assert "passwordless" in report
        assert "/setup" in report

    async def test_status_prints_it_on_the_machine(
        self, open_identity_db, tmp_path, monkeypatch, capsys,
    ):
        """`nerve status` is a terminal command on the box, which is exactly
        who the token is for."""
        from nerve.cli import _echo_setup_token

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        _echo_setup_token(NerveConfig())
        printed = capsys.readouterr().out
        assert token in printed
        assert "/setup" in printed

    @pytest.mark.parametrize("deployment", ["server", "docker"])
    async def test_the_real_command_prints_it_in_both_deployments(
        self, open_identity_db, tmp_path, monkeypatch, deployment,
    ):
        """Through `nerve status` itself, not the helper it calls.

        Docker is the deployment that *needs* the token — its callers arrive
        over the bridge network, so their peer is never loopback — and its
        branch of this command returned before ever reaching the helper.
        """
        from click.testing import CliRunner

        from nerve import cli

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        monkeypatch.setattr(cli, "_docker_compose", lambda *a, **k: 0)
        monkeypatch.setattr(cli, "_is_docker_mode", lambda config: deployment == "docker")

        result = CliRunner().invoke(
            cli.main, ["-c", str(tmp_path), "status"], obj=None,
        )
        assert token in result.output, (deployment, result.output)
        assert "/setup" in result.output

    async def test_status_says_nothing_once_the_instance_is_claimed(
        self, open_identity_db, tmp_path, monkeypatch, capsys,
    ):
        from nerve.cli import _echo_setup_token

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        _echo_setup_token(NerveConfig())
        assert capsys.readouterr().out == ""


class TestTheListenerDoesNotTrustForwardingHeaders:
    """The one place a header could reach the peer address is the server's own.

    uvicorn enables ``ProxyHeadersMiddleware`` by default and
    ``FORWARDED_ALLOW_IPS`` in the environment can widen its trust to ``*``,
    at which point a remote caller names its own address and the locality guard
    is answering a question the caller asked. Nerve reads no forwarding header
    anywhere, so the listener says so explicitly — and this pins that it keeps
    saying so, because the default is the dangerous direction.
    """

    def test_run_server_disables_proxy_headers(self, monkeypatch):
        from nerve.config import NerveConfig
        from nerve.gateway import server

        captured: dict = {}
        monkeypatch.setattr(
            server, "create_app", lambda: object(),
        )

        def _fake_run(app, **kwargs):
            captured.update(kwargs)

        import uvicorn

        monkeypatch.setattr(uvicorn, "run", _fake_run)
        server.run_server(NerveConfig())

        assert captured["proxy_headers"] is False
        assert captured["forwarded_allow_ips"] is None

    def test_uvicorn_still_defaults_the_other_way(self):
        """If this ever changes, the explicit argument stops being load-bearing
        and the comment beside it stops being true."""
        import inspect

        import uvicorn

        default = inspect.signature(uvicorn.Config.__init__).parameters[
            "proxy_headers"
        ].default
        assert default is True, (
            "uvicorn no longer trusts proxy headers by default; revisit "
            "run_server's explicit argument and the comment on it"
        )
