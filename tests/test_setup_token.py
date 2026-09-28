"""Persistence, comparison, and local delivery of the setup token."""

from __future__ import annotations

import pytest

from nerve import setup_token
from nerve.config import AuthConfig, NerveConfig
from nerve.gateway.auth import hash_password

_PASSWORD = "correct-horse-battery-staple"


class TestComparison:
    def test_nothing_matches_when_no_token_is_stored(self):
        assert setup_token.token_accepted("", "") is False
        assert setup_token.token_accepted(None, "") is False
        assert setup_token.token_accepted("anything", "") is False

    def test_not_even_the_decoy_matches(self, monkeypatch):
        monkeypatch.setattr(setup_token, "_DECOY_TOKEN", "a-synthetic-decoy")
        assert setup_token.token_accepted("a-synthetic-decoy", "") is False

    def test_only_the_exact_nonempty_token_matches(self):
        assert setup_token.token_accepted("abc", "abc") is True
        assert setup_token.token_accepted("", "abc") is False
        assert setup_token.token_accepted("abc ", "abc") is False
        assert setup_token.token_accepted("ab", "abc") is False
        assert setup_token.token_accepted("é", "abc") is False
        assert setup_token.token_accepted("\ud800", "abc") is False

    def test_every_supplied_value_uses_compare_digest(self, monkeypatch):
        compared: list[tuple[bytes, bytes]] = []

        def compare(candidate: bytes, reference: bytes) -> bool:
            compared.append((candidate, reference))
            return candidate == reference

        monkeypatch.setattr(setup_token.secrets, "compare_digest", compare)
        assert setup_token.token_accepted("guess", "") is False
        assert setup_token.token_accepted("right", "right") is True
        assert compared == [
            (b"guess", setup_token._DECOY_TOKEN.encode()),
            (b"right", b"right"),
        ]


@pytest.mark.asyncio
class TestTheTokenLifecycle:
    async def _db(self, open_identity_db, tmp_path):
        return await open_identity_db(tmp_path / "nerve.db")

    async def test_generated_once_and_kept_across_restarts(
        self, open_identity_db, tmp_path, caplog,
    ):
        db, _identity = await self._db(open_identity_db, tmp_path)
        try:
            first = await setup_token.ensure_setup_token(db, unclaimed=True)
            assert first and len(first) >= 20
            assert await setup_token.ensure_setup_token(db, unclaimed=True) == first
            assert await setup_token.stored_setup_token(db) == first
            assert first not in caplog.text
        finally:
            await db.close()

    async def test_a_claimed_instance_loses_any_token(
        self, open_identity_db, tmp_path,
    ):
        db, _identity = await self._db(open_identity_db, tmp_path)
        try:
            await setup_token.ensure_setup_token(db, unclaimed=True)
            assert await setup_token.ensure_setup_token(db, unclaimed=False) is None
            assert await setup_token.stored_setup_token(db) == ""
        finally:
            await db.close()

    async def test_unclaimed_uses_the_login_predicate(
        self, open_identity_db, tmp_path,
    ):
        db, identity = await self._db(open_identity_db, tmp_path)
        plain = NerveConfig(auth=AuthConfig())
        try:
            assert await setup_token.instance_is_unclaimed(db, plain) is True
            configured = NerveConfig(
                auth=AuthConfig(password_hash=hash_password(_PASSWORD)),
            )
            assert await setup_token.instance_is_unclaimed(db, configured) is False
            await db.update_account_login(
                identity.owner_account_id,
                credential=hash_password(_PASSWORD),
            )
            assert await setup_token.instance_is_unclaimed(db, plain) is False
        finally:
            await db.close()


@pytest.mark.asyncio
class TestLocalTokenDelivery:
    async def test_doctor_never_prints_the_token(
        self, open_identity_db, tmp_path, monkeypatch,
    ):
        from nerve.cli import doctor_report

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        report = doctor_report(NerveConfig(workspace=tmp_path / "workspace"))
        assert token not in report
        assert "Setup is not complete" in report
        assert "nerve status" in report

    async def test_status_prints_the_token_but_never_puts_it_in_a_url(
        self, open_identity_db, tmp_path, monkeypatch, capsys,
    ):
        from nerve.cli import _echo_setup_token

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        _echo_setup_token(NerveConfig())
        printed = capsys.readouterr().out
        assert f"Setup token: {token}" in printed
        assert "/setup" in printed
        assert f"/setup?{token}" not in printed
        assert f"setup_token={token}" not in printed

    @pytest.mark.parametrize("deployment", ["server", "docker"])
    async def test_the_status_command_delivers_it_in_both_deployments(
        self, open_identity_db, tmp_path, monkeypatch, deployment,
    ):
        from click.testing import CliRunner
        from nerve import cli

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        try:
            token = await setup_token.ensure_setup_token(db, unclaimed=True)
        finally:
            await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        monkeypatch.setattr(cli, "_docker_compose", lambda *args, **kwargs: 0)
        monkeypatch.setattr(
            cli, "_is_docker_mode", lambda config: deployment == "docker",
        )

        result = CliRunner().invoke(cli.main, ["-c", str(tmp_path), "status"])
        assert result.exit_code == 0, result.output
        assert token in result.output

    async def test_status_says_nothing_after_invalidation(
        self, open_identity_db, tmp_path, monkeypatch, capsys,
    ):
        from nerve.cli import _echo_setup_token

        db, _identity = await open_identity_db(tmp_path / "nerve.db")
        await db.close()
        monkeypatch.setattr("nerve.paths.db_path", lambda: tmp_path / "nerve.db")

        _echo_setup_token(NerveConfig())
        assert capsys.readouterr().out == ""
