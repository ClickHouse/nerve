"""`nerve reload` — the CLI front end for POST /api/config/reload."""

from __future__ import annotations

import httpx
import pytest
from click.testing import CliRunner

from nerve.cli import _gateway_url, main


def _config(tmp_path, extra=""):
    (tmp_path / "config.yaml").write_text(
        "auth:\n  jwt_secret: test-secret-value-long-enough-for-hs256\n" + extra, encoding="utf-8"
    )
    return tmp_path


def _locked_config(tmp_path, extra=""):
    """A config dir the daemon would read as locked.

    The flag has to come from the workspace's tracked settings.yaml: `lockdown`
    in config.yaml is ignored by design, so setting it there would produce an
    unlocked instance and a test that passes for the wrong reason.

    The workspace is a git repository with a remote for the same kind of reason.
    A locked instance with nowhere to receive reviewed config from refuses to
    start, so without one these would exercise that refusal instead of the
    reload behaviour they are about. The remote is never contacted.
    """
    import shutil
    import subprocess

    from nerve.config import workspace_settings_file

    if not shutil.which("git"):
        pytest.skip("git not available")
    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir(parents=True, exist_ok=True)
    (workspace / "config").mkdir(parents=True, exist_ok=True)
    (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
    workspace_settings_file(workspace).write_text("lockdown: true\n" + extra, encoding="utf-8")
    for args in (["init", "-q"],
                 ["remote", "add", "origin", "https://example.invalid/config.git"]):
        subprocess.run(["git", *args], cwd=str(workspace), check=True, capture_output=True)
    return config_dir


class _Response:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def _post(recorder=None, **kwargs):
    """A stand-in for httpx.post that records how it was called."""
    def fake_post(url, **call_kwargs):
        if recorder is not None:
            recorder.append({"url": url, **call_kwargs})
        return _Response(**kwargs)
    return fake_post


class TestGatewayUrl:
    """A bind address is not a destination."""

    @pytest.mark.parametrize("host", ["0.0.0.0", "::", "*", ""])
    def test_wildcard_binds_resolve_to_loopback(self, host):
        from nerve.config import GatewayConfig, NerveConfig

        cfg = NerveConfig(gateway=GatewayConfig(host=host, port=8900))
        assert _gateway_url(cfg, "/x") == "http://127.0.0.1:8900/x"

    def test_a_real_host_is_kept(self):
        from nerve.config import GatewayConfig, NerveConfig

        cfg = NerveConfig(gateway=GatewayConfig(host="10.0.0.4", port=9001))
        assert _gateway_url(cfg, "/x") == "http://10.0.0.4:9001/x"

    def test_tls_switches_the_scheme(self, tmp_path):
        from nerve.config import GatewayConfig, NerveConfig, SSLConfig

        ssl = SSLConfig(cert=tmp_path / "c.pem", key=tmp_path / "k.pem")
        cfg = NerveConfig(gateway=GatewayConfig(host="box", port=443, ssl=ssl))
        assert _gateway_url(cfg, "/x") == "https://box:443/x"


class TestReloadCommand:
    def test_reports_each_subsystem_and_succeeds(self, tmp_path, monkeypatch):
        monkeypatch.setattr(httpx, "post", _post(payload={
            "ok": True,
            "detail": {"config": "reloaded", "cron": {"added": 1, "removed": 0},
                       "mcp": "2 server(s)"},
            "errors": {},
            "restart_required": "",
        }))
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code == 0, result.output
        assert "Config reloaded" in result.output
        assert "config: reloaded" in result.output
        assert "added=1" in result.output  # dict summaries are flattened, not repr'd
        assert "mcp: 2 server(s)" in result.output

    def test_a_partial_reload_exits_non_zero(self, tmp_path, monkeypatch):
        """A reload is best-effort by design, so `ok: false` still comes back 200.
        Exiting 0 there would let a script read a partial apply as a clean one."""
        monkeypatch.setattr(httpx, "post", _post(payload={
            "ok": False,
            "detail": {"config": "reloaded", "cron": "error: bad gate type"},
            "errors": {"cron": "bad gate type"},
            "restart_required": "",
        }))
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code == 1
        assert "[ERR] cron: bad gate type" in result.output
        assert "incomplete" in result.output

    def test_restart_required_is_surfaced_without_failing(self, tmp_path, monkeypatch):
        """Nothing failed and everything reloadable was applied, so this is a
        warning — but it must not be silent, or the value looks live when it
        isn't."""
        monkeypatch.setattr(httpx, "post", _post(payload={
            "ok": True,
            "detail": {"config": "reloaded", "restart_required": "gateway.port"},
            "errors": {},
            "restart_required": "gateway.port",
        }))
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code == 0, result.output
        assert "needs a restart: gateway.port" in result.output
        # Not also printed as an ordinary subsystem line.
        assert "restart_required: gateway.port" not in result.output

    def test_no_daemon_is_a_clean_message(self, tmp_path, monkeypatch):
        def refuse(url, **_kw):
            raise httpx.ConnectError("connection refused")

        monkeypatch.setattr(httpx, "post", refuse)
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code != 0
        assert "No daemon answering" in result.output
        assert "Traceback" not in result.output

    def test_rejected_token_explains_the_likely_cause(self, tmp_path, monkeypatch):
        monkeypatch.setattr(httpx, "post", _post(status_code=401, text="nope"))
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code != 0
        assert "jwt_secret" in result.output
        assert "Traceback" not in result.output

    def test_no_jwt_secret_still_calls_the_gateway(self, tmp_path, monkeypatch):
        """An unlocked gateway with no auth.jwt_secret does not ask for a token —
        require_auth runs open there. Refusing to send one would refuse the reload
        the endpoint would have accepted, on the dev box where the hand edits this
        command exists for are most of the edits there are."""
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls, payload={"ok": True}))
        (tmp_path / "config.yaml").write_text("timezone: UTC\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["-c", str(tmp_path), "reload"])
        assert result.exit_code == 0, result.output
        assert len(calls) == 1
        assert "Authorization" not in calls[0]["headers"]

    def test_a_password_hash_is_not_a_second_source_of_a_token(self, tmp_path, monkeypatch):
        """auth.password_hash gates the browser login, which is what mints a token
        from it. require_auth reads auth.jwt_secret alone, so a password neither
        makes the endpoint ask for a token nor gives this command one to sign."""
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls, payload={"ok": True}))
        (tmp_path / "config.yaml").write_text(
            "auth:\n  password_hash: $2b$12$notarealhashatall\n", encoding="utf-8"
        )
        result = CliRunner().invoke(main, ["-c", str(tmp_path), "reload"])
        assert result.exit_code == 0, result.output
        assert "Authorization" not in calls[0]["headers"]

    def test_lockdown_without_a_secret_refuses_before_calling(self, tmp_path, monkeypatch):
        """The one case where an empty secret is a dead end: a locked gateway never
        takes the open path, so no request from this shell can be authenticated.
        Sending it anyway would report the gateway's refusal as the problem."""
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls))
        result = CliRunner().invoke(main, ["-c", str(_locked_config(tmp_path)), "reload"])
        assert result.exit_code != 0
        assert "locked gateway" in result.output
        assert not calls

    def test_lockdown_with_a_secret_authenticates_normally(self, tmp_path, monkeypatch):
        """The refusal above is about the missing secret, not about lockdown."""
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls, payload={"ok": True}))
        cfg = _locked_config(
            tmp_path, extra="auth:\n  jwt_secret: test-secret-value-long-enough-for-hs256\n"
        )
        result = CliRunner().invoke(main, ["-c", str(cfg), "reload"])
        assert result.exit_code == 0, result.output
        assert calls[0]["headers"]["Authorization"].startswith("Bearer ")

    def test_tls_verification_is_only_skipped_for_the_loopback_rewrite(
        self, tmp_path, monkeypatch
    ):
        """A wildcard bind means we ask 127.0.0.1 for a certificate issued to a
        hostname, which cannot verify. A real host is a different matter — the
        certificate should match, so a failure there must not be skipped past."""
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls, payload={"ok": True}))
        certs = "gateway:\n  ssl:\n    cert: /c.pem\n    key: /k.pem\n"

        cfg = _config(tmp_path, extra=certs + "  host: 0.0.0.0\n")
        CliRunner().invoke(main, ["-c", str(cfg), "reload"])
        assert calls[-1]["verify"] is False

        cfg = _config(tmp_path, extra=certs + "  host: nerve.example.com\n")
        CliRunner().invoke(main, ["-c", str(cfg), "reload"])
        assert calls[-1]["verify"] is True

    def test_sends_a_bearer_token_to_the_reload_endpoint(self, tmp_path, monkeypatch):
        calls: list = []
        monkeypatch.setattr(httpx, "post", _post(calls, payload={"ok": True}))
        result = CliRunner().invoke(main, ["-c", str(_config(tmp_path)), "reload"])
        assert result.exit_code == 0, result.output
        assert len(calls) == 1
        assert calls[0]["url"].endswith("/api/config/reload")
        auth = calls[0]["headers"]["Authorization"]
        assert auth.startswith("Bearer ")
        # A real token the gateway would accept, not a placeholder.
        from nerve.gateway.auth import decode_token

        assert decode_token(auth.removeprefix("Bearer "), "test-secret-value-long-enough-for-hs256")
