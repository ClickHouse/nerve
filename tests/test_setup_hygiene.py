"""Cross-cutting invariants of the setup work.

* every shell-out in the setup modules is an argv list with a timeout and
  never ``shell=True``;
* the claim's cross-origin behaviour is unchanged (no new middleware, no
  cookie);
* no name, e-mail or hostname of a real person or organisation is in the
  fixtures or tests this work added.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
_SETUP_MODULES = (
    "nerve/setup_checks.py", "nerve/bootstrap.py", "nerve/daemon.py",
    "nerve/setup_writer.py", "nerve/cli.py",
)


def _calls(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            yield node


def _is_subprocess_call(node: ast.Call) -> bool:
    func = node.func
    name = None
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "subprocess":
        name = func.attr
    return name in {"run", "Popen", "check_output", "call", "check_call"}


class TestNoShellTrue:
    @pytest.mark.parametrize("module", _SETUP_MODULES)
    def test_no_shell_true(self, module):
        for call in _calls(ROOT / module):
            if not _is_subprocess_call(call):
                continue
            for keyword in call.keywords:
                assert not (keyword.arg == "shell" and getattr(keyword.value, "value", None) is True), (
                    f"{module}:{call.lineno} uses shell=True"
                )

    def test_setup_checks_subprocesses_are_bounded(self):
        """Every subprocess call in the live-check module carries a timeout."""
        for call in _calls(ROOT / "nerve/setup_checks.py"):
            if not _is_subprocess_call(call):
                continue
            keywords = {k.arg for k in call.keywords}
            assert "timeout" in keywords, f"setup_checks.py:{call.lineno} has no timeout"
            assert "capture_output" in keywords, f"setup_checks.py:{call.lineno} does not capture output"


@pytest.mark.asyncio
async def test_claim_cross_origin_preflight_is_unchanged():
    """The gateway's permissive dev CORS stays exactly as it is: an OPTIONS
    preflight and a cross-origin POST to the claim get the same CORS headers
    they always got, and nothing sets a cookie."""
    from nerve.gateway import server

    app = server.create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
        preflight = await http.options(
            "/api/setup/claim",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )
        posted = await http.post(
            "/api/setup/claim", json={}, headers={"Origin": "https://evil.example"},
        )
    assert preflight.status_code == 200
    assert preflight.headers.get("access-control-allow-origin") == "https://evil.example"
    assert preflight.headers.get("access-control-allow-credentials") == "true"
    assert "POST" in preflight.headers.get("access-control-allow-methods", "")
    assert posted.status_code == 422  # schema, not CORS
    assert "set-cookie" not in {k.lower() for k in posted.headers}
    assert "set-cookie" not in {k.lower() for k in preflight.headers}


_FORBIDDEN = re.compile(
    r"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@(?!example\.(com|invalid|org)\b)[a-z0-9.-]+\.[a-z]{2,}",
    re.IGNORECASE,
)
# A systemd user-manager unit (``user@1000.service``) is shaped like an
# address but is not one; the restart-command tests spell it out on purpose.
_SYSTEMD_USER_UNIT = re.compile(r"^user@\d+\.service$")
# A home directory of a real person: only the synthetic ``alice`` is allowed.
_PERSONAL_PATH = re.compile(r"/(?:Users|home)/(?!alice\b)[A-Za-z0-9._-]+")
# An IPv4 literal that is neither loopback/any nor a documentation range
# (TEST-NET-1/2/3) — a private or public address would be a real network.
_REAL_IPV4 = re.compile(
    r"\b(?!127\.0\.0\.1|0\.0\.0\.0|192\.0\.2\.|198\.51\.100\.|203\.0\.113\.)"
    r"\d{1,3}(?:\.\d{1,3}){3}\b"
)
# A secret-shaped string without the ``REPLACE``/``placeholder`` marker.
_SECRET_SHAPED = re.compile(
    r"\b(?:sk-(?:ant-)?|xox[abp]-|xapp-|ghp_)(?![A-Za-z0-9_-]*(?:REPLACE|placeholder))[A-Za-z0-9_-]{8,}"
)

_HYGIENE_FILES = sorted(
    [*(ROOT / "tests" / "fixtures" / "setup_writer").rglob("*.golden")]
    + [ROOT / "tests" / name for name in (
        "test_setup_api.py", "test_setup_checklist.py", "test_setup_writer.py",
        "test_cli_init_slim.py", "test_setup_state.py", "test_daemon_restart.py",
        "test_memu_gating.py", "test_setup_extensions.py", "test_setup_provenance.py",
        "test_setup_telegram_login.py", "test_default_models.py", "test_setup_local_secrets.py",
    )]
    + [ROOT / "nerve" / name for name in (
        "setup_writer.py", "setup_checklist.py", "setup_checks.py", "setup_state.py",
        "daemon.py", "boot.py", "setup_provenance.py", "setup_telegram_login.py",
    )]
    + [ROOT / "nerve" / "gateway" / "routes" / "setup.py"],
)


class TestPublicRepoHygiene:
    @pytest.mark.parametrize("path", _HYGIENE_FILES, ids=lambda p: str(p.relative_to(ROOT)))
    def test_no_real_addresses(self, path: Path):
        text = path.read_text(encoding="utf-8")
        hits = [
            m.group(0) for m in _FORBIDDEN.finditer(text)
            if "t.me" not in m.group(0) and not _SYSTEMD_USER_UNIT.match(m.group(0))
        ]
        # Only synthetic addresses: example.com / example.invalid / example.org.
        assert hits == [], hits

    @pytest.mark.parametrize("path", _HYGIENE_FILES, ids=lambda p: str(p.relative_to(ROOT)))
    def test_no_personal_paths_private_networks_or_live_looking_secrets(self, path: Path):
        text = path.read_text(encoding="utf-8")
        hits = (
            [m.group(0) for m in _PERSONAL_PATH.finditer(text)]
            + [m.group(0) for m in _REAL_IPV4.finditer(text)]
            + [m.group(0) for m in _SECRET_SHAPED.finditer(text)]
        )
        assert hits == [], hits


class TestEveryChecklistRouteIsGated:
    def test_require_person_depends_on_require_auth(self):
        import inspect

        from nerve.gateway.auth import require_auth
        from nerve.gateway.routes.setup import require_person

        params = inspect.signature(require_person).parameters
        assert [p.default.dependency for p in params.values()] == [require_auth]

    def test_every_checklist_route_requires_a_person(self):
        import inspect

        from nerve.gateway.routes.setup import require_person, router

        for route in router.routes:
            endpoint = getattr(route, "endpoint", None)
            if endpoint is None:
                continue
            gates = [
                p.default.dependency
                for p in inspect.signature(endpoint).parameters.values()
                if getattr(p.default, "dependency", None) is not None
            ]
            assert require_person in gates, route.path
