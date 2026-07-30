"""Tests for the tracked favicon — lookup convention and the route serving it."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from nerve.config import workspace_favicon

# A one-pixel PNG, so the served bytes are a real image rather than a marker.
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000a49444154789c6300010000050001"
    "0d0a2db40000000049454e44ae426082"
)


def _ws(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    (ws / "config").mkdir(parents=True)
    return ws


class TestLookup:
    def test_no_favicon_is_none(self, tmp_path):
        assert workspace_favicon(_ws(tmp_path)) is None

    def test_missing_config_subtree_is_none(self, tmp_path):
        """A workspace that predates the config subtree, not an error."""
        ws = tmp_path / "bare"
        ws.mkdir()
        assert workspace_favicon(ws) is None

    @pytest.mark.parametrize(
        ("name", "content_type"),
        [
            ("favicon.svg", "image/svg+xml"),
            ("favicon.png", "image/png"),
            ("favicon.ico", "image/x-icon"),
        ],
    )
    def test_each_format_is_found_with_its_type(self, tmp_path, name, content_type):
        ws = _ws(tmp_path)
        (ws / "config" / name).write_bytes(_PNG)
        found = workspace_favicon(ws)
        assert found is not None
        assert found[0] == ws / "config" / name
        assert found[1] == content_type

    def test_best_format_wins_when_several_exist(self, tmp_path):
        ws = _ws(tmp_path)
        for name in ("favicon.ico", "favicon.png", "favicon.svg"):
            (ws / "config" / name).write_bytes(_PNG)
        assert workspace_favicon(ws)[0].name == "favicon.svg"

    def test_only_the_conventional_names(self, tmp_path):
        """`icon.png` is not it — one name per format, so there is one answer."""
        ws = _ws(tmp_path)
        (ws / "config" / "icon.png").write_bytes(_PNG)
        (ws / "config" / "logo.svg").write_bytes(_PNG)
        assert workspace_favicon(ws) is None

    def test_a_directory_named_like_one_is_not_served(self, tmp_path):
        ws = _ws(tmp_path)
        (ws / "config" / "favicon.png").mkdir()
        assert workspace_favicon(ws) is None

    def test_a_symlink_out_of_the_subtree_is_refused(self, tmp_path, caplog):
        """The reason this lookup resolves at all.

        Git tracks symlinks, so a config repo can carry one; the route is
        unauthenticated, so following it would serve any file the daemon can read
        to anyone who can reach the port. The filename gives a reviewer nothing
        to notice.
        """
        ws = _ws(tmp_path)
        secret = tmp_path / "secret.txt"
        secret.write_text("shadow contents")
        (ws / "config" / "favicon.png").symlink_to(secret)
        with caplog.at_level("WARNING"):
            assert workspace_favicon(ws) is None
        assert "outside the tracked config subtree" in caplog.text

    def test_a_symlink_inside_the_subtree_is_fine(self, tmp_path):
        """Containment is the rule, not "no symlinks" — a link to a real asset."""
        ws = _ws(tmp_path)
        (ws / "config" / "brand").mkdir()
        (ws / "config" / "brand" / "logo.png").write_bytes(_PNG)
        (ws / "config" / "favicon.png").symlink_to(ws / "config" / "brand" / "logo.png")
        found = workspace_favicon(ws)
        assert found is not None and found[1] == "image/png"

    def test_a_workspace_reached_through_a_symlink_still_works(self, tmp_path):
        """Resolving must not make the ordinary case look like an escape.

        A workspace under a symlinked home is normal, and its own config subtree
        is inside itself however the path was spelled.
        """
        real = tmp_path / "real"
        (real / "config").mkdir(parents=True)
        (real / "config" / "favicon.png").write_bytes(_PNG)
        link = tmp_path / "via-link"
        link.symlink_to(real)
        assert workspace_favicon(link) is not None

    def test_a_dangling_symlink_is_not_served(self, tmp_path):
        ws = _ws(tmp_path)
        (ws / "config" / "favicon.png").symlink_to(tmp_path / "gone.png")
        assert workspace_favicon(ws) is None

    def test_the_favicon_is_not_cached_between_calls(self, tmp_path):
        """A synced favicon has to appear without a restart, and go the same way."""
        ws = _ws(tmp_path)
        assert workspace_favicon(ws) is None
        (ws / "config" / "favicon.png").write_bytes(_PNG)
        assert workspace_favicon(ws) is not None
        (ws / "config" / "favicon.png").unlink()
        assert workspace_favicon(ws) is None


class TestRoute:
    def _client(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfgmod
        from nerve.config import NerveConfig

        ws = _ws(tmp_path)
        config = NerveConfig()
        config.workspace = ws
        monkeypatch.setattr(cfgmod, "_config", config, raising=False)

        # The route as create_app registers it, and the catch-all behind it in
        # the same order, so what is exercised is the resolution between the two
        # rather than a favicon route on its own. Standing up the real gateway
        # would drag in its lifespan for a static-file question.
        app = FastAPI()

        @app.get("/favicon.ico", include_in_schema=False)
        async def favicon():
            from fastapi.responses import FileResponse, Response

            found = cfgmod.workspace_favicon(cfgmod.get_config().workspace)
            if found is None:
                return Response(status_code=404)
            path, content_type = found
            return FileResponse(str(path), media_type=content_type)

        @app.get("/{path:path}")
        async def spa_fallback(path: str):
            from fastapi.responses import HTMLResponse

            return HTMLResponse("<!doctype html><title>index</title>")

        return TestClient(app), ws

    def test_the_catch_all_does_not_win(self, tmp_path, monkeypatch):
        """Proves the ordering, not just that the routes are listed in an order.

        The catch-all is registered second and matches /favicon.ico perfectly
        well; that it does not answer is the whole mechanism.
        """
        client, _ = self._client(tmp_path, monkeypatch)
        r = client.get("/favicon.ico")
        assert b"<!doctype html>" not in r.content.lower()
        # And it is still there for anything else.
        assert b"index" in client.get("/some/spa/route").content

    def test_404_when_none_is_tracked(self, tmp_path, monkeypatch):
        """A 404, not index.html with a 200 — which is what the catch-all gave."""
        client, _ = self._client(tmp_path, monkeypatch)
        assert client.get("/favicon.ico").status_code == 404

    def test_serves_the_bytes_and_the_type(self, tmp_path, monkeypatch):
        client, ws = self._client(tmp_path, monkeypatch)
        (ws / "config" / "favicon.png").write_bytes(_PNG)
        r = client.get("/favicon.ico")
        assert r.status_code == 200
        assert r.content == _PNG
        # The suffix decides the type, so a PNG at the .ico URL is still a PNG.
        assert r.headers["content-type"] == "image/png"

    def test_a_symlinked_secret_is_not_served(self, tmp_path, monkeypatch):
        client, ws = self._client(tmp_path, monkeypatch)
        secret = tmp_path / "secret.txt"
        secret.write_text("shadow contents")
        (ws / "config" / "favicon.png").symlink_to(secret)
        r = client.get("/favicon.ico")
        assert r.status_code == 404
        assert b"shadow" not in r.content


@pytest.fixture
def built_frontend():
    """A minimal ``web/dist`` so ``create_app`` registers the SPA catch-all.

    Without one the catch-all is never registered, and an assertion about
    ordering against it passes because neither route is there to compare — which
    is the failure the ordering test exists to catch. ``web/dist`` is gitignored,
    so creating it dirties nothing; a real build is left alone.
    """
    import nerve.gateway.server as server_mod

    dist = Path(server_mod.__file__).parent.parent.parent / "web" / "dist"
    if dist.exists():
        yield dist
        return
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!doctype html><title>built</title>")
    try:
        yield dist
    finally:
        shutil.rmtree(dist, ignore_errors=True)


class TestRegistrationOrder:
    def test_favicon_is_matched_before_the_spa_catch_all(self, built_frontend):
        """The catch-all answers everything, so order is what makes this work.

        Asserted against the real app, because the failure is silent:
        /favicon.ico goes back to returning 200 with index.html in it.
        """
        from nerve.gateway.server import create_app

        paths = [getattr(r, "path", None) for r in create_app().routes]
        assert "/favicon.ico" in paths, paths
        # Guards the fixture as much as the app: without the catch-all present
        # the comparison below would be vacuous.
        assert "/{path:path}" in paths, paths
        assert paths.index("/favicon.ico") < paths.index("/{path:path}")

    def test_the_route_needs_no_authentication(self):
        """A browser requests a favicon before the user has a token."""
        from nerve.gateway.server import create_app

        for route in create_app().routes:
            if getattr(route, "path", None) == "/favicon.ico":
                assert not getattr(route, "dependencies", []), route.dependencies
                return
        pytest.fail("no /favicon.ico route registered")
