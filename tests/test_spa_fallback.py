"""The SPA catch-all serves bundle files and nothing else.

The route captures the whole request path and answers before login, so the
only thing between a request and the process's filesystem is the check that
what the path names stays inside web/dist. Pinned at both levels: the path
arithmetic on its own, and the route as the gateway mounts it, behind
Starlette's own path decoding.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nerve.gateway.server import _bundle_file, _serve_web_ui

_INDEX = b"<!doctype html><title>index</title>"


@pytest.fixture
def bundle(tmp_path):
    """A throwaway web/dist, with a file beside it that must never be served."""
    dist = tmp_path / "web" / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_bytes(_INDEX)
    (dist / "robots.txt").write_text("User-agent: *\n")
    (dist / "assets" / "app.js").write_text("console.log('app')\n")
    (tmp_path / "config.local.yaml").write_text("anthropic_api_key: shadow\n")
    return dist


class TestBundleFile:
    def test_a_file_in_the_bundle(self, bundle):
        assert _bundle_file(bundle, "robots.txt") == (bundle / "robots.txt").resolve()

    def test_a_route_that_is_not_a_file(self, bundle):
        assert _bundle_file(bundle, "sessions/abc") is None

    @pytest.mark.parametrize("path", ["", ".", "assets"])
    def test_the_bundle_root_or_a_directory_is_not_a_file(self, bundle, path):
        assert _bundle_file(bundle, path) is None

    def test_dot_dot_segments_do_not_leave_the_bundle(self, bundle):
        # uvicorn hands the route the segments as sent; nothing collapses them.
        assert _bundle_file(bundle, "../../config.local.yaml") is None
        assert _bundle_file(bundle, "assets/../../../config.local.yaml") is None

    def test_an_absolute_path_is_not_joined(self, bundle, tmp_path):
        # Path("/dist") / "/etc/hosts" is Path("/etc/hosts"): the left side is
        # dropped, which is how a `//etc/hosts` request named /etc/hosts.
        assert _bundle_file(bundle, str(tmp_path / "config.local.yaml")) is None
        assert _bundle_file(bundle, "/etc/hosts") is None

    def test_a_symlink_out_of_the_bundle_is_not_a_bundle_file(self, bundle, tmp_path):
        (bundle / "link.yaml").symlink_to(tmp_path / "config.local.yaml")
        assert _bundle_file(bundle, "link.yaml") is None

    def test_a_nul_byte_is_not_an_error(self, bundle):
        assert _bundle_file(bundle, "robots.txt\x00") is None


def _client(bundle) -> TestClient:
    """The routes exactly as ``create_app`` registers them, over ``bundle``."""
    app = FastAPI()
    _serve_web_ui(app, bundle)
    return TestClient(app)


class TestRoute:
    def test_index_html_for_spa_routes(self, bundle):
        r = _client(bundle).get("/sessions/abc")
        assert r.status_code == 200
        assert r.content == _INDEX

    def test_bundle_files_and_assets_are_served(self, bundle):
        client = _client(bundle)
        assert client.get("/robots.txt").text == "User-agent: *\n"
        assert client.get("/assets/app.js").status_code == 200

    @pytest.mark.parametrize(
        "url",
        [
            # An encoded `/` inside a segment is decoded before routing, so the
            # route sees `../../…`. (The client itself collapses a plain `../`,
            # which uvicorn does not — hence the encoded forms here.)
            "/..%2F..%2Fconfig.local.yaml",
            "/%2E%2E/%2E%2E/config.local.yaml",
        ],
    )
    def test_traversal_gets_index_html(self, bundle, url):
        r = _client(bundle).get(url)
        assert r.status_code == 200
        assert b"shadow" not in r.content
        assert r.content == _INDEX

    def test_traversal_under_assets_is_refused_too(self, bundle):
        # The /assets mount answers this one; StaticFiles has its own check.
        r = _client(bundle).get("/assets/..%2F..%2F..%2Fconfig.local.yaml")
        assert r.status_code == 404
        assert b"shadow" not in r.content

    def test_an_absolute_path_gets_index_html(self, bundle, tmp_path):
        # `//x/y` is a valid request target. The catch-all captures `/x/y`,
        # and pathlib would have joined that by discarding the bundle directory.
        r = _client(bundle).get("/" + str(tmp_path / "config.local.yaml"))
        assert r.status_code == 200
        assert b"shadow" not in r.content
        assert r.content == _INDEX

    def test_a_nul_byte_gets_index_html(self, bundle):
        r = _client(bundle).get("/robots.txt%00")
        assert r.status_code == 200
        assert r.content == _INDEX
