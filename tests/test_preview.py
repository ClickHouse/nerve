"""The Preview tab on the gateway side: the switch ``artifacts.preview``, the
sandbox document that it serves in local and in external mode, and the
routes that read a bundle (``nerve/gateway/routes/preview.py``).

What these pin: with the switch off nothing of it exists and the sandbox
document is as today; with it on, also in external mode, the document is
served with the policy that allows no origin, and MCP App views, widgets and
the widget tool stay off; the file route serves only files of the bundle's
description, never as a page of this address.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nerve.config import ArtifactsConfig, NerveConfig, load_config, validate_config_keys
from nerve.gateway.mcp_apps_sandbox import (
    SANDBOX_DOCUMENT,
    SANDBOX_PATH,
    build_csp_header,
    serves_preview,
    serves_sandbox,
)

PROXY = b"<!doctype html><title>sandbox proxy</title><script>/* relay */</script>"


def _external() -> None:
    from nerve.gateway.auth import AUTH_MODE_EXTERNAL, pin_auth_mode

    pin_auth_mode(AUTH_MODE_EXTERNAL)


@pytest.fixture
def document(tmp_path):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    path = dist / SANDBOX_DOCUMENT
    path.write_bytes(PROXY)
    (dist / "index.html").write_text("<!doctype html><title>web ui</title>")
    return path


# --------------------------------------------------------------------------- #
#  The setting                                                                 #
# --------------------------------------------------------------------------- #


class TestSetting:
    def test_off_by_default(self):
        config = NerveConfig.from_dict({})
        assert config.artifacts.preview is False
        assert config.artifacts_preview_active is False

    def test_on_with_the_flag_and_without_mcp_apps(self):
        config = NerveConfig.from_dict({"artifacts": {"preview": True}})
        assert config.artifacts_preview_active is True
        assert config.mcp_apps_active is False

    def test_the_section_has_one_setting(self):
        assert list(ArtifactsConfig.__dataclass_fields__) == ["preview"]

    def test_a_bare_section_is_the_default(self):
        assert NerveConfig.from_dict({"artifacts": None}).artifacts_preview_active is False

    def test_the_documented_environment_form(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text(
            f"workspace: {tmp_path / 'ws'}\n"
            "artifacts:\n"
            "  preview: ${NERVE_ARTIFACTS_PREVIEW:-false}\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("NERVE_ARTIFACTS_PREVIEW", raising=False)
        assert load_config(tmp_path).artifacts_preview_active is False
        monkeypatch.setenv("NERVE_ARTIFACTS_PREVIEW", "true")
        assert load_config(tmp_path).artifacts_preview_active is True

    def test_the_key_is_known(self):
        assert validate_config_keys({"artifacts": {"preview": True}}) == []
        (warning,) = validate_config_keys({"artifacts": {"no_such_setting": 1}})
        assert "unknown key 'artifacts.no_such_setting'" in warning


# --------------------------------------------------------------------------- #
#  Whether the gateway serves the sandbox document for previews                #
# --------------------------------------------------------------------------- #


def _config(*, preview: bool = True, mcp_apps: bool = False) -> NerveConfig:
    return NerveConfig.from_dict({"artifacts": {"preview": preview}, "mcp_apps": {"enabled": mcp_apps}})


class TestServing:
    def test_off_by_default_and_says_nothing(self, document, caplog):
        with caplog.at_level(logging.INFO, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_preview(_config(preview=False), document.parent) is False
        assert caplog.records == []

    def test_on_in_local_mode_without_mcp_apps(self, document):
        config = _config()
        assert serves_preview(config, document.parent) is True
        assert serves_sandbox(config, document.parent) is False

    def test_on_in_external_mode_and_views_stay_off(self, document, caplog):
        _external()
        config = _config(mcp_apps=True)
        with caplog.at_level(logging.INFO, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_preview(config, document.parent) is True
            assert serves_sandbox(config, document.parent) is False
        assert "local mode only" in caplog.text

    def test_the_widget_tool_stays_out_in_external_mode(self):
        from nerve.agent.backends.base import config_excluded_tools
        from nerve.widget_state import WIDGET_STATE_TOOL, widgets_drawn

        _external()
        config = _config(mcp_apps=True)
        assert widgets_drawn(config) is False
        assert WIDGET_STATE_TOOL in config_excluded_tools(config)

    def test_the_widget_tool_does_not_follow_the_preview(self):
        from nerve.agent.backends.base import config_excluded_tools
        from nerve.widget_state import WIDGET_STATE_TOOL

        assert WIDGET_STATE_TOOL in config_excluded_tools(_config())

    def test_a_missing_document_is_said_at_startup(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_preview(_config(), tmp_path / "no-dist") is True
        assert "npm run build" in caplog.text


@pytest.fixture
def main(document, monkeypatch):
    """The gateway's own app over a built web UI that holds the document."""
    import nerve.config as cfgmod
    import nerve.gateway.server as server_mod
    from nerve.gateway.routes import _deps as deps_module

    # The preview router is registered from the configuration.
    monkeypatch.setattr(cfgmod, "_config", _config(), raising=False)
    monkeypatch.setattr(server_mod, "WEB_DIST", document.parent)

    def serving(*, views: bool = False, preview: bool = False) -> None:
        monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(
            engine=SimpleNamespace(config=NerveConfig()), db=None,
            mcp_apps_serving=views, preview_serving=preview,
        ))

    monkeypatch.setattr(deps_module, "_deps", None)
    return SimpleNamespace(client=TestClient(server_mod.create_app()), serving=serving)


class TestTheDocument:
    def test_with_both_off_the_path_is_404_under_the_policy(self, main):
        main.serving()
        r = main.client.get(SANDBOX_PATH)
        assert r.status_code == 404
        assert PROXY not in r.content
        assert r.headers["content-security-policy"] == build_csp_header()

    def test_with_the_preview_on_the_document_is_served_with_no_origin_allowed(self, main):
        main.serving(preview=True)
        r = main.client.get(SANDBOX_PATH)
        assert (r.status_code, r.content) == (200, PROXY)
        assert r.headers["content-security-policy"] == build_csp_header()
        assert "set-cookie" not in r.headers

    def test_with_the_preview_alone_a_declared_source_does_not_count(self, main):
        """A preview declares nothing. The ``csp`` parameter is for the views
        of MCP Apps, and they are off."""
        main.serving(preview=True)
        csp = {"connectDomains": ["https://api.example.com"], "resourceDomains": ["https://cdn.example.com"]}
        r = main.client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        assert r.headers["content-security-policy"] == build_csp_header()
        assert "example.com" not in r.headers["content-security-policy"]

    def test_with_views_on_a_declared_source_counts_as_today(self, main):
        main.serving(views=True, preview=True)
        csp = {"connectDomains": ["https://api.example.com"]}
        r = main.client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        assert "connect-src https://api.example.com" in r.headers["content-security-policy"]

    @pytest.mark.parametrize("views,preview,framed", [
        (False, False, False), (True, False, True), (False, True, True), (True, True, True),
    ])
    def test_the_web_ui_refuses_every_frame_while_the_document_is_served(self, main, views, preview, framed):
        main.serving(views=views, preview=preview)
        r = main.client.get("/chat")
        assert r.status_code == 200
        assert (r.headers.get("content-security-policy") == "frame-ancestors 'none'") is framed

    def test_the_mcp_apps_config_stays_off_with_the_preview_on(self, main):
        from nerve.gateway.auth import require_auth

        main.serving(preview=True)
        main.client.app.dependency_overrides[require_auth] = lambda: None
        assert main.client.get("/api/mcp-apps/config").json() == {"enabled": False}


class TestRegistration:
    """With the switch off no preview route exists, as on a Nerve without the feature."""

    @staticmethod
    def _answers(config: NerveConfig, monkeypatch) -> list:
        import nerve.config as cfgmod
        from nerve.gateway.auth import require_auth
        from nerve.gateway.routes import register_all_routes

        monkeypatch.setattr(cfgmod, "_config", config, raising=False)
        app = FastAPI()
        app.include_router(register_all_routes())
        app.dependency_overrides[require_auth] = lambda: None
        client = TestClient(app)
        return [client.get(path).json() for path in ("/api/preview/bundle", "/api/preview/file")]

    def test_off_registers_no_preview_route(self, monkeypatch):
        assert self._answers(_config(preview=False), monkeypatch) == [{"detail": "Not Found"}] * 2

    def test_on_registers_the_bundle_and_file_routes(self, monkeypatch):
        """Registered, and off until the gateway has decided at startup that it serves previews."""
        answers = self._answers(_config(), monkeypatch)
        assert [a["detail"]["code"] for a in answers] == ["preview_disabled"] * 2


# --------------------------------------------------------------------------- #
#  The routes                                                                  #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def api(tmp_path, bypass_auth, monkeypatch):
    import nerve.config as cfgmod
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes.preview import router

    workspace = tmp_path / "ws"
    deck = workspace / "decks" / "demo"
    deck.mkdir(parents=True)
    (deck / "index.html").write_text("<!doctype html><script src=app.js></script>")
    (deck / "app.js").write_text("console.log(1)")
    (deck / ".secret").write_text("left out")
    (workspace / "notes.txt").write_text("not a bundle")
    (workspace / "page.html").write_text("<p>one page</p>")
    config = NerveConfig.from_dict({"workspace": str(workspace), "artifacts": {"preview": True}})
    monkeypatch.setattr(cfgmod, "_config", config, raising=False)
    deps = deps_module.RouteDeps(engine=SimpleNamespace(config=config), db=None, preview_serving=True)
    monkeypatch.setattr(deps_module, "_deps", deps)

    app = FastAPI()
    app.include_router(router)
    bypass_auth(app)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test") as http:
        yield SimpleNamespace(http=http, deps=deps, app=app, workspace=workspace)


BUNDLE = "/api/preview/bundle"
FILE = "/api/preview/file"


@pytest.mark.asyncio
class TestRoutes:
    async def test_the_description_of_a_bundle(self, api):
        r = await api.http.get(BUNDLE, params={"path": "decks/demo/index.html"})
        assert r.status_code == 200
        body = r.json()
        assert body["path"] == "decks/demo/index.html"
        assert body["root"] == "decks/demo"
        assert body["root_path"] == str((api.workspace / "decks" / "demo").resolve())
        assert body["kind"] == "folder"
        assert body["entry"] == "index.html"
        deck = api.workspace / "decks" / "demo"
        sizes = {name: (deck / name).stat().st_size for name in ("app.js", "index.html")}
        assert body["files"] == [
            {"path": "app.js", "size": sizes["app.js"], "type": "text/javascript"},
            {"path": "index.html", "size": sizes["index.html"], "type": "text/html"},
        ]
        assert body["size"] == sum(sizes.values())
        assert len(body["fingerprint"]) == 64
        assert isinstance(body["modified"], int)

    async def test_a_refusal_has_a_code_and_a_message(self, api):
        r = await api.http.get(BUNDLE, params={"path": "notes.txt"})
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "unsupported"
        assert r.json()["detail"]["message"]
        r = await api.http.get(BUNDLE, params={"path": "../elsewhere"})
        assert (r.status_code, r.json()["detail"]["code"]) == (403, "outside_workspace")
        r = await api.http.get(BUNDLE, params={"path": "missing.html"})
        assert (r.status_code, r.json()["detail"]["code"]) == (404, "not_found")

    async def test_a_file_of_the_bundle(self, api):
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": "app.js"})
        assert r.status_code == 200
        assert r.content == b"console.log(1)"
        assert r.headers["content-type"].startswith("text/javascript")
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["content-disposition"] == "attachment"
        assert r.headers["content-security-policy"].startswith("sandbox;")
        assert r.headers["cache-control"] == "no-store"

    async def test_the_entry_is_never_a_page_of_this_address(self, api):
        r = await api.http.get(FILE, params={"path": "page.html", "file": "page.html"})
        assert r.status_code == 200
        assert r.headers["content-disposition"] == "attachment"
        assert r.headers["content-security-policy"] == "sandbox; default-src 'none'"

    @pytest.mark.parametrize("file", [".secret", "../../notes.txt", "../page.html", "/etc/hosts", "", "missing.js"])
    async def test_only_files_of_the_description(self, api, file):
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": file})
        assert (r.status_code, r.json()["detail"]["code"]) == (404, "not_in_bundle")

    async def test_a_page_bundle_has_only_its_one_file(self, api):
        (api.workspace / "other.css").write_text("x")
        r = await api.http.get(FILE, params={"path": "page.html", "file": "other.css"})
        assert r.status_code == 404

    async def test_a_file_above_the_preview_limit(self, api, monkeypatch):
        from nerve.gateway.routes import preview

        monkeypatch.setattr(preview, "PREVIEW_MAX_BYTES", 4)
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": "app.js"})
        assert (r.status_code, r.json()["detail"]["code"]) == (413, "too_large")

    async def test_while_off_the_routes_do_not_exist(self, api):
        api.deps.preview_serving = False
        for r in (
            await api.http.get(BUNDLE, params={"path": "decks/demo"}),
            await api.http.get(FILE, params={"path": "decks/demo", "file": "app.js"}),
        ):
            assert (r.status_code, r.json()["detail"]["code"]) == (404, "preview_disabled")

    async def test_every_route_needs_authentication(self, api):
        from nerve.gateway.auth import pin_jwt_secret, require_auth

        pin_jwt_secret("a-signing-secret-for-this-test")
        api.app.dependency_overrides.pop(require_auth)
        for r in (
            await api.http.get(BUNDLE, params={"path": "decks/demo"}),
            await api.http.get(FILE, params={"path": "decks/demo", "file": "app.js"}),
        ):
            assert r.status_code == 401


@pytest.mark.asyncio
class TestDescriptionCache:
    async def test_file_reads_of_one_description_do_not_walk_the_bundle_again(self, api, monkeypatch):
        from nerve.gateway.routes import preview

        monkeypatch.setattr(preview, "descriptions", preview.DescriptionCache())
        walks: list[str] = []
        real = preview.describe_bundle

        def counted(workspace, path):
            walks.append(path)
            return real(workspace, path)

        monkeypatch.setattr(preview, "describe_bundle", counted)
        body = (await api.http.get(BUNDLE, params={"path": "decks/demo"})).json()
        for name in ("index.html", "app.js", "app.js"):
            r = await api.http.get(FILE, params={"path": "decks/demo", "file": name, "fingerprint": body["fingerprint"]})
            assert r.status_code == 200
        assert walks == ["decks/demo"]

    async def test_another_fingerprint_describes_the_bundle_again(self, api, monkeypatch):
        from nerve.gateway.routes import preview

        monkeypatch.setattr(preview, "descriptions", preview.DescriptionCache())
        await api.http.get(BUNDLE, params={"path": "decks/demo"})
        (api.workspace / "decks" / "demo" / "new.js").write_text("2")
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": "new.js", "fingerprint": "0" * 64})
        assert (r.status_code, r.content) == (200, b"2")
        # Without a fingerprint, too.
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": "new.js"})
        assert r.status_code == 200

    async def test_a_kept_description_still_opens_without_following_a_link(self, api, monkeypatch, tmp_path):
        from nerve.gateway.routes import preview

        monkeypatch.setattr(preview, "descriptions", preview.DescriptionCache())
        body = (await api.http.get(BUNDLE, params={"path": "decks/demo"})).json()
        (tmp_path / "secret.js").write_text("outside")
        app_js = api.workspace / "decks" / "demo" / "app.js"
        app_js.unlink()
        app_js.symlink_to(tmp_path / "secret.js")
        r = await api.http.get(FILE, params={"path": "decks/demo", "file": "app.js", "fingerprint": body["fingerprint"]})
        assert r.status_code == 404
        assert b"outside" not in r.content

    def test_the_cache_is_bounded_and_ages(self, monkeypatch):
        from nerve.artifacts.bundle import Bundle, BundleKind
        from nerve.gateway.routes import preview

        cache = preview.DescriptionCache(size=2, max_age=10)
        def bundle(name):
            return Bundle(workspace=Path("/ws"), path=name, root="", kind=BundleKind.PAGE, entry=name, files=())
        for name in ("a", "b", "c"):
            cache.put("/ws", name, bundle(name))
        assert cache.get("/ws", "a", bundle("a").fingerprint) is None
        assert cache.get("/ws", "c", bundle("c").fingerprint) is not None
        assert cache.get("/ws", "c", "other") is None
        now = preview.time.monotonic()
        monkeypatch.setattr(preview.time, "monotonic", lambda: now + 11)
        assert cache.get("/ws", "c", bundle("c").fingerprint) is None
