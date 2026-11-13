"""The MCP Apps sandbox document (``nerve/gateway/mcp_apps_sandbox.py``).

Third-party HTML runs in it, at a path of the gateway's own address. So these
pin four things: its policy takes the document's origin away and names no
source on Nerve's own host, nor ``'self'``, which for a document without an
origin is that host; the policy cannot be widened by what a resource
declares; no route of the gateway answers for the document without the
policy; and the documents of the web UI refuse every frame while a view
could hold them.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.routing import Route

from nerve.config import NerveConfig
from nerve.gateway.mcp_apps_sandbox import (
    MAX_CSP_PARAM_CHARS,
    SANDBOX_DOCUMENT,
    SANDBOX_PATH,
    SandboxDocument,
    _csp_param,
    _host_of,
    _host_of_url,
    build_csp_header,
    sanitize_csp_domains,
    serves_sandbox,
)

PROXY = b"<!doctype html><title>sandbox proxy</title><script>/* relay */</script>"

DEFAULT_POLICY = (
    "sandbox allow-scripts allow-forms; "
    "default-src 'none'; "
    "script-src 'unsafe-inline' 'unsafe-eval' blob: data:; "
    "style-src 'unsafe-inline' blob: data:; "
    "img-src data: blob:; "
    "font-src data: blob:; "
    "media-src data: blob:; "
    "connect-src 'none'; "
    "worker-src blob:; "
    "frame-src 'none'; "
    "object-src 'none'; "
    "base-uri 'none'; "
    "form-action 'none'; "
    "frame-ancestors 'self'"
)

METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]


def _directives(policy: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for directive in policy.split("; "):
        name, *sources = directive.split(" ")
        assert name not in out, f"{name} twice in {policy!r}"
        out[name] = sources
    return out


def _sandboxes(policy: str | None) -> bool:
    """True for a policy that takes a document's origin away and lets only a
    page of the same address hold it in a frame."""
    if not policy:
        return False
    directives = _directives(policy)
    return (
        policy.startswith("sandbox ")
        and directives["sandbox"] == ["allow-scripts", "allow-forms"]
        and directives["frame-ancestors"] == ["'self'"]
    )


# --------------------------------------------------------------------------- #
#  Content-Security-Policy                                                     #
# --------------------------------------------------------------------------- #


class TestCspHeader:
    @pytest.mark.parametrize("csp", [None, {}, "x", 3, [], ["https://example.com"], {"other": 1}])
    def test_without_a_usable_csp_no_other_origin_is_allowed(self, csp):
        assert build_csp_header(csp) == DEFAULT_POLICY

    def test_declared_domains_go_where_the_reference_host_puts_them(self):
        policy = _directives(build_csp_header({
            "connectDomains": ["https://api.example.com", "wss://live.example.com"],
            "resourceDomains": ["https://cdn.example.com", "https://*.assets.example.com"],
            "frameDomains": ["https://player.example.com"],
            "baseUriDomains": ["https://base.example.com"],
        }))
        resources = ["https://cdn.example.com", "https://*.assets.example.com"]
        assert policy == {
            "sandbox": ["allow-scripts", "allow-forms"],
            "default-src": ["'none'"],
            "script-src": ["'unsafe-inline'", "'unsafe-eval'", "blob:", "data:", *resources],
            "style-src": ["'unsafe-inline'", "blob:", "data:", *resources],
            "img-src": ["data:", "blob:", *resources],
            "font-src": ["data:", "blob:", *resources],
            "media-src": ["data:", "blob:", *resources],
            "connect-src": ["https://api.example.com", "wss://live.example.com"],
            "worker-src": ["blob:", *resources],
            "frame-src": ["https://player.example.com"],
            "object-src": ["'none'"],
            "base-uri": ["https://base.example.com"],
            # A form that is sent loads its answer into a frame of the view.
            "form-action": ["https://player.example.com"],
            "frame-ancestors": ["'self'"],
        }

    def test_the_document_has_no_origin_and_only_its_own_address_may_frame_it(self):
        """``sandbox`` without ``allow-same-origin``: the browser gives the
        document an opaque origin. Without top navigation, popups, downloads
        or dialogs either."""
        for csp in (None, {"connectDomains": ["https://api.example.com"]}):
            policy = build_csp_header(csp)
            assert policy.startswith("sandbox allow-scripts allow-forms; ")
            assert policy.endswith("; frame-ancestors 'self'")
            assert "allow-same-origin" not in policy
            for token in ("allow-top-navigation", "allow-popups", "allow-modals", "allow-downloads"):
                assert token not in policy

    def test_self_is_in_no_directive_that_loads_or_navigates(self):
        """For a document without an origin ``'self'`` is the address it was
        loaded from: the gateway, with the web UI and the API. Only
        ``frame-ancestors`` names it, where it says who may hold the frame."""
        full = {
            "connectDomains": ["https://api.example.com"], "resourceDomains": ["https://cdn.example.com"],
            "frameDomains": ["https://player.example.com"], "baseUriDomains": ["https://base.example.com"],
        }
        for csp in (None, full):
            for name, sources in _directives(build_csp_header(csp)).items():
                assert ("'self'" in sources) == (name == "frame-ancestors"), name

    def test_connect_domains_do_not_load_resources_and_the_reverse(self):
        policy = _directives(build_csp_header({"connectDomains": ["https://api.example.com"]}))
        assert "https://api.example.com" not in policy["script-src"]
        policy = _directives(build_csp_header({"resourceDomains": ["https://cdn.example.com"]}))
        assert policy["connect-src"] == ["'none'"]

    @pytest.mark.parametrize("entry", [
        "https://example.com",
        "http://localhost:3000",
        "http://127.0.0.1:8080",
        "https://*.example.com",
        "https://example.com:*",
        "wss://live.example.com/socket",
        "https://cdn.example.com/lib/v1.2/",
    ])
    def test_an_origin_is_kept(self, entry):
        assert sanitize_csp_domains([entry]) == [entry]

    @pytest.mark.parametrize("entry", [
        # A quote adds a keyword.
        "'unsafe-eval'", "'self'", "https://example.com 'unsafe-eval'", '"https://example.com"',
        # A semicolon or a comma starts a directive or a policy.
        "https://example.com; script-src *", "https://example.com;", "https://a.example,https://b.example",
        # A space adds a source, a line break a header line.
        "https://a.example https://b.example", " https://example.com", "https://example.com ",
        "https://example.com\nSet-Cookie: x=1", "https://example.com\r\nX-Injected: 1",
        "https://example.com\tdata:",
        # Every host.
        "*", "*:*", "https://*", "http://*:*", "https:", "https://", "wss:", "*.com", "https://*.com",
        "https://*.*", "https://*.*.example.com",
        # Other schemes and shapes.
        "data:", "blob:", "javascript:alert(1)", "filesystem:", "file:///etc/hosts", "ftp://example.com",
        "example.com", "//example.com", "https://example..com", "https://.example.com",
        "https://example.com.", "https://user:pw@example.com", "https://[::1]",
        "https://example.com/a b", "https://example.com/'x'", "https://example.com/<script>",
        "", "https://" + "a" * 300 + ".example.com",
    ])
    def test_anything_else_is_dropped(self, entry):
        assert sanitize_csp_domains([entry]) == []
        fields = ("connectDomains", "resourceDomains", "frameDomains", "baseUriDomains")
        assert build_csp_header({field: [entry] for field in fields}) == DEFAULT_POLICY

    def test_entries_that_are_not_strings_are_dropped(self):
        domains = [None, 3, True, ["https://example.com"], {"a": 1}, "https://ok.example.com"]
        assert sanitize_csp_domains(domains) == ["https://ok.example.com"]

    @pytest.mark.parametrize("domains", [None, "https://example.com", {"0": "https://example.com"}, 7])
    def test_a_domain_list_that_is_not_a_list_is_empty(self, domains):
        assert sanitize_csp_domains(domains) == []

    def test_a_good_entry_survives_next_to_hostile_ones(self):
        policy = build_csp_header({"connectDomains": [
            "https://api.example.com", "*", "https://x.example; default-src *", "https://api.example.com",
        ]})
        assert _directives(policy)["connect-src"] == ["https://api.example.com"]

    def test_the_header_is_one_line_of_known_directives(self):
        hostile = ["x\r\ny", "a;b", "'unsafe-inline'", "*"]
        policy = build_csp_header({
            "connectDomains": hostile, "resourceDomains": hostile,
            "frameDomains": hostile, "baseUriDomains": hostile,
        })
        assert "\n" not in policy and "\r" not in policy
        assert list(_directives(policy)) == list(_directives(DEFAULT_POLICY))


class TestOwnHost:
    """No source of a view's policy is on Nerve's own host: the web UI, the
    API (the sign-in route with it) and the sandbox document are not for a
    view to frame or to call."""

    @pytest.mark.parametrize("entry", [
        "https://nerve.example.org:8900",       # the web UI, the API and the sandbox document
        "https://nerve.example.org:8901",       # another port of the host
        "https://nerve.example.org",            # the default port
        "https://nerve.example.org:*",
        "http://nerve.example.org:8900",
        "ws://nerve.example.org:8900",
        "wss://nerve.example.org:8900",
        "https://NERVE.Example.org:8900",
        "https://nerve.example.org:8900/api/",
        "https://*.example.org:8900",           # a pattern that covers it
    ])
    def test_a_source_on_the_host_is_dropped_whatever_its_scheme_port_and_path(self, entry):
        assert sanitize_csp_domains([entry], ["nerve.example.org"]) == []
        # Without a host to compare with, it is an origin like any other.
        assert sanitize_csp_domains([entry]) == [entry]

    def test_other_hosts_stay(self):
        others = [
            "https://api.example.com", "https://nerve.example.org.example.com",
            "https://xnerve.example.org", "https://*.nerve.example.org", "https://example.org",
        ]
        assert sanitize_csp_domains(others, ["nerve.example.org"]) == others

    @pytest.mark.parametrize("own", ["localhost", "127.0.0.1", "0.0.0.0", "::1", "chat.localhost"])
    @pytest.mark.parametrize("entry", [
        "https://localhost:8900", "ws://localhost", "http://127.0.0.1:8900", "https://127.0.0.2",
        "http://0.0.0.0:8900", "https://chat.localhost:8900", "https://*.localhost:8900",
    ])
    def test_on_a_loopback_address_every_loopback_name_is_the_host(self, own, entry):
        """``localhost`` and ``127.0.0.1`` are two origins for a browser and
        one gateway."""
        assert sanitize_csp_domains([entry], [own]) == []

    def test_a_loopback_name_is_another_host_for_a_gateway_on_a_public_address(self):
        assert sanitize_csp_domains(
            ["http://localhost:3000"], ["nerve.example.org", "203.0.113.7"],
        ) == ["http://localhost:3000"]

    def test_every_directive_drops_it(self):
        fields = ("connectDomains", "resourceDomains", "frameDomains", "baseUriDomains")
        csp = {f: ["https://nerve.example.org:8900", "https://ok.example.com"] for f in fields}
        header = build_csp_header(csp, ["nerve.example.org"])
        assert "nerve.example.org" not in header
        policy = _directives(header)
        assert policy["connect-src"] == ["https://ok.example.com"]
        assert policy["frame-src"] == ["https://ok.example.com"]
        assert policy["form-action"] == ["https://ok.example.com"]
        assert policy["base-uri"] == ["https://ok.example.com"]
        assert policy["img-src"] == ["data:", "blob:", "https://ok.example.com"]

    def test_a_view_that_declares_only_nerves_host_gets_the_default_policy(self):
        csp = {
            "connectDomains": ["https://nerve.example.org:8900"],
            "frameDomains": ["https://nerve.example.org:8901", "ws://nerve.example.org:8900"],
        }
        assert build_csp_header(csp, ["nerve.example.org"]) == DEFAULT_POLICY

    @pytest.mark.parametrize(("header", "host"), [
        ("nerve.example.org:8901", "nerve.example.org"), ("Nerve.Example.org", "nerve.example.org"),
        ("127.0.0.1:8901", "127.0.0.1"), ("[::1]:8901", "::1"), ("[::1]", "::1"), ("", ""),
    ])
    def test_the_host_of_a_host_header(self, header, host):
        assert _host_of(header) == host

    @pytest.mark.parametrize(("url", "host"), [
        ("https://nerve.example.org:8900/", "nerve.example.org"), ("https://Nerve.Example.org", "nerve.example.org"),
        ("http://127.0.0.1:8900/chat/s-1", "127.0.0.1"), ("https://[::1]:8900/", "::1"),
        ("", ""), ("not a url", ""), ("https://", ""), ("https://[", ""),
    ])
    def test_the_host_of_an_address(self, url, host):
        assert _host_of_url(url) == host


class TestCspParam:
    def test_the_json_of_the_parameter(self):
        csp = {"connectDomains": ["https://api.example.com"]}
        assert _csp_param(f"csp={quote(json.dumps(csp))}&x=1".encode()) == csp

    @pytest.mark.parametrize("query", [
        b"", b"other=1", b"csp=", b"csp=%7Bnot+json", b"csp=%ff%fe",
        b"csp=" + b"[" * 5000,
    ])
    def test_a_missing_or_unreadable_parameter_is_no_csp(self, query):
        assert build_csp_header(_csp_param(query)) == DEFAULT_POLICY

    def test_a_parameter_that_is_too_long_is_not_read(self):
        domains = [f"https://h{i}.example.com" for i in range(1000)]
        query = f"csp={quote(json.dumps({'connectDomains': domains}))}".encode()
        assert len(json.dumps({"connectDomains": domains})) > MAX_CSP_PARAM_CHARS
        assert _csp_param(query) is None


# --------------------------------------------------------------------------- #
#  The document, as the gateway answers for it                                 #
# --------------------------------------------------------------------------- #


@pytest.fixture
def document(tmp_path):
    path = tmp_path / "dist" / SANDBOX_DOCUMENT
    path.parent.mkdir()
    path.write_bytes(PROXY)
    # What the web UI serves next to it.
    (path.parent / "index.html").write_text("<!doctype html><title>the web UI</title>")
    (path.parent / "assets").mkdir()
    (path.parent / "assets" / "app.js").write_text("console.log('app')")
    return path


def _app(document, *, serving: bool = True, host: str = ""):
    """The document on an app of its own, at its path, for every method."""
    sandbox = SandboxDocument(document, host)

    async def endpoint(request):
        return await sandbox.respond(request, serving=serving)

    return Starlette(routes=[Route(SANDBOX_PATH, endpoint, methods=METHODS)])


@pytest.fixture
def client(document):
    return TestClient(_app(document))


class TestDocument:
    def test_the_document_and_its_headers(self, client):
        r = client.get(SANDBOX_PATH)
        assert r.status_code == 200
        assert r.content == PROXY
        assert r.headers["content-type"] == "text/html; charset=utf-8"
        assert r.headers["content-security-policy"] == DEFAULT_POLICY
        assert r.headers["cache-control"] == "no-store"
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["referrer-policy"] == "no-referrer"
        assert "set-cookie" not in r.headers

    def test_the_policy_follows_the_csp_parameter(self, client):
        csp = {
            "connectDomains": ["https://api.example.com", "*"],
            "resourceDomains": ["https://cdn.example.com", "'unsafe-eval'"],
        }
        r = client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        assert r.status_code == 200
        policy = _directives(r.headers["content-security-policy"])
        assert policy["connect-src"] == ["https://api.example.com"]
        assert policy["img-src"] == ["data:", "blob:", "https://cdn.example.com"]
        assert _sandboxes(r.headers["content-security-policy"])

    def test_no_parameter_takes_the_sandbox_or_the_framing_rule_out(self, client):
        """The query sets the sources of the fetch directives and nothing
        else: not the first directive, not the last."""
        for query in (
            "csp=%7B%22sandbox%22%3A%5B%22allow-same-origin%22%5D%7D",
            "csp=%7B%22frameAncestors%22%3A%5B%22https%3A%2F%2Fevil.example%22%5D%7D",
            "sandbox=allow-same-origin&frame-ancestors=*",
            "csp=%7B%22connectDomains%22%3A%5B%22x%3B%20sandbox%20allow-same-origin%22%5D%7D",
            "csp=%7B%22connectDomains%22%3A%5B%22https%3A%2F%2Fa.example%3B%20frame-ancestors%20*%22%5D%7D",
        ):
            r = client.get(f"{SANDBOX_PATH}?{query}")
            assert r.headers["content-security-policy"] == DEFAULT_POLICY, query

    def test_the_policy_names_no_source_on_the_host_the_browser_asked_for(self, document):
        """The sandbox document is a page of the chat's own host: a view that
        declares that host could ask the web UI and the API for things, or
        hold the web UI in a frame."""
        client = TestClient(_app(document), base_url="https://nerve.example.org:8900")
        csp = {
            "frameDomains": [
                "https://nerve.example.org:8900", "https://nerve.example.org:8901",
                "ws://nerve.example.org:8900", "https://player.example.com",
            ],
            "connectDomains": ["https://nerve.example.org:8900", "wss://nerve.example.org:8900"],
            "resourceDomains": ["https://nerve.example.org:8900"],
        }
        r = client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        policy = _directives(r.headers["content-security-policy"])
        assert policy["frame-src"] == ["https://player.example.com"]
        assert policy["form-action"] == ["https://player.example.com"]
        assert policy["connect-src"] == ["'none'"]
        assert policy["img-src"] == ["data:", "blob:"]
        assert "nerve.example.org" not in r.headers["content-security-policy"]

    def test_nor_on_the_address_the_gateway_is_bound_to(self, document):
        client = TestClient(_app(document, host="127.0.0.1"), base_url="https://nerve.example.org:8900")
        csp = {"connectDomains": ["https://127.0.0.1:8900", "http://localhost:8900", "https://api.example.com"]}
        r = client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        assert _directives(r.headers["content-security-policy"])["connect-src"] == ["https://api.example.com"]

    def test_nor_on_the_host_a_proxy_in_front_was_asked_for(self, document):
        """Behind a proxy that rewrites ``Host``, the name the browser used is
        in ``X-Forwarded-Host``, and in the ``Referer`` that the web UI sends
        with the frame's request (its origin)."""
        client = TestClient(_app(document), base_url="http://192.0.2.5:8900")
        csp = {"connectDomains": [
            "https://agent.example.net", "https://front.example.net:8443", "https://192.0.2.5:8900",
            "https://api.example.com",
        ]}
        params = {"csp": json.dumps(csp)}
        plain = client.get(SANDBOX_PATH, params=params)
        assert _directives(plain.headers["content-security-policy"])["connect-src"] == [
            "https://agent.example.net", "https://front.example.net:8443", "https://api.example.com",
        ]
        forwarded = client.get(SANDBOX_PATH, params=params, headers={
            "X-Forwarded-Host": "front.example.net:8443, inner.example.net",
            "Referer": "https://agent.example.net/",
        })
        assert _directives(forwarded.headers["content-security-policy"])["connect-src"] == [
            "https://api.example.com",
        ]

    def test_what_a_request_says_of_itself_only_takes_sources_away(self, document):
        """A request can name any host as its own. The worst it gets is a
        policy with fewer sources."""
        client = TestClient(_app(document), base_url="https://nerve.example.org:8900")
        params = {"csp": json.dumps({"connectDomains": ["https://api.example.com", "https://cdn.example.com"]})}
        r = client.get(SANDBOX_PATH, params=params, headers={
            "X-Forwarded-Host": "api.example.com", "Referer": "https://cdn.example.com/x?y",
        })
        assert r.headers["content-security-policy"] == DEFAULT_POLICY

    @pytest.mark.parametrize("raw", ["not json", "[1]", '"x"', "{"])
    def test_a_csp_parameter_of_the_wrong_shape_gets_the_default_policy(self, client, raw):
        r = client.get(SANDBOX_PATH, params={"csp": raw})
        assert (r.status_code, r.headers["content-security-policy"]) == (200, DEFAULT_POLICY)

    def test_head(self, client):
        r = client.head(SANDBOX_PATH)
        assert r.status_code == 200
        assert r.content == b""
        assert r.headers["content-security-policy"] == DEFAULT_POLICY

    @pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def test_other_methods_are_refused_under_the_policy(self, client, method):
        r = client.request(method, SANDBOX_PATH)
        assert r.status_code == 405
        assert r.headers["allow"] == "GET, HEAD"
        assert PROXY not in r.content
        assert r.headers["content-security-policy"] == DEFAULT_POLICY

    @pytest.mark.parametrize("method", METHODS)
    def test_with_the_feature_off_the_path_is_404_under_the_policy(self, document, method):
        r = TestClient(_app(document, serving=False)).request(method, SANDBOX_PATH)
        assert r.status_code == 404
        assert PROXY not in r.content
        assert r.headers["content-security-policy"] == DEFAULT_POLICY
        assert r.headers["x-content-type-options"] == "nosniff"

    def test_a_missing_document_is_503_and_says_how_to_build_it(self, tmp_path, caplog):
        client = TestClient(_app(tmp_path / "dist" / SANDBOX_DOCUMENT))
        with caplog.at_level(logging.DEBUG, logger="nerve.gateway.mcp_apps_sandbox"):
            for _ in range(3):
                r = client.get(SANDBOX_PATH)
                assert r.status_code == 503
                assert r.headers["content-security-policy"] == DEFAULT_POLICY
        warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "npm run build" in warnings[0].getMessage()

    def test_a_rebuilt_document_is_served_without_a_restart(self, document, client):
        assert client.get(SANDBOX_PATH).content == PROXY
        document.write_bytes(b"<!doctype html><title>rebuilt</title>")
        r = client.get(SANDBOX_PATH)
        assert r.content == b"<!doctype html><title>rebuilt</title>"
        assert r.headers["content-security-policy"] == DEFAULT_POLICY


# --------------------------------------------------------------------------- #
#  Whether the gateway serves it                                               #
# --------------------------------------------------------------------------- #


def _config(*, enabled: bool = True) -> NerveConfig:
    return NerveConfig.from_dict({"mcp_apps": {"enabled": enabled}})


class TestServing:
    def test_off_by_default_and_says_nothing(self, document, caplog):
        with caplog.at_level(logging.INFO, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_sandbox(_config(enabled=False), document.parent) is False
            assert serves_sandbox(NerveConfig.from_dict({}), document.parent) is False
        assert caplog.records == []

    def test_on_with_the_flag(self, document, caplog):
        with caplog.at_level(logging.INFO, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_sandbox(_config(), document.parent) is True
        (record,) = caplog.records
        assert record.levelno == logging.INFO
        assert SANDBOX_PATH in record.getMessage()

    def test_external_mode_leaves_it_off_with_a_warning(self, document, caplog):
        from nerve.gateway.auth import AUTH_MODE_EXTERNAL, pin_auth_mode

        pin_auth_mode(AUTH_MODE_EXTERNAL)
        with caplog.at_level(logging.INFO, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_sandbox(_config(), document.parent) is False
        (record,) = caplog.records
        assert record.levelno == logging.WARNING
        assert "local mode only" in record.getMessage()

    def test_a_missing_document_is_said_at_startup(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="nerve.gateway.mcp_apps_sandbox"):
            assert serves_sandbox(_config(), tmp_path / "no-dist") is True
        assert "npm run build" in caplog.text


# --------------------------------------------------------------------------- #
#  The gateway never answers for the document without the policy               #
# --------------------------------------------------------------------------- #


@pytest.fixture
def main(document, monkeypatch):
    """The gateway's own app over a built web UI that holds the document."""
    import nerve.config as cfgmod
    import nerve.gateway.server as server_mod
    from nerve.gateway.routes import _deps as deps_module

    monkeypatch.setattr(cfgmod, "_config", NerveConfig(), raising=False)
    monkeypatch.setattr(server_mod, "WEB_DIST", document.parent)
    (document.parent / "robots.txt").write_text("User-agent: *\n")
    (document.parent / "sw.js").write_text("self.addEventListener('install', () => {});")
    (document.parent / "about.html").write_text("<!doctype html><title>about</title>")

    def serving(on: bool) -> None:
        monkeypatch.setattr(
            deps_module, "_deps", deps_module.RouteDeps(engine=None, db=None, mcp_apps_serving=on),
        )

    monkeypatch.setattr(deps_module, "_deps", None)
    return SimpleNamespace(
        client=TestClient(server_mod.create_app()), server=server_mod, dist=document.parent, serving=serving,
    )


async def _raw_get(app, path: str) -> tuple[int, dict[str, str], bytes]:
    """``GET path`` as the server hands a request to the app: the path is not
    tidied the way a test client or a browser tidies it."""
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
        "scheme": "http", "path": path, "raw_path": path.encode(), "query_string": b"",
        "headers": [(b"host", b"testserver")], "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80), "root_path": "",
    }
    sent: list[dict] = []
    received = False

    async def receive():
        nonlocal received
        if received:
            return {"type": "http.disconnect"}
        received = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in start.get("headers", [])}
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], headers, body


# Other ways to write the document's path.
SPELLINGS = [
    "/assets/..%2Fmcp-app-sandbox.html",
    "/assets/../mcp-app-sandbox.html",
    "/x/..%2Fmcp-app-sandbox.html",
    "/.%2Fmcp-app-sandbox.html",
    "/./mcp-app-sandbox.html",
    "//mcp-app-sandbox.html",
    "/MCP-APP-SANDBOX.HTML",
    "/Mcp-App-Sandbox.html",
    "/mcp-app-sandbox.html%00",
    "/mcp-app-sandbox.html/",
    "/mcp-app-sandbox.html/.",
    "/mcp-app-sandbox.html;x=1",
    "/%6dcp-app-sandbox.html",
    "/mcp%2Dapp%2Dsandbox.html",
    "/mcp-app-sandbox%2Ehtml",
    "/mcp-app-sandbox.html?csp=%7B%7D",
    "/mcp-app-sandbox.html#x",
    "/index.html/../mcp-app-sandbox.html",
]


class TestTheGatewaysOwnAddress:
    def test_the_route_is_registered_before_the_spa_catch_all(self, main):
        paths = [getattr(r, "path", None) for r in main.client.app.routes]
        assert "/{path:path}" in paths, paths
        assert paths.index(SANDBOX_PATH) < paths.index("/{path:path}")

    def test_with_the_feature_on_the_gateway_serves_the_document_without_an_origin(self, main):
        main.serving(True)
        assert main.client.get("/robots.txt").text == "User-agent: *\n"
        r = main.client.get(SANDBOX_PATH)
        assert (r.status_code, r.content) == (200, PROXY)
        assert r.headers["content-security-policy"] == DEFAULT_POLICY
        assert r.headers["cache-control"] == "no-store"
        assert "set-cookie" not in r.headers
        head = main.client.head(SANDBOX_PATH)
        assert (head.status_code, head.content) == (200, b"")
        assert head.headers["content-security-policy"] == DEFAULT_POLICY

    def test_it_needs_no_token_like_the_files_of_the_web_ui(self, main):
        """A frame's load carries no ``Authorization`` header: the document
        is static and holds nothing."""
        main.serving(True)
        assert main.client.get(SANDBOX_PATH, headers={"Authorization": "Bearer nonsense"}).status_code == 200
        assert main.client.get(SANDBOX_PATH).status_code == 200

    def test_the_policy_follows_the_csp_parameter_and_drops_the_gateways_own_host(self, main):
        main.serving(True)
        csp = {"connectDomains": ["https://api.example.com", "http://testserver", "http://127.0.0.1:8900"]}
        r = main.client.get(SANDBOX_PATH, params={"csp": json.dumps(csp)})
        policy = _directives(r.headers["content-security-policy"])
        # The host the request names and the address the gateway is bound to.
        assert policy["connect-src"] == ["https://api.example.com"]
        assert _sandboxes(r.headers["content-security-policy"])

    @pytest.mark.parametrize("on", [True, False, None])
    @pytest.mark.parametrize("method", METHODS)
    def test_every_answer_for_the_path_carries_the_policy(self, main, on, method):
        """On, off, and before startup; the document, a refusal, a 404."""
        if on is not None:
            main.serving(on)
        r = main.client.request(method, SANDBOX_PATH)
        expected = (200 if method in ("GET", "HEAD") else 405) if on else 404
        assert r.status_code == expected
        assert _sandboxes(r.headers.get("content-security-policy"))
        if not on:
            assert PROXY not in r.content

    @pytest.mark.parametrize("on", [True, False])
    def test_with_the_feature_off_the_path_is_404(self, main, on):
        main.serving(on)
        r = main.client.get(SANDBOX_PATH)
        assert r.status_code == (200 if on else 404)
        assert (PROXY in r.content) == on

    @pytest.mark.parametrize("on", [True, False])
    @pytest.mark.parametrize("method", ["GET", "HEAD", "POST"])
    @pytest.mark.parametrize("url", SPELLINGS)
    def test_no_spelling_of_the_path_gets_the_document_without_the_policy(self, main, on, method, url):
        """Whatever a request calls the document, an answer that holds it
        carries the policy. The static files and the SPA fallback never hold
        it: they answer with something else."""
        main.serving(on)
        r = main.client.request(method, url)
        if PROXY in r.content or b"sandbox proxy" in r.content:
            assert on and _sandboxes(r.headers.get("content-security-policy")), (url, r.status_code)
        if not on:
            assert b"sandbox proxy" not in r.content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("on", [True, False])
    @pytest.mark.parametrize("path", [
        "/assets/../mcp-app-sandbox.html",
        "/assets/%2e%2e/mcp-app-sandbox.html",
        "/x/../mcp-app-sandbox.html",
        "/x/y/../../mcp-app-sandbox.html",
        "/./mcp-app-sandbox.html",
        "//mcp-app-sandbox.html",
        "/mcp-app-sandbox.html/.",
        "/mcp-app-sandbox.html/../mcp-app-sandbox.html",
        "/index.html/../mcp-app-sandbox.html",
        "/../dist/mcp-app-sandbox.html",
        "/MCP-APP-SANDBOX.HTML",
        "/mcp-app-sandbox.html",
    ])
    async def test_nor_a_path_that_reaches_the_app_as_it_was_sent(self, main, on, path):
        """A browser tidies ``..`` and ``.`` away before it asks; another
        client need not, and the server hands the path over as it came."""
        main.serving(on)
        status, headers, body = await _raw_get(main.client.app, path)
        if PROXY in body:
            assert on and _sandboxes(headers.get("content-security-policy")), (path, status)

    def test_the_static_files_and_the_spa_fallback_never_serve_it(self, main):
        """The file is in the bundle (``web/public`` lands in ``web/dist``),
        and is not a bundle file."""
        (main.dist / "alias.html").symlink_to(main.dist / SANDBOX_DOCUMENT)
        (main.dist / "copy.html").hardlink_to(main.dist / SANDBOX_DOCUMENT)
        (main.dist / "assets" / "linked.html").symlink_to(main.dist / SANDBOX_DOCUMENT)
        assert main.server._bundle_file(main.dist, "alias.html") is None
        assert main.server._bundle_file(main.dist, "copy.html") is None
        assert main.server._bundle_file(main.dist, SANDBOX_DOCUMENT) is None
        assert main.server._bundle_file(main.dist, "assets/../" + SANDBOX_DOCUMENT) is None
        assert main.server._bundle_file(main.dist, "robots.txt") is not None
        for on in (True, False):
            main.serving(on)
            for name in ("alias.html", "copy.html"):
                r = main.client.get(f"/{name}")
                assert PROXY not in r.content
                assert b"the web UI" in r.content

    def test_a_link_to_it_under_the_assets_is_served_by_no_one_without_the_policy(self, main):
        """The assets are a static mount of their own, with no word about the
        document. A link to it there must not be a way around the policy."""
        (main.dist / "assets" / "linked.html").symlink_to(main.dist / SANDBOX_DOCUMENT)
        main.serving(True)
        r = main.client.get("/assets/linked.html")
        if PROXY in r.content:
            assert _sandboxes(r.headers.get("content-security-policy"))
        else:
            assert r.status_code == 404

    def test_a_bundle_without_the_document_serves_its_other_files(self, main):
        (main.dist / SANDBOX_DOCUMENT).unlink()
        assert main.server._bundle_file(main.dist, "robots.txt") == (main.dist / "robots.txt").resolve()
        main.serving(False)
        assert main.client.get(SANDBOX_PATH).status_code == 404
        main.serving(True)
        r = main.client.get(SANDBOX_PATH)
        assert r.status_code == 503
        assert _sandboxes(r.headers.get("content-security-policy"))
        assert b"the web UI" not in r.content


# --------------------------------------------------------------------------- #
#  No page may hold the web UI in a frame while a view could                   #
# --------------------------------------------------------------------------- #


class TestWebUiFraming:
    """A view is third-party script in a frame of this address. With the web
    UI in a frame of its own it would have the web UI under its hand."""

    @pytest.mark.parametrize("path", ["/", "/chat", "/chat/s-1234", "/setup/mode", "/about.html"])
    def test_while_the_gateway_serves_the_sandbox_the_documents_of_the_web_ui_refuse_every_frame(self, main, path):
        main.serving(True)
        r = main.client.get(path)
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/html")
        assert r.headers["content-security-policy"] == "frame-ancestors 'none'"

    def test_the_sandbox_document_is_the_one_document_that_may_be_framed_and_only_by_this_address(self, main):
        main.serving(True)
        policy = _directives(main.client.get(SANDBOX_PATH).headers["content-security-policy"])
        assert policy["frame-ancestors"] == ["'self'"]

    @pytest.mark.parametrize("path", ["/sw.js", "/robots.txt", "/assets/app.js"])
    def test_what_is_no_document_gets_no_such_header(self, main, path):
        """The service worker for one: its response headers are the worker's."""
        main.serving(True)
        r = main.client.get(path)
        assert r.status_code == 200
        assert "content-security-policy" not in r.headers

    def test_with_mcp_apps_off_the_web_ui_gets_no_such_header(self, main):
        main.serving(False)
        for path in ("/", "/chat", "/about.html", "/robots.txt"):
            r = main.client.get(path)
            assert r.status_code == 200
            assert "content-security-policy" not in r.headers

    def test_before_startup_too(self, main):
        r = main.client.get("/")
        assert r.status_code == 200 and "content-security-policy" not in r.headers
