"""The MCP Apps sandbox document: a page of the gateway's own address that
has no origin.

The view of an MCP App, and a ```widget block of an answer, is third-party
HTML. It runs in a frame whose document is the sandbox proxy
(``web/public/mcp-app-sandbox.html``), and the gateway serves that document
itself, at one fixed path, next to the web UI. What keeps it apart from the
web UI is its Content-Security-Policy, a response header built here:

- ``sandbox allow-scripts allow-forms`` comes first. A browser gives a
  document that is served with it no origin (an opaque one, "null"), in a
  frame and as a page of its own. Such a document cannot read the storage of
  the web UI, where the session token is, nor its cookies, nor its DOM; its
  requests to this address count as another site's; it opens no window and
  navigates no page but itself.
- The fetch directives of the view follow, from the ``csp`` query parameter
  (what the view's resource declared). No source on Nerve's own host is
  among them, and no ``'self'``: in a document without an origin ``'self'``
  is the address it was loaded from, which is the gateway.
- ``frame-ancestors 'self'`` comes last: only a page of this address may hold
  the document in a frame. There ``'self'`` is wanted. A page without an
  origin, the sandbox document itself or a view, does not match it.

The path is never answered without the header: the answers that are not the
document (the feature off, another method, a web UI that is not built) carry
it too, and the web UI's static files never serve the document
(``_bundle_file`` in ``nerve/gateway/server.py``).

See docs/mcp-apps.md.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlsplit

from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

from nerve.gateway.auth import is_external_mode

logger = logging.getLogger(__name__)

SANDBOX_DOCUMENT = "mcp-app-sandbox.html"
SANDBOX_PATH = f"/{SANDBOX_DOCUMENT}"

# What the document may do: run scripts and submit forms. No
# ``allow-same-origin`` (it would have the origin of the web UI), no
# navigation of the page that holds it, no popups, no downloads, no dialogs.
SANDBOX_DIRECTIVE = "sandbox allow-scripts allow-forms"
# Who may hold the document in a frame: a page of the address it is served
# from, the web UI. The browser matches ``'self'`` against the origins of
# the pages around the frame, so a page without an origin is refused.
FRAME_ANCESTORS_DIRECTIVE = "frame-ancestors 'self'"

# A ``csp`` query value longer than this is not read: the restrictive policy
# applies.
MAX_CSP_PARAM_CHARS = 16 * 1024
MAX_CSP_SOURCE_CHARS = 255

# On every answer for the document's path.
_BASE_HEADERS = {
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}

_BUILD_HINT = "build the web UI: cd web && npm ci && npm run build (or nerve upgrade)"

# One source a resource may declare: an http(s) or ws(s) origin, with an
# optional path. The host may start with "*." and the port may be "*".
_CSP_SOURCE = re.compile(
    r"(?:https?|wss?)://"
    r"(?P<host>(?:\*\.)?[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)"
    r"(?::(?:[0-9]{1,5}|\*))?"
    r"(?:/[A-Za-z0-9._~!$&()*+=:@%/-]*)?"
)


# --------------------------------------------------------------------------- #
#  Content-Security-Policy                                                     #
# --------------------------------------------------------------------------- #


def _is_loopback(host: str) -> bool:
    """True for a name of this machine as only this machine reaches it:
    ``localhost`` and its subdomains, a loopback address, the unspecified
    address."""
    host = host.lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return address.is_loopback or address.is_unspecified


def _host_of(header: str) -> str:
    """The host of a ``Host`` header value, without its port."""
    value = header.strip().lower()
    if value.startswith("["):  # an IPv6 literal
        return value[1:value.index("]")] if "]" in value else ""
    return value.rsplit(":", 1)[0] if ":" in value else value


def _host_of_url(url: str) -> str:
    """The host of an address (a ``Referer`` value), or ``""``."""
    try:
        return (urlsplit(url.strip()).hostname or "").lower()
    except ValueError:
        return ""


def names_own_host(source_host: str, own_hosts: Iterable[str]) -> bool:
    """True when the host of a declared source is a name of Nerve's own
    host, or a pattern (``*.name``) that covers one.

    ``own_hosts`` are the names Nerve knows itself by (see
    :func:`own_hosts_of`). When one of them is a loopback name, every
    loopback name is Nerve's own: ``localhost`` and ``127.0.0.1`` are two
    origins for a browser and one gateway.
    """
    host = source_host.lower().rstrip(".")
    own = [name.lower().rstrip(".") for name in own_hosts if name]
    if any(_is_loopback(name) for name in own) and _is_loopback(host.removeprefix("*.")):
        return True
    return any(
        host == name or (host.startswith("*.") and name.endswith(host[1:]))
        for name in own
    )


def sanitize_csp_domains(domains: Any, own_hosts: Iterable[str] = ()) -> list[str]:
    """The entries of a ``csp`` domain list that may go into the header.

    The reference host drops an entry with a quote, a semicolon, a space or a
    line break, because each of them can add a keyword or a directive to the
    policy. This keeps only what reads as an origin, which excludes those too,
    and a source that allows every host: ``*``, a scheme alone, ``*.`` in
    front of a single label (``https://*.com``). A host may narrow what a
    resource declares, never widen it.

    A source on one of ``own_hosts`` (Nerve's own host, see
    :func:`names_own_host`) is dropped too, whatever its scheme and port. The
    sandbox document is served from Nerve's own address, so such a source
    would let a view ask the web UI and the API for things, the sign-in
    route included, and hold the web UI or another sandbox document in a
    frame.
    """
    if not isinstance(domains, list):
        return []
    own = tuple(own_hosts)
    kept: list[str] = []
    for domain in domains:
        if not isinstance(domain, str) or len(domain) > MAX_CSP_SOURCE_CHARS:
            continue
        match = _CSP_SOURCE.fullmatch(domain)
        if match is None:
            continue
        host = match.group("host")
        if ".." in host or (host.startswith("*.") and "." not in host[2:]):
            continue
        if own and names_own_host(host, own):
            continue
        if domain not in kept:
            kept.append(domain)
    return kept


def _sources(*parts: str) -> str:
    """The value of a directive: its sources, or ``'none'`` without any."""
    value = " ".join(part for part in parts if part)
    return value or "'none'"


def build_csp_header(csp: Any = None, own_hosts: Iterable[str] = ()) -> str:
    """The Content-Security-Policy of the sandbox document, from the ``csp``
    object of the view's resource (``_meta.ui.csp``).

    The fetch directives are those of ``buildCspHeader`` of the reference
    host (``examples/basic-host/serve.ts`` of the MCP Apps SDK) without
    ``'self'``: the document has no origin, and for such a document
    ``'self'`` is the address it was loaded from, which here is the gateway
    with its API. Without ``csp``, or with one of the wrong shape, no origin
    is allowed at all. No source on one of ``own_hosts`` is allowed in any
    directive.

    A form may go where a frame may be loaded from: a form that is sent
    loads its answer into a frame of the view.
    """
    csp = csp if isinstance(csp, dict) else {}
    own = tuple(own_hosts)
    resource = " ".join(sanitize_csp_domains(csp.get("resourceDomains"), own))
    connect = " ".join(sanitize_csp_domains(csp.get("connectDomains"), own))
    frame = " ".join(sanitize_csp_domains(csp.get("frameDomains"), own))
    base_uri = " ".join(sanitize_csp_domains(csp.get("baseUriDomains"), own))
    inline, evaluated = "'unsafe-inline'", "'unsafe-eval'"
    directives = [
        SANDBOX_DIRECTIVE,
        "default-src 'none'",
        # Inline scripts and styles: a view is one bundled document.
        "script-src " + _sources(inline, evaluated, "blob:", "data:", resource),
        "style-src " + _sources(inline, "blob:", "data:", resource),
        "img-src " + _sources("data:", "blob:", resource),
        "font-src " + _sources("data:", "blob:", resource),
        "media-src " + _sources("data:", "blob:", resource),
        "connect-src " + _sources(connect),
        "worker-src " + _sources("blob:", resource),
        "frame-src " + _sources(frame),
        "object-src 'none'",
        "base-uri " + _sources(base_uri),
        "form-action " + _sources(frame),
        FRAME_ANCESTORS_DIRECTIVE,
    ]
    return "; ".join(directives)


def _csp_param(query_string: bytes) -> Any:
    """The ``csp`` query parameter as the object it holds, or None."""
    try:
        values = parse_qs(query_string.decode("latin-1")).get("csp")
        if not values or len(values[0]) > MAX_CSP_PARAM_CHARS:
            return None
        return json.loads(values[0])
    except (ValueError, RecursionError):
        return None


def own_hosts_of(request: Request, bind_host: str = "") -> tuple[str, ...]:
    """The names Nerve's own host goes by for this request: the address the
    gateway is bound to, the host the browser asked for (``Host``), the host
    a proxy in front says it was asked for (``X-Forwarded-Host``), and the
    host of the page that holds the frame (``Referer``: the web UI sends its
    origin with the frame's request).

    A name too many only takes sources away from the policy, so what a
    request says about itself is taken as it is.
    """
    headers = request.headers
    names = [bind_host, _host_of(headers.get("host", ""))]
    names += [_host_of(value) for value in headers.get("x-forwarded-host", "").split(",")]
    names.append(_host_of_url(headers.get("referer", "")))
    return tuple(dict.fromkeys(name for name in names if name))


def sandbox_headers(policy: str) -> dict[str, str]:
    """The headers of an answer for the document's path."""
    return {**_BASE_HEADERS, "Content-Security-Policy": policy}


# --------------------------------------------------------------------------- #
#  The document                                                                #
# --------------------------------------------------------------------------- #


class SandboxDocument:
    """The sandbox document as the gateway answers for it.

    ``GET`` (or ``HEAD``) of ``SANDBOX_PATH`` answers the file ``document``
    with the policy for its ``csp`` parameter. The file is read for each
    request, so a rebuilt web UI needs no restart; without it the answer is
    503. Another method is 405, and while the feature is off the path is 404.
    Every one of these answers carries the policy: nothing at this path is
    ever a document with the origin of the web UI.

    ``host`` is the address the gateway is bound to (see
    :func:`own_hosts_of`).
    """

    def __init__(self, document: Path, host: str = "") -> None:
        self.document = Path(document)
        self.host = host
        self._missing_logged = False

    def _refusal(self, text: str, status: int, **headers: str) -> Response:
        """An answer that is not the document, under the policy that allows nothing."""
        return PlainTextResponse(
            text, status_code=status,
            headers={**sandbox_headers(build_csp_header()), **headers},
        )

    async def respond(self, request: Request, *, serving: bool) -> Response:
        if not serving:
            return self._refusal("Not found", 404)
        if request.method not in ("GET", "HEAD"):
            return self._refusal("Method not allowed", 405, Allow="GET, HEAD")
        try:
            body = await asyncio.to_thread(self.document.read_bytes)
        except OSError as e:
            # Once at warning, so that a page full of views does not fill the log.
            (logger.debug if self._missing_logged else logger.warning)(
                "MCP Apps: the sandbox document %s cannot be read (%s); %s",
                self.document, e, _BUILD_HINT,
            )
            self._missing_logged = True
            return self._refusal("The MCP Apps sandbox document is not built", 503)
        self._missing_logged = False
        policy = build_csp_header(
            _csp_param(request.scope.get("query_string") or b""),
            own_hosts=own_hosts_of(request, self.host),
        )
        logger.debug("MCP Apps sandbox document served with policy: %s", policy)
        return Response(body, media_type="text/html", headers=sandbox_headers(policy))


def serves_sandbox(config: Any, web_dist: Path) -> bool:
    """Whether this gateway serves the sandbox document, and with it shows
    the views of MCP Apps and widgets. Decided once, at startup.

    False when ``mcp_apps.enabled`` is off, and in external mode, with a
    warning: behind a gateway that signs in every request the feature is not
    turned on (this is the switch for the backend; the web UI has its own in
    ``web/src/stores/mcpAppsStore.ts``). ``web_dist`` is the built web UI,
    which holds the document: when it is missing the feature is on and the
    log says that no view can load.
    """
    if not config.mcp_apps_active:
        return False
    if is_external_mode():
        logger.warning(
            "MCP Apps stays off: it is for local mode only, and the "
            "authentication mode is external",
        )
        return False
    document = Path(web_dist) / SANDBOX_DOCUMENT
    if not document.is_file():
        logger.warning(
            "MCP Apps: the sandbox document %s is missing, so no view can load; %s",
            document, _BUILD_HINT,
        )
    logger.info("MCP Apps is on: the sandbox document is served at %s", SANDBOX_PATH)
    return True
