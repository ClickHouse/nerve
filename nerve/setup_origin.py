"""Whether the *page* making a claim may use the loopback exemption.

A different question from the one :mod:`nerve.setup_token` answers, and the
distinction is the whole point:

* **Locality** is about the machine. The socket peer address says the caller is
  on this box, which is what makes the setup token optional there.
* **Provenance** is about the code. A browser on that box runs whatever page
  the person visited, and a page on ``https://evil.example`` can post to
  ``http://127.0.0.1:8900`` — same peer address, entirely different author.
  Nerve's CORS policy is ``allow_origins=["*"]``, so the browser will not stop
  it either.

That combination means a site somebody visits can claim their unclaimed
instance: choose its username and password, and lock its owner out of it. The
loopback exemption is what makes it worth attempting, so the exemption is what
has to be narrowed: a tokenless claim must also come from a page served by this
instance. With the setup token, provenance stops mattering — whoever holds the
token read it off the machine's own log.

``allow_origins=["*"]`` is tolerable for the rest of the API because every
other endpoint requires a bearer token, which a cross-origin page cannot obtain
or attach: the browser will send cookies with ``allow_credentials``, but Nerve
authenticates on ``Authorization`` and a hostile page has no token to put
there. The claim is the one endpoint that deliberately accepts *no* credential,
which is exactly why it needs this instead.

Two headers decide it, and neither is trusted for *locality* — they cannot
make a remote caller local, only stop a local one from skipping the token:

* ``Origin`` — sent by browsers on every cross-origin request and on same-origin
  POSTs. Present and not this host: refuse the exemption.
* ``Sec-Fetch-Site`` — sent by current browsers on every request. Anything but
  ``same-origin`` / ``none`` refuses the exemption, which also covers a browser
  that omits ``Origin``.

And ``Host`` is checked against the addresses this instance answers on, because
DNS rebinding turns a name the attacker controls into a loopback address: the
peer is loopback, the page is "same-origin" with ``evil.example``, and only the
``Host`` header gives it away.
"""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

from nerve.setup_token import is_loopback_peer

# Host names that always mean "this machine", whatever the bind address is.
_LOCAL_NAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost"})


def _header(headers, name: str) -> str:
    """One header, case-insensitively, from a Starlette headers mapping."""
    try:
        return (headers.get(name) or "").strip()
    except Exception:  # pragma: no cover - defensive; headers are a mapping
        return ""


def _hostname_of(value: str) -> str:
    """The host in an ``Origin`` or ``Host`` value, without port or scheme."""
    if not value:
        return ""
    candidate = value if "//" in value else f"//{value}"
    host = urlsplit(candidate).hostname or ""
    return host.lower()


def _origin_tuple(value: str, *, default_scheme: str = "") -> tuple[str, str, int]:
    """``(scheme, host, effective port)`` — a whole origin, not just its host.

    Comparing hostnames alone makes every port and both schemes on this
    machine one origin, which they are not: a page served by something else on
    ``127.0.0.1:3000`` is a different author from the instance on
    ``127.0.0.1:8900``, and the browser's own rules say so.
    """
    if not value:
        return ("", "", 0)
    candidate = value if "//" in value else f"//{value}"
    parts = urlsplit(candidate)
    scheme = (parts.scheme or default_scheme).lower()
    host = (parts.hostname or "").lower()
    try:
        port = parts.port
    except ValueError:
        return (scheme, host, -1)   # an unparseable port matches nothing
    if port is None:
        port = {"https": 443, "wss": 443, "http": 80, "ws": 80}.get(scheme, 0)
    return (scheme, host, port)


def host_is_this_instance(host_header: str, config) -> bool:
    """Whether ``Host`` names an address this instance actually answers on.

    The DNS-rebinding check. A name that resolves to loopback today is still a
    name somebody else controls, and a page served from it is same-origin with
    itself — so the only thing that distinguishes it from the real instance is
    that Nerve was never asked to answer for that name.

    Accepted: loopback literals, the conventional local names, and the
    configured bind address when it is a specific one. A wildcard bind
    (``0.0.0.0``) says nothing about which names are legitimate, so anything
    that is not local-looking is refused — which is the conservative direction,
    and the token is always the way through.
    """
    host = _hostname_of(host_header)
    if not host:
        # No Host at all is not a browser request; the token decides.
        return False
    if host in _LOCAL_NAMES or is_loopback_peer(host):
        return True
    bind = str(getattr(config.gateway, "host", "") or "").strip().lower()
    if bind and bind not in {"0.0.0.0", "::", "[::]", "*"}:
        try:
            return ipaddress.ip_address(host) == ipaddress.ip_address(bind)
        except ValueError:
            return host == bind
    return False


def same_origin(headers, config, *, scheme: str = "http") -> tuple[bool, str]:
    """``(ok, why_not)`` for "this request came from a page this instance served".

    ``scheme`` is the request's own — an instance behind TLS sees
    ``https://host`` in ``Origin`` and nothing in ``Host`` to say so, and
    guessing ``http`` there would refuse the exemption to the instance's own
    page.

    Conservative by construction: anything that says it came from somewhere
    else, and anything that cannot be read as having come from here, refuses
    the exemption. The caller's remedy is always the same and always available
    — supply the setup token.
    """
    host_header = _header(headers, "host")
    if not host_is_this_instance(host_header, config):
        return False, (
            f"the request was addressed to {host_header or 'no host'}, which is "
            "not an address this instance answers on"
        )

    fetch_site = _header(headers, "sec-fetch-site").lower()
    if fetch_site and fetch_site not in {"same-origin", "none"}:
        return False, f"the browser reported it as a {fetch_site} request"

    origin = _header(headers, "origin")
    if not origin:
        # No Origin at all: not a page (curl, a script on the machine). The
        # peer address is the whole story for those, and Sec-Fetch-Site above
        # has already refused any browser that said otherwise.
        return True, ""
    if origin.strip().lower() == "null":
        # An opaque origin — a sandboxed frame, a `data:` document, some
        # redirects. It names nobody, so it cannot be shown to be this
        # instance, and "cannot be shown" is what the token is for.
        return False, "it came from an opaque origin"

    # The whole tuple. Host alone would make every port and both schemes on
    # this machine one origin, and a page served by something else on
    # 127.0.0.1:3000 is a different author from the instance on :8900.
    target = _origin_tuple(host_header, default_scheme=(scheme or "http").lower())
    source = _origin_tuple(origin, default_scheme=(scheme or "http").lower())
    if source != target:
        return False, f"it came from {origin}"
    return True, ""
