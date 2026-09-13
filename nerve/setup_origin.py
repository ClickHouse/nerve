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


def same_origin(headers, config) -> tuple[bool, str]:
    """``(ok, why_not)`` for "this request came from a page this instance served".

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
    if origin and origin.lower() != "null":
        if _hostname_of(origin) != _hostname_of(host_header):
            return False, f"it came from {origin}"
    return True, ""
