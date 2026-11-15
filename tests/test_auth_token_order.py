"""Which token a request is judged by, when it carries more than one.

The web UI sends its session token in the ``Authorization`` header (and as
the ``token`` parameter where a header cannot be set). A ``nerve_token``
cookie is accepted too, but a cookie belongs to a host name, not to a port:
a page on another port of the host can set one. So the cookie is read last.
Read first, a cookie set by such a page would stand in for the valid token
of every request, and the person would be locked out until they delete it by
hand.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from nerve.gateway import auth
from nerve.gateway.auth import authenticate_websocket, get_token_from_request

OWN = "the-token-of-the-web-ui"
PLANTED = "a-cookie-somebody-else-set"


def _request(*, bearer: str | None = None, query: str | None = None, cookie: str | None = None) -> Request:
    headers = []
    if bearer is not None:
        headers.append((b"authorization", f"Bearer {bearer}".encode()))
    if cookie is not None:
        headers.append((b"cookie", f"nerve_token={cookie}".encode()))
    return Request({
        "type": "http", "method": "GET", "path": "/api/sessions",
        "query_string": f"token={query}".encode() if query is not None else b"",
        "headers": headers,
    })


class TestHttp:
    def test_the_header_is_read_before_a_cookie(self):
        assert get_token_from_request(_request(bearer=OWN, cookie=PLANTED)) == OWN

    def test_the_query_parameter_is_read_before_a_cookie(self):
        """An image or a download carries the token in its address."""
        assert get_token_from_request(_request(query=OWN, cookie=PLANTED)) == OWN

    def test_the_header_is_read_before_the_query_parameter(self):
        assert get_token_from_request(_request(bearer=OWN, query="another")) == OWN

    def test_a_cookie_alone_is_a_token(self):
        assert get_token_from_request(_request(cookie=OWN)) == OWN

    def test_an_empty_bearer_is_no_token(self):
        assert get_token_from_request(_request(bearer="", cookie=OWN)) == OWN

    def test_nothing_is_401(self):
        with pytest.raises(HTTPException) as refused:
            get_token_from_request(_request())
        assert refused.value.status_code == 401


class _Socket:
    """The parts of a WebSocket upgrade that carry a token."""

    def __init__(self, token: str | None = None, cookie: str | None = None):
        self.query_params = {"token": token} if token else {}
        self.cookies = {"nerve_token": cookie} if cookie else {}


@pytest.mark.asyncio
class TestWebSocket:
    @pytest.fixture
    def judged(self, monkeypatch):
        """The tokens the upgrade is judged by, in order."""
        seen: list[str] = []

        def decode(token: str, secret: str):
            seen.append(token)
            raise HTTPException(status_code=401, detail="Invalid token")

        monkeypatch.setattr(auth, "effective_jwt_secret", lambda config: "s" * 40)
        monkeypatch.setattr(auth, "is_external_mode", lambda: False)
        monkeypatch.setattr(auth, "decode_token", decode)
        return seen

    async def test_the_query_parameter_is_read_before_a_cookie(self, judged):
        await authenticate_websocket(_Socket(token=OWN, cookie=PLANTED))
        assert judged == [OWN]

    async def test_a_cookie_alone_is_a_token(self, judged):
        await authenticate_websocket(_Socket(cookie=OWN))
        assert judged == [OWN]

    async def test_nothing_is_refused_unjudged(self, judged):
        assert await authenticate_websocket(_Socket()) is None
        assert judged == []
