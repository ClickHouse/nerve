"""A recording proxy in front of a model API, for the pinned-client gate.

The agent CLI sends its model requests to this loopback server, which
records each request body and forwards the request unchanged to the real
API, streaming the answer back. The records show what the model saw: the
tools of the request and the earlier conversation. Headers are forwarded
and never recorded, so no credential is kept or printed.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

# Hop-by-hop and length headers that the proxy sets itself.
_SKIP_REQUEST_HEADERS = frozenset({
    "host", "content-length", "connection", "accept-encoding",
    "transfer-encoding", "keep-alive", "upgrade",
})
_SKIP_RESPONSE_HEADERS = frozenset({
    "content-length", "connection", "transfer-encoding", "keep-alive",
    "content-encoding",
})


class ModelApiRecorder:
    """Forwards ``<url>/...`` to ``upstream/...`` and records request bodies."""

    def __init__(self, upstream: str):
        self.upstream = upstream.rstrip("/")
        # (path, parsed JSON body or None), in arrival order.
        self.requests: list[tuple[str, Any]] = []
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def bodies(self, suffix: str) -> list[Any]:
        """The JSON bodies of the requests whose path ends with ``suffix``."""
        with self._lock:
            return [
                body for path, body in self.requests
                if path.split("?", 1)[0].endswith(suffix) and body is not None
            ]

    def start(self) -> "ModelApiRecorder":
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args: Any) -> None:
                pass

            def _forward(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                try:
                    parsed = json.loads(body) if body else None
                except ValueError:
                    parsed = None
                with recorder._lock:
                    recorder.requests.append((self.path, parsed))
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in _SKIP_REQUEST_HEADERS
                }
                headers["Accept-Encoding"] = "identity"
                try:
                    with httpx.Client(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
                        with client.stream(
                            self.command, recorder.upstream + self.path,
                            headers=headers, content=body,
                        ) as response:
                            self.send_response(response.status_code)
                            for key, value in response.headers.items():
                                if key.lower() not in _SKIP_RESPONSE_HEADERS:
                                    self.send_header(key, value)
                            self.end_headers()
                            for chunk in response.iter_bytes():
                                self.wfile.write(chunk)
                                self.wfile.flush()
                except (httpx.HTTPError, OSError) as e:
                    try:
                        self.send_error(502, type(e).__name__)
                    except OSError:
                        pass

            do_GET = do_POST = do_PUT = do_DELETE = _forward  # noqa: N815

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(5)
            self._thread = None
