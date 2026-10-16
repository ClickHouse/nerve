"""A local Anthropic-API stand-in that answers 429 before it answers 200."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_OK = json.dumps({
    "id": "msg_1", "type": "message", "role": "assistant",
    "model": "claude-haiku-4-5", "content": [{"type": "text", "text": "ok"}],
    "stop_reason": "end_turn", "stop_sequence": None,
    "usage": {"input_tokens": 1, "output_tokens": 1},
}).encode()
_LIMITED = json.dumps({
    "type": "error",
    "error": {"type": "rate_limit_error", "message": "rate limited"},
}).encode()


class RateLimitedUpstream:
    """Local HTTP server answering 429 (no Retry-After) ``failures`` times,
    then 200 -- what Nerve's clients see from the proxy during an upstream
    rate limit. ``failures=math.inf`` never lets the limit lift.

    ``request_times`` holds the ``time.monotonic()`` of each request.
    """

    def __init__(self, failures: float) -> None:
        self.requests = 0
        self.request_times: list[float] = []
        upstream = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                self.rfile.read(int(self.headers.get("content-length", 0)))
                upstream.request_times.append(time.monotonic())
                upstream.requests += 1
                body = _LIMITED if upstream.requests <= failures else _OK
                self.send_response(429 if body is _LIMITED else 200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
