"""Loopback-only ASGI listener for backend-managed Codex MCP clients.

Codex cannot be given a private development CA per MCP server.  Instead of
disabling certificate verification or proxying back through Nerve's public TLS
socket, the gateway serves the existing authenticated MCP ASGI app directly on
an ephemeral plaintext socket bound exclusively to ``127.0.0.1``.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import Iterator
from typing import Any

import uvicorn


class _EmbeddedUvicornServer(uvicorn.Server):
    """Uvicorn without process-wide signal-handler ownership.

    Nerve owns shutdown through :meth:`McpLoopbackServer.close`. Embedded
    servers may overlap in tests and during a future listener handover; the
    default nested signal capture restores stale handlers in that case.
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class McpLoopbackServer:
    """Dedicated plaintext ASGI listener bound only to loopback."""

    def __init__(
        self,
        server: uvicorn.Server,
        task: asyncio.Task[None],
        sock: socket.socket,
    ) -> None:
        self._server = server
        self._task = task
        self._socket = sock

    @classmethod
    async def start(
        cls, app: Any, *, startup_timeout: float = 10.0,
    ) -> "McpLoopbackServer":
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        sock.setblocking(False)
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=0,
            lifespan="off",
            access_log=False,
            log_config=None,
        )
        server = _EmbeddedUvicornServer(config)
        task = asyncio.create_task(
            server.serve(sockets=[sock]), name="nerve-mcp-loopback-asgi",
        )
        deadline = asyncio.get_running_loop().time() + startup_timeout
        while not server.started:
            if task.done():
                await task
                raise RuntimeError("MCP loopback ASGI server exited during startup")
            if asyncio.get_running_loop().time() >= deadline:
                server.should_exit = True
                await asyncio.gather(task, return_exceptions=True)
                raise RuntimeError("MCP loopback ASGI server startup timed out")
            await asyncio.sleep(0.01)
        return cls(server, task, sock)

    @property
    def port(self) -> int:
        return int(self._socket.getsockname()[1])

    async def close(self) -> None:
        self._server.should_exit = True
        try:
            await asyncio.wait_for(self._task, timeout=5)
        except asyncio.TimeoutError:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        finally:
            self._socket.close()
