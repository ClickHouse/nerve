"""Asyncio JSON-RPC 2.0 client for ``codex app-server`` over stdio.

Why not the official ``openai-codex`` SDK: its reader loop dispatches
server-initiated requests (approvals) *synchronously on the reader
thread* — a blocking approval handler stalls all message routing,
including the ``turn/interrupt`` response, deadlocking the client — and
its async wrapper accepts no approval handler at all (verified against
0.1.0b2; see docs/plans/codex-backend.md §0/§6). Nerve's approvals wait
on user input for up to an hour, so server requests here are dispatched
as independent asyncio tasks: the reader never blocks, deltas keep
flowing while an approval is pending, and interrupts stay responsive.

Protocol shapes verified against the schema exported from codex-cli
0.144.1 (``codex app-server generate-json-schema``); see
``tests/fixtures/codex_schema_meta.json``. Parsing is defensive:
unknown notifications are debug-logged, unknown server requests get a
safe response, missing fields never raise.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
from typing import Any, Awaitable, Callable

from nerve.agent.backends.base import TransportDiedError

logger = logging.getLogger(__name__)

# async (method, params) -> result dict for the server's request
ServerRequestHandler = Callable[[str, dict], Awaitable[dict]]

# Notification surfaces nerve never consumes — suppressed at initialize
# so the reader doesn't churn through them.
_OPT_OUT_NOTIFICATIONS = [
    "thread/realtime/started",
    "thread/realtime/closed",
    "thread/realtime/error",
    "thread/realtime/itemAdded",
    "thread/realtime/outputAudio/delta",
    "thread/realtime/sdp",
    "thread/realtime/transcript/delta",
    "thread/realtime/transcript/done",
    "fuzzyFileSearch/sessionUpdated",
    "fuzzyFileSearch/sessionCompleted",
]

_STDERR_TAIL_LINES = 40
_STDERR_TAIL_LINE_CHARS = 500

# StreamReader buffer limit for the subprocess pipes. Not a message-size
# cap — ``_read_jsonl_line`` accumulates past it — just the flow-control
# granularity (bigger limit = fewer accumulation round-trips per line).
_STREAM_LIMIT = 1024 * 1024

# Absolute per-line ceiling. A line past this is drained to its newline and
# then fails the transport explicitly. Purely an OOM guard against a runaway
# peer; silently dropping a terminal/accounting notification is unsafe.
_MAX_LINE_BYTES = 64 * 1024 * 1024
_MAX_NOTIFICATION_BACKLOG = 4_096


async def _read_jsonl_line(reader: asyncio.StreamReader, label: str) -> bytes:
    """``readline()`` without the StreamReader line-length ceiling.

    ``readline()`` raises ``ValueError`` on any line longer than the
    stream limit (asyncio default: 64 KiB) and — once the buffer holds
    more than the limit without a separator — *clears the buffer*,
    losing JSONL framing. codex app-server ships whole MCP tool results
    inside single ``item/completed`` lines, so one large tool response
    would kill the transport. Instead, accumulate ``readuntil()`` chunks
    across ``LimitOverrunError`` (``e.consumed`` bytes are already
    buffered, so ``readexactly`` never blocks).

    Mirrors ``readline()`` semantics otherwise: returns the line
    including its newline, the partial line at EOF, ``b""`` at clean EOF.
    Lines beyond ``_MAX_LINE_BYTES`` are consumed fully and then raise a
    controlled :class:`TransportDiedError`.
    """
    chunks: list[bytes] = []
    kept = 0
    truncated = False

    def _keep(chunk: bytes) -> None:
        nonlocal kept, truncated
        if truncated or not chunk:
            return
        room = _MAX_LINE_BYTES - kept
        if len(chunk) > room:
            truncated = True
            logger.error(
                "codex app-server %s line exceeds %d bytes — draining and "
                "failing the transport",
                label, _MAX_LINE_BYTES,
            )
            chunk = chunk[:room]
        chunks.append(chunk)
        kept += len(chunk)

    while True:
        try:
            chunk = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as e:  # EOF before newline
            _keep(e.partial)
            if truncated:
                raise TransportDiedError(
                    f"codex app-server {label} line exceeded "
                    f"{_MAX_LINE_BYTES} bytes"
                )
            return b"".join(chunks)
        except asyncio.LimitOverrunError as e:
            # Buffer holds e.consumed separator-less bytes: drain them
            # and keep looking for the newline.
            _keep(await reader.readexactly(e.consumed))
            continue
        _keep(chunk)
        if truncated:
            raise TransportDiedError(
                f"codex app-server {label} line exceeded {_MAX_LINE_BYTES} bytes"
            )
        return b"".join(chunks)


class CodexAppServerClient:
    """One ``codex app-server`` subprocess speaking JSONL JSON-RPC."""

    def __init__(
        self,
        *,
        bin_path: str,
        cwd: str,
        env: dict[str, str],
        server_request_handler: ServerRequestHandler,
        config_overrides: list[str] | None = None,
        client_name: str = "nerve",
        client_version: str = "1.0.0",
        request_timeout: float = 60.0,
    ) -> None:
        self._bin_path = bin_path
        self._cwd = cwd
        self._env = env
        self._config_overrides = list(config_overrides or [])
        self._client_name = client_name
        self._client_version = client_version
        self._request_timeout = request_timeout
        self._on_server_request = server_request_handler

        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._server_request_tasks: set[asyncio.Task] = set()
        self._write_lock = asyncio.Lock()
        self._next_id = 0
        self._pending: dict[Any, asyncio.Future] = {}
        # Lifecycle/control notifications must never be silently evicted.
        # A pathological producer fails visibly at the bound instead of
        # dropping turn/completed or tokenUsage and hanging/corrupting
        # accounting.
        self.notifications: asyncio.Queue[dict] = asyncio.Queue(
            maxsize=_MAX_NOTIFICATION_BACKLOG,
        )
        self._stderr_tail: list[str] = []
        self._closed = False

    # -- lifecycle ------------------------------------------------------ #

    async def start(self) -> dict:
        """Spawn the subprocess and run the ``initialize`` handshake.

        Returns the ``initialize`` response payload.
        """
        args = [self._bin_path]
        for kv in self._config_overrides:
            args.extend(["--config", kv])
        args.extend(["app-server", "--listen", "stdio://"])

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._cwd,
                env=self._env,
                limit=_STREAM_LIMIT,
                start_new_session=True,
            )
        except (OSError, ValueError) as e:
            raise TransportDiedError(
                f"Failed to spawn codex app-server ({self._bin_path}): {e}"
            ) from e

        self._reader_task = asyncio.create_task(
            self._reader_loop(), name=f"codex-reader:{self._proc.pid}",
        )
        self._stderr_task = asyncio.create_task(
            self._stderr_loop(), name=f"codex-stderr:{self._proc.pid}",
        )

        try:
            response = await self.request("initialize", {
                "clientInfo": {
                    "name": self._client_name,
                    "title": "Nerve",
                    "version": self._client_version,
                },
                "capabilities": {
                    "optOutNotificationMethods": _OPT_OUT_NOTIFICATIONS,
                    "experimentalApi": True,
                    "mcpServerOpenaiFormElicitation": True,
                },
            })
            await self.notify("initialized", None)
            return response
        except BaseException:
            await self.close()
            raise

    def is_alive(self) -> bool:
        return (
            not self._closed
            and self._proc is not None
            and self._proc.returncode is None
        )

    async def close(self) -> None:
        """Terminate the subprocess and fail everything pending."""
        self._closed = True
        tasks = [
            task for task in (self._reader_task, self._stderr_task)
            if task is not None and task is not asyncio.current_task()
        ]
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        request_tasks = [
            task for task in self._server_request_tasks
            if task is not asyncio.current_task()
        ]
        for task in request_tasks:
            task.cancel()

        proc, self._proc = self._proc, None
        if proc is not None:
            self._signal_process_tree(proc, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except (asyncio.TimeoutError, ProcessLookupError):
                self._signal_process_tree(proc, signal.SIGKILL)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=2.0)

        self._fail_pending(TransportDiedError("codex app-server closed"))
        if tasks or request_tasks:
            await asyncio.gather(*tasks, *request_tasks, return_exceptions=True)
        self._reader_task = None
        self._stderr_task = None
        self._server_request_tasks.clear()

    @staticmethod
    def _signal_process_tree(
        proc: asyncio.subprocess.Process, sig: signal.Signals,
    ) -> None:
        """Signal the app-server process group, including plugin workers."""
        if proc.pid is None:
            return
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            with contextlib.suppress(ProcessLookupError):
                proc.send_signal(sig)

    # -- JSON-RPC ------------------------------------------------------- #

    async def request(
        self, method: str, params: dict | None = None,
        timeout: float | None = None,
    ) -> dict:
        """Send a request; await and return its ``result``.

        Raises :class:`CodexRpcError` on a JSON-RPC error response and
        :class:`TransportDiedError` when the subprocess dies first.
        """
        if not self.is_alive():
            raise TransportDiedError(
                f"codex app-server is not running{self._stderr_hint()}"
            )
        self._next_id += 1
        req_id = self._next_id
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = future
        try:
            await self._write({
                "jsonrpc": "2.0", "id": req_id,
                "method": method, "params": params or {},
            })
            return await asyncio.wait_for(
                future, timeout=timeout or self._request_timeout,
            )
        except asyncio.TimeoutError:
            raise TransportDiedError(
                f"codex app-server did not answer {method} within "
                f"{timeout or self._request_timeout:.0f}s{self._stderr_hint()}"
            ) from None
        finally:
            self._pending.pop(req_id, None)

    async def notify(self, method: str, params: dict | None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        await self._write(payload)

    async def _write(self, payload: dict) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise TransportDiedError("codex app-server stdin is closed")
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        async with self._write_lock:
            try:
                proc.stdin.write(line.encode("utf-8"))
                await proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                raise TransportDiedError(
                    f"codex app-server pipe broken: {e}{self._stderr_hint()}"
                ) from e

    # -- reader --------------------------------------------------------- #

    async def _reader_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            while True:
                line = await _read_jsonl_line(proc.stdout, "stdout")
                if not line:
                    break  # EOF — process died or closed stdout
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except ValueError:
                    logger.warning(
                        "codex app-server emitted non-JSON line: %.200s", line,
                    )
                    continue
                if not isinstance(msg, dict):
                    continue
                self._dispatch(msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # pragma: no cover - defensive
            logger.error("codex app-server reader crashed: %s", e, exc_info=True)
        finally:
            # Once the reader is gone the transport is deaf: writes would
            # still succeed and then time out one by one (a zombie —
            # ``is_alive()`` used to stay True while the process lived).
            # Mark the client dead so the engine health check recreates
            # it immediately.
            self._closed = True
            self._fail_pending(TransportDiedError(
                f"codex app-server stream ended{self._stderr_hint()}"
            ))

    def _dispatch(self, msg: dict) -> None:
        has_method = "method" in msg
        has_id = "id" in msg

        if has_method and has_id:
            # Server-initiated request (approval, user input, ...).
            # Dispatch as an independent task so a long user wait never
            # blocks the reader (this is the whole reason this client
            # exists — see module docstring).
            task = asyncio.create_task(
                self._answer_server_request(msg),
                name=f"codex-server-request:{msg.get('method')}",
            )
            self._server_request_tasks.add(task)
            task.add_done_callback(self._server_request_tasks.discard)
            return

        if has_method:
            # Notification.
            method = msg.get("method")
            params = msg.get("params")
            note = {
                "method": method if isinstance(method, str) else "",
                "params": params if isinstance(params, dict) else {},
            }
            if self.notifications.qsize() >= _MAX_NOTIFICATION_BACKLOG:
                logger.error(
                    "codex notification backlog exceeded %d; failing "
                    "transport rather than dropping lifecycle events",
                    _MAX_NOTIFICATION_BACKLOG,
                )
                self._closed = True
                proc = self._proc
                if proc is not None:
                    self._signal_process_tree(proc, signal.SIGTERM)
                while not self.notifications.empty():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        self.notifications.get_nowait()
                self._fail_pending(TransportDiedError(
                    "codex notification backlog exhausted",
                ))
                return
            self.notifications.put_nowait(note)
            return

        # Response to one of our requests.
        req_id = msg.get("id")
        future = self._pending.get(req_id)
        if future is None or future.done():
            logger.debug("codex app-server response for unknown id %r", req_id)
            return
        if "error" in msg and msg["error"] is not None:
            err = msg["error"] or {}
            future.set_exception(CodexRpcError(
                code=int(err.get("code") or 0),
                message=str(err.get("message") or "unknown error"),
                data=err.get("data"),
            ))
        else:
            result = msg.get("result")
            future.set_result(result if isinstance(result, dict) else {})

    async def _answer_server_request(self, msg: dict) -> None:
        method = msg.get("method")
        params = msg.get("params")
        req_id = msg.get("id")
        method_str = method if isinstance(method, str) else ""
        params_dict = params if isinstance(params, dict) else {}
        try:
            result = await self._on_server_request(method_str, params_dict)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "Server-request handler failed for %s: %s", method_str, e,
                exc_info=True,
            )
            with contextlib.suppress(Exception):
                await self._write({
                    "jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32603, "message": str(e)[:500]},
                })
            return
        with contextlib.suppress(TransportDiedError):
            await self._write({
                "jsonrpc": "2.0", "id": req_id,
                "result": result if isinstance(result, dict) else {},
            })

    async def _stderr_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                # Same tolerant reader as stdout: a >limit stderr line
                # used to raise ValueError, silently kill this loop, and
                # leave stderr undrained — the subprocess then blocks on
                # its next stderr write and wedges.
                line = await _read_jsonl_line(proc.stderr, "stderr")
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if not text:
                    continue
                # Clamp stored lines — the tail rides in exception
                # messages and lines can now be arbitrarily long.
                self._stderr_tail.append(text[:_STDERR_TAIL_LINE_CHARS])
                if len(self._stderr_tail) > _STDERR_TAIL_LINES:
                    del self._stderr_tail[0]
                lowered = text.lower()
                if "error" in lowered or "panic" in lowered:
                    logger.warning("codex stderr: %.2000s", text)
                else:
                    logger.debug("codex stderr: %.2000s", text)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("codex app-server stderr loop crashed: %s", e)

    # -- helpers -------------------------------------------------------- #

    def _fail_pending(self, error: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        # Wake a parked notification consumer so it observes the death
        # instead of waiting forever.
        with contextlib.suppress(asyncio.QueueFull):
            self.notifications.put_nowait({"method": "__transport_died__", "params": {}})

    def _stderr_hint(self) -> str:
        if not self._stderr_tail:
            return ""
        return " | stderr tail: " + " / ".join(self._stderr_tail[-5:])[:800]


class CodexRpcError(Exception):
    """JSON-RPC error response from the app-server."""

    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"codex rpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data
