#!/usr/bin/env python3
"""Fake ``codex app-server`` for offline transport/backend tests.

Speaks newline-delimited JSON-RPC 2.0 on stdio, mimicking the surface
nerve's CodexAppServerClient uses (schema shapes from codex-cli 0.144.1,
see tests/fixtures/codex_schema_meta.json). Behavior is selected via the
FAKE_CODEX_MODE env var:

  basic        — one text turn: deltas → tokenUsage → turn/completed
  tools        — command + multi-file fileChange + mcp tool items
  approval     — emits a commandExecution approval REQUEST mid-turn and
                 KEEPS STREAMING deltas while the request is pending
                 (proves the client's reader never blocks on approvals —
                 the exact deadlock the official beta SDK has); completes
                 only after the client answers
  resume_fail  — thread/resume|fork answer with a JSON-RPC error;
                 thread/start succeeds (resume-miss recovery path)
  interrupt    — turn runs "forever" until turn/interrupt, then emits
                 turn/completed with status=interrupted
  die_mid_turn — emits one delta then exits(1) mid-turn
  failed_turn  — emits an error notification then turn/completed(failed)
  big_line     — mcpToolCall item/completed whose result is ~2 MiB on a
                 single JSONL line (asyncio 64 KiB StreamReader-limit
                 regression: one large MCP response must not kill the
                 transport)
  big_stderr   — writes one ~2.5 MiB stderr line mid-turn, then
                 completes normally (stderr loop must keep draining)
  close_stdout_mid_turn — closes stdout mid-turn but KEEPS RUNNING
                 (reader-death liveness: is_alive() must flip False even
                 though the process is still up)

The process also mirrors received config overrides (argv --config k=v)
back in the initialize response under _fake.configOverrides so tests can
assert what nerve sent (mcp_servers wiring etc.).
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time

MODE = os.environ.get("FAKE_CODEX_MODE", "basic")

_out_lock = threading.Lock()
_pending_approval_answer = threading.Event()
_approval_decision: dict = {}
_interrupted = threading.Event()
_active_turn: dict = {"threadId": None, "turnId": None}


def send(payload: dict) -> None:
    with _out_lock:
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()


def notify(method: str, params: dict) -> None:
    send({"jsonrpc": "2.0", "method": method, "params": params})


def respond(req_id, result: dict) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "result": result})


def respond_error(req_id, code: int, message: str) -> None:
    send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


_next_server_id = [1000]


def server_request(method: str, params: dict) -> None:
    _next_server_id[0] += 1
    send({
        "jsonrpc": "2.0", "id": _next_server_id[0],
        "method": method, "params": params,
    })


def _config_overrides() -> list[str]:
    out, args = [], sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--config" and i + 1 < len(args):
            out.append(args[i + 1])
    return out


def _usage(turn_id: str, thread_id: str) -> None:
    notify("thread/tokenUsage/updated", {
        "threadId": thread_id, "turnId": turn_id,
        "tokenUsage": {
            "last": {
                "inputTokens": 1200, "cachedInputTokens": 1000,
                "outputTokens": 50, "reasoningOutputTokens": 10,
                "totalTokens": 1250,
            },
            "total": {
                "inputTokens": 1200, "cachedInputTokens": 1000,
                "outputTokens": 50, "reasoningOutputTokens": 10,
                "totalTokens": 1250,
            },
            "modelContextWindow": 272000,
        },
    })


def _completed(turn_id: str, thread_id: str, status: str = "completed",
               error: dict | None = None) -> None:
    turn = {
        "id": turn_id, "status": status, "items": [],
        "durationMs": 1234, "startedAt": 1, "completedAt": 2,
    }
    if error:
        turn["error"] = error
    notify("turn/completed", {"threadId": thread_id, "turn": turn})


def run_turn(thread_id: str, turn_id: str) -> None:
    """Emit the scripted turn for the current MODE (worker thread)."""
    if MODE in ("basic", "approval", "tools", "failed_turn"):
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        notify("item/reasoning/textDelta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "r1",
                "delta": "thinking..."})
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": "Hello "})

    if MODE == "approval":
        # Approval request mid-turn; deltas keep flowing while pending.
        server_request("item/commandExecution/requestApproval", {
            "threadId": thread_id, "turnId": turn_id,
            "itemId": "c1", "reason": "sandbox policy",
        })
        # These MUST reach the client while the approval is unanswered.
        for chunk in ("streaming ", "while ", "pending "):
            notify("item/agentMessage/delta",
                   {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                    "delta": chunk})
        _pending_approval_answer.wait(timeout=30)
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": f"decision={_approval_decision.get('decision')}"})

    if MODE == "tools":
        notify("item/started", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "c1", "type": "commandExecution",
                     "command": ["echo", "hi"], "cwd": "/tmp",
                     "status": "inProgress"},
        })
        notify("item/commandExecution/outputDelta", {
            "threadId": thread_id, "turnId": turn_id, "itemId": "c1",
            "delta": "hi\n",
        })
        notify("item/completed", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "c1", "type": "commandExecution",
                     "command": ["echo", "hi"], "cwd": "/tmp",
                     "aggregatedOutput": "hi\n", "exitCode": 0,
                     "status": "completed"},
        })
        # kind is a TAGGED OBJECT in the v2 schema (PatchChangeKind).
        changes = [
            {"path": "/tmp/fake_a.txt", "kind": {"type": "update"},
             "diff": "-a\n+b\n"},
            {"path": "/tmp/fake_b.txt", "kind": {"type": "add"},
             "diff": "+new\n"},
        ]
        notify("item/started", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "f1", "type": "fileChange", "changes": changes,
                     "status": "inProgress"},
        })
        notify("item/completed", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "f1", "type": "fileChange", "changes": changes,
                     "status": "completed"},
        })
        notify("item/started", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "t1", "type": "mcpToolCall", "server": "nerve",
                     "tool": "memorize", "arguments": {"content": "x"},
                     "status": "inProgress"},
        })
        notify("item/completed", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "t1", "type": "mcpToolCall", "server": "nerve",
                     "tool": "memorize",
                     "result": {"content": [{"type": "text",
                                             "text": "Memorized: x"}]},
                     "status": "completed"},
        })

    if MODE == "big_line":
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        notify("item/started", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "t1", "type": "mcpToolCall", "server": "nerve",
                     "tool": "task_read", "arguments": {"task_id": "big"},
                     "status": "inProgress"},
        })
        # The whole result rides ONE JSONL line — far past asyncio's
        # default 64 KiB StreamReader limit and past nerve's 1 MiB
        # stream limit, forcing the accumulation path.
        notify("item/completed", {
            "threadId": thread_id, "turnId": turn_id,
            "item": {"id": "t1", "type": "mcpToolCall", "server": "nerve",
                     "tool": "task_read", "result": "B" * 2_000_000,
                     "status": "completed"},
        })
        _usage(turn_id, thread_id)
        _completed(turn_id, thread_id)
        return

    if MODE == "big_stderr":
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        sys.stderr.write("E" * 2_500_000 + "\n")
        sys.stderr.flush()
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": "still here"})
        _usage(turn_id, thread_id)
        _completed(turn_id, thread_id)
        return

    if MODE == "close_stdout_mid_turn":
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": "going deaf"})
        with _out_lock:
            sys.stdout.flush()
            os.close(1)
        time.sleep(30)  # stay alive — this is the zombie window
        return

    if MODE == "die_mid_turn":
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": "about to die"})
        sys.stdout.flush()
        os._exit(1)

    if MODE == "interrupt":
        notify("turn/started", {"threadId": thread_id,
                                "turn": {"id": turn_id, "status": "inProgress", "items": []}})
        notify("item/agentMessage/delta",
               {"threadId": thread_id, "turnId": turn_id, "itemId": "m1",
                "delta": "working forever..."})
        _interrupted.wait(timeout=30)
        _usage(turn_id, thread_id)
        _completed(turn_id, thread_id, status="interrupted")
        return

    if MODE == "failed_turn":
        notify("error", {
            "threadId": thread_id, "turnId": turn_id,
            "error": {"message": "model exploded"}, "willRetry": False,
        })
        _usage(turn_id, thread_id)
        _completed(turn_id, thread_id, status="failed",
                   error={"message": "model exploded"})
        return

    _usage(turn_id, thread_id)
    _completed(turn_id, thread_id)


def main() -> None:
    threads_started = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue

        # Response to one of OUR server requests (approval answer).
        if "method" not in msg and "id" in msg:
            _approval_decision.update(
                msg.get("result") if isinstance(msg.get("result"), dict) else {},
            )
            _pending_approval_answer.set()
            continue

        method = msg.get("method")
        req_id = msg.get("id")

        if method == "initialize":
            respond(req_id, {
                "userAgent": "fake-codex/0.144.1",
                "_fake": {"configOverrides": _config_overrides(),
                          "env": {"CODEX_HOME": os.environ.get("CODEX_HOME", ""),
                                  "NERVE_MCP_TOKEN_SET": bool(os.environ.get("NERVE_MCP_TOKEN"))}},
            })
        elif method == "initialized":
            pass  # notification, no response
        elif method == "account/read":
            respond(req_id, {"account": {"type": "chatgpt", "email": "fake@example.com"}})
        elif method == "account/login/start":
            respond(req_id, {"loginId": "fake-login"})
        elif method == "thread/start":
            threads_started += 1
            respond(req_id, {"thread": {"id": f"th_fake_{threads_started}"}})
        elif method in ("thread/resume", "thread/fork"):
            if MODE == "resume_fail":
                respond_error(req_id, -32600, "no rollout found for thread")
            else:
                respond(req_id, {"thread": {"id": msg["params"]["threadId"]}})
        elif method == "turn/start":
            thread_id = msg["params"]["threadId"]
            turn_id = f"turn_{threads_started}_1"
            _active_turn.update({"threadId": thread_id, "turnId": turn_id})
            respond(req_id, {"turn": {"id": turn_id, "status": "inProgress",
                                      "items": []}})
            threading.Thread(
                target=run_turn, args=(thread_id, turn_id), daemon=True,
            ).start()
        elif method == "turn/interrupt":
            respond(req_id, {})
            _interrupted.set()
        else:
            respond(req_id, {})


if __name__ == "__main__":
    main()
