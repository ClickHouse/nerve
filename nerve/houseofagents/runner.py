"""houseofagents subprocess execution with NDJSON progress streaming."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.houseofagents.service import HoAService

logger = logging.getLogger(__name__)


@dataclass
class HoARunResult:
    """Result of a houseofagents execution."""

    exit_code: int
    output_dir: str | None = None
    stdout_json: dict | None = None
    stdout_raw: str = ""
    stderr_log: str = ""
    events: list[dict] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.exit_code == 0


async def _tail_session_jsonl(
    inner_session_id: str,
    nerve_session_id: str,
    block_label: str,
    broadcaster: "StreamBroadcaster",
    workspace: str,
) -> None:
    """Tail a claude CLI session JSONL and broadcast tool calls to Nerve UI.

    This is UI-only — the broadcaster sends to WebSocket listeners, not to the
    SDK agent.  The agent only sees the final hoa_execute tool result.
    """
    from nerve.agent.streaming import StreamBroadcaster as _SB  # noqa: F811 — type hint

    # Claude CLI writes session logs to ~/.claude/projects/<encoded-cwd>/<session-id>.jsonl
    # The cwd is encoded by replacing / with - (keeping the leading dash for absolute paths).
    cwd_encoded = workspace.replace("/", "-")
    projects_dir = Path.home() / ".claude" / "projects"
    jsonl_path = projects_dir / cwd_encoded / f"{inner_session_id}.jsonl"

    # Wait for the file to appear (claude CLI creates it after init)
    for _ in range(30):
        if jsonl_path.exists():
            break
        await asyncio.sleep(1)
    else:
        logger.warning("JSONL not found for session %s at %s", inner_session_id, jsonl_path)
        return

    logger.info("Tailing inner session %s → %s", inner_session_id, jsonl_path)

    def _read_new(pos: int) -> tuple[str, int]:
        """Read appended JSONL content from ``pos``.  Sync — runs in a thread."""
        with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(pos)
            return f.read(), f.tell()

    last_pos = 0
    while True:
        try:
            stat = await asyncio.to_thread(jsonl_path.stat)
            if stat.st_size > last_pos:
                chunk, last_pos = await asyncio.to_thread(_read_new, last_pos)
                for line in chunk.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    # Only forward assistant tool_use entries
                    if msg.get("type") != "assistant":
                        continue
                    content = msg.get("message", {}).get("content", [])
                    if not isinstance(content, list):
                        continue
                    for block in content:
                        if block.get("type") != "tool_use":
                            continue
                        name = block.get("name", "?")
                        inp = block.get("input", {})
                        # Build a concise summary
                        if name == "Bash":
                            detail = str(inp.get("command", ""))[:120]
                        elif name in ("Read", "Write", "Edit"):
                            detail = str(inp.get("file_path", ""))[:100]
                        elif name in ("Glob", "Grep"):
                            detail = str(inp.get("pattern", inp.get("path", "")))[:100]
                        elif name == "Task":
                            detail = str(inp.get("description", ""))[:80]
                        else:
                            detail = str(inp)[:80]

                        await broadcaster.broadcast(nerve_session_id, {
                            "type": "hoa_progress",
                            "session_id": nerve_session_id,
                            "event": {
                                "event": "tool_call",
                                "label": block_label,
                                "agent": "Claude",
                                "tool": name,
                                "detail": detail,
                            },
                        })
        except FileNotFoundError:
            pass
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.debug("JSONL tail error: %s", e)

        await asyncio.sleep(2)


class HoARunner:
    """Spawn houseofagents subprocess and stream NDJSON progress to Nerve UI."""

    def __init__(self, service: HoAService) -> None:
        self.service = service

    async def execute(
        self,
        prompt: str,
        mode: str = "relay",
        agents: list[str] | None = None,
        iterations: int = 3,
        pipeline_file: Path | None = None,
        session_name: str | None = None,
        session_id: str | None = None,
        forward_prompt: bool = True,
        cwd: str | None = None,
    ) -> HoARunResult:
        """Run houseofagents as a subprocess.

        When *session_id* is provided, NDJSON progress events from stderr
        are broadcast to the Nerve streaming system in real-time (the event
        loop is active while this coroutine awaits subprocess I/O).
        """
        binary = await self.service.ensure_binary()
        hoa_cfg = self.service._hoa

        # Ensure houseofagents has its own config
        self.service.generate_config()

        cmd: list[str] = [str(binary)]

        if pipeline_file:
            cmd.extend(["--pipeline", str(pipeline_file)])
            # Pass prompt alongside pipeline — overrides the TOML's initial_prompt
            if prompt:
                cmd.extend(["--prompt", prompt])
        else:
            cmd.extend(["--prompt", prompt])
            cmd.extend(["--mode", mode])
            cmd.extend(["--iterations", str(iterations)])

        resolved_agents = agents or hoa_cfg.default_agents
        if resolved_agents and not pipeline_file:
            cmd.extend(["--agents", ",".join(resolved_agents)])

        if forward_prompt and mode == "relay" and not pipeline_file:
            cmd.append("--forward-prompt")

        if session_name:
            cmd.extend(["--session-name", session_name])

        # --output-format json: final result as JSON on stdout, progress as NDJSON on stderr
        # NOTE: do NOT use --quiet — it suppresses the NDJSON progress stream we need
        cmd.extend(["--output-format", "json"])
        cmd.extend(["--config", str(hoa_cfg.config_path.expanduser())])

        workspace = cwd or str(self.service.config.workspace.expanduser())

        logger.info("Running houseofagents (session=%s): %s", session_id, " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=workspace,
        )

        events: list[dict] = []
        stderr_lines: list[str] = []

        # Stream stderr NDJSON → broadcaster (if session_id provided)
        broadcaster = None
        if session_id:
            try:
                from nerve.agent.streaming import broadcaster as _broadcaster
                broadcaster = _broadcaster
            except ImportError:
                pass

        # Track inner claude session IDs → tail their JSONL for tool-level activity
        _tailing_sessions: set[str] = set()
        _tail_tasks: list[asyncio.Task] = []

        assert proc.stderr is not None
        async for raw_line in proc.stderr:
            text = raw_line.decode(errors="replace").rstrip()
            if not text:
                continue
            stderr_lines.append(text)
            try:
                event = json.loads(text)
                events.append(event)
                if broadcaster and session_id:
                    evt_type = event.get("event", "?")
                    evt_detail = event.get("message", event.get("label", ""))
                    logger.info("HoA → %s: %s %s", session_id, evt_type, evt_detail)
                    await broadcaster.broadcast(session_id, {
                        "type": "hoa_progress",
                        "session_id": session_id,
                        "event": event,
                    })

                    # Start tailing inner session JSONL when we see a Session ID
                    msg = event.get("message", "")
                    if "Session ID:" in msg and broadcaster:
                        inner_sid = msg.split("Session ID:")[-1].strip()
                        if inner_sid and inner_sid not in _tailing_sessions:
                            _tailing_sessions.add(inner_sid)
                            label = event.get("label", event.get("agent", "?"))
                            task = asyncio.create_task(
                                _tail_session_jsonl(
                                    inner_sid, session_id, label, broadcaster, workspace
                                )
                            )
                            _tail_tasks.append(task)
            except json.JSONDecodeError:
                # Non-JSON stderr line — just log it
                logger.debug("hoa stderr: %s", text)

        # Cancel any session tailers when the subprocess exits
        for t in _tail_tasks:
            t.cancel()
        await asyncio.gather(*_tail_tasks, return_exceptions=True)

        assert proc.stdout is not None
        stdout_bytes = await proc.stdout.read()
        await proc.wait()

        stdout_raw = stdout_bytes.decode(errors="replace").strip()
        stdout_json = None
        output_dir = None
        if stdout_raw:
            try:
                stdout_json = json.loads(stdout_raw)
                # In text mode stdout is the output dir path
            except json.JSONDecodeError:
                # stdout might just be the output directory path
                if stdout_raw and not stdout_raw.startswith("{"):
                    output_dir = stdout_raw

        if stdout_json and isinstance(stdout_json, dict):
            output_dir = stdout_json.get("output_dir", output_dir)

        result = HoARunResult(
            exit_code=proc.returncode or 0,
            output_dir=output_dir,
            stdout_json=stdout_json,
            stdout_raw=stdout_raw,
            stderr_log="\n".join(stderr_lines),
            events=events,
        )

        if result.success:
            logger.info("houseofagents completed successfully")
        else:
            logger.warning(
                "houseofagents exited with code %d", result.exit_code,
            )

        return result
