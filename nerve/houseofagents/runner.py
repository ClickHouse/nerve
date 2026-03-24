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
                    logger.info("HoA progress → session %s: %s", session_id, event.get("event", "?"))
                    await broadcaster.broadcast(session_id, {
                        "type": "hoa_progress",
                        "session_id": session_id,
                        "event": event,
                    })
            except json.JSONDecodeError:
                # Non-JSON stderr line — just log it
                logger.debug("hoa stderr: %s", text)

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
