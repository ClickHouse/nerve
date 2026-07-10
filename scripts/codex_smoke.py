#!/usr/bin/env python3
"""Codex backend smoke test — run against the REAL codex app-server.

Not part of CI (requires a codex binary + auth). Run before flipping any
config to the codex backend:

    CODEX_HOME=~/.nerve/codex codex login     # once, if using chatgpt auth
    .venv/bin/python scripts/codex_smoke.py [--model gpt-5.6-codex]

Verifies, and prints results for docs/plans/codex-backend.md §17:
  1. spawn + initialize handshake + auth state
  2. thread/start → thread id
  3. one trivial turn → streamed text + usage + per-turn cost
  4. RSS of the app-server process (per-session memory footprint)
  5. fileChange item/started ordering probe: asks the model to edit a
     scratch file and reports whether item/started fired BEFORE the file
     content changed on disk (validates the pre-apply snapshot
     assumption; if post-apply, the reverse-diff fallback is needed)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nerve.agent.backends import BackendDeps, SessionSpec, TurnInput  # noqa: E402
from nerve.agent.backends import events as ev  # noqa: E402
from nerve.agent.backends.codex import CodexBackend  # noqa: E402
from nerve.config import NerveConfig  # noqa: E402


def _rss_mb(pid: int) -> float:
    import subprocess
    out = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True,
    ).stdout.strip()
    return int(out or 0) / 1024


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=None)
    parser.add_argument("--home", default=os.path.expanduser("~/.nerve/codex"))
    parser.add_argument("--skip-filechange", action="store_true")
    args = parser.parse_args()

    workspace = Path(tempfile.mkdtemp(prefix="codex-smoke-"))
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace),
        "codex": {
            "home_dir": args.home,
            **({"model": args.model} if args.model else {}),
        },
    })
    deps = BackendDeps(
        config=cfg, db=None, registry=None,
        tool_ctx_factory=lambda sid: None,
        external_mcp_servers=lambda: [],
        gateway_port=lambda: None,          # no MCP bridge in the smoke
        mint_session_token=None,
    )
    backend = CodexBackend(deps)

    snapshots: list[tuple[str, str | None]] = []

    async def snapshot(sid: str, path: str, content: str | None) -> None:
        snapshots.append((path, content))

    spec = SessionSpec(
        session_id="smoke", source="web", model=args.model, effort="low",
        system_prompt="You are a smoke test. Be terse.",
        cwd=str(workspace), interactive=None, snapshot=snapshot,
        idle_timeout=120.0,
    )

    print(f"→ spawning codex app-server (home={args.home})")
    client = await backend.create_client(spec)
    proc = client._transport._proc
    print(f"✓ thread started: {client.native_session_id}")
    print(f"  app-server RSS after connect: {_rss_mb(proc.pid):.1f} MB")

    # --- trivial turn -------------------------------------------------- #
    await client.start_turn(TurnInput(text="Reply with exactly: SMOKE-OK"))
    text, done = "", None
    async for event in client.receive_turn():
        if isinstance(event, ev.TextDelta):
            text += event.text
        elif isinstance(event, ev.TurnCompleted):
            done = event
    assert done is not None
    print(f"✓ turn completed: status={done.status} model={done.model}")
    print(f"  text: {text.strip()[:80]!r}")
    if done.usage:
        print(
            f"  usage: in={done.usage.input_tokens} cached={done.usage.cache_read_tokens}"
            f" out={done.usage.output_tokens} ctx_window={done.context_window}",
        )
    print(f"  cost: ${done.total_cost_usd}" if done.total_cost_usd is not None
          else "  cost: None (no pricing entry — check codex.pricing)")
    print(f"  app-server RSS after turn: {_rss_mb(proc.pid):.1f} MB")

    # --- fileChange ordering probe ------------------------------------- #
    if not args.skip_filechange:
        target = workspace / "probe.txt"
        target.write_text("ORIGINAL\n")
        pre_images: dict[str, str] = {}

        async def probing_snapshot(sid: str, path: str, content: str | None) -> None:
            # capture what the DISK held at snapshot time
            try:
                pre_images[path] = Path(path).read_text()
            except OSError:
                pre_images[path] = "<unreadable>"

        client._spec.snapshot = probing_snapshot
        await client.start_turn(TurnInput(
            text=(
                f"Edit the file {target} replacing ORIGINAL with CHANGED. "
                "Do nothing else."
            ),
        ))
        async for event in client.receive_turn():
            if isinstance(event, ev.TurnCompleted):
                break
        final = target.read_text() if target.exists() else "<gone>"
        pre = pre_images.get(str(target), "<no snapshot>")
        print(f"✓ fileChange probe: final={final.strip()!r} snapshot-time={pre.strip()!r}")
        if "ORIGINAL" in pre:
            print("  → item/started fires PRE-APPLY: snapshots are valid ✅")
        else:
            print("  → item/started fired POST-APPLY: enable the reverse-diff "
                  "fallback (docs/plans/codex-backend.md §13) ⚠️")

    await client.disconnect()
    print("✓ disconnect clean — smoke PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
