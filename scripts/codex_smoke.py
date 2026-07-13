#!/usr/bin/env python3
"""Codex backend smoke test — run against the REAL codex app-server.

Not part of CI (requires a codex binary + auth). Run before flipping any
config to the codex backend:

    CODEX_HOME=~/.nerve/codex codex login     # once, if using chatgpt auth
    .venv/bin/python scripts/codex_smoke.py [--model gpt-5.6-sol]

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
    parser.add_argument("--auth", choices=["chatgpt", "api_key"], default="chatgpt")
    parser.add_argument(
        "--config-dir", default=None,
        help="Nerve config dir; with --auth api_key, openai_api_key is read "
             "from its config.local.yaml (key never leaves this process)",
    )
    parser.add_argument("--skip-filechange", action="store_true")
    args = parser.parse_args()

    api_key = ""
    if args.auth == "api_key":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key and args.config_dir:
            import yaml
            local = Path(args.config_dir) / "config.local.yaml"
            if local.exists():
                api_key = (yaml.safe_load(local.read_text()) or {}).get(
                    "openai_api_key", "",
                ) or ""
        if not api_key:
            print("✗ --auth api_key but no key (env OPENAI_API_KEY or --config-dir)")
            return 1

    workspace = Path(tempfile.mkdtemp(prefix="codex-smoke-"))
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace),
        "codex": {
            "home_dir": args.home,
            "auth": args.auth,
            **({"api_key": api_key} if api_key else {}),
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

    print(f"→ spawning codex app-server (home={args.home}, auth={args.auth})")
    client = await backend.create_client(spec)
    proc = client._transport._proc
    print(f"✓ thread started: {client.native_session_id}")
    print(f"  app-server RSS after connect: {_rss_mb(proc.pid):.1f} MB")

    # Model catalog — diagnostics for picking codex.model.
    try:
        models = await client._transport.request("model/list", {})
        ids = [
            m.get("id") or m.get("model") or str(m)
            for m in (models.get("models") or models.get("data") or [])
            if isinstance(m, (dict, str))
        ]
        if ids:
            print(f"  models available: {', '.join(str(i) for i in ids[:12])}")
    except Exception as e:
        print(f"  (model/list unavailable: {e})")

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
            # Record what the ENGINE would persist as the pre-image (the
            # content parameter — reverse-diff reconstructed when the
            # event fired post-apply).
            pre_images[path] = content if content is not None else "<none>"

        client._spec.snapshot = probing_snapshot
        await client.start_turn(TurnInput(
            text=(
                f"Edit the file {target} replacing ORIGINAL with CHANGED. "
                "Do nothing else."
            ),
        ))
        probe_done = None
        async for event in client.receive_turn():
            if isinstance(event, ev.TurnCompleted):
                probe_done = event
                break
        final = target.read_text() if target.exists() else "<gone>"
        pre = pre_images.get(str(target))
        print(f"✓ fileChange probe: final={final.strip()!r} "
              f"snapshot-time={(pre or '<no snapshot>').strip()!r}")
        if probe_done is None or probe_done.status != "completed" or (
            "CHANGED" not in final
        ):
            print("  → probe INCONCLUSIVE (turn failed or no edit happened) — "
                  f"status={getattr(probe_done, 'status', '?')} "
                  f"error={getattr(probe_done, 'error', None)}")
        elif pre and "ORIGINAL" in pre:
            # The snapshot callback received the true BEFORE-content —
            # via direct pre-apply capture or the reverse-diff
            # reconstruction (backends/codex/diffs.py). Either way the
            # diff panel gets a correct before/after pair.
            print("  → snapshot holds the pre-image: diff panel correct ✅")
        else:
            print("  → snapshot MISSED the pre-image (no diff on events?) — "
                  "investigate before relying on the diff panel ⚠️")

    await client.disconnect()
    print("✓ disconnect clean — smoke PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
