"""Consume and discard whatever Slack has queued for the test app.

Slack holds events that occur while no Socket Mode connection is open and
replays them to the next one that appears. The live test suite posts for
several minutes, and its outbound half holds no connection at all, so a run
leaves a queue behind it. The next run then opens a socket, receives the
previous run's messages, and fails on them — a cycle that does not clear
itself, because each failed run leaves its own queue in turn.

So the workflow starts here: connect, acknowledge everything Slack has to
say, and exit once it has been quiet long enough to be believed. The replay
is paced with gaps, so "quiet" has to mean several seconds of silence rather
than one idle moment.

Run it before the tests, not after. After is not enough on its own: a run
that is cancelled or crashes never reaches its cleanup, and the point is to
be robust to the previous run having ended badly.

    python scripts/drain_slack_events.py

Exits 0 when the queue is clear, and also when the credentials are absent,
so it is safe as an unconditional step.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# Silence long enough to trust, and the ceiling on how long to keep waiting.
QUIET_SECONDS = 8.0
MAX_SECONDS = 240.0


async def main() -> int:
    bot_token = os.environ.get("NERVE_SLACK_TEST_BOT_TOKEN", "")
    app_token = os.environ.get("NERVE_SLACK_TEST_APP_TOKEN", "")
    if not bot_token or not app_token:
        print("Slack test credentials not set — nothing to drain.")
        return 0

    from slack_sdk.socket_mode.aiohttp import SocketModeClient
    from slack_sdk.socket_mode.response import SocketModeResponse
    from slack_sdk.web.async_client import AsyncWebClient

    web = AsyncWebClient(token=bot_token)
    client = SocketModeClient(app_token=app_token, web_client=web)

    seen = 0
    last_event = time.monotonic()

    async def consume(c, req) -> None:
        nonlocal seen, last_event
        # Acknowledge it, or Slack keeps the envelope and redelivers it to
        # whoever connects next, which is the loop this exists to break.
        await c.send_socket_mode_response(
            SocketModeResponse(envelope_id=req.envelope_id),
        )
        seen += 1
        last_event = time.monotonic()

    client.socket_mode_request_listeners.append(consume)
    await client.connect()
    print("Connected. Draining queued events…", flush=True)

    started = time.monotonic()
    while time.monotonic() - started < MAX_SECONDS:
        if time.monotonic() - last_event >= QUIET_SECONDS:
            break
        await asyncio.sleep(0.5)
    else:
        print(
            f"Still receiving after {MAX_SECONDS:.0f}s ({seen} events). "
            "Continuing anyway; the tests drain again on connect.",
            flush=True,
        )

    await client.close()
    print(f"Drained {seen} queued event(s).", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
