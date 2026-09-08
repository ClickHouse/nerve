# Live Slack tests

These tests cover behavior a fake cannot verify: Slack API payloads, scopes,
text rewriting, and end-to-end Socket Mode delivery. They are optional and
skip when their credentials are absent.

Use a throwaway workspace. The suite creates and deletes messages, reactions,
and uploads, but an interrupted run can leave test data behind.

## Setup

1. Create the app from the [Slack manifest](config.md#setting-up-the-slack-app).
2. Add the user token scopes `chat:write`, `im:write`, and `reactions:write`.
3. In **App Home → Messages Tab**, enable messages and **Allow users to send
   Slash commands and messages**. The manifest cannot set this option.
4. Invite both the bot and the user who installed the app to the test channel.

Set these test-only environment variables:

| Variable | Value | Required for |
|---|---|---|
| `NERVE_SLACK_TEST_BOT_TOKEN` | Bot token (`xoxb-…`) | All live tests |
| `NERVE_SLACK_TEST_APP_TOKEN` | App token (`xapp-…`) with `connections:write` | All live tests |
| `NERVE_SLACK_TEST_CHANNEL` | Test channel ID (`C…`) | All live tests |
| `NERVE_SLACK_TEST_USER_TOKEN` | User token (`xoxp-…`) | Inbound tests |
| `NERVE_SLACK_TEST_BOT_TOKEN_NO_EMAIL` | Bot token without `users:read.email` | One scope test |

These variables do not configure Nerve itself. See [config.md](config.md#slack)
for production configuration.

## Run

Ensure no CI job or other developer is using the same test workspace. Then run
the inbound suite first and the outbound suite second, as separate processes:

```bash
uv run --extra test pytest tests/test_slack_live_inbound.py -v -s
uv run --extra test pytest tests/test_slack_live.py -v -s
```

Do not combine the modules in one pytest command. Slack sends each event to
only one active Socket Mode connection. Process isolation prevents the
outbound suite's acknowledgement-only connection from stealing inbound events
and prevents unacknowledged events from being retried into later tests.

## CI and failures

The `Slack live integration` workflow uses the same variable names as GitHub
secrets and serializes runs against the shared workspace.

With `-s`, diagnostic lines prefixed by `SLACK_LIVE` show connection state,
probe latency, retries, event age, and a final summary. They omit tokens, IDs,
and message text, so they are safe to share when investigating a failure.

Slash commands require manual testing because Slack provides no API for
invoking them.
