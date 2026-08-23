# Testing Slack against a real workspace

The unit suite proves the channel is self-consistent against a hand-written
fake. That is worth having, but a fake only encodes what its author already
believed about Slack — so it confirms the client agrees with itself, not
that its assumptions hold. These tests exist for the rest.

Everything here is test-only. None of these variables configures Nerve; see
[config.md](config.md#slack) for that.

Two files run the channel against a live Slack workspace, covering what a
fake cannot: whether Slack accepts a Block Kit payload, whether an emoji
short name exists, and whether an event completes the trip from a person's
keystroke to an `InboundMessage`. Every test skips unless the credentials are
set, so the ordinary suite is unaffected.

```bash
pytest tests/test_slack_live.py          # outbound: Web API + ack-only event sink
pytest tests/test_slack_live_inbound.py  # inbound: holds a Socket Mode connection
```

**Run them as two processes.** Slack gives each event to exactly one of an
app's open Socket Mode connections. The outbound process holds an ack-only
connection while it posts, reacts, uploads, and cleans up; without that sink,
Slack retries every undelivered envelope at +60 seconds and again around five
minutes, which can make a later inbound connection deaf to fresh events. The
process split ensures the sink can never steal an inbound test event.

CI prints structured lines prefixed with `SLACK_LIVE`. They record Socket Mode
handshakes and connection counts, readiness-probe latency, retry attempt/reason
and event age, quiet waits, and an aggregate summary by envelope type. The
diagnostics deliberately omit tokens, channel/user ids, envelope ids, and
message text, so a failed run can be shared without exposing workspace data.
The inbound harness rejects both marked retries and callbacks whose documented
`event_time` predates the current test window; Slack has produced old callbacks
without retry metadata, so either signal alone is insufficient.

| Variable | Value | Needed for |
|---|---|---|
| `NERVE_SLACK_TEST_BOT_TOKEN` | `xoxb-…` | everything |
| `NERVE_SLACK_TEST_APP_TOKEN` | `xapp-…` | everything |
| `NERVE_SLACK_TEST_CHANNEL` | `C…`, bot invited | everything |
| `NERVE_SLACK_TEST_USER_TOKEN` | `xoxp-…`, scopes `chat:write` + `im:write` | inbound tests |
| `NERVE_SLACK_TEST_BOT_TOKEN_NO_EMAIL` | `xoxb-…` without `users:read.email` | one scope test |

The user token is what makes the inbound half testable: the tests post as
that person so the bot receives a real event. Without it only the outbound
half runs.

Use a throwaway workspace — the tests post, edit, react, and upload. They
delete what they create, but a failure mid-run can leave messages behind.

## Setting up the app

Use the manifest in [config.md](config.md#setting-up-the-slack-app), with
these additions for the tests:

- **User token scopes** `chat:write`, `im:write`, `reactions:write`, so the
  tests can post as a person. Installing the app grants the `xoxp-` token to
  whoever clicks Install, and that account must be a member of the test
  channel or posting fails with `not_in_channel`.
- **App Home → Messages Tab** on, with *Allow users to send Slash commands
  and messages* ticked. Without it the DM with the bot is read-only and
  Slack refuses the direct-message test with
  `restricted_action_read_only_channel`. The manifest cannot set this.

## What only a real workspace can settle

These are the questions the fake cannot answer, and each has already caught
something:

| Question | What it found |
|---|---|
| Does Slack accept this Block Kit payload? | the 25-element actions cap is real |
| Is this emoji short name one Slack knows? | all 44 are valid, contrary to expectation |
| Does Slack store the mrkdwn we sent? | it rewrites a bare `&` inside a link |
| Does `blocks=[]` clear the buttons? | yes, but Slack adds a `rich_text` block |
| Does an event reach the router? | every `bot_id` was being read as our own, dropping anyone posting through an integration |
| Does the watchdog recover a dropped socket? | it was leaking a connection, so a blip left the bot receiving a share of its events |

## Slash commands are not covered

There is no API to invoke one — they only originate from a Slack client, so
`/nerve …` has to be exercised by hand. Slack refuses them inside threads
outright (*"/nerve is not supported in threads"*), which is why the
commands resolve across a channel's threads rather than trusting their own
payload.
