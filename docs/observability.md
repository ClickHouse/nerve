# Observability — Langfuse

Nerve has an optional Langfuse integration for tracing the agent loop and
the memU memory pipeline. When configured, every Claude Agent SDK turn,
tool call, and direct Anthropic SDK call (memU embeddings/condensation)
becomes a span in your Langfuse project, tagged with `session_id`,
`source` (`web` / `cron` / `telegram` / `hook`), `model`, and `channel`.

When the keys aren't set, the integration is a complete no-op — Nerve
runs identically with zero observability overhead.

## What gets captured

| Surface                       | Source                                               | Tags                                              |
|-------------------------------|------------------------------------------------------|---------------------------------------------------|
| Agent turns + tool calls      | `claude_agent_sdk` via LangSmith integration         | `source:*`, `model:*`, `channel:*` (when present) |
| memU chat / summarize / embed | `anthropic` SDK via `AnthropicInstrumentor`          | `component:memu`, `purpose:summarize`             |

Trace-level attributes (`session_id`, `metadata.parent_session_id`,
`metadata.fork_from`) are propagated to every span emitted inside a turn
via OpenTelemetry Baggage.

## Setup

### 1. Get a Langfuse project

Two options:

- **Langfuse Cloud** — sign up at <https://cloud.langfuse.com> and create
  a project. Region picks: `https://cloud.langfuse.com` (EU, default),
  `https://us.cloud.langfuse.com` (US),
  `https://jp.cloud.langfuse.com` (JP).
- **Self-hosted** — follow the upstream deployment guide at
  <https://langfuse.com/self-hosting/deployment/docker-compose>, then
  point Nerve at the resulting host URL.

### 2. Get API keys

In the Langfuse UI: *Project Settings → API Keys → Create new API keys*.
Copy the public (`pk-lf-...`) and secret (`sk-lf-...`) keys.

### 3. Configure Nerve

Add to `config.local.yaml` (gitignored):

```yaml
langfuse:
  public_key: pk-lf-...
  secret_key: sk-lf-...
  host: https://cloud.langfuse.com
```

Restart Nerve. On startup you should see one of:

- `Langfuse: enabled (host=...)` — keys valid, tracing active.
- `Langfuse: disabled (no public_key/secret_key in config)` — keys absent.
- `Langfuse: auth_check failed against ...` — keys present but rejected.

Visit the diagnostics page (`/diagnostics`) to confirm the live status.

## Configuration reference

| Field             | Default                          | Notes                                                           |
|-------------------|----------------------------------|-----------------------------------------------------------------|
| `public_key`      | `""`                             | `pk-lf-...` — required to activate.                             |
| `secret_key`      | `""`                             | `sk-lf-...` — required to activate.                             |
| `host`            | `https://cloud.langfuse.com`     | Region endpoint or self-hosted URL.                             |
| `redact_patterns` | (built-in secret regexes)        | List of regexes — matched substrings are replaced with `[REDACTED]`. |

The default `redact_patterns` strip common secret formats: Anthropic API
keys, Langfuse keys, and bcrypt hashes. Add more for any project-specific
secret formats you don't want to leave the host.

## Privacy note

When enabled, **prompt content, tool inputs, and model outputs leave the
host** to whichever Langfuse instance you point at. The `host` field is
the boundary — make sure it points where you want the data to go. For
strict data residency, self-host Langfuse on infrastructure you control.

`redact_patterns` is a defensive layer — useful even with trusted
endpoints in case a secret leaks into a prompt accidentally.

## Disabling

Remove or empty the `public_key` / `secret_key` fields. No restart-time
flags, no feature gates — the lack of keys is the off switch.

## Cost cross-check

Langfuse computes its own cost based on token counts and a price model
maintained by Langfuse. Nerve's `db/usage.py` computes cost in-process
via a hardcoded `MODEL_PRICING` dict and the SDK's
`ResultMessage.total_cost_usd`. Expect minor mismatches between the two —
they're independent calculations. Treat Langfuse as a second source of
truth for catching local cost-tracking bugs.

## Troubleshooting

- **Spans aren't appearing.** Check `/api/observability/status` —
  if `auth_ok: false`, the keys are wrong. If `enabled: false` despite
  keys being set, look at startup logs for an `ImportError` on the
  `langfuse` package itself (run `uv pip install -e .` to refresh).
- **Spans are tagged but session_id is missing.** That can happen if the
  installed Langfuse SDK doesn't accept `session_id=` kwarg in
  `propagate_attributes`. Upgrade to a newer Langfuse Python SDK.
- **The host runs out of memory under heavy load.** The Langfuse SDK
  buffers spans and ships them async. If memory is tight you can drop
  the Anthropic instrumentation by editing `init_langfuse`, or deploy
  Langfuse self-hosted on a separate machine.
