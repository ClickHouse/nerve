# Observability — Langfuse

Nerve has an optional Langfuse integration for tracing the agent loop.
When configured, every Claude Agent SDK turn and tool call, and every
direct Anthropic SDK call, becomes a span in your Langfuse project. Agent
turns are tagged with `session_id`, `source` (`web` / `cron` /
`telegram` / `hook`), `model`, and `channel`.

When the keys aren't set, the integration is a complete no-op — Nerve
runs identically with zero observability overhead.

## What gets captured

| Surface                                  | Source                                       | Tags                                              |
|------------------------------------------|----------------------------------------------|---------------------------------------------------|
| Agent turns + tool calls (Claude backend) | `claude_agent_sdk` via LangSmith integration | `source:*`, `model:*`, `channel:*` (when present) |
| memU chat / summarize on Bedrock         | `anthropic` SDK via `AnthropicInstrumentor`  | `component:memu`, `purpose:summarize`             |
| Other direct Anthropic calls (session titles, prompt rewrite, source condensing, image analysis, memU date resolution) | `anthropic` SDK via `AnthropicInstrumentor` | none (no session id) |

Not captured:

- **Codex turns.** Langfuse tracing covers the Claude backend only.
- **memU on providers other than Bedrock.** memU's own client uses the
  OpenAI SDK, which Nerve does not instrument, so memU extraction and
  recall calls do not appear.

Trace-level attributes (`session_id`, `metadata.parent_session_id`,
`metadata.fork_from`) are propagated to every span emitted inside a turn
through the in-process OpenTelemetry context.

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

Restart Nerve. On startup Nerve checks the keys with
`GET <host>/api/public/projects` and logs one of:

- `Langfuse: enabled (host=..., project=...)` — keys valid, tracing active.
- `Langfuse: disabled (no public_key/secret_key in config)` — keys absent.
- `Langfuse: ... rejected the key pair (HTTP 401)` — keys present but
  rejected. Tracing is off until you fix the keys and restart.
- `Langfuse: could not verify the keys against ...` — the host did not
  answer. Tracing stays on, and spans are sent when the host answers.

Visit the diagnostics page (`/diagnostics`) to confirm the live status.

The keys go only to the Langfuse client inside the Nerve process. Nerve
does not export them to the environment, so the Claude CLI, the agent's
shell tools and Codex do not inherit them. An agent that can read
`config.local.yaml` can still read them there.

## Configuration reference

| Field             | Default                          | Notes                                                           |
|-------------------|----------------------------------|-----------------------------------------------------------------|
| `public_key`      | `""`                             | `pk-lf-...` — required to activate.                             |
| `secret_key`      | `""`                             | `sk-lf-...` — required to activate.                             |
| `host`            | `https://cloud.langfuse.com`     | Region endpoint or self-hosted URL.                             |
| `redact_patterns` | (built-in secret regexes)        | List of regexes — matched substrings are replaced with `[REDACTED]`. |

All four fields are read once at startup; change them and restart.

The default `redact_patterns` strip common secret formats: Anthropic API
keys, Langfuse keys, and bcrypt hashes. Add more for any project-specific
secret formats you don't want to leave the host. An explicit empty list
(`redact_patterns: []`) turns redaction off.

Nerve applies the patterns to the string attributes of every span before
it leaves the process: prompts, completions, tool inputs and outputs,
tags and metadata. It uses the Langfuse SDK's `mask_otel_spans` export
hook, so spans from every instrumentation are covered. Span names and
span events (for example exception messages) are not redacted.

## Privacy note

When enabled, **prompt content, tool inputs, and model outputs leave the
host** to whichever Langfuse instance you point at. The `host` field is
the boundary — make sure it points where you want the data to go. For
strict data residency, self-host Langfuse on infrastructure you control.

`redact_patterns` is a defensive layer — useful even with trusted
endpoints in case a secret leaks into a prompt accidentally. It only
removes text that matches a pattern; everything else is sent as is.

## Disabling

Remove or empty the `public_key` / `secret_key` fields and restart. No
feature gates — the lack of keys is the off switch.

## Session links

When the startup key check succeeds, the chat header shows a **Langfuse**
link to `<host>/project/<projectId>/sessions/<sessionId>`. Nerve gets the
project id from the key check. If the host did not answer at startup,
there is no link until the next successful check, which happens at the
next restart.

## Cost cross-check

Langfuse computes its own cost based on token counts and a price model
maintained by Langfuse. Nerve's `db/usage.py` computes cost in-process
via a hardcoded `MODEL_PRICING` dict and the SDK's
`ResultMessage.total_cost_usd`. Expect minor mismatches between the two —
they're independent calculations. Treat Langfuse as a second source of
truth for catching local cost-tracking bugs.

### Prompt-cache pricing (usage rewriter)

The LangSmith `claude-agent-sdk` integration reports usage in
LangSmith's canonical format: `input_tokens` *includes* prompt-cache
reads and writes, with the breakdown only in `input_token_details`.
Langfuse's OTEL ingestion doesn't read that detail field, so without
correction it prices every cached token at the full uncached input rate
— a ~5-10x cost overcount on agent sessions, which are typically >95%
cache reads billed at 10% of the input price.

Nerve fixes this at export time: `init_langfuse` wraps the Langfuse
OTLP exporter with `nerve/observability/usage_rewrite.py`, which
rewrites each agent span's `gen_ai.usage.*` attributes from the
accurate `langsmith.metadata.usage_metadata` payload into the same
shape `opentelemetry-instrumentation-anthropic` emits (uncached input +
explicit cache-read / cache-creation counts). Langfuse then applies its
managed per-model cache prices.

The diagnostics status block reports this as `usage_rewriter: true`.
If it's `false` while `enabled` is `true`, the SDK layout probably
changed underneath the installer — agent costs in Langfuse will be
inflated until it's fixed. Known approximation: 1-hour cache writes are
priced at the 5-minute rate (Langfuse's OTEL mapping has no separate 1h
key).

## Troubleshooting

- **Spans aren't appearing.** Check `/api/observability/status`:
  - `enabled: false` with keys set: the host rejected the keys, or the
    `langfuse` package failed to import. The startup log says which; for
    an import error, run `uv sync` to refresh.
  - `enabled: true` with `auth_ok: false`: the host did not answer the
    startup check. Spans are sent when it answers; check the network path
    to `host`.
- **No Langfuse link in the chat header.** The link needs `project_id`
  from a successful startup check. Restart Nerve once the host answers.
- **The host runs out of memory under heavy load.** The Langfuse SDK
  buffers spans and ships them async. If memory is tight you can drop
  the Anthropic instrumentation by editing `init_langfuse`, or deploy
  Langfuse self-hosted on a separate machine.
