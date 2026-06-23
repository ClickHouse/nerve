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

---

# Observability — OpenTelemetry (OTLP)

A vendor-neutral alternative (or complement) to Langfuse: export **traces,
metrics, and logs** over OTLP to an endpoint **you** run. Nerve does **not**
package or run a collector — point it at your own collector or any
OTLP-speaking backend (Grafana Alloy/Tempo/Mimir/Loki, Honeycomb, Datadog,
Grafana Cloud, …).

Off by default: with no `telemetry.endpoint`, there is zero overhead and no
per-request instrumentation. It can run **alongside** Langfuse — Nerve owns
the global OTel tracer provider and Langfuse attaches to it, so the same spans
reach both.

## What gets captured

- **Traces:** FastAPI HTTP server requests, outbound `httpx` calls, the Claude
  Agent SDK agent loop + tool calls, and direct Anthropic (memU) calls — the
  same spans Langfuse sees, plus HTTP server/client spans.
- **Metrics:** HTTP server metrics, system/process metrics (CPU, memory, GC),
  and a few nerve counters: `nerve.memorize.runs` / `.errors`,
  `nerve.notifications.sent`, `nerve.agent.turns`.
- **Logs (opt-in):** stdlib logs exported over OTLP with `trace_id`/`span_id`
  correlation, when `telemetry.logs: true`.

## Setup

1. Run an OTLP endpoint (your collector or backend). For local testing,
   `otel-tui` or `otelcol-contrib` with the `debug` exporter on `:4318` works.
2. Configure Nerve (`config.yaml`; put secrets/headers in `config.local.yaml`):

   ```yaml
   telemetry:
     endpoint: http://localhost:4318    # empty disables export
     protocol: http/protobuf            # or "grpc"
     # headers: {authorization: "Bearer <token>"}
     traces: true
     metrics: true
     logs: false
     system_metrics: true
     log_format: console                # or "json"
   ```

3. Standard `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_HEADERS` /
   `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_SERVICE_NAME` environment variables
   are honored by the exporters and **override** the config values.

## Configuration reference

| Key | Default | Description |
|-----|---------|-------------|
| `telemetry.endpoint` | `""` | OTLP endpoint. Empty = disabled. |
| `telemetry.protocol` | `http/protobuf` | `http/protobuf` or `grpc` (grpc needs `opentelemetry-exporter-otlp-proto-grpc`). |
| `telemetry.headers` | `{}` | OTLP auth headers (set in `config.local.yaml`). |
| `telemetry.service_name` | `nerve` | `service.name` resource attribute. |
| `telemetry.traces` | `true` | Export traces. |
| `telemetry.metrics` | `true` | Export metrics. |
| `telemetry.logs` | `false` | Export logs over OTLP. |
| `telemetry.system_metrics` | `true` | Process/runtime metrics sampler. |
| `telemetry.metric_interval_ms` | `60000` | Metric export interval. |
| `telemetry.log_format` | `console` | Console log rendering: `console` or `json`. Honored even with no endpoint; `NERVE_LOG_FORMAT` overrides. |

## Logging

Console logs keep the classic `HH:MM:SS [LEVEL] name: message` format and gain
`trace_id`/`span_id` when a span is active. Set `telemetry.log_format: json`
(or `NERVE_LOG_FORMAT=json`) to emit JSON for a log shipper — this is
independent of OTLP export.

## Privacy / PII

Unlike the Langfuse integration, the OTLP path has **no redaction**. Be
deliberate about what you export and where:

- **Traces** capture request URLs (incl. query strings), outbound URLs, and
  agent/tool span attributes.
- **`telemetry.logs: true` exports *all* stdlib log bodies** over OTLP, which
  in this codebase can include prompt/response content and tokens. It is
  off by default for that reason — enable it only when exporting to a trusted
  backend, and prefer keeping logs local (console/file) when pointing at a
  third-party endpoint.
- `langfuse.redact_patterns` does **not** apply here. If you need scrubbing on
  this path, do it at your collector (an attributes/redaction processor)
  before forwarding to a third-party backend.

## Running alongside Langfuse

Both can be enabled at once. Nerve initializes the OTLP provider first, so
Langfuse v3 reuses it rather than creating its own; agent spans then export to
both. Langfuse's cache-aware usage rewriter is unaffected.

## Troubleshooting

- **Nothing arrives.** Check `/api/diagnostics` → `otel.enabled`. If false,
  `telemetry.endpoint` is unset. Confirm your endpoint includes the scheme
  (`http://…`) and is reachable.
- **`protocol: grpc` does nothing.** The grpc exporter is optional; install
  `opentelemetry-exporter-otlp-proto-grpc` or use `http/protobuf` (default).
  Nerve logs a warning and falls back to http.
- **Logs lack trace IDs.** Trace correlation only populates inside an active
  span; startup logs before a request/turn won't have one.
