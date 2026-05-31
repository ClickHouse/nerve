import { useState } from 'react';

interface ContextUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  // 5m vs 1h ephemeral cache TTL split — emitted by the Anthropic API
  // under usage.cache_creation. Both default to 0 when the API omits
  // the split (older responses) or when no cache was written.
  cache_creation_5m_input_tokens?: number;
  cache_creation_1h_input_tokens?: number;
  max_context_tokens: number;
  num_turns: number;
}

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export function ContextBar({ usage, sessionCostUsd }: { usage: ContextUsage; sessionCostUsd?: number }) {
  const [hovering, setHovering] = useState(false);

  // The SDK's ResultMessage.usage aggregates tokens across ALL API sub-calls
  // in a turn (each tool use triggers a new API call). So cache_read can be
  // N× the context window — it's summed across calls, not one call's context.
  //
  // To estimate the ACTUAL context window occupancy for the most recent call,
  // we divide total input tokens by the number of API sub-calls (num_turns).
  // Output tokens are excluded — they don't consume the context window.
  const totalInput = usage.input_tokens + usage.cache_read_input_tokens
    + usage.cache_creation_input_tokens;
  const max = usage.max_context_tokens;
  const numCalls = usage.num_turns || Math.max(1, Math.ceil(totalInput / max));
  const estimatedContext = Math.round(totalInput / numCalls);
  const pct = Math.min((estimatedContext / max) * 100, 100);

  // Cache hit rate (across all sub-calls — this IS a billing metric, so aggregate is correct)
  const cacheRate = totalInput > 0
    ? (usage.cache_read_input_tokens / totalInput * 100)
    : 0;

  // Color based on usage level
  let barColor = '#3b82f6'; // blue
  if (pct > 80) barColor = '#ef4444'; // red
  else if (pct > 60) barColor = '#f59e0b'; // amber

  return (
    <div
      className="relative flex items-center gap-2 cursor-default"
      onMouseEnter={() => setHovering(true)}
      onMouseLeave={() => setHovering(false)}
    >
      <span className="text-[11px] text-text-dim whitespace-nowrap">
        ~{formatTokens(estimatedContext)} / {formatTokens(max)}
      </span>
      <div className="w-20 h-1.5 bg-border-subtle rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{ width: `${pct}%`, backgroundColor: barColor }}
        />
      </div>

      {hovering && (
        <div className="absolute right-0 top-full mt-2 z-50 bg-surface-raised border border-border-subtle rounded-lg p-3 shadow-xl min-w-[220px]">
          <div className="text-[11px] text-text-muted uppercase tracking-wider mb-2">Context Window</div>
          <div className="space-y-1.5 text-[12px]">
            <Row label="Est. context" value={estimatedContext} bold />
            <Row label="Max context" value={max} />
            <Row label="Remaining" value={Math.max(0, max - estimatedContext)} color={pct > 80 ? '#ef4444' : '#22c55e'} />

            {/* Per-turn token breakdown */}
            <div className="border-t border-border-subtle my-1.5" />
            <div className="text-[11px] text-text-muted uppercase tracking-wider mb-1">
              Turn Tokens (cumulative)
            </div>
            <Row label="Fresh input" value={usage.input_tokens} />
            <Row label="Output" value={usage.output_tokens} />
            {usage.cache_read_input_tokens > 0 && (
              <Row label="Cache read" value={usage.cache_read_input_tokens} color="#22c55e" />
            )}
            {usage.cache_creation_input_tokens > 0 && (
              <Row label="Cache created" value={usage.cache_creation_input_tokens} color="#a855f7" />
            )}
            {(usage.cache_creation_5m_input_tokens ?? 0) > 0 && (
              <Row label="  ↳ 5m TTL" value={usage.cache_creation_5m_input_tokens!} color="#c084fc" />
            )}
            {(usage.cache_creation_1h_input_tokens ?? 0) > 0 && (
              <Row label="  ↳ 1h TTL" value={usage.cache_creation_1h_input_tokens!} color="#e879f9" />
            )}
            {numCalls > 1 && (
              <div className="flex justify-between items-center">
                <span className="text-text-muted">API sub-calls</span>
                <span className="text-text-muted">{numCalls}</span>
              </div>
            )}

            {/* Cost section */}
            <div className="border-t border-border-subtle my-1.5" />
            <div className="text-[11px] text-text-muted uppercase tracking-wider mb-1">Cost</div>
            {(sessionCostUsd ?? 0) > 0 && (
              <CostRow label="Session total" value={sessionCostUsd!} bold />
            )}
            {cacheRate > 0 && (
              <div className="flex justify-between items-center">
                <span className="text-text-muted">Cache hit rate</span>
                <span className="text-text-muted" style={{ color: cacheRate > 50 ? '#22c55e' : undefined }}>
                  {cacheRate.toFixed(1)}%
                </span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function Row({ label, value, color, bold }: { label: string; value: number; color?: string; bold?: boolean }) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-text-muted">{label}</span>
      <span className={bold ? 'text-text font-medium' : 'text-text-muted'} style={color ? { color } : undefined}>
        {formatTokens(value)}
      </span>
    </div>
  );
}

function CostRow({ label, value, bold }: { label: string; value: number; bold?: boolean }) {
  const formatted = value < 0.01 ? `$${value.toFixed(4)}` : `$${value.toFixed(2)}`;
  return (
    <div className="flex justify-between items-center">
      <span className="text-text-muted">{label}</span>
      <span className={bold ? 'text-text font-medium' : 'text-text-muted'}>
        {formatted}
      </span>
    </div>
  );
}
