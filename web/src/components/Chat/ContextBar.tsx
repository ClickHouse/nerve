import { useState } from 'react';

interface ContextUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  max_context_tokens: number;
}

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export function ContextBar({ usage }: { usage: ContextUsage }) {
  const [hovering, setHovering] = useState(false);

  const used = usage.input_tokens + usage.output_tokens;
  const max = usage.max_context_tokens;
  const pct = Math.min((used / max) * 100, 100);

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
        {formatTokens(used)} / {formatTokens(max)}
      </span>
      <div className="w-20 h-1.5 bg-border-subtle rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{ width: `${pct}%`, backgroundColor: barColor }}
        />
      </div>

      {hovering && (
        <div className="absolute right-0 top-full mt-2 z-50 bg-surface-raised border border-border-subtle rounded-lg p-3 shadow-xl min-w-[220px]">
          <div className="text-[11px] text-text-muted uppercase tracking-wider mb-2">Context Usage</div>
          <div className="space-y-1.5 text-[12px]">
            <Row label="Input tokens" value={usage.input_tokens} />
            <Row label="Output tokens" value={usage.output_tokens} />
            {usage.cache_read_input_tokens > 0 && (
              <Row label="Cache read" value={usage.cache_read_input_tokens} color="#22c55e" />
            )}
            {usage.cache_creation_input_tokens > 0 && (
              <Row label="Cache created" value={usage.cache_creation_input_tokens} color="#a855f7" />
            )}
            <div className="border-t border-border-subtle my-1.5" />
            <Row label="Total used" value={used} bold />
            <Row label="Max context" value={max} />
            <Row label="Remaining" value={max - used} color={pct > 80 ? '#ef4444' : '#22c55e'} />
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
