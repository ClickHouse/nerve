import type { ReactNode } from 'react';
import type { SessionContext } from '../../api/client';
import { Button } from '../ui';
import { Loader2 } from '../ui/icons';
import {
  COMPACT_CONFIRM_TEXT, formatDollars, formatTokens, meterColor, turnCalls, turnInputTokens,
  type ContextUsage, type SessionCost,
} from './contextUsage';

/**
 * Fill of each context category, by the runtime's category name, so a
 * category keeps its colour whatever else is in the list. One set for both
 * themes. Checked for colour-blind and normal vision on both popover
 * surfaces: the five categories of a usual session (system prompt, system
 * tools, MCP tools, skills, messages) differ from each other, and each of the
 * seven differs from its neighbours in the runtime's order. The legend names
 * every segment, so colour never carries the category alone.
 */
const CATEGORY_COLORS: Record<string, string> = {
  'System prompt': '#aa00ff',
  'System tools': '#d55181',
  'MCP tools': '#008300',
  'Custom agents': '#9085e9',
  'Memory files': '#bb33ff',
  Skills: '#959900',
  Messages: '#00a0ba',
};
const OTHER_CATEGORY_COLOR = 'var(--palette-neutral-500)';

export type CompactState =
  | { step: 'idle' }
  | { step: 'confirm' }
  | { step: 'running' }
  | { step: 'done'; freed: number }
  | { step: 'error'; message: string };

/**
 * The context popover: the context use by category (live Claude sessions) or
 * the totals, the last turn's tokens (or, while a turn runs, its calls so
 * far) and the cost, and Compact.
 */
export function ContextPopover({
  id, used, exact, max, context, loadFailed, usage, live = false, backend, busy, cost,
  compact, onCompact, onConfirm, onCancel,
}: {
  id: string;
  used: number;
  exact: boolean;
  max: number;
  /** null while it loads. */
  context: SessionContext | null;
  loadFailed: boolean;
  usage: ContextUsage;
  /** `usage` is the running turn's calls so far. */
  live?: boolean;
  backend: string;
  busy: boolean;
  cost: SessionCost;
  compact: CompactState;
  onCompact: () => void;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const pct = max > 0 ? Math.min((used / max) * 100, 100) : 0;
  const categories = context?.live ? context.categories : [];
  const threshold = context?.live ? context.auto_compact_threshold : null;

  let note: ReactNode = null;
  if (loadFailed) note = 'Could not load the breakdown.';
  else if (!context) note = <><Loader2 size={12} className="motion-safe:animate-spin" aria-hidden="true" />Loading the breakdown…</>;
  else if (!context.live) {
    note = backend === 'codex'
      ? 'Codex does not report a breakdown.'
      : 'The breakdown shows after the next message.';
  }

  return (
    <div
      id={id}
      role="dialog"
      aria-label="Context window"
      className="absolute right-0 top-full mt-2 z-50 w-[min(20rem,calc(100vw-1.5rem))] max-h-[calc(100dvh-7rem)] overflow-y-auto bg-surface-raised border border-border-subtle rounded-lg shadow-xl p-3 text-xs"
    >
      <SectionTitle>Context window</SectionTitle>
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-medium text-text tabular-nums">
          {exact ? '' : '~'}{formatTokens(used)}
          <span className="text-xs font-normal text-text-muted"> of {formatTokens(max)} tokens</span>
        </span>
        <span className="text-text-muted tabular-nums">{Math.round(pct)}%</span>
      </div>
      <CategoryBar categories={categories} used={used} max={max} threshold={threshold} />
      {categories.length > 0 && (
        <ul className="mt-3 space-y-1.5" aria-label="Context by category">
          {categories.map(c => (
            <li key={c.name} className="flex items-center gap-2">
              <span
                className="w-2 h-2 rounded-sm shrink-0"
                style={{ backgroundColor: CATEGORY_COLORS[c.name] ?? OTHER_CATEGORY_COLOR }}
                aria-hidden="true"
              />
              <span className="flex-1 min-w-0 truncate text-text-secondary">{c.name}</span>
              <span className="tabular-nums text-text">{formatTokens(c.tokens)}</span>
            </li>
          ))}
        </ul>
      )}
      {threshold != null && (
        <p className="mt-2 flex items-center gap-2 text-text-muted">
          <span className="w-0.5 h-3 rounded-full bg-text-muted shrink-0" aria-hidden="true" />
          Auto-compact at {formatTokens(threshold)}
        </p>
      )}
      {note && <p className="mt-2 flex items-center gap-1.5 text-text-muted">{note}</p>}

      {context?.can_compact && (
        <CompactSection state={compact} busy={busy} onCompact={onCompact} onConfirm={onConfirm} onCancel={onCancel} />
      )}

      <LastTurn usage={usage} live={live} />
      <Cost usage={usage} cost={cost} />
    </div>
  );
}

function SectionTitle({ children }: { children: ReactNode }) {
  return <div className="text-xs text-text-muted uppercase tracking-wider mb-2">{children}</div>;
}

/** One segment per category on a track of the whole window, 2px apart, and a
 *  tick at the auto-compact threshold. The legend carries the values. */
function CategoryBar({ categories, used, max, threshold }: {
  categories: SessionContext['categories'];
  used: number;
  max: number;
  threshold: number | null;
}) {
  const share = (n: number) => `${max > 0 ? Math.min((n / max) * 100, 100) : 0}%`;
  return (
    <div className="relative mt-2" aria-hidden="true">
      <div className="flex gap-[2px] h-2 rounded-full overflow-hidden">
        {categories.length > 0
          ? categories.map(c => (
            <div
              key={c.name}
              title={`${c.name}: ${formatTokens(c.tokens)}`}
              className="h-full shrink-0 min-w-[2px]"
              style={{ width: share(c.tokens), backgroundColor: CATEGORY_COLORS[c.name] ?? OTHER_CATEGORY_COLOR }}
            />
          ))
          : (
            <div
              className="h-full shrink-0"
              style={{ width: share(used), backgroundColor: meterColor((used / (max || 1)) * 100) }}
            />
          )}
        <div className="h-full flex-1 bg-border" />
      </div>
      {threshold != null && (
        <div
          className="absolute -top-1 -bottom-1 w-0.5 -ml-px rounded-full bg-text-muted"
          style={{ left: share(threshold) }}
        />
      )}
    </div>
  );
}

function CompactSection({ state, busy, onCompact, onConfirm, onCancel }: {
  state: CompactState;
  busy: boolean;
  onCompact: () => void;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  if (state.step === 'confirm') {
    return (
      <div className="mt-3 pt-3 border-t border-border">
        <p className="text-text-secondary leading-relaxed">{COMPACT_CONFIRM_TEXT}</p>
        <div className="mt-2.5 flex justify-end gap-2">
          <Button variant="ghost" size="xs" className="px-2 py-1 text-text-muted hover:text-text" onClick={onCancel}>
            Cancel
          </Button>
          <Button variant="primary" size="xs" className="px-2.5 py-1" onClick={onConfirm} autoFocus>
            Compact
          </Button>
        </div>
      </div>
    );
  }
  const running = state.step === 'running';
  let line: ReactNode = null;
  if (running) line = <span className="text-text-muted">This can take a minute.</span>;
  else if (state.step === 'done') {
    line = (
      <span className="text-success">
        {state.freed > 0 ? `Freed ${formatTokens(state.freed)} tokens.` : 'Compacted.'}
      </span>
    );
  } else if (state.step === 'error') line = <span className="text-error">{state.message}</span>;
  else if (busy) line = <span className="text-text-muted">Available when the agent is idle.</span>;
  return (
    <div className="mt-3 pt-3 border-t border-border">
      <Button variant="secondary" size="xs" fullWidth className="py-1.5" disabled={busy || running} onClick={onCompact}>
        {running && <Loader2 size={12} className="motion-safe:animate-spin" aria-hidden="true" />}
        {running ? 'Compacting…' : 'Compact…'}
      </Button>
      {line && <p className="mt-1.5" role="status">{line}</p>}
    </div>
  );
}

function LastTurn({ usage, live }: { usage: ContextUsage; live: boolean }) {
  if (usage.input_tokens + usage.output_tokens + usage.cache_read_input_tokens === 0) return null;
  const calls = turnCalls(usage);
  return (
    <div className="mt-3 pt-3 border-t border-border space-y-1.5 leading-tight">
      <SectionTitle>{live ? 'This turn so far' : 'Last turn'}</SectionTitle>
      <Row label="Fresh input" value={usage.input_tokens} />
      <Row label="Output" value={usage.output_tokens} />
      {usage.cache_read_input_tokens > 0 && (
        <Row label="Cache read" value={usage.cache_read_input_tokens} color="var(--theme-success)" />
      )}
      {usage.cache_creation_input_tokens > 0 && (
        <Row label="Cache created" value={usage.cache_creation_input_tokens} color="var(--theme-hue-purple)" />
      )}
      {(usage.cache_creation_5m_input_tokens ?? 0) > 0 && (
        <Row label="  ↳ 5m TTL" value={usage.cache_creation_5m_input_tokens!} color="var(--theme-hue-violet)" />
      )}
      {(usage.cache_creation_1h_input_tokens ?? 0) > 0 && (
        <Row label="  ↳ 1h TTL" value={usage.cache_creation_1h_input_tokens!} color="var(--theme-hue-violet)" />
      )}
      {calls > 1 && (
        <div className="flex justify-between items-center">
          <span className="text-text-muted">API sub-calls</span>
          <span className="text-text-muted">{calls}</span>
        </div>
      )}
    </div>
  );
}

function Cost({ usage, cost }: { usage: ContextUsage; cost: SessionCost }) {
  // Cache hit rate over all model calls of the turn: a billing metric, so the
  // sum over the calls is right here.
  const input = turnInputTokens(usage);
  const cacheRate = input > 0 ? (usage.cache_read_input_tokens / input) * 100 : 0;
  if (!(cost.total > 0) && cacheRate <= 0) return null;
  return (
    <div className="mt-3 pt-3 border-t border-border space-y-1.5 leading-tight">
      <SectionTitle>Cost</SectionTitle>
      {cost.total > 0 && (
        <CostRow label={cost.turn ? 'Session (live)' : 'Session total'} value={cost.total} estimate={cost.estimate} bold />
      )}
      {cost.turn && (
        <CostRow
          label={`This turn, ${cost.turn.calls} call${cost.turn.calls === 1 ? '' : 's'}`}
          value={cost.turn.costUsd}
          estimate
        />
      )}
      {cost.agentsUsd > 0 && (
        <CostRow label={cost.turn ? 'Agents this turn' : 'Agents since the last turn'} value={cost.agentsUsd} estimate />
      )}
      {cacheRate > 0 && (
        <div className="flex justify-between items-center">
          <span className="text-text-muted">Cache hit rate</span>
          <span className={cacheRate > 50 ? 'text-success' : 'text-text-muted'}>{cacheRate.toFixed(1)}%</span>
        </div>
      )}
      {cost.estimate && (
        <p className="text-text-faint pt-1">
          ~ is an estimate at list prices. The billed cost replaces it when the turn ends.
        </p>
      )}
    </div>
  );
}

function Row({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-text-muted">{label}</span>
      <span className="text-text-muted tabular-nums" style={color ? { color } : undefined}>
        {formatTokens(value)}
      </span>
    </div>
  );
}

function CostRow({ label, value, estimate, bold }: {
  label: string; value: number; estimate?: boolean; bold?: boolean;
}) {
  return (
    <div className="flex justify-between items-center gap-3">
      <span className="text-text-muted">{label}</span>
      <span className={`tabular-nums ${bold ? 'text-text font-medium' : 'text-text-muted'}`}>
        {estimate ? '~' : ''}{formatDollars(value)}
      </span>
    </div>
  );
}
