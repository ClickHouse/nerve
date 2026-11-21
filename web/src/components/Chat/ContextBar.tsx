import { useEffect, useRef, useState } from 'react';
import { api, apiErrorDetail, type SessionContext } from '../../api/client';
import { ContextPopover, type CompactState } from './ContextPopover';
import type { LiveUsage } from '../../api/websocket';
import {
  contextTokens, formatDollars, formatTokens, liveContextUsage, meterColor, sessionCost, type ContextUsage,
} from './contextUsage';

const POPOVER_ID = 'context-popover';

/**
 * The chat header's context meter and the session's cost in dollars. A click
 * opens the context popover, which loads the breakdown of the session's
 * context and offers Compact. Render it with `key={sessionId}`, so a session
 * switch starts it fresh.
 *
 * While a turn runs, `live` (the server's usage_update, once per model call
 * start and end) gives the latest call's context and a cost estimate, so both
 * move during the turn instead of after it. An estimate reads "~$".
 */
export function ContextBar({
  usage, live, sessionId, backend, busy, sessionCostUsd, agentsCostUsd, agentsCostBase,
}: {
  /** The last turn's usage; null before the session's first turn ends. */
  usage: ContextUsage | null;
  /** The running turn's estimate; null between turns. */
  live?: LiveUsage | null;
  sessionId: string;
  backend: string;
  /** A turn runs: Compact is disabled until the agent is idle. */
  busy: boolean;
  sessionCostUsd?: number;
  /** The agents' estimated cost so far, and the part the billed total covers. */
  agentsCostUsd?: number | null;
  agentsCostBase?: number | null;
}) {
  const [open, setOpen] = useState(false);
  // The last breakdown, with the usage it was loaded after: the next turn's
  // usage makes it old.
  const [loaded, setLoaded] = useState<{ usage: ContextUsage | null; context: SessionContext } | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [compact, setCompact] = useState<CompactState>({ step: 'idle' });
  const wrapRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  // Load the breakdown when the popover opens, and again after each turn.
  useEffect(() => {
    if (!open) return;
    let stale = false;
    api.getSessionContext(sessionId)
      .then(context => {
        if (stale) return;
        setLoaded({ usage, context });
        setLoadFailed(false);
      })
      .catch(() => { if (!stale) setLoadFailed(true); });
    return () => { stale = true; };
  }, [open, sessionId, usage]);

  // Close on an outside press, and on Escape, which must not also reach the
  // global Escape that stops the turn.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      e.preventDefault();
      e.stopPropagation();
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey, true);
    };
  }, [open]);

  // While a turn runs its calls so far stand for the turn; the meter shows
  // the latest call's context. Between turns it shows the measured use while
  // no turn ended after it.
  const shown = live ? liveContextUsage(live) : usage;
  const cost = sessionCost({ live, sessionCostUsd, agentsCostUsd, agentsCostBase });
  if (!shown) return null;
  const measured = !live && loaded?.usage === usage && loaded.context.live ? loaded.context : null;
  const estimate = contextTokens(shown);
  const used = measured?.total_tokens ?? estimate.tokens;
  const exact = measured != null || estimate.exact;
  const max = measured?.max_tokens || shown.max_context_tokens;
  const pct = max > 0 ? Math.min((used / max) * 100, 100) : 0;

  const toggle = () => {
    if (!open && compact.step !== 'running') setCompact({ step: 'idle' });
    setOpen(!open);
  };

  const runCompact = async () => {
    setCompact({ step: 'running' });
    try {
      const after = await api.compactSession(sessionId);
      setLoaded({ usage, context: after });
      setCompact({ step: 'done', freed: used - after.total_tokens });
    } catch (e) {
      const message = apiErrorDetail(e)?.message;
      setCompact({ step: 'error', message: typeof message === 'string' ? message : 'Compact failed.' });
    }
  };

  return (
    <div ref={wrapRef} className="relative flex items-center gap-2">
      <button
        ref={triggerRef}
        type="button"
        aria-expanded={open}
        aria-controls={open ? POPOVER_ID : undefined}
        aria-label={`Context window: ${exact ? '' : 'about '}${formatTokens(used)} of ${formatTokens(max)} tokens`}
        onClick={toggle}
        // On a phone, the pseudo-element makes the tap target 44 px high
        // (28 px + 2 x 8 px) and keeps the header and the meter as they are.
        className="relative flex items-center gap-2 h-7 px-1.5 rounded hover:bg-surface-raised cursor-pointer transition-colors before:absolute before:inset-x-0 before:-inset-y-2 md:before:hidden"
      >
        <span className="hidden md:inline text-xs text-text-dim whitespace-nowrap tabular-nums">
          {exact ? '' : '~'}{formatTokens(used)} / {formatTokens(max)}
        </span>
        <span className="md:hidden text-xs text-text-dim tabular-nums">{Math.round(pct)}%</span>
        <span className="w-10 md:w-20 h-1.5 bg-border-subtle rounded-full overflow-hidden">
          <span
            className="block h-full rounded-full transition-all duration-300"
            style={{ width: `${pct}%`, backgroundColor: meterColor(pct) }}
          />
        </span>
      </button>
      {cost.total > 0 && (
        <span
          className="hidden md:inline text-xs text-text-dim whitespace-nowrap tabular-nums"
          aria-label={`Session cost: ${cost.estimate ? 'about ' : ''}${formatDollars(cost.total)}`}
        >
          {cost.estimate ? '~' : ''}{formatDollars(cost.total)}
        </span>
      )}
      {open && (
        <ContextPopover
          id={POPOVER_ID}
          used={used}
          exact={exact}
          max={max}
          context={loaded?.context ?? null}
          loadFailed={loadFailed}
          usage={shown}
          live={Boolean(live)}
          backend={backend}
          busy={busy}
          cost={cost}
          compact={compact}
          onCompact={() => setCompact({ step: 'confirm' })}
          onConfirm={() => void runCompact()}
          onCancel={() => setCompact({ step: 'idle' })}
        />
      )}
    </div>
  );
}
