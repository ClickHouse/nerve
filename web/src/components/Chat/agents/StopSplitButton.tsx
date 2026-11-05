import { useEffect, useRef, useState } from 'react';
import { useAgentStore } from '../../../stores/agentStore';
import { Button } from '../../ui';
import { ChevronUp, Loader2, Send, Square, OctagonX } from '../../ui/icons';

/**
 * The composer's Stop while a turn streams: the red button stops the turn
 * (background agents keep running where the runtime allows it), and its caret
 * opens "Stop turn", "Stop turn and send" (with text in the composer) and
 * "Stop all agents…" (while agents run).
 *
 * The caret's popup is a disclosure of ordinary buttons (no ARIA menu roles,
 * the same recipe as the composer's "More options").
 */
export function StopSplitButton({ onStopTurn, hasContent, onStopAndSend }: {
  onStopTurn: () => void;
  hasContent: boolean;
  onStopAndSend: () => void;
}) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const caretRef = useRef<HTMLButtonElement>(null);
  const live = useAgentStore(s => s.summary.live);
  const spares = useAgentStore(s => s.capabilities.interrupt_spares_background);
  const stopping = useAgentStore(s => s.stopInFlight);

  // Close on an outside press, and on Escape — which must not also reach the
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
      caretRef.current?.focus();
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey, true);
    };
  }, [open]);

  const pick = (action: () => void) => () => {
    setOpen(false);
    action();
  };

  const agentsNote = live > 0
    ? spares
      ? 'Agents keep running'
      : `Also stops ${live} background agent${live === 1 ? '' : 's'}`
    : null;

  return (
    <div ref={wrapRef} className="relative flex shrink-0">
      {/* Native buttons: a solid destructive fill, which neither Button nor
          IconButton offers at this size. `bg-error-solid` carries white text
          in both themes. */}
      <button
        type="button"
        onClick={onStopTurn}
        aria-label="Stop the turn"
        title="Stop the turn (Esc)"
        className="h-10 w-10 bg-error-solid hover:bg-error-solid/90 text-white rounded-l-xl inline-flex items-center justify-center cursor-pointer transition-colors"
      >
        {stopping
          ? <Loader2 size={16} className="motion-safe:animate-spin" aria-hidden="true" />
          : <Square size={16} aria-hidden="true" />}
      </button>
      <button
        ref={caretRef}
        type="button"
        aria-expanded={open}
        aria-label="Stop options"
        title="Stop options"
        onClick={() => setOpen(v => !v)}
        className="h-10 w-6 bg-error-solid hover:bg-error-solid/90 text-white rounded-r-xl border-l border-white/20 inline-flex items-center justify-center cursor-pointer transition-colors"
      >
        <ChevronUp size={14} aria-hidden="true" className={`transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 bottom-full mb-1 z-50 w-[240px] bg-surface-raised border border-border-subtle rounded-lg shadow-xl py-1">
          <Button
            variant="subtle"
            size="sm"
            fullWidth
            onClick={pick(onStopTurn)}
            className="justify-start items-start gap-2.5 px-3 py-1.5 rounded-none text-left"
          >
            <Square size={14} className="mt-0.5 shrink-0" aria-hidden="true" />
            <span className="flex flex-col">
              <span>Stop turn</span>
              {agentsNote && <span className={`text-2xs ${spares ? 'text-text-faint' : 'text-warning'}`}>{agentsNote}</span>}
            </span>
          </Button>
          {hasContent && (
            <Button
              variant="subtle"
              size="sm"
              fullWidth
              onClick={pick(onStopAndSend)}
              className="justify-start items-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              <Send size={14} className="mt-0.5 shrink-0" aria-hidden="true" />
              <span className="flex flex-col">
                <span>Stop turn and send</span>
                <span className="text-2xs text-text-faint">Your message runs next</span>
              </span>
            </Button>
          )}
          {live > 0 && (
            <>
              <div className="my-1 border-t border-border" />
              <Button
                variant="subtle"
                size="sm"
                fullWidth
                onClick={pick(() => useAgentStore.getState().setStopDialog('all'))}
                className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left text-hue-red hover:text-hue-red"
              >
                <OctagonX size={14} className="shrink-0" aria-hidden="true" />
                Stop all agents…
              </Button>
            </>
          )}
        </div>
      )}
    </div>
  );
}
