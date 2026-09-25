import { useState, useRef, useEffect, useCallback } from 'react';
import { Sparkles, X, RotateCcw, CornerDownLeft, ChevronRight, Loader2 } from '../ui/icons';
import { Button, IconButton, TextArea } from '../ui';

export type RewriteCardState =
  | { status: 'loading' }
  | { status: 'ready'; rewritten: string; model: string }
  | { status: 'error'; message: string };

/** "claude-haiku-4-5-20251001" → "haiku-4-5" */
function formatModel(model: string): string {
  return model.replace(/^claude-/, '').replace(/-\d{8}$/, '');
}

const STATE_LABELS: Record<RewriteCardState['status'], string> = {
  loading: 'Refining prompt',
  ready: 'Refined prompt',
  error: 'Rewrite failed',
};

/**
 * Preview card for the prompt rewrite flow. Shown above the composer
 * after the user sends the first message of a new chat with rewrite
 * enabled. Nothing is sent until the user explicitly approves — either
 * the (editable) rewritten prompt or their original message.
 */
export function PromptRewriteCard({ state, original, onApprove, onSendOriginal, onDiscard, onRetry }: {
  state: RewriteCardState;
  original: string;
  onApprove: (text: string) => void;
  onSendOriginal: () => void;
  onDiscard: () => void;
  onRetry: () => void;
}) {
  const [edited, setEdited] = useState(state.status === 'ready' ? state.rewritten : '');
  const [showOriginal, setShowOriginal] = useState(false);
  const editRef = useRef<HTMLTextAreaElement>(null);

  const autosize = useCallback(() => {
    const el = editRef.current;
    if (el) {
      el.style.height = 'auto';
      el.style.height = Math.min(el.scrollHeight, 280) + 'px';
    }
  }, []);

  // When a new rewrite arrives, reset the editor to it
  // (setState-during-render pattern for derived state).
  const rewritten = state.status === 'ready' ? state.rewritten : null;
  const [lastRewritten, setLastRewritten] = useState<string | null>(null);
  if (rewritten !== null && rewritten !== lastRewritten) {
    setLastRewritten(rewritten);
    setEdited(rewritten);
  }

  // Focus at the end when the rewrite first appears.
  useEffect(() => {
    if (rewritten === null) return;
    const el = editRef.current;
    if (el) {
      el.focus();
      el.setSelectionRange(el.value.length, el.value.length);
    }
  }, [rewritten]);

  // Keep the editor sized to its content (runs after the value renders).
  useEffect(() => {
    autosize();
  }, [edited, autosize]);

  const canApprove = state.status === 'ready' && edited.trim().length > 0;

  return (
    <div className="rewrite-card relative rounded-xl border border-hue-purple/25 bg-surface overflow-hidden shadow-[0_8px_40px_-12px] shadow-hue-purple/25">
      {/* Gradient hairline */}
      <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-hue-purple/60 to-transparent" />

      {/* Header */}
      <div className="flex items-center gap-2 px-4 pt-3 pb-2">
        <Sparkles size={14} className="text-hue-purple shrink-0" />
        <span className="text-xs font-medium uppercase tracking-wider text-hue-purple">
          {STATE_LABELS[state.status]}
        </span>
        {state.status === 'ready' && (
          <span className="text-2xs text-text-muted bg-surface-raised px-1.5 py-0.5 rounded">
            {formatModel(state.model)}
          </span>
        )}
        {state.status === 'ready' && edited !== state.rewritten && (
          <span className="text-2xs text-text-faint italic">edited</span>
        )}
        <div className="flex-1" />
        <IconButton label="Dismiss (Esc)" size="xs" onClick={onDiscard}>
          <X size={15} />
        </IconButton>
      </div>

      {/* Body */}
      {state.status === 'loading' && (
        <div className="px-4 pb-3">
          <div className="flex items-center gap-2 text-sm text-text-muted mb-3">
            <Loader2 size={13} className="animate-spin text-hue-purple" />
            <span>Refining your prompt…</span>
          </div>
          <div className="space-y-2" aria-hidden>
            <div className="rewrite-shimmer h-3 rounded w-[92%]" />
            <div className="rewrite-shimmer h-3 rounded w-[78%]" />
            <div className="rewrite-shimmer h-3 rounded w-[55%]" />
          </div>
          <div className="flex justify-end mt-3">
            <Button variant="ghost" size="sm" onClick={onDiscard} className="rounded-lg">
              Cancel
            </Button>
          </div>
        </div>
      )}

      {state.status === 'ready' && (
        <div className="px-4 pb-3">
          <TextArea
            bare
            ref={editRef}
            value={edited}
            onChange={(e) => setEdited(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (canApprove) onApprove(edited.trim());
              }
            }}
            rows={2}
            className="text-sm leading-relaxed"
            placeholder="Rewritten prompt…"
          />

          {/* Original message — collapsible */}
          <Button
            variant="ghost"
            size="xs"
            onClick={() => setShowOriginal(v => !v)}
            aria-expanded={showOriginal}
            className="mt-1 px-0 py-0 gap-1"
          >
            <ChevronRight
              size={12}
              className={`transition-transform ${showOriginal ? 'rotate-90' : ''}`}
            />
            <span>Original message</span>
          </Button>
          {showOriginal && (
            <div className="mt-1.5 ml-1 pl-3 border-l-2 border-border text-xs text-text-muted whitespace-pre-wrap leading-relaxed max-h-40 overflow-y-auto">
              {original}
            </div>
          )}

          {/* Actions */}
          <div className="flex items-center gap-2 mt-3">
            <span className="text-xs text-text-faint hidden sm:block">
              Enter to send · Esc to dismiss
            </span>
            <div className="flex-1" />
            <Button size="sm" onClick={onSendOriginal}>
              Send original
            </Button>
            <Button
              variant="primary"
              size="sm"
              onClick={() => canApprove && onApprove(edited.trim())}
              disabled={!canApprove}
              className="px-3.5 gap-1.5"
            >
              <CornerDownLeft size={12} />
              <span>Send</span>
            </Button>
          </div>
        </div>
      )}

      {state.status === 'error' && (
        <div className="px-4 pb-3">
          <div className="text-sm text-error/90 mb-3 break-words">
            {state.message}
          </div>
          <div className="flex items-center gap-2 justify-end">
            <Button variant="ghost" size="sm" onClick={onRetry} className="rounded-lg gap-1.5">
              <RotateCcw size={12} />
              <span>Retry</span>
            </Button>
            <Button variant="primary" size="sm" onClick={onSendOriginal} className="px-3.5">
              Send original
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
