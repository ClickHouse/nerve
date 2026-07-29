import { useState } from 'react';
import { ChevronDown, ChevronRight, Repeat, Loader2 } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';

const ENGINES = [
  { id: '', label: 'default' },
  { id: 'claude-workflow', label: 'Claude (Workflow)' },
  { id: 'codex-ultracode', label: 'Codex (Ultracode)' },
];

/**
 * New-chat review-loop config panel. Renders above the composer while the
 * chat is virtual and the toggle is on. "Start review loop" materializes
 * the session with the config bound (ensureRealSession) — the session
 * becomes the loop's observer and milestones stream into it.
 */
export function ReviewLoopPanel({ disabled }: { disabled?: boolean }) {
  const rl = useChatStore(s => s.newChatReviewLoop);
  const setRL = useChatStore(s => s.setNewChatReviewLoop);
  const ensureRealSession = useChatStore(s => s.ensureRealSession);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!rl) return null;
  const canStart = !disabled && !starting && rl.goal.trim().length > 0 && rl.verifier.trim().length > 0;

  const start = async () => {
    if (!canStart) return;
    setStarting(true);
    setError(null);
    try {
      // ensureRealSession reads newChatReviewLoop from the store and binds
      // it into POST /api/sessions — same path the first message takes.
      await ensureRealSession(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setStarting(false);
      return;
    }
    setStarting(false);
  };

  const inputCls = 'w-full px-3 py-2 bg-surface-raised border border-border rounded-lg text-[13px] text-text outline-none focus:border-accent/50 placeholder:text-text-faint';
  const labelCls = 'text-[11px] font-medium uppercase tracking-wider text-text-muted';

  return (
    <div className="px-4 pt-3 pb-1">
      <div className="max-w-[var(--chat-width)] mx-auto">
        <div className="rounded-xl border border-emerald-400/25 bg-emerald-400/5 p-3 space-y-2.5">
          <div className="flex items-center gap-2">
            <Repeat size={14} className="text-emerald-400" />
            <span className="text-[12px] font-medium text-emerald-400 uppercase tracking-wider">Review loop</span>
            <span className="text-[11px] text-text-faint">
              implementer builds toward the goal · an independent verifier judges the criteria · iterates until pass
            </span>
          </div>

          <div className="space-y-1">
            <div className={labelCls}>Goal — what to build</div>
            <textarea
              value={rl.goal}
              onChange={(e) => setRL({ goal: e.target.value })}
              placeholder="Implement X in this repo…"
              rows={3}
              disabled={disabled || starting}
              className={inputCls + ' resize-none'}
            />
          </div>

          <div className="space-y-1">
            <div className={labelCls}>Verifier — completion criteria (one per line; prefer checkable statements)</div>
            <textarea
              value={rl.verifier}
              onChange={(e) => setRL({ verifier: e.target.value })}
              placeholder={'- tests pass (pytest tests/)\n- endpoint /foo returns 200\n- docs updated'}
              rows={3}
              disabled={disabled || starting}
              className={inputCls + ' resize-none'}
            />
          </div>

          <div className="flex items-end gap-3 flex-wrap">
            <div className="space-y-1">
              <div className={labelCls}>Budget $</div>
              <input
                type="number"
                min="0.5"
                step="0.5"
                value={rl.budget}
                onChange={(e) => setRL({ budget: e.target.value })}
                placeholder="10"
                disabled={disabled || starting}
                className={inputCls + ' w-24'}
              />
            </div>
            <button
              onClick={() => setAdvancedOpen(v => !v)}
              className="h-9 px-2 flex items-center gap-1 text-[12px] text-text-muted hover:text-text-secondary cursor-pointer transition-colors"
            >
              {advancedOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
              advanced
            </button>
            <div className="flex-1" />
            <button
              onClick={() => void start()}
              disabled={!canStart}
              className="h-9 px-4 bg-emerald-500/80 hover:bg-emerald-500 text-white text-[13px] font-medium rounded-lg flex items-center gap-2 disabled:opacity-30 cursor-pointer transition-colors"
              title="Create the session and start the loop — milestones will appear in this chat"
            >
              {starting ? <Loader2 size={14} className="animate-spin" /> : <Repeat size={14} />}
              Start review loop
            </button>
          </div>

          {advancedOpen && (
            <div className="grid grid-cols-2 gap-3 pt-1 border-t border-border-subtle">
              <div className="space-y-1 col-span-2">
                <div className={labelCls}>Workdir — shared workspace for all legs (created if missing)</div>
                <input
                  type="text"
                  value={rl.cwd}
                  onChange={(e) => setRL({ cwd: e.target.value })}
                  placeholder="default: the global workspace"
                  disabled={disabled || starting}
                  className={inputCls + ' font-mono'}
                  spellCheck={false}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Implementer engine</div>
                <select
                  value={rl.implementerEngine}
                  onChange={(e) => setRL({ implementerEngine: e.target.value })}
                  disabled={disabled || starting}
                  className={inputCls + ' cursor-pointer'}
                >
                  {ENGINES.map(e => <option key={e.id} value={e.id}>{e.label}</option>)}
                </select>
                <input
                  type="text"
                  value={rl.implementerModel}
                  onChange={(e) => setRL({ implementerModel: e.target.value })}
                  placeholder="model (default)"
                  disabled={disabled || starting}
                  className={inputCls}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Verifier engine</div>
                <select
                  value={rl.verifierEngine}
                  onChange={(e) => setRL({ verifierEngine: e.target.value })}
                  disabled={disabled || starting}
                  className={inputCls + ' cursor-pointer'}
                >
                  {ENGINES.map(e => <option key={e.id} value={e.id}>{e.label}</option>)}
                </select>
                <input
                  type="text"
                  value={rl.verifierModel}
                  onChange={(e) => setRL({ verifierModel: e.target.value })}
                  placeholder="model (default)"
                  disabled={disabled || starting}
                  className={inputCls}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Criteria adoption</div>
                <select
                  value={rl.adoption}
                  onChange={(e) => setRL({ adoption: e.target.value as 'no' | 'ask' | 'auto' })}
                  disabled={disabled || starting}
                  className={inputCls + ' cursor-pointer'}
                  title="What happens when the verifier discovers missing criteria: advisory only (no), ask you (ask), or auto-adopt with caps (auto — research goals)"
                >
                  <option value="no">no — criteria are fixed</option>
                  <option value="ask">ask — adoption needs my decision</option>
                  <option value="auto">auto — verifier may extend (capped)</option>
                </select>
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Max iterations</div>
                <input
                  type="number"
                  min="1"
                  max="8"
                  value={rl.maxIterations}
                  onChange={(e) => setRL({ maxIterations: e.target.value })}
                  placeholder="3"
                  disabled={disabled || starting}
                  className={inputCls + ' w-24'}
                />
              </div>
            </div>
          )}

          {error && (
            <div className="text-[12px] text-error">{error}</div>
          )}
        </div>
      </div>
    </div>
  );
}
