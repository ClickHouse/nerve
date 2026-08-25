import { useState } from 'react';
import { ChevronDown, ChevronRight, Repeat, Loader2 } from '../ui/icons';
import { Button, Select, TextArea, TextField, type SelectOption } from '../ui';
import { useChatStore } from '../../stores/chatStore';

const ENGINES: SelectOption[] = [
  { value: '', label: 'default' },
  { value: 'claude-workflow', label: 'Claude (Workflow)' },
  { value: 'codex-ultracode', label: 'Codex (Ultracode)' },
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

  const labelCls = 'text-xs font-medium uppercase tracking-wider text-text-muted';
  const busy = disabled || starting;

  return (
    <div className="px-4 pt-3 pb-1">
      {/* No `max-w-[var(--chat-width)]`: this renders inside the composer, and
          the composer stack is not capped to the reading column. See the note
          on the main input row in ChatInput. */}
      <div>
        {/* A low-alpha wash of the success *foreground* rather than
            `bg-success-bg`: this is a full-width config panel, and the opaque
            feedback tint is sized for a chip or an alert, not a slab. */}
        <div className="rounded-xl border border-success-border bg-success/5 p-3 space-y-2.5">
          <div className="flex items-center gap-2">
            <Repeat size={14} className="text-success" />
            <span className="text-xs font-medium text-success uppercase tracking-wider">Review loop</span>
            <span className="text-xs text-text-faint">
              implementer builds toward the goal · an independent verifier judges the criteria · iterates until pass
            </span>
          </div>

          <div className="space-y-1">
            <div className={labelCls}>Goal — what to build</div>
            <TextArea
              value={rl.goal}
              onChange={(e) => setRL({ goal: e.target.value })}
              placeholder="Implement X in this repo…"
              rows={3}
              disabled={busy}
            />
          </div>

          <div className="space-y-1">
            <div className={labelCls}>Verifier — completion criteria (one per line; prefer checkable statements)</div>
            <TextArea
              value={rl.verifier}
              onChange={(e) => setRL({ verifier: e.target.value })}
              placeholder={'- tests pass (pytest tests/)\n- endpoint /foo returns 200\n- docs updated'}
              rows={3}
              disabled={busy}
            />
          </div>

          <div className="flex items-end gap-3 flex-wrap">
            <div className="space-y-1">
              <div className={labelCls}>Budget $</div>
              <TextField
                type="number"
                min="0.5"
                step="0.5"
                fullWidth={false}
                value={rl.budget}
                onChange={(e) => setRL({ budget: e.target.value })}
                placeholder="10"
                disabled={busy}
                className="w-24"
              />
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setAdvancedOpen(v => !v)}
              aria-expanded={advancedOpen}
              className="h-9 px-2 gap-1"
            >
              {advancedOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
              advanced
            </Button>
            <div className="flex-1" />
            <Button
              variant="success"
              size="md"
              onClick={() => void start()}
              disabled={!canStart}
              className="h-9 px-4 rounded-lg"
              title="Create the session and start the loop — milestones will appear in this chat"
            >
              {starting ? <Loader2 size={14} className="animate-spin" /> : <Repeat size={14} />}
              Start review loop
            </Button>
          </div>

          {advancedOpen && (
            <div className="grid grid-cols-2 gap-3 pt-1 border-t border-border-subtle">
              <div className="space-y-1 col-span-2">
                <div className={labelCls}>Workdir — shared workspace for all legs (created if missing)</div>
                <TextField
                  value={rl.cwd}
                  onChange={(e) => setRL({ cwd: e.target.value })}
                  placeholder="default: the global workspace"
                  disabled={busy}
                  className="font-mono"
                  spellCheck={false}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Implementer engine</div>
                <Select
                  fullWidth
                  options={ENGINES}
                  value={rl.implementerEngine}
                  onChange={(e) => setRL({ implementerEngine: e.target.value })}
                  disabled={busy}
                />
                <TextField
                  value={rl.implementerModel}
                  onChange={(e) => setRL({ implementerModel: e.target.value })}
                  placeholder="model (default)"
                  disabled={busy}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Verifier engine</div>
                <Select
                  fullWidth
                  options={ENGINES}
                  value={rl.verifierEngine}
                  onChange={(e) => setRL({ verifierEngine: e.target.value })}
                  disabled={busy}
                />
                <TextField
                  value={rl.verifierModel}
                  onChange={(e) => setRL({ verifierModel: e.target.value })}
                  placeholder="model (default)"
                  disabled={busy}
                />
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Criteria adoption</div>
                <Select
                  fullWidth
                  value={rl.adoption}
                  onChange={(e) => setRL({ adoption: e.target.value as 'no' | 'ask' | 'auto' })}
                  disabled={busy}
                  title="What happens when the verifier discovers missing criteria: advisory only (no), ask you (ask), or auto-adopt with caps (auto — research goals)"
                >
                  <option value="no">no — criteria are fixed</option>
                  <option value="ask">ask — adoption needs my decision</option>
                  <option value="auto">auto — verifier may extend (capped)</option>
                </Select>
              </div>
              <div className="space-y-1">
                <div className={labelCls}>Max iterations</div>
                <TextField
                  type="number"
                  min="1"
                  max="8"
                  fullWidth={false}
                  value={rl.maxIterations}
                  onChange={(e) => setRL({ maxIterations: e.target.value })}
                  placeholder="3"
                  disabled={busy}
                  className="w-24"
                />
              </div>
            </div>
          )}

          {error && (
            <div className="text-xs text-error">{error}</div>
          )}
        </div>
      </div>
    </div>
  );
}
