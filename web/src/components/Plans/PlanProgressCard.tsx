import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ws, type WSMessage } from '../../api/websocket';
import { useNow } from '../../hooks/useNow';
import { usePlanStore, type Plan } from '../../stores/planStore';
import { parseTimestamp } from '../../utils/dateGroups';
import { formatElapsed } from '../Chat/agents/agentVisuals';
import { implementationState, type ImplState } from './planProgress';
import { Button, cx } from '../ui';
import { CircleCheck, CircleX, MessageSquare } from '../ui/icons';

/** The words of each state, and the dot of a live one (the session list's hues). */
const STATES: Record<ImplState, { label: string; dot?: string }> = {
  running: { label: 'Running', dot: 'bg-hue-emerald motion-safe:animate-pulse' },
  parked: { label: 'Waiting on background work', dot: 'bg-hue-violet motion-safe:animate-pulse' },
  waiting: { label: 'Waiting for you', dot: 'bg-hue-blue' },
  done: { label: 'Done' },
  failed: { label: 'Failed' },
};

/** Session events after which the card loads the plan again. */
const SESSION_EVENTS = new Set<WSMessage['type']>([
  'session_running', 'session_awaiting_input', 'session_agents', 'session_updated',
]);

/** Whether a WS event can change the state of this implementation. */
function concerns(msg: WSMessage, sessionId: string, taskId: string): boolean {
  if (msg.type === 'task_updated') return msg.task.id === taskId;
  return SESSION_EVENTS.has(msg.type) && 'session_id' in msg && msg.session_id === sessionId;
}

/**
 * The implementation session of an approved plan: its title, its state and
 * the time since the approval. While the plan is implementing, the card loads
 * the plan again on each event of the session or the task, and on each
 * reconnect, so it follows the session live.
 */
export function PlanProgressCard({ plan }: { plan: Plan }) {
  const navigate = useNavigate();
  const refreshPlan = usePlanStore(s => s.refreshPlan);
  const sessionId = plan.impl_session_id;
  const live = plan.status === 'implementing';
  const now = useNow(1000, live);

  useEffect(() => {
    if (!live || !sessionId) return;
    const refresh = () => void refreshPlan(plan.id);
    const offMessage = ws.onMessage(msg => {
      if (concerns(msg, sessionId, plan.task_id)) refresh();
    });
    const offOpen = ws.onOpen(refresh);
    return () => {
      offMessage();
      offOpen();
    };
  }, [live, sessionId, plan.id, plan.task_id, refreshPlan]);

  if (!sessionId) return null;

  const state = implementationState(plan);
  const ended = state === 'done' || state === 'failed';
  const startedAt = plan.reviewed_at ? parseTimestamp(plan.reviewed_at).getTime() : null;
  const endedAt = !ended ? now
    : plan.impl_session?.updated_at ? parseTimestamp(plan.impl_session.updated_at).getTime() : null;
  const elapsed = startedAt !== null && endedAt !== null ? formatElapsed(endedAt - startedAt) : '';
  const look = state ? STATES[state] : null;

  return (
    <section
      aria-label="Implementation"
      className="mb-4 flex flex-wrap items-center gap-x-3 gap-y-2 rounded-lg border border-border-subtle bg-surface px-4 py-3"
    >
      {/* On a phone the button wraps under the text, which keeps the full width. */}
      <div className="min-w-0 flex-1 basis-60">
        <p className="line-clamp-2 text-sm font-medium text-text">
          {plan.impl_session?.title || 'Implementation session'}
        </p>
        {look && (
          <p aria-live="polite" className="mt-1 flex items-center gap-1.5 text-xs text-text-muted">
            {state === 'done' ? <CircleCheck size={13} className="shrink-0 text-success" aria-hidden="true" />
              : state === 'failed' ? <CircleX size={13} className="shrink-0 text-error" aria-hidden="true" />
              : <span className={cx('h-2 w-2 shrink-0 rounded-full', look.dot)} aria-hidden="true" />}
            <span>
              {look.label}
              {elapsed && <span className="tabular-nums">{ended ? ` after ${elapsed}` : ` · ${elapsed}`}</span>}
            </span>
          </p>
        )}
      </div>
      <Button variant="secondary" size="sm" className="shrink-0" onClick={() => navigate(`/chat/${sessionId}`)}>
        <MessageSquare size={12} />
        Open session
      </Button>
    </section>
  );
}
