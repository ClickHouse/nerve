import { useEffect, useState } from 'react';
import { api, type TaskEvent } from '../../api/client';
import { StatusBadge } from './StatusControls';
import { formatTimeAgo } from '../../utils/dateGroups';

/** Human duration between two ISO stamps, at the coarsest useful unit. */
function span(fromIso: string, toIso: string): string {
  const ms = new Date(toIso).getTime() - new Date(fromIso).getTime();
  if (Number.isNaN(ms) || ms < 0) return '';
  const mins = Math.round(ms / 60_000);
  if (mins < 60) return `${mins}m`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.round(hours / 24)}d`;
}

/**
 * A task's status history.
 *
 * Reads as "how long did each stage take", not just "what happened" — the
 * dwell time between consecutive events is the part that answers why a
 * task took as long as it did, and it's why the transitions are stored
 * rather than derived.
 */
export function TaskTimeline({ taskId, currentStatus }: {
  taskId: string;
  currentStatus: string;
}) {
  const [events, setEvents] = useState<TaskEvent[] | null>(null);

  useEffect(() => {
    let cancelled = false;
    api.listTaskEvents(taskId)
      .then(({ events }) => { if (!cancelled) setEvents(events); })
      .catch(() => { if (!cancelled) setEvents([]); });
    return () => { cancelled = true; };
  }, [taskId]);

  if (events === null) {
    return <p className="text-xs text-text-faint">Loading history...</p>;
  }
  if (events.length === 0) {
    // Tasks that last changed status before v044 have no history, and
    // inventing one would be worse than saying so.
    return <p className="text-xs text-text-faint">No status history recorded.</p>;
  }

  const nowIso = new Date().toISOString();

  return (
    <ol className="space-y-2.5">
      {events.map((event, i) => {
        const next = events[i + 1];
        const dwell = span(event.created_at, next?.created_at ?? nowIso);
        const isCurrent = !next && event.to_status === currentStatus;
        return (
          <li key={event.id} className="flex items-start gap-2.5 text-xs">
            <span
              className={`mt-1 w-1.5 h-1.5 rounded-full shrink-0
                ${isCurrent ? 'bg-accent' : 'bg-border'}`}
            />
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2 flex-wrap">
                {event.from_status ? (
                  <>
                    <span className="text-text-faint">{event.from_status}</span>
                    <span className="text-text-faint">→</span>
                  </>
                ) : (
                  <span className="text-text-faint">created as</span>
                )}
                <StatusBadge status={event.to_status} />
                {dwell && (
                  <span className="text-text-faint">
                    {isCurrent ? `for ${dwell}` : `· ${dwell}`}
                  </span>
                )}
              </div>
              <div className="text-text-faint mt-0.5">
                <span title={event.created_at}>{formatTimeAgo(event.created_at)}</span>
                {event.actor && event.actor !== 'system' && (
                  <span> · {event.actor}</span>
                )}
              </div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
