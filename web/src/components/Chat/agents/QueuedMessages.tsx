import { useLayoutEffect, useMemo, useRef } from 'react';
import type { QueueEntryView } from '../../../types/agents';
import { useAgentStore, type PendingSend } from '../../../stores/agentStore';
import { useAuthStore } from '../../../stores/authStore';
import { useChatStore } from '../../../stores/chatStore';
import { IconButton } from '../../ui';
import { ChevronDown, ChevronUp, Clock, Loader2, Pencil, X } from '../../ui/icons';
import { focusComposer } from './agentActions';

type Row =
  | { kind: 'queued'; entry: QueueEntryView }
  | { kind: 'sending'; send: PendingSend };

type Direction = 'up' | 'down';

/** Row actions: a larger hit area on a phone, the inline size from `md` up. */
const ACTION_CLASS = 'p-1.5 md:p-1';

function flat(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

/** `ids` with `id` moved next to `target`: before it, or after it. */
function moveBeside(ids: string[], id: string, target: string, after: boolean): string[] {
  const rest = ids.filter(x => x !== id);
  const at = rest.indexOf(target) + (after ? 1 : 0);
  return [...rest.slice(0, at), id, ...rest.slice(at)];
}

/**
 * Messages waiting for the running turn to end, one row each, above the
 * composer and in run order (the top row runs next): the server's queue
 * (from every tab and channel) plus this tab's sends the server has not
 * confirmed yet ("Sending…"). A message that is already a bubble in this tab
 * is not repeated. Any queued row can move up or down; your own rows can
 * also be cancelled (×) or taken back into the composer to edit (✎).
 * Internal entries are shown muted.
 */
export function QueuedMessages() {
  const queue = useAgentStore(s => s.queue);
  const pendingSends = useAgentStore(s => s.pendingSends);
  const messages = useChatStore(s => s.messages);
  const self = useAuthStore(s => s.viewer?.id ?? null);
  const rowRefs = useRef(new Map<string, HTMLLIElement>());
  const refocus = useRef<{ id: string; dir: Direction } | null>(null);

  const rows = useMemo<Row[]>(() => {
    const bubbles = new Set(messages.map(m => m.client_msg_id).filter((id): id is string => !!id));
    const queuedIds = new Set(queue.map(q => q.client_msg_id).filter((id): id is string => !!id));
    return [
      ...queue
        .filter(entry => !entry.client_msg_id || !bubbles.has(entry.client_msg_id))
        .map(entry => ({ kind: 'queued' as const, entry })),
      ...Object.values(pendingSends)
        .filter(send => !queuedIds.has(send.clientMsgId) && !bubbles.has(send.clientMsgId))
        .sort((a, b) => a.at - b.at)
        .map(send => ({ kind: 'sending' as const, send })),
    ];
  }, [queue, pendingSends, messages]);

  // A moved row keeps the keyboard focus: on its button for the same
  // direction, or on the other one when that end of the list is reached.
  useLayoutEffect(() => {
    const want = refocus.current;
    refocus.current = null;
    const row = want && rowRefs.current.get(want.id);
    if (!want || !row) return;
    const button = row.querySelector<HTMLButtonElement>(`[data-move="${want.dir}"]:not(:disabled)`)
      ?? row.querySelector<HTMLButtonElement>('[data-move]:not(:disabled)');
    button?.focus();
  }, [queue]);

  if (rows.length === 0) return null;

  const visible = rows.flatMap(row => (row.kind === 'queued' ? [row.entry.queue_id] : []));
  const cancel = (queueId: string) => { void useAgentStore.getState().cancelQueued(queueId); };
  const edit = async (queueId: string) => {
    await useAgentStore.getState().cancelQueued(queueId);
    focusComposer();
  };
  // Moves past the next visible row; entries hidden here keep their place.
  const move = (queueId: string, dir: Direction) => {
    const at = visible.indexOf(queueId);
    const target = visible[dir === 'up' ? at - 1 : at + 1];
    if (!target) return;
    refocus.current = { id: queueId, dir };
    const ids = moveBeside(queue.map(q => q.queue_id), queueId, target, dir === 'down');
    void useAgentStore.getState().reorderQueue(ids);
  };

  return (
    <ul
      aria-label="Queued messages"
      className="mb-2 max-h-48 overflow-y-auto rounded-xl border border-border bg-surface divide-y divide-border-subtle"
    >
      {rows.map(row => {
        if (row.kind === 'sending') {
          const text = row.send.content || 'Attachment';
          return (
            <li key={`send:${row.send.clientMsgId}`} className="flex min-h-9 items-center gap-2 px-3 py-1 text-xs md:min-h-8">
              <Loader2 size={12} className="shrink-0 text-text-faint motion-safe:animate-spin" aria-hidden="true" />
              <span className="min-w-0 flex-1 truncate text-text-secondary" title={text}>{flat(text)}</span>
              <span className="shrink-0 pr-1 text-text-faint">Sending…</span>
            </li>
          );
        }
        const { entry } = row;
        const own = !entry.internal && entry.actor_id === self;
        const at = visible.indexOf(entry.queue_id);
        return (
          <li
            key={`queue:${entry.queue_id}`}
            ref={el => { if (el) rowRefs.current.set(entry.queue_id, el); else rowRefs.current.delete(entry.queue_id); }}
            className="flex min-h-9 items-center gap-2 pl-3 pr-1.5 py-0.5 text-xs md:min-h-8"
          >
            <Clock size={12} className="shrink-0 text-text-faint" aria-hidden="true" />
            <span className="sr-only">Queued: </span>
            <span
              title={entry.preview}
              className={`min-w-0 flex-1 truncate ${entry.internal ? 'text-text-faint' : 'text-text-secondary'}`}
            >
              {flat(entry.preview)}
            </span>
            {entry.source !== 'web' && (
              <span className="shrink-0 rounded px-1 bg-surface-raised text-2xs text-text-faint">{entry.source}</span>
            )}
            {own && (
              <span className="flex shrink-0 items-center">
                <IconButton size="xs" className={ACTION_CLASS} label="Edit the queued message" onClick={() => void edit(entry.queue_id)}>
                  <Pencil size={12} />
                </IconButton>
                <IconButton size="xs" className={ACTION_CLASS} label="Cancel the queued message" onClick={() => cancel(entry.queue_id)}>
                  <X size={12} />
                </IconButton>
              </span>
            )}
            {visible.length > 1 && (
              <span className="flex shrink-0 items-center border-l border-border-subtle pl-1">
                <IconButton
                  size="xs" className={ACTION_CLASS} label="Move up" data-move="up"
                  disabled={at === 0} onClick={() => move(entry.queue_id, 'up')}
                >
                  <ChevronUp size={14} />
                </IconButton>
                <IconButton
                  size="xs" className={ACTION_CLASS} label="Move down" data-move="down"
                  disabled={at === visible.length - 1} onClick={() => move(entry.queue_id, 'down')}
                >
                  <ChevronDown size={14} />
                </IconButton>
              </span>
            )}
          </li>
        );
      })}
    </ul>
  );
}
