import { useLayoutEffect, useMemo, useRef } from 'react';
import type { QueueEntryView } from '../../../types/agents';
import { useAgentStore, type PendingSend } from '../../../stores/agentStore';
import { useAuthStore } from '../../../stores/authStore';
import { IconButton } from '../../ui';
import { ChevronDown, ChevronUp, Clock, Loader2, Pencil, X } from '../../ui/icons';
import { focusComposer } from './agentActions';

type Row =
  | { kind: 'queued'; entry: QueueEntryView }
  | { kind: 'sending'; send: PendingSend };

type Direction = 'up' | 'down';

/** Row actions: a larger hit area on a phone, the inline size from `md` up. */
const ACTION_CLASS = 'p-1.5 md:p-1';
/** Preview length in the accessible name of a move button. */
const LABEL_PREVIEW_CHARS = 40;

function flat(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

function short(text: string): string {
  const one = flat(text);
  return one.length > LABEL_PREVIEW_CHARS ? `${one.slice(0, LABEL_PREVIEW_CHARS)}…` : one;
}

/**
 * Messages waiting for the running turn to end, one row each, above the
 * composer and in run order (the top row runs next): the whole server queue
 * (from every tab and channel) plus this tab's sends the server has not
 * confirmed yet ("Sending…"). Any queued row can move up or down; your own
 * rows can also be cancelled (×) or taken back into the composer to edit
 * (✎). Internal entries are shown muted. The transcript shows no bubble for
 * a message while it is queued (see MessageList).
 */
export function QueuedMessages() {
  const queue = useAgentStore(s => s.queue);
  const pendingSends = useAgentStore(s => s.pendingSends);
  const self = useAuthStore(s => s.viewer?.id ?? null);
  const rowRefs = useRef(new Map<string, HTMLLIElement>());
  const refocus = useRef<{ id: string; dir: Direction } | null>(null);

  const rows = useMemo<Row[]>(() => {
    const queuedIds = new Set(queue.map(q => q.client_msg_id).filter((id): id is string => !!id));
    return [
      ...queue.map(entry => ({ kind: 'queued' as const, entry })),
      ...Object.values(pendingSends)
        .filter(send => !queuedIds.has(send.clientMsgId))
        .sort((a, b) => a.at - b.at)
        .map(send => ({ kind: 'sending' as const, send })),
    ];
  }, [queue, pendingSends]);

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

  const cancel = (queueId: string) => { void useAgentStore.getState().cancelQueued(queueId); };
  const edit = async (queueId: string) => {
    await useAgentStore.getState().cancelQueued(queueId);
    focusComposer();
  };
  // Swaps the entry at `at` with its neighbour in the queue.
  const move = (at: number, dir: Direction) => {
    const to = dir === 'up' ? at - 1 : at + 1;
    if (to < 0 || to >= queue.length) return;
    const ids = queue.map(q => q.queue_id);
    [ids[at], ids[to]] = [ids[to], ids[at]];
    refocus.current = { id: ids[to], dir };
    void useAgentStore.getState().reorderQueue(ids);
  };

  return (
    <ul
      aria-label="Queued messages"
      className="mb-2 max-h-48 overflow-y-auto rounded-xl border border-border bg-surface divide-y divide-border-subtle"
    >
      {rows.map((row, at) => {
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
        const name = short(entry.preview);
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
            {queue.length > 1 && (
              <span className="flex shrink-0 items-center border-l border-border-subtle pl-1">
                <IconButton
                  size="xs" className={ACTION_CLASS} label={`Move up: ${name}`} data-move="up"
                  disabled={at === 0} onClick={() => move(at, 'up')}
                >
                  <ChevronUp size={14} />
                </IconButton>
                <IconButton
                  size="xs" className={ACTION_CLASS} label={`Move down: ${name}`} data-move="down"
                  disabled={at === queue.length - 1} onClick={() => move(at, 'down')}
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
