import { useMemo } from 'react';
import type { QueueEntryView } from '../../../types/agents';
import { useAgentStore, type PendingSend } from '../../../stores/agentStore';
import { useAuthStore } from '../../../stores/authStore';
import { useChatStore } from '../../../stores/chatStore';
import { Badge, IconButton } from '../../ui';
import { Clock, Loader2, Pencil, X } from '../../ui/icons';
import { focusComposer } from './agentActions';

/** Text shown for a chip before it is cut (the whole text is the chip's title). */
const PREVIEW_CHARS = 80;

type Chip =
  | { kind: 'queued'; entry: QueueEntryView }
  | { kind: 'sending'; send: PendingSend };

function preview(text: string): string {
  const flat = text.replace(/\s+/g, ' ').trim();
  return flat.length > PREVIEW_CHARS ? `${flat.slice(0, PREVIEW_CHARS)}…` : flat;
}

/**
 * Messages waiting for the running turn to end, one chip each, above the
 * composer: the server's queue (from every tab and channel) plus this tab's
 * sends the server has not confirmed yet ("Sending…"). A message that is
 * already a bubble in this tab is not repeated. Your own entries can be
 * cancelled (×) or taken back into the composer to edit (✎); internal
 * entries are shown muted, without actions.
 */
export function QueuedMessages() {
  const queue = useAgentStore(s => s.queue);
  const pendingSends = useAgentStore(s => s.pendingSends);
  const messages = useChatStore(s => s.messages);
  const self = useAuthStore(s => s.viewer?.id ?? null);

  const chips = useMemo<Chip[]>(() => {
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

  if (chips.length === 0) return null;

  const cancel = (queueId: string) => { void useAgentStore.getState().cancelQueued(queueId); };
  const edit = async (queueId: string) => {
    await useAgentStore.getState().cancelQueued(queueId);
    focusComposer();
  };

  return (
    <ul aria-label="Queued messages" className="flex flex-wrap gap-1.5 pb-2">
      {chips.map(chip => {
        if (chip.kind === 'sending') {
          const text = chip.send.content || 'Attachment';
          return (
            <li key={`send:${chip.send.clientMsgId}`}>
              <Badge tone="neutral" size="sm" className="max-w-full gap-1.5" title={text}>
                <Loader2 size={12} className="shrink-0 motion-safe:animate-spin" aria-hidden="true" />
                <span className="truncate max-w-[18rem]">{preview(text)}</span>
                <span className="shrink-0 text-text-faint">Sending…</span>
              </Badge>
            </li>
          );
        }
        const { entry } = chip;
        const own = !entry.internal && entry.actor_id === self;
        return (
          <li key={`queue:${entry.queue_id}`}>
            <Badge
              tone="neutral"
              size="sm"
              title={entry.preview}
              className={`max-w-full gap-1.5 pr-1 ${entry.internal ? 'opacity-60' : ''}`}
            >
              <Clock size={12} className="shrink-0" aria-hidden="true" />
              <span className="sr-only">Queued: </span>
              <span className="truncate max-w-[18rem]">{preview(entry.preview)}</span>
              {entry.source !== 'web' && (
                <span className="shrink-0 rounded px-1 bg-surface-raised text-2xs text-text-faint">{entry.source}</span>
              )}
              {own ? (
                <span className="flex items-center shrink-0">
                  <IconButton size="xs" label="Edit the queued message" onClick={() => void edit(entry.queue_id)}>
                    <Pencil size={12} />
                  </IconButton>
                  <IconButton size="xs" label="Cancel the queued message" onClick={() => cancel(entry.queue_id)}>
                    <X size={12} />
                  </IconButton>
                </span>
              ) : <span className="w-1 shrink-0" />}
            </Badge>
          </li>
        );
      })}
    </ul>
  );
}
