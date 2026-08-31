import { useEffect, useRef, useCallback, memo } from 'react';
import { Link } from 'react-router-dom';
import type { ChatMessage, MessageBlock } from '../../types/chat';
import { UserMessage } from './UserMessage';
import { AssistantMessage } from './AssistantMessage';
import { StreamingMessage } from './StreamingMessage';
import { SelectionToolbar } from './SelectionToolbar';
import { MessageActions } from './MessageActions';
import { GitBranch } from '../ui/icons';

function MessageListImpl({ messages, streamingBlocks, isStreaming, onForkMessage, messageForks }: {
  messages: ChatMessage[];
  streamingBlocks: MessageBlock[];
  isStreaming: boolean;
  /** Fork the chat from a specific message (undefined = feature unavailable
      for this session, e.g. a virtual chat). */
  onForkMessage?: (messageId: number) => void;
  /** Existing forks of this session keyed by their anchor message id —
      renders a "branched here" pill under those messages. */
  messageForks?: Map<string, { id: string; title: string }[]>;
}) {
  const endRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const isNearBottom = useRef(true);
  const prevMessageCount = useRef(0);

  // A message-anchored fork resolves to a backend-native turn; sessions
  // predating turn recording can only be forked whole (header action).
  const hasForkAnchors = messages.some(m => m.native_turn_id != null);

  const handleScroll = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    isNearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100;
  }, []);

  useEffect(() => {
    if (!isNearBottom.current) {
      prevMessageCount.current = messages.length;
      return;
    }
    // Initial load (0 → N messages): jump instantly, no scroll animation
    const wasEmpty = prevMessageCount.current === 0 && messages.length > 0;
    prevMessageCount.current = messages.length;
    endRef.current?.scrollIntoView({ behavior: wasEmpty ? 'instant' : 'smooth' });
  }, [messages.length, streamingBlocks.length, isStreaming]);

  return (
    <div className="flex-1 overflow-y-auto relative" ref={containerRef} onScroll={handleScroll}>
      <SelectionToolbar containerRef={containerRef} />

      {messages.length === 0 && !isStreaming && (
        <div className="flex items-center justify-center h-full text-text-faint text-lg">
          Start a conversation
        </div>
      )}

      {messages.map((msg, i) => {
        // "Fork from here" needs a persisted row id (live-streamed messages
        // gain one on the next reload) and at least one native turn mapping
        // in the transcript to anchor the branch to.
        const canFork = !!onForkMessage && msg.id != null && hasForkAnchors
          && msg.channel !== 'review-loop';
        const actions = msg.channel === 'review-loop' ? undefined : (
          <MessageActions
            message={msg}
            onFork={canFork ? () => onForkMessage!(msg.id!) : undefined}
          />
        );
        const forks = msg.id != null ? messageForks?.get(String(msg.id)) : undefined;
        return (
          <div key={msg.id ?? i}>
            {msg.role === 'user'
              ? <UserMessage message={msg} actions={actions} />
              : <AssistantMessage message={msg} actions={actions} />
            }
            {forks && forks.length > 0 && (
              <div className="px-5 -mt-2 pb-2">
                <div className="max-w-[var(--chat-width)] mx-auto pl-10 flex flex-wrap gap-1.5">
                  {forks.map(f => (
                    <Link
                      key={f.id}
                      to={`/chat/${f.id}`}
                      title={`Open fork: ${f.title}`}
                      className="inline-flex items-center gap-1 max-w-[280px] px-2 py-0.5 rounded-full
                                 border border-hue-violet/25 bg-hue-violet/10 text-2xs text-hue-violet
                                 no-underline hover:bg-hue-violet/20 transition-colors"
                    >
                      <GitBranch size={10} className="shrink-0" />
                      <span className="truncate">{f.title}</span>
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </div>
        );
      })}

      {isStreaming && <StreamingMessage blocks={streamingBlocks} />}

      <div ref={endRef} />
    </div>
  );
}

// Memoized so unrelated store updates (notably the per-keystroke draft write
// from the composer) don't re-render the whole message list. Every prop is a
// stable store slice, so the list re-renders only when messages,
// streamingBlocks, or isStreaming actually change.
export const MessageList = memo(MessageListImpl);
