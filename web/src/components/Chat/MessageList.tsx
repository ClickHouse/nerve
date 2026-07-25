import { useEffect, useRef, useCallback, memo } from 'react';
import type { ChatMessage, MessageBlock } from '../../types/chat';
import { UserMessage } from './UserMessage';
import { AssistantMessage } from './AssistantMessage';
import { StreamingMessage } from './StreamingMessage';
import { SelectionToolbar } from './SelectionToolbar';

function MessageListImpl({ messages, streamingBlocks, isStreaming }: {
  messages: ChatMessage[];
  streamingBlocks: MessageBlock[];
  isStreaming: boolean;
}) {
  const endRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const isNearBottom = useRef(true);
  const prevMessageCount = useRef(0);

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

      {messages.map((msg, i) => (
        <div key={msg.id ?? i}>
          {msg.role === 'user'
            ? <UserMessage message={msg} />
            : <AssistantMessage message={msg} />
          }
        </div>
      ))}

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
