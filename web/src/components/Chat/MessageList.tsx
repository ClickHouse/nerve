import { useEffect, useRef, useCallback, memo } from 'react';
import { Link } from 'react-router-dom';
import type { ChatMessage } from '../../types/chat';
import { useVisibleActorIds } from '../../stores/actorStore';
import { useAuthStore } from '../../stores/authStore';
import { useChatStore } from '../../stores/chatStore';
import { providerConfigured, stepById, useSetupStore } from '../../stores/setupStore';
import { CHAT_EMPTY_NO_PROVIDER, CHAT_EMPTY_PROVIDER_CLOSED } from '../Setup/copy';
import { stepPlace } from '../Setup/navigation';
import { UserMessage } from './UserMessage';
import { AssistantMessage } from './AssistantMessage';
import { StreamingMessage } from './StreamingMessage';
import { SelectionToolbar } from './SelectionToolbar';
import { MessageActions } from './MessageActions';
import { GitBranch } from '../ui/icons';
import { followScrollEnd } from '../../utils/followScrollEnd';

function MessageListImpl({ messages, isStreaming, onForkMessage, messageForks }: {
  messages: ChatMessage[];
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
  // The count only: the blocks change for each streamed token, and
  // StreamingMessage subscribes to them itself.
  const streamingBlockCount = useChatStore(s => s.streamingBlocks.length);

  // A message-anchored fork resolves to a backend-native turn; sessions
  // predating turn recording can only be forked whole (header action).
  const hasForkAnchors = messages.some(m => m.native_turn_id != null);

  const viewerActorId = useAuthStore((s) => s.viewer?.id ?? null);
  const namedSenders = useVisibleActorIds(
    messages.map(m => m.actor_id), viewerActorId,
  );
  // `false` only when the server says no provider is configured; unknown
  // (`null`) keeps the ordinary empty state, so a failed read never nags.
  const needsProvider = useSetupStore(providerConfigured) === false;
  // A Model step that a lockdown closed is not on the setup page, so the
  // empty state gives the server's reason and no link to the step.
  const providerStep = useSetupStore((s) => stepById(s, 'provider'));
  const providerClosed = !!providerStep && stepPlace(providerStep) !== 'own';

  const handleScroll = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    isNearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100;
  }, []);

  // Stops the follow of the end that starts after the first load, while it runs.
  const stopFollow = useRef<(() => void) | null>(null);
  useEffect(() => () => stopFollow.current?.(), []);

  useEffect(() => {
    // While the end is followed after the first load, the follow scrolls.
    if (stopFollow.current || !isNearBottom.current) {
      prevMessageCount.current = messages.length;
      return;
    }
    // Initial load (0 → N messages): jump instantly, no scroll animation
    const wasEmpty = prevMessageCount.current === 0 && messages.length > 0;
    prevMessageCount.current = messages.length;
    endRef.current?.scrollIntoView({ behavior: wasEmpty ? 'instant' : 'smooth' });
    // The rows near the end take their real height only after this jump.
    if (wasEmpty && containerRef.current) {
      stopFollow.current = followScrollEnd(containerRef.current, () => {
        stopFollow.current = null;
      });
    }
  }, [messages.length, streamingBlockCount, isStreaming]);

  return (
    <div className="flex-1 overflow-y-auto relative" ref={containerRef} onScroll={handleScroll}>
      <SelectionToolbar containerRef={containerRef} />

      {messages.length === 0 && !isStreaming && (
        needsProvider ? (
          <div role="status" className="flex flex-col items-center justify-center h-full gap-2 text-center px-6">
            {providerClosed ? (
              <>
                <p className="text-text-faint text-lg">{CHAT_EMPTY_PROVIDER_CLOSED}</p>
                {providerStep?.blocked_reason && (
                  <p className="text-sm text-text-muted max-w-md">{providerStep?.blocked_reason}</p>
                )}
              </>
            ) : (
              <>
                <p className="text-text-faint text-lg">{CHAT_EMPTY_NO_PROVIDER}</p>
                <Link to="/setup/provider" className="text-sm text-accent hover:underline underline-offset-2">
                  Finish setup
                </Link>
              </>
            )}
          </div>
        ) : (
          <div className="flex items-center justify-center h-full text-text-faint text-lg">
            Start a conversation
          </div>
        )
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
          <div key={msg.id ?? i} className="chat-row">
            {msg.role === 'user'
              ? <UserMessage
                  message={msg}
                  actions={actions}
                  showActor={!!msg.actor_id && namedSenders.has(msg.actor_id)}
                />
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

      {isStreaming && <StreamingMessage />}

      <div ref={endRef} />
    </div>
  );
}

// Memoized so unrelated store updates (notably the per-keystroke draft write
// from the composer) don't re-render the whole message list. Every prop is a
// stable store slice, so the list re-renders only when messages, isStreaming,
// or the streaming block count actually change.
export const MessageList = memo(MessageListImpl);
