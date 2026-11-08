import { useCallback, useLayoutEffect, useRef, useState, type KeyboardEvent } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useShallow } from 'zustand/react/shallow';
import { useChatStore } from '../stores/chatStore';
import { useSetupStore } from '../stores/setupStore';
import { SessionSidebar } from '../components/Chat/SessionSidebar';
import { stepOf } from '../components/Setup/navigation';
import { IconButton, PageHeader } from '../components/ui';
import { ArrowUp, MessagesSquare } from '../components/ui/icons';
import { useIsMobile, useIsTouch } from '../hooks/useMediaQuery';
import type { SetupProfileDetail, SetupState } from '../api/client';

/** Height in pixels that the composer grows to before its text scrolls. */
const COMPOSER_MAX_HEIGHT = 160;

/** The agent's name from IDENTITY.md, as the setup checklist reads it. */
function agentNameOf(state: SetupState | null): string {
  const profile = state ? stepOf(state, 'profile')?.detail as SetupProfileDetail | undefined : undefined;
  return profile?.agent_name?.trim() || 'Nerve';
}

/**
 * The phone's start page: the session list on the full page, the sessions
 * that need you first, and a composer that starts a new chat. On desktop
 * the chat page shows the same list next to the chat, so this route opens
 * `/chat` there.
 */
export function InboxPage() {
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const { sessions, activeSession, agentStatus, createSession, deleteSession } = useChatStore(useShallow(s => ({
    sessions: s.sessions,
    activeSession: s.activeSession,
    agentStatus: s.agentStatus,
    createSession: s.createSession,
    deleteSession: s.deleteSession,
  })));

  // "New chat": the unsent new chat, opened in the chat page, which shows
  // its settings. `createSession` makes it active before it returns.
  const handleCreate = useCallback(async () => {
    await createSession();
    navigate(`/chat/${useChatStore.getState().activeSession}`);
  }, [createSession, navigate]);

  if (!isMobile) return <Navigate to="/chat" replace />;

  return (
    <div className="h-full flex flex-col">
      <PageHeader icon={<MessagesSquare size={18} className="text-accent shrink-0" />} title="Inbox" />
      <SessionSidebar
        layout="page"
        sessions={sessions}
        activeSession={activeSession}
        agentStatus={agentStatus}
        onCreate={handleCreate}
        onDelete={(id) => void deleteSession(id)}
      />
      <NewChatComposer onStarted={(id) => navigate(`/chat/${id}`)} />
    </div>
  );
}

/**
 * A message box that starts a new chat with its text. When the chat cannot
 * be started, the text stays in the box and a line under it says so, until
 * the person types or sends again. The next send goes to the chat that the
 * failed send created, if there is one.
 */
function NewChatComposer({ onStarted }: { onStarted: (sessionId: string) => void }) {
  const [text, setText] = useState('');
  const [sending, setSending] = useState(false);
  const [failed, setFailed] = useState(false);
  // The chat that a failed send created, for the next send.
  const unsentChat = useRef<string | undefined>(undefined);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const agentName = useSetupStore(s => agentNameOf(s.state));
  // As in the chat composer: on a touch device Enter types a new line and
  // the Send button sends. Cmd/Ctrl+Enter sends everywhere.
  const enterSends = !useIsTouch();
  const canSend = !!text.trim() && !sending;

  useLayoutEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${Math.min(el.scrollHeight, COMPOSER_MAX_HEIGHT)}px`;
  }, [text]);

  const send = async () => {
    if (!canSend) return;
    setSending(true);
    setFailed(false);
    const started = await useChatStore.getState().startChat(text.trim(), unsentChat.current);
    if (started?.sent) {
      onStarted(started.sessionId);
    } else {
      unsentChat.current = started?.sessionId;
      setSending(false);
      setFailed(true);
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.nativeEvent.isComposing) return;
    if (e.key === 'Enter' && !e.shiftKey && (enterSends || e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      void send();
    }
  };

  return (
    <form
      onSubmit={(e) => { e.preventDefault(); void send(); }}
      className="shrink-0 border-t border-border-subtle bg-bg px-3 pt-2 pb-3"
    >
      <div className="flex items-end gap-2 rounded-2xl border border-border bg-surface-raised py-1.5 pl-4 pr-1.5 transition-colors has-[textarea:focus]:border-accent/50">
        <textarea
          ref={textareaRef}
          value={text}
          onChange={(e) => { setText(e.target.value); setFailed(false); }}
          onKeyDown={handleKeyDown}
          enterKeyHint={enterSends ? 'send' : 'enter'}
          placeholder={`Message ${agentName}...`}
          aria-label={`Message ${agentName}`}
          rows={1}
          disabled={sending}
          className="block min-w-0 flex-1 resize-none bg-transparent py-1.5 text-sm text-text outline-none placeholder:text-text-faint disabled:opacity-50"
        />
        <IconButton type="submit" label="Send" variant="primary" size="sm" disabled={!canSend} className="rounded-lg">
          <ArrowUp size={16} />
        </IconButton>
      </div>
      {failed && (
        <p role="alert" className="mt-1.5 px-4 text-xs text-error">Could not send. Try again.</p>
      )}
    </form>
  );
}
