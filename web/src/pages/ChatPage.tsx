import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useChatStore } from '../stores/chatStore';
import { SessionSidebar } from '../components/Chat/SessionSidebar';
import { MessageList } from '../components/Chat/MessageList';
import { ChatInput } from '../components/Chat/ChatInput';
import { ContextBar } from '../components/Chat/ContextBar';
import { TodoPanel } from '../components/Chat/TodoPanel';
import { SidePanel } from '../components/Chat/SidePanel';
import { BackgroundJobs } from '../components/Chat/BackgroundJobs';
import { Loader2, PanelLeftOpen, PanelLeftClose, Files, ExternalLink } from 'lucide-react';
import { api } from '../api/client';
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts';
import type { ShortcutDef } from '../utils/keyboard';
import type { ChatMessage, TextBlockData } from '../types/chat';

const STATUS_LABELS: Record<string, string> = {
  thinking: 'Thinking...',
  writing: 'Writing...',
};

/** Format a model identifier into a short display label. */
function formatModelLabel(model: string): string {
  const m = model.replace(/^claude-/, '');
  const match = m.match(/^(\w+)-(\d+)-(\d+)/);
  if (match) {
    const name = match[1].charAt(0).toUpperCase() + match[1].slice(1);
    return `${name} ${match[2]}.${match[3]}`;
  }
  return m.charAt(0).toUpperCase() + m.slice(1);
}

export function ChatPage() {
  const { sessionId } = useParams();
  const navigate = useNavigate();
  const {
    sessions, activeSession, messages,
    streamingBlocks, isStreaming, loading,
    agentStatus, contextUsage, currentTodos, currentCCTasks,
    sidebarCollapsed, panels,
    modifiedFiles, modifiedFilesCount,
    loadSessions, switchSession, createSession, deleteSession,
    sendMessage, stopSession, toggleSidebar, openFilesPanel,
  } = useChatStore();

  // URL → activeSession is handled by the useEffect[sessionId] below.
  // activeSession → URL is intentionally NOT done as a mirror effect —
  // that races with `loadSessions()` (which starts with sessions=[], so any
  // "URL is unknown to us" check is unreliable on a fresh tab) and with the
  // server's `session_switched` WS message that fires before our store
  // knows the URL's session exists. Instead we navigate explicitly from
  // each call-site that changes the active session without a URL change.
  const handleCreateSession = useCallback(async () => {
    await createSession();
    const next = useChatStore.getState().activeSession;
    if (next) navigate(`/chat/${next}`, { replace: true });
  }, [createSession, navigate]);

  const handleDeleteSession = useCallback(async (id: string) => {
    await deleteSession(id);
    const next = useChatStore.getState().activeSession;
    if (next) navigate(`/chat/${next}`, { replace: true });
    else navigate('/chat', { replace: true });
  }, [deleteSession, navigate]);

  // Chat-scoped keyboard shortcuts. Global ones (new chat, search, modal,
  // Esc cascade) live in <GlobalShortcuts /> in App.tsx.
  const chatShortcuts = useMemo<ShortcutDef[]>(() => [
    {
      id: 'chat-toggle-panel',
      combo: { mod: true, key: '\\' },
      description: 'Toggle side panel',
      section: 'chat',
      action: () => useChatStore.getState().togglePanel(),
    },
    {
      id: 'chat-toggle-sidebar',
      combo: { mod: true, shift: true, key: 's' },
      description: 'Toggle session sidebar',
      section: 'chat',
      action: () => useChatStore.getState().toggleSidebar(),
    },
    {
      id: 'chat-focus-input',
      combo: { mod: true, shift: true, key: ';' },
      description: 'Focus message input',
      section: 'chat',
      allowInInput: true,
      action: () => {
        const el = document.getElementById('nerve-chat-input');
        if (el instanceof HTMLTextAreaElement) el.focus();
      },
    },
    {
      id: 'chat-copy-last',
      combo: { mod: true, shift: true, key: 'c' },
      description: 'Copy last response',
      section: 'chat',
      action: () => {
        const text = getLastAssistantText(useChatStore.getState().messages);
        if (text) void navigator.clipboard.writeText(text);
      },
    },
    {
      id: 'chat-delete-current',
      combo: { mod: true, shift: true, key: 'Backspace' },
      description: 'Delete current conversation',
      section: 'chat',
      action: () => {
        const id = useChatStore.getState().activeSession;
        if (!id) return;
        if (window.confirm('Delete this conversation?')) {
          void handleDeleteSession(id);
        }
      },
    },
  ], [handleDeleteSession]);

  useKeyboardShortcuts(chatShortcuts);

  // Mirror the active session's title into the browser tab. Same cleaning
  // rules as the sidebar (strip leading '#' and 'Implement:' prefix).
  // Restored to plain "Nerve" when leaving the chat page or when there's
  // no active session yet.
  useEffect(() => {
    const session = sessions.find(s => s.id === activeSession);
    if (!session) {
      document.title = 'Nerve';
      return;
    }
    const raw = session.title || session.id;
    const clean = raw.replace(/^#+\s*/, '').replace(/^Implement:\s*/i, '');
    document.title = clean;
    return () => { document.title = 'Nerve'; };
  }, [activeSession, sessions]);

  // Langfuse deep-link status — fetched once. Shows a small "external link"
  // icon when observability is enabled so we can jump from a session to
  // its trace in Langfuse.
  const [langfuse, setLangfuse] = useState<{ host: string | null; enabled: boolean } | null>(null);
  useEffect(() => {
    api.getObservabilityStatus()
      .then(s => setLangfuse({ host: s.langfuse.host, enabled: s.langfuse.enabled }))
      .catch(() => setLangfuse({ host: null, enabled: false }));
  }, []);

  useEffect(() => {
    loadSessions().then(() => {
      if (sessionId) {
        // URL has explicit session — switch to it
        if (sessionId !== activeSession || messages.length === 0) {
          switchSession(sessionId);
        }
      } else if (!activeSession) {
        // No URL param and no active session yet — pick the most recent
        const { sessions: loaded } = useChatStore.getState();
        if (loaded.length > 0) {
          switchSession(loaded[0].id);
        }
        // Otherwise, the server's session_switched WS message will set it
      }
    });
  }, [sessionId]); // eslint-disable-line react-hooks/exhaustive-deps


  const statusLabel = agentStatus.state === 'tool'
    ? `Using ${agentStatus.toolName}...`
    : STATUS_LABELS[agentStatus.state] || null;

  const fileCount = modifiedFiles.length || modifiedFilesCount;
  const filesPanelActive = panels.some(p => p.id === 'files-panel');

  return (
    <div className="h-full flex">
      <SessionSidebar
        sessions={sessions}
        activeSession={activeSession}
        agentStatus={agentStatus}
        onCreate={handleCreateSession}
        onDelete={handleDeleteSession}
        collapsed={sidebarCollapsed}
      />

      {/* Main content area: chat column + optional plan panel */}
      <div className="flex-1 flex min-w-0">
        {/* Chat column */}
        <div className="flex-1 flex flex-col min-w-0">
          {/* Header */}
          <div className="border-b border-border-subtle px-5 py-2.5 flex items-center justify-between bg-bg shrink-0">
            <div className="flex items-center gap-2">
              <button
                onClick={toggleSidebar}
                className="w-6 h-6 flex items-center justify-center text-text-faint hover:text-text-muted cursor-pointer transition-colors rounded"
                title={sidebarCollapsed ? 'Show sidebar' : 'Hide sidebar'}
              >
                {sidebarCollapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
              </button>
              <span className="font-medium text-[15px]">
                {sessions.find(s => s.id === activeSession)?.title || activeSession}
              </span>
              {(() => {
                const model = sessions.find(s => s.id === activeSession)?.model;
                return model ? (
                  <span className="text-[11px] text-text-faint bg-surface-raised px-1.5 py-0.5 rounded">
                    {formatModelLabel(model)}
                  </span>
                ) : null;
              })()}
              {statusLabel && (
                <div className="flex items-center gap-1.5 text-[12px] text-text-muted">
                  <Loader2 size={12} className="animate-spin text-accent" />
                  <span>{statusLabel}</span>
                </div>
              )}
            </div>
            <div className="flex items-center gap-2">
              <BackgroundJobs
                sessions={sessions}
                activeSession={activeSession}
                onSelect={switchSession}
              />
              {fileCount > 0 && (
                <button
                  onClick={openFilesPanel}
                  className={`flex items-center gap-1.5 px-2 py-1 rounded text-[12px] transition-colors cursor-pointer ${
                    filesPanelActive
                      ? 'text-hue-teal bg-teal-400/10'
                      : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'
                  }`}
                  title="Modified files"
                >
                  <Files size={14} />
                  <span className="tabular-nums">{fileCount}</span>
                </button>
              )}
              {contextUsage && <ContextBar usage={contextUsage} sessionCostUsd={sessions.find(s => s.id === activeSession)?.total_cost_usd} />}
              {langfuse?.enabled && langfuse.host && activeSession && (
                <a
                  href={`${langfuse.host}/sessions?sessionId=${encodeURIComponent(activeSession)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1 px-2 py-1 rounded text-[12px] text-text-faint hover:text-text-secondary hover:bg-surface-raised transition-colors cursor-pointer"
                  title="View this session's trace in Langfuse"
                >
                  <ExternalLink size={12} />
                  <span>Langfuse</span>
                </a>
              )}
            </div>
          </div>

          {loading ? (
            <div className="flex-1 flex items-center justify-center text-text-faint">Loading...</div>
          ) : (
            <MessageList
              messages={messages}
              streamingBlocks={streamingBlocks}
              isStreaming={isStreaming}
            />
          )}

          <TodoPanel todos={currentTodos} ccTasks={currentCCTasks} />

          <ChatInput
            onSend={sendMessage}
            onStop={stopSession}
            isStreaming={isStreaming}
            disabled={isStreaming}
          />
        </div>

        {/* Side panel — sub-agents, plans, files, etc. (always render when tabs exist for animation) */}
        {panels.length > 0 && <SidePanel />}
      </div>
    </div>
  );
}

/** Walk messages backwards, return the joined text of the most recent assistant turn. */
function getLastAssistantText(messages: ChatMessage[]): string | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role !== 'assistant') continue;
    const text = m.blocks
      .filter((b): b is TextBlockData => b.type === 'text')
      .map((b) => b.content)
      .join('\n');
    return text || null;
  }
  return null;
}
