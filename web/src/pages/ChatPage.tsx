import { useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { useChatStore } from '../stores/chatStore';
import { SessionSidebar } from '../components/Chat/SessionSidebar';
import { MessageList } from '../components/Chat/MessageList';
import { ChatInput } from '../components/Chat/ChatInput';
import { ContextBar } from '../components/Chat/ContextBar';
import { TodoPanel } from '../components/Chat/TodoPanel';
import { SidePanel } from '../components/Chat/SidePanel';
import { BackgroundJobs } from '../components/Chat/BackgroundJobs';
import { Loader2, PanelLeftOpen, PanelLeftClose, Files } from 'lucide-react';

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
  const {
    sessions, activeSession, messages,
    streamingBlocks, isStreaming, loading,
    agentStatus, contextUsage, currentTodos,
    sidebarCollapsed, panels,
    modifiedFiles, modifiedFilesCount,
    loadSessions, switchSession, createSession, deleteSession,
    sendMessage, stopSession, toggleSidebar, openFilesPanel,
  } = useChatStore();

  // Cmd/Ctrl + \ toggles side panel
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === '\\') {
        e.preventDefault();
        useChatStore.getState().togglePanel();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
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
        onSelect={switchSession}
        onCreate={() => createSession()}
        onDelete={deleteSession}
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
                  <Loader2 size={12} className="animate-spin text-[#6366f1]" />
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
                      ? 'text-teal-400 bg-teal-400/10'
                      : 'text-text-muted hover:text-[#ccc] hover:bg-surface-raised'
                  }`}
                  title="Modified files"
                >
                  <Files size={14} />
                  <span className="tabular-nums">{fileCount}</span>
                </button>
              )}
              {contextUsage && <ContextBar usage={contextUsage} />}
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

          <TodoPanel todos={currentTodos} />

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
