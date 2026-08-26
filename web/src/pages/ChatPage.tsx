import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useChatStore } from '../stores/chatStore';
import { SessionSidebar } from '../components/Chat/SessionSidebar';
import { MessageList } from '../components/Chat/MessageList';
import { ChatInput } from '../components/Chat/ChatInput';
import { ApprovalCard } from '../components/Chat/ApprovalCard';
import { InteractiveQuestionCard } from '../components/Chat/InteractiveQuestionCard';
import { ContextBar } from '../components/Chat/ContextBar';
import { TodoPanel } from '../components/Chat/TodoPanel';
import { SidePanel } from '../components/Chat/SidePanel';
import { ChatWidthHandle } from '../components/Chat/ChatWidthHandle';
import { BackgroundJobs } from '../components/Chat/BackgroundJobs';
import { ReviewLoopCard } from '../components/Chat/ReviewLoopCard';
import { Loader2, Files, ExternalLink } from '../components/ui/icons';
import { Button, PaneToggle } from '../components/ui';
import { api } from '../api/client';
import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts';
import { useIsMobile } from '../hooks/useMediaQuery';
import type { ShortcutDef } from '../utils/keyboard';
import { copyToClipboard } from '../utils/clipboard';
import { findSessionById } from '../utils/findSession';
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
    sessions, archivedSessions, systemSessions, activeSession, virtualSession, messages,
    streamingBlocks, isStreaming, loading,
    agentStatus, contextUsage, backendStatus, currentTodos, currentCCTasks,
    sidebarCollapsed, mobileSidebarOpen, panels, panelVisible,
    modifiedFiles, modifiedFilesCount,
    backendDefault, newChatBackend,
    loadSessions, switchSession, createSession, deleteSession,
    sendMessage, stopSession, toggleSessionList, setMobileSidebarOpen, openFilesPanel,
  } = useChatStore();

  // The active session row may live in the feed or a lazy archived/system group.
  const activeSessionRow = findSessionById(activeSession, sessions, archivedSessions, systemSessions);

  // Below `md` the session list becomes an off-canvas drawer. Its open state is
  // deliberately NOT `sidebarCollapsed`: that one is a persisted desktop
  // preference (default: expanded), and reusing it would both pop the drawer
  // open on first load and let a phone overwrite the desktop layout. It lives
  // in the store rather than here so the global Cmd+K shortcut can open it.
  const isMobile = useIsMobile();
  const sessionListOpen = isMobile ? mobileSidebarOpen : !sidebarCollapsed;

  // Retire the drawer on the way out of the phone layout, so a later resize
  // back down doesn't arrive with an overlay already on screen. Nothing
  // flashes on the way out: above `md` the drawer state is not read at all.
  useEffect(() => {
    if (!isMobile) setMobileSidebarOpen(false);
  }, [isMobile, setMobileSidebarOpen]);

  // Picking a conversation should reveal it, not leave the list covering it.
  // The drawer closes itself the moment one of its links is tapped — including
  // a tap on the conversation that is already open, which never reaches here
  // because `activeSession` doesn't change. This is the backstop for switches
  // from anywhere else (browser Back, a deep link). The first resolution of
  // `activeSession` out of '' is skipped: it lands after the initial load and
  // would otherwise slam the drawer shut right after Cmd+K opened it.
  const previousSession = useRef(activeSession);
  useEffect(() => {
    const switched = previousSession.current && previousSession.current !== activeSession;
    previousSession.current = activeSession;
    if (switched) setMobileSidebarOpen(false);
  }, [activeSession, setMobileSidebarOpen]);

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
      // Not `toggleSidebar`: below `md` that would silently rewrite the
      // persisted desktop preference and move nothing on screen.
      action: () => useChatStore.getState().toggleSessionList(),
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
        if (text) void copyToClipboard(text);
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
          void useChatStore.getState().deleteSession(id);
        }
      },
    },
  ], []);

  useKeyboardShortcuts(chatShortcuts);

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

  // Mirror the active session's title into the browser tab. Same cleaning
  // rules as the sidebar (strip leading '#' and 'Implement:' prefix).
  // Restored to plain "Nerve" when leaving the chat page or when there's
  // no active session yet.
  useEffect(() => {
    if (!activeSessionRow) {
      document.title = 'Nerve';
      return;
    }
    const raw = activeSessionRow.title || activeSessionRow.id;
    const clean = raw.replace(/^#+\s*/, '').replace(/^Implement:\s*/i, '');
    document.title = clean;
    return () => { document.title = 'Nerve'; };
  }, [activeSession, activeSessionRow]);

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
        // No URL param and no active session yet. A restored unsent chat wins
        // over the most recent real one: it only survives a reload when it
        // still holds draft text, and dropping the user somewhere else would
        // hide the very prompt they were writing.
        const { sessions: loaded, virtualSession } = useChatStore.getState();
        if (virtualSession) {
          switchSession(virtualSession.id);
        } else if (loaded.length > 0) {
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
  // SidePanel renders nothing without a tab, so it only covers the column when
  // there is one.
  const panelCoversColumn = isMobile && panelVisible && panels.length > 0;

  return (
    // `relative` anchors the mobile side panel, which covers this column but
    // deliberately not the bottom nav below it.
    <div className="h-full flex relative">
      <SessionSidebar
        sessions={sessions}
        activeSession={activeSession}
        agentStatus={agentStatus}
        onCreate={handleCreateSession}
        onDelete={handleDeleteSession}
        collapsed={!sessionListOpen}
        mobile={isMobile}
        onRequestClose={() => setMobileSidebarOpen(false)}
      />

      {/* Main content area: chat column + optional plan panel */}
      <div className="flex-1 flex min-w-0">
        {/* Chat column. On a phone the side panel covers it completely, so it
            goes inert while that is open: the panel is not a modal — the nav
            below it stays reachable on purpose — and without this, Tab would
            walk through a transcript and a composer nobody can see. Marking it
            inert also moves focus off the covered composer, so keystrokes stop
            landing in a box that is no longer on screen. */}
        <div
          className="flex-1 flex flex-col min-w-0"
          inert={panelCoversColumn ? true : undefined}
        >
          {/* Header */}
          <div className="border-b border-border-subtle px-3 md:px-5 py-2.5 flex items-center justify-between gap-2 bg-bg shrink-0">
            <div className="flex items-center gap-2 min-w-0">
              <PaneToggle open={sessionListOpen} onToggle={toggleSessionList} label="sidebar" />
              <span className="font-medium text-base truncate">
                {virtualSession?.id === activeSession
                  ? 'New chat'
                  : (activeSessionRow?.title || activeSession)}
              </span>
              {(() => {
                const backend = virtualSession?.id === activeSession
                  ? (newChatBackend ?? backendDefault ?? 'claude')
                  : (activeSessionRow?.backend ?? 'claude');
                return (
                  <span
                    title={`Agent backend: ${backend}`}
                    // Which backend is running is identity, not status, so
                    // this stays on the `hue-*` scale.
                    className={`hidden md:inline shrink-0 text-2xs uppercase tracking-wide px-1.5 py-0.5 rounded border ${
                      backend === 'codex'
                        ? 'text-hue-teal border-hue-teal/25 bg-hue-teal/10'
                        : 'text-hue-orange border-hue-orange/25 bg-hue-orange/10'
                    }`}
                  >
                    {backend}
                  </span>
                );
              })()}
              {(() => {
                const model = activeSessionRow?.model;
                return model ? (
                  <span className="hidden md:inline shrink-0 text-xs text-text-faint bg-surface-raised px-1.5 py-0.5 rounded">
                    {formatModelLabel(model)}
                  </span>
                ) : null;
              })()}
              {(() => {
                // Live review-loop chip for observer sessions (fed by the
                // global review_loop_update event; rehydrate via reload).
                const rl = activeSessionRow?.review_loop;
                if (!rl) return null;
                const live = rl.status === 'implementing' || rl.status === 'verifying' || rl.status === 'pending';
                const label = rl.status === 'awaiting_user'
                  ? `needs decision${rl.failure_reason ? ` (${rl.failure_reason})` : ''}`
                  : rl.status === 'passed' ? 'passed'
                  : rl.status === 'failed' ? `failed${rl.failure_reason ? ` (${rl.failure_reason})` : ''}`
                  : rl.status === 'killed' ? 'killed'
                  : rl.status;
                // A review loop's state is status, so these are the feedback
                // tokens. The default branch is the live loop, which takes the
                // same green as `passed`.
                const tone = rl.status === 'passed'
                  ? 'text-success border-success-border bg-success-bg'
                  : rl.status === 'awaiting_user'
                  ? 'text-warning border-warning-border bg-warning-bg'
                  : rl.status === 'failed' || rl.status === 'killed'
                  ? 'text-error border-error-border bg-error-bg'
                  : 'text-success border-success-border bg-success-bg';
                return (
                  <span
                    title={`Review loop ${rl.id}: ${rl.status}${rl.failure_reason ? ` — ${rl.failure_reason}` : ''}`}
                    className={`text-xs px-1.5 py-0.5 rounded border flex items-center gap-1.5 ${tone}`}
                  >
                    {live && <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />}
                    🔁 {rl.iteration}/{rl.max_iterations} — {label}
                  </span>
                );
              })()}
              {statusLabel && (
                <div className="flex items-center gap-1.5 min-w-0 text-xs text-text-muted">
                  <Loader2 size={12} className="shrink-0 animate-spin text-accent" />
                  <span className="truncate">{statusLabel}</span>
                </div>
              )}
              {backendStatus?.subtype === 'codex_rate_limits' && (() => {
                const rateLimits = backendStatus.data.rateLimits as
                  | { primary?: { usedPercent?: number } }
                  | undefined;
                const used = rateLimits?.primary?.usedPercent;
                return (
                  <span
                    className="text-xs text-text-faint"
                    title={JSON.stringify(backendStatus.data)}
                  >
                    Codex limit{typeof used === 'number' ? ` ${used}% used` : ' updated'}
                  </span>
                );
              })()}
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <BackgroundJobs
                sessions={sessions}
                activeSession={activeSession}
                onSelect={switchSession}
              />
              {/* House convention for a tinted action: the identity hue rides
                  the icon and the label stays neutral, so nothing competes
                  with `subtle`'s own text colour. */}
              {fileCount > 0 && (
                <Button variant="subtle" size="xs" onClick={openFilesPanel} title="Modified files">
                  <Files size={14} className={filesPanelActive ? 'text-hue-teal' : undefined} />
                  <span className="tabular-nums">{fileCount}</span>
                </Button>
              )}
              {contextUsage && (
                <div className="hidden md:flex">
                  <ContextBar usage={contextUsage} sessionCostUsd={activeSessionRow?.total_cost_usd} />
                </div>
              )}
              {langfuse?.enabled && langfuse.host && activeSession && (
                <a
                  href={`${langfuse.host}/sessions?sessionId=${encodeURIComponent(activeSession)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hidden md:flex items-center gap-1 px-2 py-1 rounded text-xs text-text-faint hover:text-text-secondary hover:bg-surface-raised transition-colors cursor-pointer"
                  title="View this session's trace in Langfuse"
                >
                  <ExternalLink size={12} />
                  <span>Langfuse</span>
                </a>
              )}
            </div>
          </div>

          {/* Review-loop dashboard — sticky above the transcript for
              observer sessions: live criteria, attempt timeline with
              watch-the-leg jumps, inline decisions when parked. */}
          {(() => {
            const rl = activeSessionRow?.review_loop;
            return rl ? <ReviewLoopCard key={rl.id} loopId={rl.id} /> : null;
          })()}

          {/* Messages region: wraps the scrollable list so the width handle
              anchors to the reading-column edge. The header and composer keep
              their own full width. */}
          <div className="relative flex-1 flex flex-col min-h-0">
            {loading ? (
              <div className="flex-1 flex items-center justify-center text-text-faint">Loading...</div>
            ) : (
              <MessageList
                messages={messages}
                streamingBlocks={streamingBlocks}
                isStreaming={isStreaming}
              />
            )}
            <ChatWidthHandle />
          </div>

          <TodoPanel todos={currentTodos} ccTasks={currentCCTasks} />

          <InteractiveQuestionCard />
          <ApprovalCard />

          {/* The composer stays typeable while a turn runs so a reply is never
              lost mid-stream. `isStreaming` still swaps Send↔Stop and blocks
              sending (canSend), so the text is just held as the session's draft
              until the turn ends — a turn in progress is not a reason to block
              typing, hence no `disabled`. */}
          <ChatInput
            onSend={sendMessage}
            onStop={stopSession}
            isStreaming={isStreaming}
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
