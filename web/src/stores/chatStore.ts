import { create } from 'zustand';
import { api } from '../api/client';
import { ws } from '../api/websocket';
import type { WSMessage } from '../api/websocket';
import type { ChatMessage, MessageBlock, Session, AgentStatus, PanelTab, ModifiedFileSummary } from '../types/chat';
import { hydrateMessage } from '../utils/hydrateMessage';
import { isMobileViewport } from '../hooks/useMediaQuery';
import { randomUUID } from '../utils/uuid';
// Helpers
import { cancelAutoClose, clearAllAutoCloseTimers, MAX_COMPLETED_TABS } from './helpers/blockHelpers';
import { extractTodosFromMessages, extractCCTasksFromMessages } from './helpers/bufferReplay';
import { loadDrafts, persistDraft, removeDraft, pruneDrafts } from './helpers/draftStorage';
import { loadVirtualSession, persistVirtualSession, clearVirtualSession } from './helpers/virtualSessionStorage';
// Handlers
import { handleThinking, handleToken, handleToolUse, handleToolResult, handleToolOutput, handleDone, handleStopped, handleError, handleWakeup, handleAutoTurn, handleModelChanged } from './handlers/streamingHandlers';
import { handleSessionUpdated, handleSessionStatus, handleSessionSwitched, handleSessionForked, handleSessionResumed, handleSessionArchived, handleSessionRunning, handleSessionAwaitingInput, handleAnswerInjected, handleUserMessage, handleReviewLoopUpdate } from './handlers/sessionHandlers';
import { handlePlanUpdate, handleSubagentStart, handleSubagentComplete, handleWorkflowProgress } from './handlers/panelHandlers';
import { handleInteraction, handleInteractionResolved, handleFileChanged, handleNotification, handleNotificationAnswered, handleNotificationExpired, handleBackgroundTasksUpdate } from './handlers/auxiliaryHandlers';

export interface TodoItem {
  content: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm: string;
}

/**
 * Claude Code 2.1.x task (from TaskCreate / TaskUpdate / TaskList / TaskGet).
 * Stored per-session in ~/.claude/tasks/<id>/ on the CLI side; tracked here
 * so the in-chat "Tasks" panel reflects what the model is planning during the
 * turn. Replaces the older TodoWrite todo list.
 */
export interface CCTask {
  id: string;             // numeric string assigned by the CLI ("1", "2", ...)
  subject: string;        // brief title
  activeForm?: string;    // present continuous, shown while in_progress
  status: 'pending' | 'in_progress' | 'completed';
  owner?: string;
  blockedBy?: string[];
}

/**
 * Review-loop config drafted in the new-chat composer panel. Form-shaped
 * (strings for numeric fields); converted to the wire payload at session
 * materialization (ensureRealSession). Must live in the store — not in
 * ChatInput state — because ensureRealSession has TWO call sites
 * (sendMessage and the pre-message file-upload path).
 */
export interface NewChatReviewLoop {
  goal: string;
  verifier: string;
  budget: string;                 // '' = config default
  adoption: 'no' | 'ask' | 'auto';
  implementerEngine: string;      // '' = config default
  implementerModel: string;
  verifierEngine: string;
  verifierModel: string;
  maxIterations: string;          // '' = config default
  cwd: string;                    // '' = global workspace; created if missing
}

export const EMPTY_REVIEW_LOOP: NewChatReviewLoop = {
  goal: '', verifier: '', budget: '', adoption: 'no',
  implementerEngine: '', implementerModel: '',
  verifierEngine: '', verifierModel: '', maxIterations: '', cwd: '',
};

export type QuoteAction = 'add' | 'remove' | 'improve' | 'question' | 'note';

export interface QuoteEntry {
  id: string;
  text: string;
  action: QuoteAction;
  instruction: string;
}

const QUOTE_DEFAULTS: Record<QuoteAction, string> = {
  add: '',
  remove: 'Remove this',
  improve: 'Improve this',
  question: '',
  note: '',
};

let _quoteId = 0;

// WS event types that mutate the *active* chat view — stream tokens, panels,
// interaction prompts, file changes. They're dropped when their session_id
// doesn't match the active session: a reconnect binds the socket to the
// channel's last real session (server.py get_last_session), which — while a
// not-yet-sent "new chat" is on screen — differs from it, and the replayed
// buffer would otherwise hijack the view with a phantom "Thinking…" and a
// disabled composer. Sidebar/list events (session_running, session_updated, …)
// stay unguarded so background sessions keep updating their row.
const VIEW_SCOPED_EVENTS = new Set<WSMessage['type']>([
  'thinking', 'token', 'tool_use', 'tool_result', 'tool_output', 'done', 'stopped', 'error',
  'wakeup', 'auto_turn', 'model_changed', 'session_status', 'plan_update',
  'backend_status',
  'subagent_start', 'subagent_complete', 'interaction',
  'interaction_resolved', 'file_changed',
]);

interface ChatState {
  sessions: Session[];
  activeSession: string;
  // Not-yet-persisted "new chat" from the + button. Materializes in the API
  // on the first sent message; rendered pinned at the top of the sidebar.
  virtualSession: Session | null;
  // Per-session unsent input text, keyed by session id (incl. the virtual one).
  drafts: Record<string, string>;
  messages: ChatMessage[];
  // Streaming state — blocks built incrementally
  streamingBlocks: MessageBlock[];
  isStreaming: boolean;
  loading: boolean;
  // Agent activity status
  agentStatus: AgentStatus;
  // Context window usage from last agent turn
  contextUsage: {
    input_tokens: number;
    output_tokens: number;
    cache_creation_input_tokens: number;
    cache_read_input_tokens: number;
    cache_creation_5m_input_tokens?: number;
    cache_creation_1h_input_tokens?: number;
    max_context_tokens: number;
    num_turns: number;
  } | null;
  backendStatus: { subtype: string; data: Record<string, unknown> } | null;
  // TodoWrite panel state (legacy Claude Code todos)
  currentTodos: TodoItem[];
  // Claude Code 2.1+ task panel state (TaskCreate / TaskUpdate / TaskList)
  currentCCTasks: CCTask[];
  // Text selection quotes
  quotes: QuoteEntry[];

  // Side panel — generic tabbed panel for sub-agents, plans, etc.
  panels: PanelTab[];
  activePanelId: string | null;
  panelVisible: boolean;
  panelWidth: number;
  // Conversation reading-column width in px (drag-resizable, persisted to
  // localStorage 'nerve_chat_width'). Default 768 = the previous fixed cap.
  chatWidth: number;
  // Session list (left sidebar) width in px (drag-resizable, persisted to
  // localStorage 'nerve_sidebar_width'). Default 240 = the previous w-60.
  sidebarWidth: number;

  // Pending interactive tool (AskUserQuestion, ExitPlanMode, etc.)
  pendingInteraction: {
    interactionId: string;
    interactionType: 'question' | 'plan_exit' | 'plan_enter' | 'command_approval' | 'file_approval' | 'permission_approval';
    toolName: string;
    toolInput: Record<string, unknown>;
  } | null;

  // Sidebar collapse (desktop column — persisted)
  sidebarCollapsed: boolean;
  /**
   * Whether the phone-sized off-canvas session drawer is showing. Deliberately
   * separate from `sidebarCollapsed`: that one is a persisted *desktop*
   * preference, so driving the drawer with it would both pop the drawer open on
   * first load and let a phone overwrite the desktop layout. Lives in the store
   * rather than in ChatPage so the global Cmd+K shortcut can open it.
   */
  mobileSidebarOpen: boolean;

  // Modified files tracking
  modifiedFiles: ModifiedFileSummary[];
  modifiedFilesCount: number;

  // Background tasks (run_in_background)
  backgroundTasks: { task_id: string; label: string; tool: string; status: 'running' | 'done' | 'failed' | 'timeout'; startedAt: number }[];

  // Session search
  searchQuery: string;
  searchResults: Session[] | null;  // null = not searching
  searchLoading: boolean;
  /** Bumped whenever something wants the sidebar search input focused (e.g. Cmd+K). */
  searchFocusNonce: number;
  // Composer model picker: options from GET /api/models (Anthropic default +
  // locally-installed Ollama models), the server's default id, and the user's
  // current pick (null = use the server default).
  availableModels: { id: string; provider: string; backend: string }[];
  modelDefaults: Record<string, string>;
  // Agent backends for the new-chat selector (claude / codex).
  backendOptions: { id: string; label: string; model: string; models?: string[]; available?: boolean; reason?: string }[];
  backendDefault: string | null;
  // Backend picked for the CURRENT virtual (unsent) chat; null = default.
  // Bound at session materialization (ensureRealSession) and reset after.
  newChatBackend: string | null;
  // Review-loop config for the CURRENT virtual chat; null = panel closed.
  // Bound at session materialization (like newChatBackend) and reset after.
  newChatReviewLoop: NewChatReviewLoop | null;
  // Model picked for the CURRENT virtual (unsent) chat, keyed by backend so
  // a Claude pick can't leak into Codex when the backend toggle moves
  // (null/absent = backend default). Bound at session materialization and
  // reset after. Real sessions don't use this — their model lives on the
  // session row (sessions[].model) and is changed via setSessionModel, so
  // a pick in one chat never affects any other chat.
  newChatModels: Record<string, string | null>;

  loadSessions: () => Promise<void>;
  switchSession: (id: string) => Promise<void>;
  createSession: () => Promise<void>;
  /**
   * Materialize the virtual "new chat" in the API and adopt the server-minted
   * id, returning it. No-op (returns the active id unchanged) once the chat is
   * already a real, persisted session. Pass `running: true` when a message is
   * being sent at the same time so the sidebar row shows the spinner instantly.
   */
  ensureRealSession: (running?: boolean) => Promise<string>;
  discardVirtualSession: () => void;
  setDraft: (sessionId: string, text: string) => void;
  deleteSession: (id: string) => Promise<void>;
  archiveSession: (id: string) => Promise<void>;
  renameSession: (id: string, title: string) => Promise<void>;
  toggleStar: (id: string) => Promise<void>;
  searchSessions: (query: string) => Promise<void>;
  clearSearch: () => void;
  /** Trigger the sidebar to mount + focus the search input (used by Cmd+K). */
  requestSearchFocus: () => void;
  sendMessage: (content: string) => void;
  /** Defer a composed prompt into a new session without running the model
   *  now. ``delay`` is one of "30m" | "1h" | "24h" | "none". */
  runLater: (
    content: string,
    delay: string,
    fileIds?: string[],
    imageBlocks?: Array<{ url: string; filename: string; media_type: string }>,
  ) => Promise<void>;
  /** Fetch selectable models for the composer picker (GET /api/models). */
  loadModels: () => Promise<void>;
  setNewChatBackend: (backend: string | null) => void;
  /** Patch (or close with null) the new-chat review-loop panel state. */
  setNewChatReviewLoop: (patch: Partial<NewChatReviewLoop> | null) => void;
  /** Set the model for the current virtual chat (null → server default). */
  setNewChatModel: (backend: string, model: string | null) => void;
  /** Re-point ONE existing session's model (persisted on its row). */
  setSessionModel: (sessionId: string, model: string) => Promise<void>;
  stopSession: () => void;
  handleWSMessage: (msg: WSMessage) => void;
  addQuote: (text: string, action: QuoteAction) => void;
  removeQuote: (id: string) => void;
  updateQuoteInstruction: (id: string, instruction: string) => void;
  clearQuotes: () => void;
  // Side panel actions
  openPanelTab: (tab: PanelTab) => void;
  closePanelTab: (tabId: string) => void;
  focusPanelTab: (tabId: string) => void;
  updatePanelTab: (tabId: string, updates: Partial<PanelTab>) => void;
  togglePanel: () => void;
  setPanelWidth: (width: number) => void;
  setChatWidth: (width: number) => void;
  setSidebarWidth: (width: number) => void;
  pruneCompletedTabs: () => void;
  // Interactions
  answerInteraction: (result: Record<string, string> | null) => void;
  denyInteraction: (message?: string) => void;
  toggleSidebar: () => void;
  setMobileSidebarOpen: (open: boolean) => void;
  /** Show/hide the session list, whichever form it takes on this viewport. */
  toggleSessionList: () => void;
  /** Make sure the session list is on screen (Cmd+K, before focusing search). */
  revealSessionList: () => void;
  // Modified files
  fetchModifiedFiles: (sessionId: string) => Promise<void>;
  openFilesPanel: () => void;
}

/**
 * Rebuild the unsent chat from its persisted id, if there is one. Dropped
 * unless it still has draft text — a restored empty "New chat" is just noise
 * in the sidebar, and the whole point of restoring is the unsent text.
 */
function restoreVirtualSession(): Session | null {
  const stored = loadVirtualSession();
  if (!stored) return null;
  const drafts = loadDrafts();
  if (!(drafts[stored.id] || '').trim()) {
    clearVirtualSession();
    return null;
  }
  return {
    id: stored.id, title: '', source: 'web', status: 'created',
    updated_at: stored.created, is_running: false,
  };
}

export const useChatStore = create<ChatState>((set, get) => ({
  sessions: [],
  activeSession: '',
  // Rehydrated too: an unsent chat's id is the key its draft is stored under,
  // so losing the id on reload orphaned the draft. Restoring it brings the
  // half-written prompt back with the chat.
  virtualSession: restoreVirtualSession(),
  // Rehydrated from localStorage so unsent composer text survives a reload.
  drafts: loadDrafts(),
  messages: [],
  streamingBlocks: [],
  isStreaming: false,
  loading: false,
  agentStatus: { state: 'idle' },
  contextUsage: null,
  backendStatus: null,
  currentTodos: [],
  currentCCTasks: [],
  quotes: [],
  panels: [],
  activePanelId: null,
  panelVisible: false,
  panelWidth: parseFloat(localStorage.getItem('nerve_panel_width') || '45'),
  chatWidth: parseFloat(localStorage.getItem('nerve_chat_width') || '768'),
  sidebarWidth: parseFloat(localStorage.getItem('nerve_sidebar_width') || '240'),
  pendingInteraction: null,
  sidebarCollapsed: localStorage.getItem('nerve_sidebar_collapsed') === 'true',
  mobileSidebarOpen: false,
  modifiedFiles: [],
  modifiedFilesCount: 0,
  backgroundTasks: [],
  searchQuery: '',
  searchResults: null,
  searchLoading: false,
  searchFocusNonce: 0,
  availableModels: [],
  modelDefaults: {},
  backendOptions: [],
  backendDefault: null,
  newChatBackend: null,
  newChatReviewLoop: null,
  newChatModels: {},

  addQuote: (text: string, action: QuoteAction) => {
    const id = `q${++_quoteId}`;
    const instruction = QUOTE_DEFAULTS[action];
    set(s => ({ quotes: [...s.quotes, { id, text, action, instruction }] }));
  },
  removeQuote: (id: string) => set(s => ({ quotes: s.quotes.filter(q => q.id !== id) })),
  updateQuoteInstruction: (id: string, instruction: string) => set(s => ({
    quotes: s.quotes.map(q => q.id === id ? { ...q, instruction } : q),
  })),
  clearQuotes: () => set({ quotes: [] }),

  // ------------------------------------------------------------------ //
  //  Side panel actions                                                  //
  // ------------------------------------------------------------------ //

  openPanelTab: (tab: PanelTab) => {
    const s = get();
    const existing = s.panels.find(p => p.id === tab.id);
    if (existing) {
      // Tab already exists — just focus it
      set({ activePanelId: tab.id, panelVisible: true });
    } else {
      set({
        panels: [...s.panels, tab],
        activePanelId: tab.id,
        panelVisible: true,
      });
      // Auto-prune after adding
      get().pruneCompletedTabs();
    }
  },

  closePanelTab: (tabId: string) => {
    cancelAutoClose(tabId);
    set(s => {
      const remaining = s.panels.filter(p => p.id !== tabId);
      let nextActive = s.activePanelId;
      if (s.activePanelId === tabId) {
        const idx = s.panels.findIndex(p => p.id === tabId);
        nextActive = remaining[Math.min(idx, remaining.length - 1)]?.id || null;
      }
      return {
        panels: remaining,
        activePanelId: nextActive,
        panelVisible: remaining.length > 0 ? s.panelVisible : false,
      };
    });
  },

  focusPanelTab: (tabId: string) => {
    set({ activePanelId: tabId, panelVisible: true });
  },

  updatePanelTab: (tabId: string, updates: Partial<PanelTab>) => {
    set(s => ({
      panels: s.panels.map(p => p.id === tabId ? { ...p, ...updates } : p),
    }));
  },

  togglePanel: () => {
    set(s => ({ panelVisible: !s.panelVisible }));
  },

  setPanelWidth: (width: number) => {
    const clamped = Math.max(20, Math.min(65, width));
    localStorage.setItem('nerve_panel_width', String(clamped));
    set({ panelWidth: clamped });
  },

  setChatWidth: (width: number) => {
    // Clamp to a readable band (~60 chars min, very wide max). Mirrors the
    // setPanelWidth persistence pattern above.
    const clamped = Math.max(480, Math.min(2000, width));
    localStorage.setItem('nerve_chat_width', String(clamped));
    set({ chatWidth: clamped });
  },

  setSidebarWidth: (width: number) => {
    const clamped = Math.max(180, Math.min(480, width));
    localStorage.setItem('nerve_sidebar_width', String(clamped));
    set({ sidebarWidth: clamped });
  },

  pruneCompletedTabs: () => {
    set(s => {
      const completed = s.panels.filter(p => p.status === 'complete' || p.status === 'error');
      if (completed.length <= MAX_COMPLETED_TABS) return {};
      const running = s.panels.filter(p => p.status === 'running');
      // Keep the most recent completed tabs
      const sorted = [...completed].sort((a, b) => (b.completedAt || 0) - (a.completedAt || 0));
      const keep = new Set([
        ...running.map(p => p.id),
        ...sorted.slice(0, MAX_COMPLETED_TABS).map(p => p.id),
      ]);
      // Never prune the focused tab
      if (s.activePanelId) keep.add(s.activePanelId);
      return { panels: s.panels.filter(p => keep.has(p.id)) };
    });
  },

  // ------------------------------------------------------------------ //
  //  Interactions                                                        //
  // ------------------------------------------------------------------ //

  answerInteraction: (result: Record<string, string> | null) => {
    const pending = get().pendingInteraction;
    if (!pending) return;
    ws.answerInteraction(get().activeSession, pending.interactionId, result);
    set({ pendingInteraction: null });
    // Panel cleanup is handled by the SidePanel component (closePanelTab on approve)
  },

  denyInteraction: (message?: string) => {
    const pending = get().pendingInteraction;
    if (!pending) return;
    ws.answerInteraction(get().activeSession, pending.interactionId, null, true, message || '');
    set({ pendingInteraction: null });
  },

  toggleSidebar: () => {
    const next = !get().sidebarCollapsed;
    localStorage.setItem('nerve_sidebar_collapsed', String(next));
    set({ sidebarCollapsed: next });
  },

  setMobileSidebarOpen: (open: boolean) => set({ mobileSidebarOpen: open }),

  // Both entry points below branch on the viewport so that the header button
  // and the keyboard shortcuts stay in agreement — and so neither writes the
  // persisted desktop preference from a phone.
  toggleSessionList: () => {
    if (isMobileViewport()) set({ mobileSidebarOpen: !get().mobileSidebarOpen });
    else get().toggleSidebar();
  },

  revealSessionList: () => {
    if (isMobileViewport()) set({ mobileSidebarOpen: true });
    else if (get().sidebarCollapsed) get().toggleSidebar();
  },

  // ------------------------------------------------------------------ //
  //  Modified files                                                       //
  // ------------------------------------------------------------------ //

  fetchModifiedFiles: async (sessionId: string) => {
    try {
      const data = await api.getModifiedFiles(sessionId);
      set({
        modifiedFiles: data.files,
        modifiedFilesCount: data.files.length,
      });
    } catch {
      // Silently fail — modified files is non-critical
    }
  },

  openFilesPanel: () => {
    const s = get();
    const existing = s.panels.find(p => p.id === 'files-panel');
    if (existing) {
      set({ activePanelId: 'files-panel', panelVisible: true });
    } else {
      get().openPanelTab({
        id: 'files-panel',
        type: 'files',
        label: 'Files',
        subagentType: 'files',
        description: '',
        content: null,
        prompt: '',
        streaming: false,
        status: 'complete',
        startedAt: Date.now(),
        blocks: [],
      });
    }
  },

  // ------------------------------------------------------------------ //
  //  Session management                                                  //
  // ------------------------------------------------------------------ //

  loadSessions: async () => {
    try {
      const { sessions } = await api.listSessions();
      set({ sessions });
      // Reclaim persisted drafts whose session no longer exists (deleted or
      // archived elsewhere) — but never the chat you're in or an unsent one.
      const keep = new Set(sessions.map(s => s.id));
      const { activeSession, virtualSession } = get();
      if (activeSession) keep.add(activeSession);
      if (virtualSession) keep.add(virtualSession.id);
      pruneDrafts(keep);
    } catch (e) {
      console.error('Failed to load sessions:', e);
    }
  },

  switchSession: async (id: string) => {
    // Leaving an untouched (empty-draft) virtual chat discards it, so the
    // sidebar never accumulates empty "New chat" entries. A filled
    // review-loop form counts as touched — don't silently drop it.
    const vs = get().virtualSession;
    const rl = get().newChatReviewLoop;
    const rlDirty = !!(rl && (rl.goal.trim() || rl.verifier.trim()));
    if (vs && get().activeSession === vs.id && id !== vs.id
        && !(get().drafts[vs.id] || '').trim() && !rlDirty) {
      clearVirtualSession();
      set((s) => {
        const drafts = { ...s.drafts };
        delete drafts[vs.id];
        return { virtualSession: null, drafts };
      });
    }
    if (id === get().activeSession && get().messages.length > 0) return;
    // Clear all auto-close timers
    clearAllAutoCloseTimers();
    set({
      activeSession: id, messages: [], loading: true, streamingBlocks: [],
      isStreaming: false, agentStatus: { state: 'idle' }, contextUsage: null,
      backendStatus: null,
      currentTodos: [], currentCCTasks: [], pendingInteraction: null,
      panels: [], activePanelId: null, panelVisible: false,
      modifiedFiles: [], modifiedFilesCount: 0, backgroundTasks: [],
    });
    // A virtual chat isn't known to the server (it's created on first send),
    // so don't announce a switch to it — that would raise "Session not found"
    // and drop the socket. The active-session event guard isolates the view
    // from the previously-bound session, and there's nothing to fetch.
    if (id === get().virtualSession?.id) {
      set({ loading: false });
      return;
    }
    ws.switchSession(id);
    // Note: opening a chat deliberately does NOT touch updated_at (locally or
    // server-side) — updated_at means "last message activity", so browsing
    // never reorders the session list.
    try {
      const data = await api.getMessages(id);
      const hydrated = data.messages.map(hydrateMessage);
      const update: Record<string, unknown> = {
        messages: hydrated,
        loading: false,
      };
      // Restore context usage from last turn (for context bar)
      if (data.last_usage) {
        const cc = data.last_usage.cache_creation as
          | { ephemeral_5m_input_tokens?: number; ephemeral_1h_input_tokens?: number }
          | undefined;
        update.contextUsage = {
          input_tokens: data.last_usage.input_tokens || 0,
          output_tokens: data.last_usage.output_tokens || 0,
          cache_creation_input_tokens: data.last_usage.cache_creation_input_tokens || 0,
          cache_read_input_tokens: data.last_usage.cache_read_input_tokens || 0,
          cache_creation_5m_input_tokens: cc?.ephemeral_5m_input_tokens ?? 0,
          cache_creation_1h_input_tokens: cc?.ephemeral_1h_input_tokens ?? 0,
          max_context_tokens: data.last_usage.max_context_tokens || 200_000,
          num_turns: data.last_usage.num_turns || 1,
        };
      }
      // Restore todos from last TodoWrite call in history (legacy)
      update.currentTodos = extractTodosFromMessages(hydrated);
      // Restore Claude Code 2.1+ task panel from history
      update.currentCCTasks = extractCCTasksFromMessages(hydrated);
      set(update);
      // Fetch modified files for this session (non-blocking)
      get().fetchModifiedFiles(id);
    } catch {
      set({ loading: false });
    }
  },

  createSession: async () => {
    // The + button no longer hits the API: it mints a local "virtual" chat
    // that's created server-side (POST) only on its first message, then adopts
    // the server id. Reuse an existing unsent one rather than stacking empty
    // chats. The temp id is a full UUID so it never collides with a real
    // server id (uuid4()[:8]) and is never sent to the backend.
    const existing = get().virtualSession;
    if (existing) {
      if (get().activeSession !== existing.id) await get().switchSession(existing.id);
      return;
    }
    const id = randomUUID();
    const now = new Date().toISOString();
    const virtual: Session = {
      id, title: '', source: 'web', status: 'created',
      updated_at: now, is_running: false,
    };
    // Persist the id: it's the key this chat's draft is written under, and
    // without it on disk a reload strands the draft under an id nothing
    // remembers.
    persistVirtualSession(id, now);
    set({ virtualSession: virtual });
    await get().switchSession(id);
  },

  ensureRealSession: async (running = false) => {
    const session = get().activeSession;
    const vs = get().virtualSession;
    // Already a real, persisted session (or no virtual chat) — nothing to do.
    if (!vs || vs.id !== session) return session;
    // Create it server-side (deferred from the + click) and adopt the
    // server-minted id, so anything needing a persisted session — the first
    // message OR a file upload before it — targets a real row, not the
    // client-only temp id (which the backend has never seen → 404).
    const rl = get().newChatReviewLoop;
    const rlPayload = rl && rl.goal.trim() && rl.verifier.trim()
      ? {
          goal: rl.goal.trim(),
          verifier: rl.verifier.trim(),
          ...(rl.budget.trim() && !isNaN(parseFloat(rl.budget)) ? { budget_usd: parseFloat(rl.budget) } : {}),
          ...(rl.maxIterations.trim() && !isNaN(parseInt(rl.maxIterations, 10)) ? { max_iterations: parseInt(rl.maxIterations, 10) } : {}),
          // Always sent: an explicit UI "no" must override an operator-
          // configured ask/auto default.
          criteria_adoption: rl.adoption,
          ...(rl.implementerEngine || rl.implementerModel ? {
            implementer: {
              ...(rl.implementerEngine ? { engine: rl.implementerEngine } : {}),
              ...(rl.implementerModel ? { model: rl.implementerModel } : {}),
            },
          } : {}),
          ...(rl.verifierEngine || rl.verifierModel ? {
            verifier_leg: {
              ...(rl.verifierEngine ? { engine: rl.verifierEngine } : {}),
              ...(rl.verifierModel ? { model: rl.verifierModel } : {}),
            },
          } : {}),
        }
      : null;
    // The loop's workdir rides the session-level cwd (the loop inherits the
    // observer session's cwd server-side); only sent when a loop is bound.
    const rlCwd = rlPayload && rl ? rl.cwd.trim() || undefined : undefined;
    // Composer's model pick for the chosen backend — sent at creation so
    // the session row (and the header badge) carries it from the start,
    // instead of the backend default until the first turn resolves it.
    const effBackend = get().newChatBackend ?? get().backendDefault ?? 'claude';
    const pickedModel = get().newChatModels[effBackend] ?? undefined;
    const real: Session = await api.createSession(
      undefined, get().newChatBackend, rlCwd, rlPayload, pickedModel,
    );
    set((state) => {
      const drafts = { ...state.drafts };
      // Carry any unsent draft text across to the real id so the composer,
      // which reloads from drafts[activeSession] on id change, doesn't blank.
      const carried = drafts[vs.id];
      delete drafts[vs.id];
      if (carried !== undefined) drafts[real.id] = carried;
      // Mirror the carry in storage: the temp id's key is dead; the real id
      // now owns the draft.
      removeDraft(vs.id);
      if (carried) persistDraft(real.id, carried);
      // The chat is real now — nothing left to restore on reload.
      clearVirtualSession();
      return {
        // Don't yank the view if the user navigated away during the POST.
        ...(state.activeSession === vs.id ? { activeSession: real.id } : {}),
        virtualSession: null,
        newChatBackend: null,  // bound into the created session; reset for the next chat
        newChatModels: {},     // ditto — the pick now lives on the session row
        newChatReviewLoop: null,  // ditto — the loop (if any) is now running server-side
        drafts,
        // POST /api/sessions returns a partial row (no updated_at); fill the
        // fields the sidebar needs so date-grouping doesn't choke.
        sessions: [
          { ...real, title: 'New chat', is_running: running, updated_at: new Date().toISOString() },
          ...state.sessions,
        ],
      };
    });
    return real.id;
  },

  discardVirtualSession: () => {
    const vs = get().virtualSession;
    if (!vs) return;
    set({ newChatBackend: null, newChatModels: {}, newChatReviewLoop: null });
    removeDraft(vs.id);
    clearVirtualSession();
    set((s) => {
      const drafts = { ...s.drafts };
      delete drafts[vs.id];
      return { virtualSession: null, drafts };
    });
    // If it was the active chat, fall back to the most recent real session.
    if (get().activeSession === vs.id) {
      const remaining = get().sessions;
      if (remaining.length > 0) get().switchSession(remaining[0].id);
      else set({ activeSession: '', messages: [] });
    }
  },

  setDraft: (sessionId: string, text: string) =>
    set((s) => {
      persistDraft(sessionId, text);
      return { drafts: { ...s.drafts, [sessionId]: text } };
    }),

  deleteSession: async (id: string) => {
    try {
      await api.deleteSession(id);
      removeDraft(id);
      set(s => { const drafts = { ...s.drafts }; delete drafts[id]; return { drafts }; });
      await get().loadSessions();
      if (get().activeSession === id) {
        // Switch to most recent remaining session
        const remaining = get().sessions.filter(s => s.id !== id);
        if (remaining.length > 0) {
          await get().switchSession(remaining[0].id);
        }
      }
    } catch (e) {
      console.error('Failed to delete session:', e);
    }
  },

  archiveSession: async (id: string) => {
    try {
      await api.archiveSession(id);
      removeDraft(id);
      set(s => { const drafts = { ...s.drafts }; delete drafts[id]; return { drafts }; });
      await get().loadSessions();
      if (get().activeSession === id) {
        // Switch to most recent remaining session
        const remaining = get().sessions.filter(s => s.id !== id);
        if (remaining.length > 0) {
          await get().switchSession(remaining[0].id);
        }
      }
    } catch (e) {
      console.error('Failed to archive session:', e);
    }
  },

  renameSession: async (id: string, title: string) => {
    try {
      await api.updateSession(id, { title });
      set(s => ({
        sessions: s.sessions.map(sess =>
          sess.id === id ? { ...sess, title } : sess
        ),
      }));
    } catch (e) {
      console.error('Failed to rename session:', e);
    }
  },

  toggleStar: async (id: string) => {
    const session = get().sessions.find(s => s.id === id);
    if (!session) return;
    const starred = !session.starred;
    try {
      await api.updateSession(id, { starred });
      set(s => ({
        sessions: s.sessions.map(sess =>
          sess.id === id ? { ...sess, starred } : sess
        ),
      }));
    } catch (e) {
      console.error('Failed to toggle star:', e);
    }
  },

  searchSessions: async (query: string) => {
    if (!query.trim()) {
      set({ searchResults: null, searchLoading: false, searchQuery: '' });
      return;
    }
    set({ searchQuery: query, searchLoading: true });
    try {
      const { sessions } = await api.searchSessions(query.trim());
      // Only apply if query hasn't changed while we were fetching
      if (get().searchQuery === query) {
        set({ searchResults: sessions, searchLoading: false });
      }
    } catch (e) {
      console.error('Failed to search sessions:', e);
      if (get().searchQuery === query) {
        set({ searchLoading: false });
      }
    }
  },

  clearSearch: () => {
    set({ searchQuery: '', searchResults: null, searchLoading: false });
  },

  requestSearchFocus: () => {
    set(s => ({ searchFocusNonce: s.searchFocusNonce + 1 }));
  },

  loadModels: async () => {
    try {
      const res = await api.getModels();
      // The pick used to be a global localStorage preference that leaked
      // into every chat; it's per-chat state now — drop the legacy keys.
      localStorage.removeItem('nerve_selected_model');
      localStorage.removeItem('nerve_selected_model_claude');
      localStorage.removeItem('nerve_selected_model_codex');
      set((state) => {
        // Drop a stale new-chat pick (e.g. an Ollama model no longer
        // installed) so we never create a session on a model the server
        // can't route.
        const newChatModels = { ...state.newChatModels };
        for (const [backend, selected] of Object.entries(newChatModels)) {
          const ids = new Set(res.models.filter(m => m.backend === backend).map(m => m.id));
          if (selected && !ids.has(selected)) newChatModels[backend] = null;
        }
        return {
          availableModels: res.models,
          modelDefaults: res.defaults ?? { claude: res.default },
          backendOptions: res.backends?.options ?? [],
          backendDefault: res.backends?.default ?? null,
          newChatModels,
        };
      });
    } catch (e) {
      console.error('Failed to load models:', e);
    }
  },

  setNewChatBackend: (backend: string | null) => set({ newChatBackend: backend }),

  setNewChatReviewLoop: (patch) => set((s) => ({
    newChatReviewLoop: patch === null
      ? null
      : { ...(s.newChatReviewLoop ?? EMPTY_REVIEW_LOOP), ...patch },
  })),

  setNewChatModel: (backend: string, model: string | null) => set((state) => ({
    newChatModels: { ...state.newChatModels, [backend]: model },
  })),

  setSessionModel: async (sessionId: string, model: string) => {
    // Optimistic: the picker and header badge read sessions[].model, so
    // re-point the row immediately; revert if the server rejects the pick
    // (e.g. a model the session's backend can't serve).
    const prev = get().sessions.find(s => s.id === sessionId)?.model;
    const repoint = (m: string | undefined) => set((state) => ({
      sessions: state.sessions.map(s => s.id === sessionId ? { ...s, model: m } : s),
    }));
    repoint(model);
    try {
      await api.updateSession(sessionId, { model });
    } catch (e) {
      console.error('Failed to update session model:', e);
      repoint(prev);
    }
  },

  sendMessage: async (content: string, fileIds?: string[], imageBlocks?: Array<{ url: string; filename: string; media_type: string }>) => {
    let session = get().activeSession;
    const blocks: import('../types/chat').MessageBlock[] = [];
    if (content) blocks.push({ type: 'text', content });
    if (imageBlocks) {
      for (const img of imageBlocks) {
        blocks.push({ type: 'image', url: img.url, filename: img.filename, media_type: img.media_type });
      }
    }
    const vs = get().virtualSession;
    // Optimistic update: append the user message, flip to streaming. If the
    // socket isn't open, send() returns 'queued' (will flush on reconnect)
    // or 'dropped' (revert below).
    set((state) => ({
      messages: [...state.messages, { role: 'user' as const, blocks, created_at: new Date().toISOString() }],
      streamingBlocks: [],
      isStreaming: true,
      agentStatus: { state: 'thinking' as const },
    }));
    // First message in a virtual "new chat": materialize it in the API now
    // (deferred from the + click) and adopt the server-minted id for this turn,
    // so it becomes a real, selectable session that survives switching away.
    if (vs && vs.id === session) {
      try {
        session = await get().ensureRealSession(true);
      } catch (e) {
        console.error('Failed to create session:', e);
        set((state) => ({
          messages: [
            ...state.messages.slice(0, -1),
            { role: 'assistant' as const, blocks: [{ type: 'text', content: 'Error: could not start the chat. Please retry.' }] },
          ],
          streamingBlocks: [],
          isStreaming: false,
          agentStatus: { state: 'idle' },
        }));
        return;
      }
    }
    // No per-message model override: the session row is the source of
    // truth (bound at creation for new chats, PATCHed by setSessionModel
    // for existing ones), so a pick in another chat can never leak here.
    const status = ws.sendMessage(content, session, fileIds);
    if (status === 'dropped') {
      // The message could not reach the server. Revert the optimistic
      // state and surface the failure inline so the user knows to retry.
      set((state) => ({
        messages: [
          ...state.messages.slice(0, -1),
          {
            role: 'assistant' as const,
            blocks: [{
              type: 'text',
              content: 'Error: Message could not be sent. The connection is closed; please retry.',
            }],
          },
        ],
        streamingBlocks: [],
        isStreaming: false,
        agentStatus: { state: 'idle' },
      }));
    }
  },

  runLater: async (content, delay, fileIds, imageBlocks) => {
    // Build the optimistic block list once (pending user message shape).
    const buildBlocks = (): import('../types/chat').MessageBlock[] => {
      const blocks: import('../types/chat').MessageBlock[] = [];
      if (content) blocks.push({ type: 'text', content });
      if (imageBlocks) {
        for (const img of imageBlocks) {
          blocks.push({ type: 'image', url: img.url, filename: img.filename, media_type: img.media_type });
        }
      }
      return blocks;
    };
    // Run later ALWAYS mints a brand-new session — it must never write the
    // deferred prompt into the current chat (fresh, virtual, or existing).
    // If a composer model/backend pick is in flight for a new chat, carry it
    // onto the created row so the header badge is right from the first render.
    const vs = get().virtualSession;
    const effBackend = get().newChatBackend ?? null;
    const pickedModel = effBackend
      ? (get().newChatModels[effBackend] ?? null)
      : null;
    const real: Session = await api.createSession(
      undefined, effBackend, undefined, null, pickedModel,
    );
    const res = await api.runLater(real.id, content, delay, fileIds, imageBlocks);
    const now = new Date().toISOString();
    set((state) => {
      // Drop the transient new-chat state so we don't strand an empty virtual
      // chat or leak its draft/model picks into the next new chat.
      const drafts = { ...state.drafts };
      if (vs) delete drafts[vs.id];
      return {
        sessions: [
          { ...real, title: 'New chat', is_running: false, updated_at: now },
          ...state.sessions,
        ],
        activeSession: real.id,
        virtualSession: null,
        newChatBackend: null,
        newChatModels: {},
        newChatReviewLoop: null,
        drafts,
        streamingBlocks: [],
        isStreaming: false,
        agentStatus: { state: 'idle' as const },
        messages: [
          { role: 'user' as const, blocks: buildBlocks(), created_at: now },
          { role: 'assistant' as const, blocks: [{ type: 'text', content: res.ack }], created_at: now },
        ],
      };
    });
    if (vs) clearVirtualSession();
  },

  stopSession: () => {
    const session = get().activeSession;
    ws.stopSession(session);
  },

  // ------------------------------------------------------------------ //
  //  WebSocket message handler — thin dispatcher                         //
  // ------------------------------------------------------------------ //

  handleWSMessage: (msg: WSMessage) => {
    const sid = (msg as { session_id?: string }).session_id;
    if (sid && sid !== get().activeSession && VIEW_SCOPED_EVENTS.has(msg.type)) return;
    switch (msg.type) {
      // Streaming
      case 'thinking':     return handleThinking(msg, get, set);
      case 'token':        return handleToken(msg, get, set);
      case 'tool_use':     return handleToolUse(msg, get, set);
      case 'tool_result':  return handleToolResult(msg, get, set);
      case 'tool_output':  return handleToolOutput(msg, get, set);
      case 'done':         return handleDone(msg, get, set);
      case 'wakeup':       return handleWakeup(msg, get, set);
      case 'auto_turn':    return handleAutoTurn(msg, get, set);
      case 'model_changed': return handleModelChanged(msg, get, set);
      case 'stopped':      return handleStopped(msg, get, set);
      case 'error':        return handleError(msg, get, set);
      // Sessions
      case 'session_updated':  return handleSessionUpdated(msg, get, set);
      case 'session_status':   return handleSessionStatus(msg, get, set);
      case 'session_switched': return handleSessionSwitched(msg, get, set);
      case 'session_forked':   return handleSessionForked(msg, get, set);
      case 'session_resumed':  return handleSessionResumed(msg, get, set);
      case 'session_archived': return handleSessionArchived(msg, get, set);
      case 'session_running':  return handleSessionRunning(msg, get, set);
      case 'session_awaiting_input': return handleSessionAwaitingInput(msg, get, set);
      case 'answer_injected':  return handleAnswerInjected(msg, get, set);
      case 'user_message':     return handleUserMessage(msg, get, set);
      // Panels
      case 'plan_update':        return handlePlanUpdate(msg, get, set);
      case 'backend_status':
        set({ backendStatus: { subtype: msg.subtype, data: msg.data } });
        return;
      case 'subagent_start':     return handleSubagentStart(msg, get, set);
      case 'subagent_complete':  return handleSubagentComplete(msg, get, set);
      case 'workflow_progress':  return handleWorkflowProgress(msg, get, set);
      // Workflow runs — global event (session_id may be null); upsert into
      // the runs store so the /workflow-runs page stays live.
      case 'workflow_run_update':
        void import('./workflowRunStore').then(({ useWorkflowRunStore }) =>
          useWorkflowRunStore.getState().handleRunUpdate(msg.run)
        );
        return;
      // Review loops — global event (session_running pattern): chip state on
      // the observer session row + live milestone append for the open view.
      case 'review_loop_update': return handleReviewLoopUpdate(msg, get, set);
      // Tasks — global event (session_id is always null), so the board
      // reflects a change made by the agent in any session, by another
      // tab, or over the API. Must stay out of VIEW_SCOPED_EVENTS.
      case 'task_updated':
        void import('./taskStore').then(({ useTaskStore }) =>
          useTaskStore.getState().handleTaskEvent(msg.task)
        );
        return;
      // Auxiliary
      case 'interaction':              return handleInteraction(msg, get, set);
      case 'interaction_resolved':     return handleInteractionResolved(msg, get, set);
      case 'file_changed':             return handleFileChanged(msg, get, set);
      case 'notification':             return handleNotification(msg, get, set);
      case 'notification_answered':    return handleNotificationAnswered(msg, get, set);
      case 'notification_expired':     return handleNotificationExpired(msg, get, set);
      case 'background_tasks_update':  return handleBackgroundTasksUpdate(msg, get, set);
    }
  },
}));

// Re-export ChatState for handler type imports
export type { ChatState };
