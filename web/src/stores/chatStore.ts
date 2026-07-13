import { create } from 'zustand';
import { api } from '../api/client';
import { ws } from '../api/websocket';
import type { WSMessage } from '../api/websocket';
import type { ChatMessage, MessageBlock, Session, AgentStatus, PanelTab, ModifiedFileSummary } from '../types/chat';
import { hydrateMessage } from '../utils/hydrateMessage';
import { randomUUID } from '../utils/uuid';
// Helpers
import { cancelAutoClose, clearAllAutoCloseTimers, MAX_COMPLETED_TABS } from './helpers/blockHelpers';
import { extractTodosFromMessages, extractCCTasksFromMessages } from './helpers/bufferReplay';
// Handlers
import { handleThinking, handleToken, handleToolUse, handleToolResult, handleToolOutput, handleDone, handleStopped, handleError, handleWakeup, handleAutoTurn, handleModelChanged } from './handlers/streamingHandlers';
import { handleSessionUpdated, handleSessionStatus, handleSessionSwitched, handleSessionForked, handleSessionResumed, handleSessionArchived, handleSessionRunning, handleSessionAwaitingInput, handleAnswerInjected, handleUserMessage } from './handlers/sessionHandlers';
import { handlePlanUpdate, handleSubagentStart, handleSubagentComplete, handleHoaProgress, handleWorkflowProgress } from './handlers/panelHandlers';
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
  'subagent_start', 'subagent_complete', 'hoa_progress', 'interaction',
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

  // Sidebar collapse
  sidebarCollapsed: boolean;

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
  selectedModels: Record<string, string | null>;

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
  renameSession: (id: string, title: string) => Promise<void>;
  toggleStar: (id: string) => Promise<void>;
  searchSessions: (query: string) => Promise<void>;
  clearSearch: () => void;
  /** Trigger the sidebar to mount + focus the search input (used by Cmd+K). */
  requestSearchFocus: () => void;
  sendMessage: (content: string) => void;
  /** Fetch selectable models for the composer picker (GET /api/models). */
  loadModels: () => Promise<void>;
  setNewChatBackend: (backend: string | null) => void;
  /** Set the model for the next message (null → server default). */
  setSelectedModel: (backend: string, model: string | null) => void;
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
  // Modified files
  fetchModifiedFiles: (sessionId: string) => Promise<void>;
  openFilesPanel: () => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
  sessions: [],
  activeSession: '',
  virtualSession: null,
  drafts: {},
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
  selectedModels: {
    claude: localStorage.getItem('nerve_selected_model_claude')
      || localStorage.getItem('nerve_selected_model') || null,
    codex: localStorage.getItem('nerve_selected_model_codex') || null,
  },

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
    } catch (e) {
      console.error('Failed to load sessions:', e);
    }
  },

  switchSession: async (id: string) => {
    // Leaving an untouched (empty-draft) virtual chat discards it, so the
    // sidebar never accumulates empty "New chat" entries.
    const vs = get().virtualSession;
    if (vs && get().activeSession === vs.id && id !== vs.id
        && !(get().drafts[vs.id] || '').trim()) {
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
    const real: Session = await api.createSession(
      undefined, get().newChatBackend,
    );
    set((state) => {
      const drafts = { ...state.drafts };
      // Carry any unsent draft text across to the real id so the composer,
      // which reloads from drafts[activeSession] on id change, doesn't blank.
      const carried = drafts[vs.id];
      delete drafts[vs.id];
      if (carried !== undefined) drafts[real.id] = carried;
      return {
        // Don't yank the view if the user navigated away during the POST.
        ...(state.activeSession === vs.id ? { activeSession: real.id } : {}),
        virtualSession: null,
        newChatBackend: null,  // bound into the created session; reset for the next chat
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
    set({ newChatBackend: null });
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
    set((s) => ({ drafts: { ...s.drafts, [sessionId]: text } })),

  deleteSession: async (id: string) => {
    try {
      await api.deleteSession(id);
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
      set((state) => {
        // Drop a stale pick (e.g. an Ollama model no longer installed) so we
        // never send a model the server can't route.
        const selectedModels = { ...state.selectedModels };
        for (const backend of ['claude', 'codex']) {
          const ids = new Set(res.models.filter(m => m.backend === backend).map(m => m.id));
          const selected = selectedModels[backend];
          if (selected && !ids.has(selected)) {
            selectedModels[backend] = null;
            localStorage.removeItem(`nerve_selected_model_${backend}`);
          }
        }
        localStorage.removeItem('nerve_selected_model');
        return {
          availableModels: res.models,
          modelDefaults: res.defaults ?? { claude: res.default },
          backendOptions: res.backends?.options ?? [],
          backendDefault: res.backends?.default ?? null,
          selectedModels,
        };
      });
    } catch (e) {
      console.error('Failed to load models:', e);
    }
  },

  setNewChatBackend: (backend: string | null) => set({ newChatBackend: backend }),

  setSelectedModel: (backend: string, model: string | null) => {
    const key = `nerve_selected_model_${backend}`;
    if (model) localStorage.setItem(key, model);
    else localStorage.removeItem(key);
    set((state) => ({
      selectedModels: { ...state.selectedModels, [backend]: model },
    }));
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
      messages: [...state.messages, { role: 'user' as const, blocks }],
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
    const state = get();
    const backend = state.sessions.find(s => s.id === session)?.backend
      ?? state.backendDefault ?? 'claude';
    const status = ws.sendMessage(
      content, session, fileIds, state.selectedModels[backend] ?? undefined,
    );
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
      case 'hoa_progress':       return handleHoaProgress(msg, get, set);
      case 'workflow_progress':  return handleWorkflowProgress(msg, get, set);
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
