import { create } from 'zustand';
import { api } from '../api/client';
import { ws } from '../api/websocket';
import type { WSMessage } from '../api/websocket';
import type { ChatMessage, MessageBlock, Session, AgentStatus, PanelTab, ModifiedFileSummary } from '../types/chat';
import { hydrateMessage } from '../utils/hydrateMessage';
// Helpers
import { cancelAutoClose, clearAllAutoCloseTimers, MAX_COMPLETED_TABS } from './helpers/blockHelpers';
import { extractTodosFromMessages } from './helpers/bufferReplay';
// Handlers
import { handleThinking, handleToken, handleToolUse, handleToolResult, handleDone, handleStopped, handleError } from './handlers/streamingHandlers';
import { handleSessionUpdated, handleSessionStatus, handleSessionSwitched, handleSessionForked, handleSessionResumed, handleSessionArchived, handleSessionRunning, handleAnswerInjected } from './handlers/sessionHandlers';
import { handlePlanUpdate, handleSubagentStart, handleSubagentComplete, handleHoaProgress } from './handlers/panelHandlers';
import { handleInteraction, handleFileChanged, handleNotification, handleNotificationAnswered, handleBackgroundTasksUpdate } from './handlers/auxiliaryHandlers';

export interface TodoItem {
  content: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm: string;
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

interface ChatState {
  sessions: Session[];
  activeSession: string;
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
    max_context_tokens: number;
    num_turns: number;
  } | null;
  // TodoWrite panel state
  currentTodos: TodoItem[];
  // Text selection quotes
  quotes: QuoteEntry[];

  // Side panel — generic tabbed panel for sub-agents, plans, etc.
  panels: PanelTab[];
  activePanelId: string | null;
  panelVisible: boolean;
  panelWidth: number;

  // Pending interactive tool (AskUserQuestion, ExitPlanMode, etc.)
  pendingInteraction: {
    interactionId: string;
    interactionType: 'question' | 'plan_exit' | 'plan_enter';
    toolName: string;
    toolInput: Record<string, unknown>;
  } | null;

  // Sidebar collapse
  sidebarCollapsed: boolean;

  // Modified files tracking
  modifiedFiles: ModifiedFileSummary[];
  modifiedFilesCount: number;

  // Background tasks (run_in_background)
  backgroundTasks: { task_id: string; label: string; tool: string; status: 'running' | 'done' | 'timeout'; startedAt: number }[];

  // Session search
  searchQuery: string;
  searchResults: Session[] | null;  // null = not searching
  searchLoading: boolean;

  loadSessions: () => Promise<void>;
  switchSession: (id: string) => Promise<void>;
  createSession: (title?: string) => Promise<void>;
  deleteSession: (id: string) => Promise<void>;
  renameSession: (id: string, title: string) => Promise<void>;
  toggleStar: (id: string) => Promise<void>;
  searchSessions: (query: string) => Promise<void>;
  clearSearch: () => void;
  sendMessage: (content: string) => void;
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
  messages: [],
  streamingBlocks: [],
  isStreaming: false,
  loading: false,
  agentStatus: { state: 'idle' },
  contextUsage: null,
  currentTodos: [],
  quotes: [],
  panels: [],
  activePanelId: null,
  panelVisible: false,
  panelWidth: parseFloat(localStorage.getItem('nerve_panel_width') || '45'),
  pendingInteraction: null,
  sidebarCollapsed: localStorage.getItem('nerve_sidebar_collapsed') === 'true',
  modifiedFiles: [],
  modifiedFilesCount: 0,
  backgroundTasks: [],
  searchQuery: '',
  searchResults: null,
  searchLoading: false,

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
    if (id === get().activeSession && get().messages.length > 0) return;
    // Clear all auto-close timers
    clearAllAutoCloseTimers();
    set({
      activeSession: id, messages: [], loading: true, streamingBlocks: [],
      isStreaming: false, agentStatus: { state: 'idle' }, contextUsage: null,
      currentTodos: [], pendingInteraction: null,
      panels: [], activePanelId: null, panelVisible: false,
      modifiedFiles: [], modifiedFilesCount: 0, backgroundTasks: [],
    });
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
        update.contextUsage = {
          input_tokens: data.last_usage.input_tokens || 0,
          output_tokens: data.last_usage.output_tokens || 0,
          cache_creation_input_tokens: data.last_usage.cache_creation_input_tokens || 0,
          cache_read_input_tokens: data.last_usage.cache_read_input_tokens || 0,
          max_context_tokens: data.last_usage.max_context_tokens || 200_000,
          num_turns: data.last_usage.num_turns || 1,
        };
      }
      // Restore todos from last TodoWrite call in history
      update.currentTodos = extractTodosFromMessages(hydrated);
      set(update);
      // Fetch modified files for this session (non-blocking)
      get().fetchModifiedFiles(id);
    } catch {
      set({ loading: false });
    }
  },

  createSession: async (title?: string) => {
    try {
      const session = await api.createSession(title);
      await get().loadSessions();
      await get().switchSession(session.id);
    } catch (e) {
      console.error('Failed to create session:', e);
    }
  },

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

  sendMessage: (content: string, fileIds?: string[], imageBlocks?: Array<{ url: string; filename: string; media_type: string }>) => {
    const session = get().activeSession;
    const blocks: import('../types/chat').MessageBlock[] = [];
    if (content) blocks.push({ type: 'text', content });
    if (imageBlocks) {
      for (const img of imageBlocks) {
        blocks.push({ type: 'image', url: img.url, filename: img.filename, media_type: img.media_type });
      }
    }
    set((state) => ({
      messages: [...state.messages, { role: 'user', blocks }],
      streamingBlocks: [],
      isStreaming: true,
      agentStatus: { state: 'thinking' },
    }));
    ws.sendMessage(content, session, fileIds);
  },

  stopSession: () => {
    const session = get().activeSession;
    ws.stopSession(session);
  },

  // ------------------------------------------------------------------ //
  //  WebSocket message handler — thin dispatcher                         //
  // ------------------------------------------------------------------ //

  handleWSMessage: (msg: WSMessage) => {
    switch (msg.type) {
      // Streaming
      case 'thinking':     return handleThinking(msg, get, set);
      case 'token':        return handleToken(msg, get, set);
      case 'tool_use':     return handleToolUse(msg, get, set);
      case 'tool_result':  return handleToolResult(msg, get, set);
      case 'done':         return handleDone(msg, get, set);
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
      case 'answer_injected':  return handleAnswerInjected(msg, get, set);
      // Panels
      case 'plan_update':        return handlePlanUpdate(msg, get, set);
      case 'subagent_start':     return handleSubagentStart(msg, get, set);
      case 'subagent_complete':  return handleSubagentComplete(msg, get, set);
      case 'hoa_progress':       return handleHoaProgress(msg, get, set);
      // Auxiliary
      case 'interaction':              return handleInteraction(msg, get, set);
      case 'file_changed':             return handleFileChanged(msg, get, set);
      case 'notification':             return handleNotification(msg, get, set);
      case 'notification_answered':    return handleNotificationAnswered(msg, get, set);
      case 'background_tasks_update':  return handleBackgroundTasksUpdate(msg, get, set);
    }
  },
}));

// Re-export ChatState for handler type imports
export type { ChatState };
