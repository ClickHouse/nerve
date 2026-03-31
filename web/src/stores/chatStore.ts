import { create } from 'zustand';
import { api } from '../api/client';
import { ws } from '../api/websocket';
import type { WSMessage } from '../api/websocket';
import type { ChatMessage, MessageBlock, Session, AgentStatus, PanelTab, ModifiedFileSummary } from '../types/chat';
import { hydrateMessage } from '../utils/hydrateMessage';
import { extractResultText } from '../utils/extractResultText';

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

/** Max completed tabs to keep before pruning oldest. */
const MAX_COMPLETED_TABS = 5;

/** Auto-close delay for completed non-plan tabs (ms). */
const AUTOCLOSE_DELAY = 5000;

/** Track pending auto-close timers so we can cancel on manual close. */
const _autoCloseTimers = new Map<string, ReturnType<typeof setTimeout>>();

/** Schedule auto-close for a completed non-plan tab. */
function _scheduleAutoClose(tabId: string) {
  // Cancel any existing timer for this tab
  const existing = _autoCloseTimers.get(tabId);
  if (existing) clearTimeout(existing);

  const timer = setTimeout(() => {
    _autoCloseTimers.delete(tabId);
    const state = useChatStore.getState();
    const tab = state.panels.find(p => p.id === tabId);
    // Only auto-close if still completed and not plan/files tab
    if (tab && tab.status !== 'running' && tab.type !== 'plan' && tab.type !== 'files') {
      state.closePanelTab(tabId);
    }
  }, AUTOCLOSE_DELAY);
  _autoCloseTimers.set(tabId, timer);
}

/** Cancel a pending auto-close (e.g., user manually closed the tab). */
function _cancelAutoClose(tabId: string) {
  const timer = _autoCloseTimers.get(tabId);
  if (timer) {
    clearTimeout(timer);
    _autoCloseTimers.delete(tabId);
  }
}

/**
 * Append a MessageBlock to a specific panel tab's blocks array.
 * Merges consecutive thinking/text blocks for efficiency.
 */
function _appendBlockToPanel(panels: PanelTab[], panelId: string, block: MessageBlock): PanelTab[] {
  return panels.map(p => {
    if (p.id !== panelId) return p;
    const blocks = [...p.blocks];
    const last = blocks[blocks.length - 1];
    if (block.type === 'thinking' && last?.type === 'thinking') {
      blocks[blocks.length - 1] = { ...last, content: last.content + block.content };
    } else if (block.type === 'text' && last?.type === 'text') {
      blocks[blocks.length - 1] = { ...last, content: last.content + block.content };
    } else {
      blocks.push(block);
    }
    return { ...p, blocks };
  });
}

/** Update a tool_call block's result in a specific panel tab. */
function _updateToolResultInPanel(
  panels: PanelTab[],
  panelId: string,
  toolUseId: string,
  result: string,
  isError?: boolean,
): PanelTab[] {
  return panels.map(p => {
    if (p.id !== panelId) return p;
    const blocks = p.blocks.map(b => {
      if (b.type === 'tool_call' && b.toolUseId === toolUseId) {
        return { ...b, result, isError, status: 'complete' as const };
      }
      return b;
    });
    return { ...p, blocks };
  });
}

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
    _cancelAutoClose(tabId);
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
    for (const [, timer] of _autoCloseTimers) {
      clearTimeout(timer);
    }
    _autoCloseTimers.clear();
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

  sendMessage: (content: string) => {
    const session = get().activeSession;
    set((state) => ({
      messages: [...state.messages, { role: 'user', blocks: [{ type: 'text', content }] }],
      streamingBlocks: [],
      isStreaming: true,
      agentStatus: { state: 'thinking' },
    }));
    ws.sendMessage(content, session);
  },

  stopSession: () => {
    const session = get().activeSession;
    ws.stopSession(session);
  },

  // ------------------------------------------------------------------ //
  //  WebSocket message handler                                           //
  // ------------------------------------------------------------------ //

  handleWSMessage: (msg: WSMessage) => {
    const state = get();

    switch (msg.type) {
      case 'thinking': {
        const thinkParent = (msg as any).parent_tool_use_id as string | undefined;
        if (thinkParent && state.panels.some(p => p.id === thinkParent && p.status === 'running')) {
          set(s => ({
            panels: _appendBlockToPanel(s.panels, thinkParent, { type: 'thinking', content: msg.content }),
          }));
        } else {
          const blocks = [...state.streamingBlocks];
          const last = blocks[blocks.length - 1];
          if (last?.type === 'thinking') {
            blocks[blocks.length - 1] = { ...last, content: last.content + msg.content };
          } else {
            blocks.push({ type: 'thinking', content: msg.content });
          }
          set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });
        }
        break;
      }

      case 'token': {
        const tokenParent = (msg as any).parent_tool_use_id as string | undefined;
        if (tokenParent && state.panels.some(p => p.id === tokenParent && p.status === 'running')) {
          set(s => ({
            panels: _appendBlockToPanel(s.panels, tokenParent, { type: 'text', content: msg.content }),
          }));
        } else {
          const blocks = [...state.streamingBlocks];
          const last = blocks[blocks.length - 1];
          if (last?.type === 'text') {
            blocks[blocks.length - 1] = { ...last, content: last.content + msg.content };
          } else {
            blocks.push({ type: 'text', content: msg.content });
          }
          set({ streamingBlocks: blocks, agentStatus: { state: 'writing' } });
        }
        break;
      }

      case 'tool_use': {
        // Is this a Task (sub-agent) call?
        if (msg.tool === 'Task') {
          const toolUseId = msg.tool_use_id || '';
          // Add compact card to main chat
          const blocks = [...state.streamingBlocks];
          blocks.push({
            type: 'tool_call',
            toolUseId,
            tool: msg.tool,
            input: msg.input,
            status: 'running',
          });
          set({ streamingBlocks: blocks, agentStatus: { state: 'tool', toolName: msg.tool } });

          // Open panel tab
          const subagentType = String(msg.input?.subagent_type || msg.input?.model || 'agent');
          const isPlan = subagentType === 'Plan';
          get().openPanelTab({
            id: toolUseId,
            type: isPlan ? 'plan' : 'subagent',
            label: subagentType,
            subagentType,
            description: String(msg.input?.description || ''),
            model: msg.input?.model ? String(msg.input.model) : undefined,
            content: null,
            prompt: String(msg.input?.prompt || ''),
            streaming: true,
            status: 'running',
            startedAt: Date.now(),
            blocks: [],
          });
          break;
        }

        // Is this a child tool call inside a running sub-agent?
        const useParent = (msg as any).parent_tool_use_id as string | undefined;
        if (useParent && state.panels.some(p => p.id === useParent && p.status === 'running')) {
          set(s => ({
            panels: _appendBlockToPanel(s.panels, useParent, {
              type: 'tool_call',
              toolUseId: msg.tool_use_id || '',
              tool: msg.tool,
              input: msg.input,
              status: 'running',
            }),
          }));
        } else {
          // Normal: add to main chat
          const blocks = [...state.streamingBlocks];
          blocks.push({
            type: 'tool_call',
            toolUseId: msg.tool_use_id || '',
            tool: msg.tool,
            input: msg.input,
            status: 'running',
          });
          const extraUpdate: Record<string, unknown> = {};
          if (msg.tool === 'TodoWrite' && Array.isArray(msg.input?.todos)) {
            extraUpdate.currentTodos = msg.input.todos as TodoItem[];
          }
          set({ streamingBlocks: blocks, agentStatus: { state: 'tool', toolName: msg.tool }, ...extraUpdate });
        }
        break;
      }

      case 'tool_result': {
        // Is this a sub-agent (Task) completing?
        // Check if this tool_use_id matches a panel tab (= it's a Task result)
        const completingTab = state.panels.find(p => p.id === msg.tool_use_id && p.status === 'running');
        if (completingTab) {
          // Update compact card in main chat
          const blocks = state.streamingBlocks.map(b => {
            if (b.type === 'tool_call' && b.toolUseId === msg.tool_use_id) {
              return { ...b, result: msg.result, isError: msg.is_error, status: 'complete' as const };
            }
            return b;
          });
          set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });

          // Update panel tab with final content
          if (completingTab) {
            get().updatePanelTab(msg.tool_use_id!, {
              content: extractResultText(msg.result),
              streaming: false,
              status: msg.is_error ? 'error' : 'complete',
              isError: msg.is_error || false,
              completedAt: Date.now(),
            });
            // Auto-close non-plan tabs after delay
            if (completingTab.type !== 'plan') {
              _scheduleAutoClose(msg.tool_use_id!);
            }
          }
          break;
        }

        // Is this a child tool result inside a sub-agent?
        const resultParent = (msg as any).parent_tool_use_id as string | undefined;
        if (resultParent && state.panels.some(p => p.id === resultParent && p.status === 'running')) {
          set(s => ({
            panels: _updateToolResultInPanel(s.panels, resultParent, msg.tool_use_id || '', msg.result, msg.is_error),
          }));
        } else {
          // Normal: update main chat
          const blocks = state.streamingBlocks.map(b => {
            if (b.type === 'tool_call' && b.toolUseId === msg.tool_use_id) {
              return { ...b, result: msg.result, isError: msg.is_error, status: 'complete' as const };
            }
            return b;
          });
          set({ streamingBlocks: blocks, agentStatus: { state: 'thinking' } });

          // Update matching panel tab (for non-sub-agent panels like plan_update)
          const matchingTab = state.panels.find(p => p.id === msg.tool_use_id);
          if (matchingTab) {
            get().updatePanelTab(msg.tool_use_id!, {
              content: extractResultText(msg.result),
              streaming: false,
              status: msg.is_error ? 'error' : 'complete',
              isError: msg.is_error || false,
              completedAt: Date.now(),
            });
          }
        }
        break;
      }

      case 'done': {
        // (panels reset handles sub-agent cleanup)
        const doneUpdate: Record<string, unknown> = {
          agentStatus: { state: 'idle' },
        };
        if (msg.usage) {
          doneUpdate.contextUsage = {
            input_tokens: msg.usage.input_tokens || 0,
            output_tokens: msg.usage.output_tokens || 0,
            cache_creation_input_tokens: msg.usage.cache_creation_input_tokens || 0,
            cache_read_input_tokens: msg.usage.cache_read_input_tokens || 0,
            max_context_tokens: msg.max_context_tokens || 200_000,
          };
        }
        if (state.streamingBlocks.length > 0) {
          // Mark any running tool calls as complete
          const finalBlocks = state.streamingBlocks.map(b =>
            b.type === 'tool_call' && b.status === 'running'
              ? { ...b, status: 'complete' as const }
              : b
          );
          set((s) => ({
            messages: [...s.messages, { role: 'assistant' as const, blocks: finalBlocks }],
            streamingBlocks: [],
            isStreaming: false,
            ...doneUpdate,
          }));
        } else {
          set({ isStreaming: false, ...doneUpdate });
        }
        // Mark any still-running panel tabs as complete
        for (const panel of state.panels) {
          if (panel.status === 'running') {
            get().updatePanelTab(panel.id, {
              status: 'complete',
              streaming: false,
              completedAt: Date.now(),
            });
            if (panel.type !== 'plan') {
              _scheduleAutoClose(panel.id);
            }
          }
        }
        // Reload sessions to pick up updated_at changes
        get().loadSessions();
        break;
      }

      case 'stopped': {
        // (panels reset handles sub-agent cleanup)
        const finalBlocks = state.streamingBlocks.map(b =>
          b.type === 'tool_call' && b.status === 'running'
            ? { ...b, status: 'complete' as const }
            : b
        );
        if (finalBlocks.length > 0) {
          finalBlocks.push({ type: 'text', content: '\n\n*[Stopped by user]*' });
        }
        set((s) => ({
          messages: [...s.messages, {
            role: 'assistant' as const,
            blocks: finalBlocks.length > 0
              ? finalBlocks
              : [{ type: 'text', content: '*[Stopped by user]*' }],
          }],
          streamingBlocks: [],
          isStreaming: false,
          agentStatus: { state: 'idle' },
        }));
        // Mark any still-running panel tabs as complete
        for (const panel of state.panels) {
          if (panel.status === 'running') {
            get().updatePanelTab(panel.id, {
              status: 'complete',
              streaming: false,
              completedAt: Date.now(),
            });
            if (panel.type !== 'plan') {
              _scheduleAutoClose(panel.id);
            }
          }
        }
        get().loadSessions();
        break;
      }

      case 'session_updated': {
        if ('title' in msg) {
          set((s) => ({
            sessions: s.sessions.map(sess =>
              sess.id === msg.session_id ? { ...sess, title: (msg as any).title } : sess
            ),
          }));
        }
        break;
      }

      case 'session_status': {
        if ('is_running' in msg && (msg as any).is_running) {
          // Rebuild streaming state from buffered events
          let blocks: MessageBlock[] = [];
          const bufferedEvents = (msg as any).buffered_events as WSMessage[] | undefined;
          if (bufferedEvents) {
            for (const event of bufferedEvents) {
              blocks = applyStreamEvent(blocks, event);
            }
            // Rebuild panel tabs from buffered events
            const restored = rebuildPanelTabsFromBuffer(bufferedEvents, blocks);

            // Restore pending interaction from buffer (last interaction event wins)
            let restoredInteraction: typeof state.pendingInteraction = null;
            for (const event of bufferedEvents) {
              if (event.type === 'interaction') {
                const ie = event as Extract<WSMessage, { type: 'interaction' }>;
                restoredInteraction = {
                  interactionId: ie.interaction_id,
                  interactionType: ie.interaction_type,
                  toolName: ie.tool_name,
                  toolInput: ie.tool_input,
                };
              }
            }

            // Restore plan content from buffer (last plan_update event wins)
            let lastPlanContent: string | null = null;
            for (const event of bufferedEvents) {
              if (event.type === 'plan_update') {
                lastPlanContent = (event as any).content;
              }
            }
            if (lastPlanContent !== null) {
              const planTab = restored.panels.find(p => p.type === 'plan');
              if (planTab) {
                planTab.content = lastPlanContent;
              } else {
                // No plan tab from Task tool_use — main agent wrote the plan directly.
                // Create a transient tab so the user can see the plan content.
                const transientPlan: PanelTab = {
                  id: `plan-update-restored`,
                  type: 'plan',
                  label: 'Plan',
                  subagentType: 'Plan',
                  description: 'Plan',
                  content: lastPlanContent,
                  prompt: '',
                  streaming: false,
                  status: 'complete',
                  startedAt: Date.now(),
                  completedAt: Date.now(),
                  blocks: [],
                };
                restored.panels.push(transientPlan);
                restored.activePanelId = transientPlan.id;
              }
            }

            set({
              isStreaming: true,
              streamingBlocks: blocks,
              agentStatus: deriveStatus(blocks),
              panels: restored.panels,
              activePanelId: restored.activePanelId,
              panelVisible: restored.panels.length > 0,
              pendingInteraction: restoredInteraction,
            });
          } else {
            set({ isStreaming: true, streamingBlocks: blocks, agentStatus: deriveStatus(blocks) });
          }
        }
        break;
      }

      case 'session_switched': {
        // Server assigned a session (e.g., on WebSocket connect via auto-session)
        const switchedId = (msg as any).session_id;
        if (switchedId && !get().activeSession) {
          get().switchSession(switchedId);
        }
        break;
      }

      case 'session_forked': {
        // Reload sessions to include the new fork
        get().loadSessions();
        break;
      }

      case 'session_resumed': {
        // Reload sessions to reflect status change
        get().loadSessions();
        break;
      }

      case 'session_archived': {
        // Reload sessions to remove archived session
        get().loadSessions();
        if (state.activeSession === (msg as any).session_id) {
          // Switch to most recent remaining session
          const remaining = get().sessions.filter(s => s.id !== (msg as any).session_id);
          if (remaining.length > 0) {
            get().switchSession(remaining[0].id);
          }
        }
        break;
      }

      case 'session_running': {
        // Global broadcast: a session started or stopped running
        const runMsg = msg as Extract<WSMessage, { type: 'session_running' }>;
        set(s => {
          const updates: Record<string, unknown> = {
            sessions: s.sessions.map(sess =>
              sess.id === runMsg.session_id
                ? { ...sess, is_running: runMsg.is_running }
                : sess,
            ),
            // Also update search results if present
            searchResults: s.searchResults?.map(sess =>
              sess.id === runMsg.session_id
                ? { ...sess, is_running: runMsg.is_running }
                : sess,
            ) ?? null,
          };
          // Active session started running — enter streaming mode so the
          // response is visible and input is disabled.  No isStreaming guard:
          // sendMessage() sets identical values (idempotent) and removing the
          // guard ensures server-initiated runs (auto-resume, answer injection)
          // always transition the UI into streaming mode reliably.
          if (runMsg.session_id === s.activeSession && runMsg.is_running) {
            updates.isStreaming = true;
            updates.streamingBlocks = [];
            updates.agentStatus = { state: 'thinking' };
          }
          return updates;
        });
        break;
      }

      case 'interaction': {
        // Agent is paused waiting for user input (AskUserQuestion, ExitPlanMode, etc.)
        const imsg = msg as Extract<WSMessage, { type: 'interaction' }>;
        set({
          pendingInteraction: {
            interactionId: imsg.interaction_id,
            interactionType: imsg.interaction_type,
            toolName: imsg.tool_name,
            toolInput: imsg.tool_input,
          },
        });
        // Auto-open the plan panel when ExitPlanMode fires, so the user sees the plan
        if (imsg.interaction_type === 'plan_exit') {
          const planTab = [...get().panels].reverse().find(p => p.type === 'plan');
          if (planTab) {
            set({ activePanelId: planTab.id, panelVisible: true });
          }
        }
        break;
      }

      case 'plan_update': {
        // Backend detected a Write/Edit to a plan file — update panel content.
        // Prefer running plan tab, fall back to any existing plan tab (update in-place).
        const planTab = state.panels.find(p => p.type === 'plan' && p.status === 'running')
          || [...state.panels].reverse().find(p => p.type === 'plan');
        if (planTab) {
          get().updatePanelTab(planTab.id, { content: (msg as any).content });
        } else {
          // No plan tab at all — open a transient one (main agent wrote a plan file directly)
          get().openPanelTab({
            id: `plan-update-${Date.now()}`,
            type: 'plan',
            label: 'Plan',
            subagentType: 'Plan',
            description: 'Plan updated',
            content: (msg as any).content,
            prompt: '',
            streaming: false,
            status: 'complete',
            startedAt: Date.now(),
            completedAt: Date.now(),
            blocks: [],
          });
        }
        break;
      }

      case 'subagent_start': {
        // Server-side sub-agent lifecycle event — update or create panel tab
        const sa = msg as any;
        const existing = state.panels.find(p => p.id === sa.tool_use_id);
        if (existing) {
          get().updatePanelTab(sa.tool_use_id, {
            subagentType: sa.subagent_type,
            label: sa.subagent_type,
            description: sa.description,
            model: sa.model,
            type: sa.subagent_type === 'Plan' ? 'plan' : 'subagent',
          });
        } else {
          get().openPanelTab({
            id: sa.tool_use_id,
            type: sa.subagent_type === 'Plan' ? 'plan' : 'subagent',
            label: sa.subagent_type,
            subagentType: sa.subagent_type,
            description: sa.description,
            model: sa.model,
            content: null,
            prompt: '',
            streaming: true,
            status: 'running',
            startedAt: Date.now(),
            blocks: [],
          });
        }
        break;
      }

      case 'subagent_complete': {
        // Server-side sub-agent lifecycle event — mark complete
        const sc = msg as any;
        const tab = state.panels.find(p => p.id === sc.tool_use_id);
        if (tab) {
          get().updatePanelTab(sc.tool_use_id, {
            status: sc.is_error ? 'error' : 'complete',
            isError: sc.is_error || false,
            completedAt: Date.now(),
            streaming: false,
          });
          if (tab.type !== 'plan') {
            _scheduleAutoClose(sc.tool_use_id);
          }
        }
        get().pruneCompletedTabs();
        break;
      }

      case 'hoa_progress': {
        // houseofagents NDJSON progress — update the running hoa_execute tool block
        const hp = msg as Extract<WSMessage, { type: 'hoa_progress' }>;
        const blocks = [...state.streamingBlocks];
        for (let i = blocks.length - 1; i >= 0; i--) {
          const b = blocks[i];
          if (b.type === 'tool_call' && b.tool.includes('hoa_execute') && b.status === 'running') {
            // Immutable append — new array reference so React detects the change
            const prev = b.hoaEvents || [];
            blocks[i] = { ...b, hoaEvents: [...prev, hp.event] };
            set({ streamingBlocks: blocks });
            break;
          }
        }
        break;
      }

      case 'file_changed': {
        const fc = msg as Extract<WSMessage, { type: 'file_changed' }>;
        set(s => {
          const exists = s.modifiedFiles.some(f => f.path === fc.path);
          if (exists) {
            // Already tracked — just bump count (stats will refresh on panel open)
            return { modifiedFilesCount: s.modifiedFilesCount + 1 };
          }
          // Add placeholder entry — real stats fetched when panel opens
          const shortPath = fc.path.split('/').slice(-2).join('/');
          return {
            modifiedFiles: [...s.modifiedFiles, {
              path: fc.path,
              short_path: shortPath,
              status: fc.operation === 'write' ? 'created' : 'modified',
              stats: { additions: 0, deletions: 0 },
              created_at: new Date().toISOString(),
            }],
            modifiedFilesCount: s.modifiedFilesCount + 1,
          };
        });
        break;
      }

      case 'error': {
        set((s) => ({
          messages: [...s.messages, { role: 'assistant' as const, blocks: [{ type: 'text', content: `Error: ${msg.error}` }] }],
          streamingBlocks: [],
          isStreaming: false,
          agentStatus: { state: 'idle' },
        }));
        break;
      }

      case 'notification': {
        import('./notificationStore').then(({ useNotificationStore }) =>
          useNotificationStore.getState().handleWSNotification(msg)
        );
        break;
      }

      case 'notification_answered': {
        import('./notificationStore').then(({ useNotificationStore }) =>
          useNotificationStore.getState().handleWSNotificationAnswered(msg)
        );
        break;
      }

      case 'answer_injected': {
        // Show the injected answer as a user message in the chat
        const ai = msg as Extract<WSMessage, { type: 'answer_injected' }>;
        if (ai.session_id === state.activeSession) {
          set(s => ({
            messages: [...s.messages, {
              role: 'user' as const,
              blocks: [{ type: 'text' as const, content: ai.content }],
            }],
          }));
        }
        break;
      }

      case 'auto_resume_start': {
        // Background tasks completed — the backend is about to auto-resume.
        // Enter streaming mode immediately so the thinking cursor is visible
        // and chat input is disabled (prevents user from typing mid-resume).
        const arMsg = msg as Extract<WSMessage, { type: 'auto_resume_start' }>;
        if (arMsg.session_id === state.activeSession) {
          set({
            isStreaming: true,
            streamingBlocks: [],
            agentStatus: { state: 'thinking' },
          });
        }
        break;
      }

      case 'background_tasks_update': {
        const bt = msg as Extract<WSMessage, { type: 'background_tasks_update' }>;
        if (bt.session_id === state.activeSession) {
          set(s => {
            // Merge: keep startedAt from existing entries, add new ones
            const existing = new Map(s.backgroundTasks.map(t => [t.task_id, t]));
            const updated = bt.tasks.map(t => ({
              ...t,
              startedAt: existing.get(t.task_id)?.startedAt || Date.now(),
            }));
            return { backgroundTasks: updated };
          });
        }
        break;
      }
    }
  },
}));

/** Extract the latest TodoWrite todos from loaded message history. Skip if all done. */
function extractTodosFromMessages(messages: ChatMessage[]): TodoItem[] {
  // Walk backwards to find the most recent TodoWrite tool call
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== 'assistant') continue;
    for (let j = msg.blocks.length - 1; j >= 0; j--) {
      const block = msg.blocks[j];
      if (block.type === 'tool_call' && block.tool === 'TodoWrite' && Array.isArray(block.input?.todos)) {
        const todos = block.input.todos as TodoItem[];
        // Don't restore a fully-completed list — nothing useful to show
        if (todos.every(t => t.status === 'completed')) return [];
        return todos;
      }
    }
  }
  return [];
}

/**
 * Apply a single stream event to a blocks array (pure function for replay).
 * Skips events with parent_tool_use_id — those belong to panels, not main chat.
 */
function applyStreamEvent(blocks: MessageBlock[], event: WSMessage): MessageBlock[] {
  // Sub-agent child events go to panels, not main chat
  const parentId = (event as any).parent_tool_use_id;
  if (parentId) return blocks;

  const result = [...blocks];
  switch (event.type) {
    case 'thinking': {
      const last = result[result.length - 1];
      if (last?.type === 'thinking') {
        result[result.length - 1] = { ...last, content: last.content + event.content };
      } else {
        result.push({ type: 'thinking', content: event.content });
      }
      break;
    }
    case 'token': {
      const last = result[result.length - 1];
      if (last?.type === 'text') {
        result[result.length - 1] = { ...last, content: last.content + event.content };
      } else {
        result.push({ type: 'text', content: event.content });
      }
      break;
    }
    case 'tool_use': {
      result.push({
        type: 'tool_call',
        toolUseId: event.tool_use_id || '',
        tool: event.tool,
        input: event.input,
        status: 'running',
      });
      break;
    }
    case 'tool_result': {
      for (let i = 0; i < result.length; i++) {
        const b = result[i];
        if (b.type === 'tool_call' && b.toolUseId === event.tool_use_id) {
          result[i] = { ...b, result: event.result, isError: event.is_error, status: 'complete' as const };
          break;
        }
      }
      break;
    }
    case 'hoa_progress': {
      for (let i = result.length - 1; i >= 0; i--) {
        const b = result[i];
        if (b.type === 'tool_call' && b.tool.includes('hoa_execute')) {
          const prev = b.hoaEvents || [];
          result[i] = { ...b, hoaEvents: [...prev, (event as any).event] };
          break;
        }
      }
      break;
    }
  }
  return result;
}

/** Rebuild panel tabs from buffered WS events (for reconnect replay). */
function rebuildPanelTabsFromBuffer(
  events: WSMessage[],
  blocks: MessageBlock[],
): { panels: PanelTab[]; activePanelId: string | null } {
  const panels: PanelTab[] = [];
  const panelMap = new Map<string, PanelTab>();

  // First pass: create panel tabs for Task tool_use events
  for (const event of events) {
    if (event.type === 'tool_use' && event.tool === 'Task') {
      const subagentType = String(event.input?.subagent_type || event.input?.model || 'agent');
      const toolUseId = event.tool_use_id || '';
      const block = blocks.find(
        b => b.type === 'tool_call' && b.toolUseId === toolUseId,
      );
      const isComplete = block?.type === 'tool_call' && block.status === 'complete';
      const tab: PanelTab = {
        id: toolUseId,
        type: subagentType === 'Plan' ? 'plan' : 'subagent',
        label: subagentType,
        subagentType,
        description: String(event.input?.description || ''),
        model: event.input?.model ? String(event.input.model) : undefined,
        content: isComplete && block?.type === 'tool_call'
          ? extractResultText(block.result || '')
          : null,
        prompt: String(event.input?.prompt || ''),
        streaming: !isComplete,
        status: isComplete
          ? (block?.type === 'tool_call' && block.isError ? 'error' : 'complete')
          : 'running',
        startedAt: Date.now(),
        completedAt: isComplete ? Date.now() : undefined,
        isError: block?.type === 'tool_call' ? block.isError : false,
        blocks: [],
      };
      panels.push(tab);
      panelMap.set(toolUseId, tab);
    }
  }

  // Second pass: collect child events into their parent panel's blocks
  for (const event of events) {
    const parentId = (event as any).parent_tool_use_id as string | undefined;
    if (!parentId) continue;
    const panel = panelMap.get(parentId);
    if (!panel) continue;

    if (event.type === 'thinking') {
      const last = panel.blocks[panel.blocks.length - 1];
      if (last?.type === 'thinking') {
        last.content += event.content;
      } else {
        panel.blocks.push({ type: 'thinking', content: event.content });
      }
    } else if (event.type === 'token') {
      const last = panel.blocks[panel.blocks.length - 1];
      if (last?.type === 'text') {
        last.content += event.content;
      } else {
        panel.blocks.push({ type: 'text', content: event.content });
      }
    } else if (event.type === 'tool_use') {
      panel.blocks.push({
        type: 'tool_call',
        toolUseId: event.tool_use_id || '',
        tool: event.tool,
        input: event.input,
        status: 'running',
      });
    } else if (event.type === 'tool_result') {
      for (const b of panel.blocks) {
        if (b.type === 'tool_call' && b.toolUseId === event.tool_use_id) {
          b.result = event.result;
          b.isError = event.is_error;
          b.status = 'complete';
          break;
        }
      }
    }
  }

  // Focus last running tab, or last tab overall
  const lastRunning = [...panels].reverse().find(p => p.status === 'running');
  return {
    panels,
    activePanelId: lastRunning?.id || panels[panels.length - 1]?.id || null,
  };
}

/** Derive agent status from current blocks state. */
function deriveStatus(blocks: MessageBlock[]): AgentStatus {
  if (blocks.length === 0) return { state: 'thinking' };
  const last = blocks[blocks.length - 1];
  if (last.type === 'thinking') return { state: 'thinking' };
  if (last.type === 'text') return { state: 'writing' };
  if (last.type === 'tool_call' && last.status === 'running') return { state: 'tool', toolName: last.tool };
  return { state: 'thinking' };
}
