import type { WSMessage } from '../../api/websocket';
import type { PanelTab } from '../../types/chat';
import { applyStreamEvent, rebuildPanelTabsFromBuffer, deriveStatus, extractTodosFromBuffer } from '../helpers/bufferReplay';
import type { Get, Set } from './types';

// ------------------------------------------------------------------ //
//  Session management handlers                                        //
// ------------------------------------------------------------------ //

export function handleSessionUpdated(
  msg: Extract<WSMessage, { type: 'session_updated' }>,
  _get: Get,
  set: Set,
): void {
  set((s) => ({
    sessions: s.sessions.map(sess =>
      sess.id === msg.session_id ? { ...sess, title: msg.title } : sess
    ),
  }));
}

export function handleSessionStatus(
  msg: Extract<WSMessage, { type: 'session_status' }>,
  get: Get,
  set: Set,
): void {
  if (!msg.is_running) return;

  // Rebuild streaming state from buffered events
  let blocks = get().streamingBlocks.length > 0 ? [...get().streamingBlocks] : [];
  if (blocks.length === 0) {
    // Only rebuild from scratch if we're not already streaming
    blocks = [];
  }
  const bufferedEvents = msg.buffered_events;
  if (bufferedEvents) {
    // Reset blocks and rebuild from buffer
    blocks = [];
    for (const event of bufferedEvents) {
      blocks = applyStreamEvent(blocks, event);
    }
    // Rebuild panel tabs from buffered events
    const restored = rebuildPanelTabsFromBuffer(bufferedEvents, blocks);

    // Restore pending interaction from buffer (last interaction event wins)
    let restoredInteraction: ReturnType<Get>['pendingInteraction'] = null;
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
        lastPlanContent = (event as Extract<WSMessage, { type: 'plan_update' }>).content;
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

    // Restore todos panel from the freshest TodoWrite in the buffer. Without
    // this, a client that reconnects mid-turn (page refresh, WS drop, tab
    // backgrounded) sees a stale snapshot from persisted history because the
    // buffered TodoWrite tool_use events fed only streamingBlocks.
    const restoredTodos = extractTodosFromBuffer(bufferedEvents);

    const update: Record<string, unknown> = {
      isStreaming: true,
      streamingBlocks: blocks,
      agentStatus: deriveStatus(blocks),
      panels: restored.panels,
      activePanelId: restored.activePanelId,
      panelVisible: restored.panels.length > 0,
      pendingInteraction: restoredInteraction,
    };
    if (restoredTodos !== null) {
      update.currentTodos = restoredTodos;
    }
    set(update);
  } else {
    set({ isStreaming: true, streamingBlocks: blocks, agentStatus: deriveStatus(blocks) });
  }
}

export function handleSessionSwitched(
  msg: Extract<WSMessage, { type: 'session_switched' }>,
  get: Get,
  _set: Set,
): void {
  // Server assigned a session (e.g., on WebSocket connect via auto-session)
  if (msg.session_id && !get().activeSession) {
    get().switchSession(msg.session_id);
  }
}

export function handleSessionForked(
  _msg: Extract<WSMessage, { type: 'session_forked' }>,
  get: Get,
  _set: Set,
): void {
  // Reload sessions to include the new fork
  get().loadSessions();
}

export function handleSessionResumed(
  _msg: Extract<WSMessage, { type: 'session_resumed' }>,
  get: Get,
  _set: Set,
): void {
  // Reload sessions to reflect status change
  get().loadSessions();
}

export function handleSessionArchived(
  msg: Extract<WSMessage, { type: 'session_archived' }>,
  get: Get,
  _set: Set,
): void {
  // Reload sessions to remove archived session
  get().loadSessions();
  if (get().activeSession === msg.session_id) {
    // Switch to most recent remaining session
    const remaining = get().sessions.filter(s => s.id !== msg.session_id);
    if (remaining.length > 0) {
      get().switchSession(remaining[0].id);
    }
  }
}

export function handleSessionRunning(
  msg: Extract<WSMessage, { type: 'session_running' }>,
  _get: Get,
  set: Set,
): void {
  // Global broadcast: a session started or stopped running
  set(s => {
    const updates: Record<string, unknown> = {
      sessions: s.sessions.map(sess =>
        sess.id === msg.session_id
          ? { ...sess, is_running: msg.is_running }
          : sess,
      ),
      // Also update search results if present
      searchResults: s.searchResults?.map(sess =>
        sess.id === msg.session_id
          ? { ...sess, is_running: msg.is_running }
          : sess,
      ) ?? null,
    };
    // Active session started running from a background trigger (e.g.,
    // background task completion, answer injection) — enter streaming mode
    // so the response is visible and input is disabled.
    // Guard: sendMessage() already sets isStreaming before the WS message,
    // so this only fires for server-initiated runs.
    if (msg.session_id === s.activeSession && msg.is_running && !s.isStreaming) {
      updates.isStreaming = true;
      updates.streamingBlocks = [];
      updates.agentStatus = { state: 'thinking' };
    }
    // Defensive: server says the run ended but the frontend is still in
    // streaming mode.  This happens when done/stopped/error never made
    // it to the client (lost WS message during reconnect, post-stream
    // exception on the server before broadcast_done fired, etc.).
    // Without this branch the chat detail stays on "thinking..." while
    // the sidebar entry has already dropped out of the "Running" group,
    // which looks like the chat is stuck between steps.  The server's
    // backstop in engine.run() ships a synthetic done in most of those
    // cases; this is belt-and-suspenders for when even that signal is
    // missed.
    if (msg.session_id === s.activeSession && !msg.is_running && s.isStreaming) {
      const finalBlocks = s.streamingBlocks.map(b =>
        b.type === 'tool_call' && b.status === 'running'
          ? { ...b, status: 'complete' as const }
          : b,
      );
      if (finalBlocks.length > 0) {
        updates.messages = [
          ...s.messages,
          { role: 'assistant' as const, blocks: finalBlocks },
        ];
      }
      updates.streamingBlocks = [];
      updates.isStreaming = false;
      updates.agentStatus = { state: 'idle' };
    }
    return updates;
  });
}

export function handleAnswerInjected(
  msg: Extract<WSMessage, { type: 'answer_injected' }>,
  get: Get,
  set: Set,
): void {
  // Show the injected answer as a user message in the chat
  if (msg.session_id === get().activeSession) {
    set(s => ({
      messages: [...s.messages, {
        role: 'user' as const,
        blocks: [{ type: 'text' as const, content: msg.content }],
      }],
    }));
  }
}
