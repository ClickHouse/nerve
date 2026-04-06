import type { WSMessage } from '../../api/websocket';
import type { PanelTab } from '../../types/chat';
import { applyStreamEvent, rebuildPanelTabsFromBuffer, deriveStatus } from '../helpers/bufferReplay';
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
