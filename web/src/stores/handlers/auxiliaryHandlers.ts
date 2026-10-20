import type { WSMessage } from '../../api/websocket';
import type { Get, Set } from './types';

// ------------------------------------------------------------------ //
//  Auxiliary handlers: interaction, file_changed, notifications, etc.  //
// ------------------------------------------------------------------ //

export function handleInteraction(
  msg: Extract<WSMessage, { type: 'interaction' }>,
  get: Get,
  set: Set,
): void {
  // Agent is paused waiting for user input (AskUserQuestion, ExitPlanMode, etc.).
  // `agent` names the subagent that asked (null: the orchestrator itself).
  set({
    pendingInteraction: {
      interactionId: msg.interaction_id,
      interactionType: msg.interaction_type,
      toolName: msg.tool_name,
      toolInput: msg.tool_input,
      agent: msg.agent ?? null,
    },
  });
  // Auto-open the plan panel when ExitPlanMode fires, so the user sees the plan
  if (msg.interaction_type === 'plan_exit') {
    const planTab = [...get().panels].reverse().find(p => p.type === 'plan');
    if (planTab) {
      set({ activePanelId: planTab.id, panelVisible: true });
    }
  }
}

export function handleInteractionResolved(
  msg: Extract<WSMessage, { type: 'interaction_resolved' }>,
  get: Get,
  set: Set,
): void {
  // The interaction was answered/denied (possibly by a parallel client) — drop
  // the pending prompt so this client's poll/plan UI stops soliciting an answer.
  if (get().pendingInteraction?.interactionId === msg.interaction_id) {
    set({ pendingInteraction: null });
  }
}

export function handleFileChanged(
  msg: Extract<WSMessage, { type: 'file_changed' }>,
  _get: Get,
  set: Set,
): void {
  set(s => {
    const exists = s.modifiedFiles.some(f => f.path === msg.path);
    if (exists) {
      // Already tracked — just bump count (stats will refresh on panel open)
      return { modifiedFilesCount: s.modifiedFilesCount + 1 };
    }
    // Add placeholder entry — real stats fetched when panel opens
    const shortPath = msg.path.split('/').slice(-2).join('/');
    return {
      modifiedFiles: [...s.modifiedFiles, {
        path: msg.path,
        short_path: shortPath,
        status: msg.operation === 'write' ? 'created' : 'modified',
        stats: { additions: 0, deletions: 0 },
        created_at: new Date().toISOString(),
      }],
      modifiedFilesCount: s.modifiedFilesCount + 1,
    };
  });
}

export function handleNotification(
  msg: Extract<WSMessage, { type: 'notification' }>,
  _get: Get,
  _set: Set,
): void {
  import('../notificationStore').then(({ useNotificationStore }) =>
    useNotificationStore.getState().handleWSNotification(msg)
  );
}

export function handleNotificationAnswered(
  msg: Extract<WSMessage, { type: 'notification_answered' }>,
  _get: Get,
  _set: Set,
): void {
  import('../notificationStore').then(({ useNotificationStore }) =>
    useNotificationStore.getState().handleWSNotificationAnswered(msg)
  );
}

export function handleNotificationExpired(
  msg: Extract<WSMessage, { type: 'notification_expired' }>,
  _get: Get,
  _set: Set,
): void {
  import('../notificationStore').then(({ useNotificationStore }) =>
    useNotificationStore.getState().handleWSNotificationExpired(msg)
  );
}
