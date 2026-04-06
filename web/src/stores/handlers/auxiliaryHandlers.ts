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
  // Agent is paused waiting for user input (AskUserQuestion, ExitPlanMode, etc.)
  set({
    pendingInteraction: {
      interactionId: msg.interaction_id,
      interactionType: msg.interaction_type,
      toolName: msg.tool_name,
      toolInput: msg.tool_input,
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

export function handleBackgroundTasksUpdate(
  msg: Extract<WSMessage, { type: 'background_tasks_update' }>,
  get: Get,
  set: Set,
): void {
  if (msg.session_id === get().activeSession) {
    set(s => {
      // Merge: keep startedAt from existing entries, add new ones
      const existing = new Map(s.backgroundTasks.map(t => [t.task_id, t]));
      const updated = msg.tasks.map(t => ({
        ...t,
        startedAt: existing.get(t.task_id)?.startedAt || Date.now(),
      }));
      return { backgroundTasks: updated };
    });
  }
}
