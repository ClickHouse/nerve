import type { PanelTab, MessageBlock } from '../../types/chat';

/** Max completed tabs to keep before pruning oldest. */
export const MAX_COMPLETED_TABS = 5;

/** Auto-close delay for completed non-plan tabs (ms). */
export const AUTOCLOSE_DELAY = 5000;

/** Track pending auto-close timers so we can cancel on manual close. */
const _autoCloseTimers = new Map<string, ReturnType<typeof setTimeout>>();

/**
 * Schedule auto-close for a completed non-plan tab.
 * Requires a `getState` thunk to lazily access the store (avoids circular imports).
 */
export function scheduleAutoClose(
  tabId: string,
  getState: () => { panels: PanelTab[]; closePanelTab: (id: string) => void },
) {
  // Cancel any existing timer for this tab
  const existing = _autoCloseTimers.get(tabId);
  if (existing) clearTimeout(existing);

  const timer = setTimeout(() => {
    _autoCloseTimers.delete(tabId);
    const state = getState();
    const tab = state.panels.find(p => p.id === tabId);
    // Only auto-close if still completed and not plan/files tab
    if (tab && tab.status !== 'running' && tab.type !== 'plan' && tab.type !== 'files') {
      state.closePanelTab(tabId);
    }
  }, AUTOCLOSE_DELAY);
  _autoCloseTimers.set(tabId, timer);
}

/** Cancel a pending auto-close (e.g., user manually closed the tab). */
export function cancelAutoClose(tabId: string) {
  const timer = _autoCloseTimers.get(tabId);
  if (timer) {
    clearTimeout(timer);
    _autoCloseTimers.delete(tabId);
  }
}

/** Clear all auto-close timers (e.g., on session switch). */
export function clearAllAutoCloseTimers() {
  for (const [, timer] of _autoCloseTimers) {
    clearTimeout(timer);
  }
  _autoCloseTimers.clear();
}

/**
 * Append a MessageBlock to a specific panel tab's blocks array.
 * Merges consecutive thinking/text blocks for efficiency.
 */
export function appendBlockToPanel(panels: PanelTab[], panelId: string, block: MessageBlock): PanelTab[] {
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
export function updateToolResultInPanel(
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
