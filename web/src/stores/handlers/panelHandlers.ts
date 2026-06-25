import type { WSMessage } from '../../api/websocket';
import { scheduleAutoClose } from '../helpers/blockHelpers';
import type { Get, Set } from './types';

// ------------------------------------------------------------------ //
//  Panel handlers: plan_update, subagent_start/complete, hoa_progress //
// ------------------------------------------------------------------ //

export function handlePlanUpdate(
  msg: Extract<WSMessage, { type: 'plan_update' }>,
  get: Get,
  _set: Set,
): void {
  const state = get();
  // Backend detected a Write/Edit to a plan file — update panel content.
  // Prefer running plan tab, fall back to any existing plan tab (update in-place).
  const planTab = state.panels.find(p => p.type === 'plan' && p.status === 'running')
    || [...state.panels].reverse().find(p => p.type === 'plan');
  if (planTab) {
    get().updatePanelTab(planTab.id, { content: msg.content });
  } else {
    // No plan tab at all — open a transient one (main agent wrote a plan file directly)
    get().openPanelTab({
      id: `plan-update-${Date.now()}`,
      type: 'plan',
      label: 'Plan',
      subagentType: 'Plan',
      description: 'Plan updated',
      content: msg.content,
      prompt: '',
      streaming: false,
      status: 'complete',
      startedAt: Date.now(),
      completedAt: Date.now(),
      blocks: [],
    });
  }
}

export function handleSubagentStart(
  msg: Extract<WSMessage, { type: 'subagent_start' }>,
  get: Get,
  _set: Set,
): void {
  const state = get();
  // Server-side sub-agent lifecycle event — update or create panel tab
  const existing = state.panels.find(p => p.id === msg.tool_use_id);
  if (existing) {
    get().updatePanelTab(msg.tool_use_id, {
      subagentType: msg.subagent_type,
      label: msg.subagent_type,
      description: msg.description,
      model: msg.model,
      type: msg.subagent_type === 'Plan' ? 'plan' : 'subagent',
    });
  } else {
    get().openPanelTab({
      id: msg.tool_use_id,
      type: msg.subagent_type === 'Plan' ? 'plan' : 'subagent',
      label: msg.subagent_type,
      subagentType: msg.subagent_type,
      description: msg.description,
      model: msg.model,
      content: null,
      prompt: '',
      streaming: true,
      status: 'running',
      startedAt: Date.now(),
      blocks: [],
    });
  }
}

export function handleSubagentComplete(
  msg: Extract<WSMessage, { type: 'subagent_complete' }>,
  get: Get,
  _set: Set,
): void {
  const state = get();
  // Server-side sub-agent lifecycle event — mark complete
  const tab = state.panels.find(p => p.id === msg.tool_use_id);
  if (tab) {
    // A background sub-agent's "complete" fires immediately when the Agent tool
    // returns its task id — but the sub-agent keeps streaming afterward. Keep
    // the panel running; it settles when the background task actually ends
    // (handleBackgroundTasksUpdate). Completing here would send its later
    // tools/thoughts into the main chat instead of this panel.
    if (tab.background) return;
    get().updatePanelTab(msg.tool_use_id, {
      status: msg.is_error ? 'error' : 'complete',
      isError: msg.is_error || false,
      completedAt: Date.now(),
      streaming: false,
    });
    if (tab.type !== 'plan') {
      scheduleAutoClose(msg.tool_use_id, get);
    }
  }
  get().pruneCompletedTabs();
}

export function handleWorkflowProgress(
  msg: Extract<WSMessage, { type: 'workflow_progress' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  const wf = msg.workflow;
  const terminal = wf.status === 'completed' || wf.status === 'failed' || wf.status === 'stopped';

  // 1) Update (or open) the dedicated workflow panel tab.
  const tab = state.panels.find(p => p.id === msg.tool_use_id);
  if (tab) {
    get().updatePanelTab(msg.tool_use_id, {
      workflow: wf,
      label: wf.name || tab.label,
      content: terminal && wf.summary ? wf.summary : tab.content,
      streaming: !terminal,
      status: wf.status === 'failed' ? 'error' : terminal ? 'complete' : 'running',
      isError: wf.status === 'failed',
      ...(terminal ? { completedAt: Date.now() } : {}),
    });
  } else {
    // No panel (pruned/closed, or progress arrived first) — recreate it.
    get().openPanelTab({
      id: msg.tool_use_id,
      type: 'workflow',
      label: wf.name || 'Workflow',
      subagentType: 'Workflow',
      description: '',
      content: terminal && wf.summary ? wf.summary : null,
      prompt: '',
      streaming: !terminal,
      status: wf.status === 'failed' ? 'error' : terminal ? 'complete' : 'running',
      isError: wf.status === 'failed',
      startedAt: Date.now(),
      ...(terminal ? { completedAt: Date.now() } : {}),
      blocks: [],
      workflow: wf,
    });
  }

  // 2) Fold the snapshot onto the Workflow tool_call block so the inline chat
  //    card reflects progress. The workflow can settle after the launching
  //    turn finalized, so check the live stream first, then persisted messages.
  const inStreaming = state.streamingBlocks.some(
    b => b.type === 'tool_call' && b.toolUseId === msg.tool_use_id,
  );
  if (inStreaming) {
    set({
      streamingBlocks: state.streamingBlocks.map(b =>
        b.type === 'tool_call' && b.toolUseId === msg.tool_use_id
          ? { ...b, workflow: wf }
          : b,
      ),
    });
  } else {
    let touched = false;
    const messages = state.messages.map(m => {
      if (m.role !== 'assistant') return m;
      if (!m.blocks.some(b => b.type === 'tool_call' && b.toolUseId === msg.tool_use_id)) return m;
      touched = true;
      return {
        ...m,
        blocks: m.blocks.map(b =>
          b.type === 'tool_call' && b.toolUseId === msg.tool_use_id
            ? { ...b, workflow: wf }
            : b,
        ),
      };
    });
    if (touched) set({ messages });
  }
}

export function handleHoaProgress(
  msg: Extract<WSMessage, { type: 'hoa_progress' }>,
  get: Get,
  set: Set,
): void {
  const state = get();
  // houseofagents NDJSON progress — update the running hoa_execute tool block
  const blocks = [...state.streamingBlocks];
  for (let i = blocks.length - 1; i >= 0; i--) {
    const b = blocks[i];
    if (b.type === 'tool_call' && b.tool.includes('hoa_execute') && b.status === 'running') {
      // Immutable append — new array reference so React detects the change
      const prev = b.hoaEvents || [];
      blocks[i] = { ...b, hoaEvents: [...prev, msg.event] };
      set({ streamingBlocks: blocks });
      break;
    }
  }
}
