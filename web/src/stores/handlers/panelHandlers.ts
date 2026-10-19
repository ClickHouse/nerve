import type { WSMessage } from '../../api/websocket';
import type { Get, Set } from './types';

// ------------------------------------------------------------------ //
//  Panel handlers: plan_update, workflow_progress                     //
// ------------------------------------------------------------------ //
// Subagents open no panel of their own any more: agent records drive the
// Agents panel and the per-agent transcript tabs (agentStore).

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
