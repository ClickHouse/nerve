// Subagent and background-task records of one session, as the gateway serves
// them (REST `GET /api/sessions/{id}/agents`, `session_status.agents`, and the
// `agent_*` / `queue_update` WebSocket events). The shapes mirror the server's
// `AgentRegistry.to_view()` key for key; every field is always present.

import type { WorkflowSnapshot } from './chat';

export type AgentKind = 'agent' | 'workflow' | 'shell' | 'monitor' | 'teammate' | 'remote_agent' | 'mcp_task' | 'other';
export type AgentStatus = 'starting' | 'running' | 'stopping' | 'completed' | 'failed' | 'stopped' | 'interrupted' | 'lost';
export type AgentEndReason =
  | 'completed' | 'error' | 'spawn_failed'
  | 'user' | 'stop_all' | 'turn_stop' | 'orchestrator' | 'unknown_stop'
  | 'process_ended' | 'gateway_restart' | 'force_stop' | 'model_switch' | 'archived'
  | 'vanished';
export type AgentAttention = 'needs_input' | 'failed' | 'interrupted' | 'lost' | 'stop_unconfirmed';
export type ClientAttention = AgentAttention | 'quiet';
export const LIVE_STATUSES: ReadonlySet<AgentStatus> = new Set<AgentStatus>(['starting', 'running', 'stopping']);

export interface AgentControls { stop: boolean; transcript: boolean; output: boolean; message: boolean }
export interface AgentModelChange { from: string; to: string; at: string | null }

export interface AgentView {
  id: string; session_id: string; backend: 'claude' | 'codex'; kind: AgentKind;
  task_id: string | null; task_type: string | null; spawn_tool_use_id: string | null;
  parent_agent_id: string | null; depth: number;
  name: string | null; label: string; description: string; agent_type: string | null; command: string | null;
  background: boolean; ambient: boolean;
  status: AgentStatus; end_reason: AgentEndReason | null; error: string | null; attention: AgentAttention | null;
  activity: string | null; summary: string | null; result_preview: string | null;
  requested_model: string | null; model: string | null; model_changed: AgentModelChange | null;
  tokens: number; tool_uses: number; cost_usd: number; runs: number;
  created_at: string; started_at: string | null; run_started_at: string | null;
  last_activity_at: string | null; ended_at: string | null;
  stop_requested_at: string | null; stopped_by_actor_id: string | null;
  resumable: boolean; controls: AgentControls; workflow: WorkflowSnapshot | null; rev: number;
}

export interface AgentSummary {
  total: number; live: number; starting: number; running: number; stopping: number;
  completed: number; failed: number; stopped: number; interrupted: number; lost: number;
  attention: number; shells_live: number; tokens: number; cost_usd: number;
  oldest_live_started_at: string | null;
}
export const EMPTY_AGENT_SUMMARY: AgentSummary = {
  total: 0, live: 0, starting: 0, running: 0, stopping: 0, completed: 0, failed: 0, stopped: 0,
  interrupted: 0, lost: 0, attention: 0, shells_live: 0, tokens: 0, cost_usd: 0, oldest_live_started_at: null,
};

export interface AgentCapabilities {
  stop_task: boolean; transcript: boolean; output: boolean;
  interrupt_spares_background: boolean; quiet_after_minutes: number;
}
export const DEFAULT_AGENT_CAPABILITIES: AgentCapabilities = {
  stop_task: false, transcript: false, output: false, interrupt_spares_background: false, quiet_after_minutes: 10,
};

export interface QueueEntryView {
  queue_id: string; client_msg_id: string | null; preview: string; source: string;
  internal: boolean; actor_id: string | null; created_at: string;
}

export interface AgentSnapshot {
  session_id: string; agents: AgentView[]; summary: AgentSummary;
  capabilities: AgentCapabilities; queue: QueueEntryView[]; generated_at: string;
}

export type LifecycleEvent = 'started' | 'resumed' | 'stop_requested' | 'completed' | 'failed' | 'stopped' | 'interrupted' | 'lost';
export type AgentEventItem = { seq: number; at: string } & (
  | { kind: 'text'; text: string }
  | { kind: 'thinking'; text: string }
  | { kind: 'tool_call'; tool_use_id: string; tool: string; input: Record<string, unknown>; input_truncated?: boolean; child_agent_id?: string | null }
  | { kind: 'tool_result'; tool_use_id: string; result: string; is_error: boolean; truncated?: boolean; full_size?: number }
  | { kind: 'lifecycle'; event: LifecycleEvent; run?: number; reason?: string | null; actor_id?: string | null }
);

export interface AgentTranscriptPage {
  agent_id: string; items: AgentEventItem[]; first_seq: number | null; last_seq: number | null; has_more_before: boolean;
}
export interface AgentDetail { agent: AgentView; prompt: string | null; result_text: string | null }
export interface AgentOutput { agent_id: string; text: string; size: number; truncated: boolean; available: boolean }
export interface AgentBrief { id: string; label: string; kind: AgentKind; status: AgentStatus; started_at: string | null; tokens: number }

export interface StopTurnResult { scope: 'turn'; turn: 'stopped' | 'not_running' | 'not_stopped' | 'needs_confirmation' | 'killed'; would_end: AgentBrief[]; shell_count: number }
export interface StopAllResult { scope: 'all'; turn: 'stopped' | 'not_running' | 'killed'; stopping: string[]; unsupported: string[] }
export interface ForceStopResult { scope: 'force'; turn: 'killed' | 'not_running'; ended: string[] }
export interface StopAgentResponse { agent: AgentView; stopping: string[] }

export interface AutoTurnTrigger { agent_id: string; label: string; kind: AgentKind; status: AgentStatus }
export interface InteractionAgentRef { agent_id: string | null; task_id: string; label: string }
export interface QueueRemoval { queue_id: string | null; client_msg_id: string | null; reason: 'started' | 'cancelled' | 'duplicate' }

export type AgentWSMessage =
  | { type: 'agent_update'; session_id: string; agents: AgentView[]; summary: AgentSummary }
  | { type: 'agent_event'; session_id: string; agent_id: string; item: AgentEventItem }
  | { type: 'queue_update'; session_id: string; queue: QueueEntryView[]; removed?: QueueRemoval[] }
  | { type: 'session_agents'; session_id: string; background_count: number; has_background_tasks: boolean };
