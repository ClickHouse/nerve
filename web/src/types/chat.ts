export interface ThinkingBlockData {
  type: 'thinking';
  content: string;
}

export interface TextBlockData {
  type: 'text';
  content: string;
}

export interface ToolCallBlockData {
  type: 'tool_call';
  toolUseId: string;
  tool: string;
  input: Record<string, unknown>;
  result?: string;
  isError?: boolean;
  status: 'running' | 'complete';
  /** houseofagents NDJSON progress events (populated during hoa_execute runs) */
  hoaEvents?: Record<string, unknown>[];
  /** Dynamic-workflow progress snapshot (populated during Workflow runs) */
  workflow?: WorkflowSnapshot;
}

// --- Dynamic workflow (Claude Code Workflow tool) progress ---

export interface WorkflowPhase {
  index?: number;
  title?: string;
}

export interface WorkflowAgent {
  label?: string;
  phaseIndex?: number;
  phaseTitle?: string;
  /** queued | running | done | failed (CLI vocabulary) */
  state?: string;
  model?: string;
  backend?: string;
  cwd?: string;
  tokens?: number;
  toolCalls?: number;
  lastToolName?: string;
  lastToolSummary?: string;
  durationMs?: number;
}

export interface WorkflowSnapshot {
  name: string;
  /** running | completed | failed | stopped */
  status: string;
  summary?: string;
  phases: WorkflowPhase[];
  agents: WorkflowAgent[];
  totalTokens?: number;
  totalToolCalls?: number;
  agentCount?: number;
}

export interface ImageBlockData {
  type: 'image';
  url: string;
  filename: string;
  media_type: string;
}

export interface FileBlockData {
  type: 'file';
  url: string;
  filename: string;
  size?: number;
}

/** Leading marker on a turn that fired from a self-scheduled ScheduleWakeup. */
export interface WakeupBlockData {
  type: 'wakeup';
}

/** Leading marker on an autonomous turn the CLI ran on its own (e.g. after
 *  a background task settled) — no user message preceded it. */
export interface AutoTurnBlockData {
  type: 'auto';
}

/** Marker emitted when the API switched the model serving this session —
 *  e.g. a silent capacity downgrade away from the configured model
 *  (downgrade: true), or the later recovery back to it. */
export interface ModelChangeBlockData {
  type: 'model_change';
  from?: string;
  to: string;
  downgrade?: boolean;
}

export type MessageBlock = ThinkingBlockData | TextBlockData | ToolCallBlockData | ImageBlockData | FileBlockData | WakeupBlockData | AutoTurnBlockData | ModelChangeBlockData;

export interface ChatMessage {
  id?: number;
  role: 'user' | 'assistant';
  blocks: MessageBlock[];
  channel?: string;
  created_at?: string;
}

export interface Session {
  id: string;
  title: string;
  source: string;
  updated_at: string;
  // V3 lifecycle fields
  status?: string;
  sdk_session_id?: string;
  parent_session_id?: string;
  connected_at?: string;
  message_count?: number;
  total_cost_usd?: number;
  model?: string;
  backend?: string;
  cwd?: string;
  // Real-time running status (set by backend + WS updates)
  is_running?: boolean;
  // Paused mid-turn waiting for user input (AskUserQuestion / plan mode).
  // Drives the sidebar "waiting" indicator. Set by backend + WS updates.
  awaiting_input?: boolean;
  starred?: boolean;
}

export type AgentStatus =
  | { state: 'idle' }
  | { state: 'thinking' }
  | { state: 'tool'; toolName: string }
  | { state: 'writing' };

export interface PanelTab {
  id: string;              // toolUseId
  type: 'plan' | 'subagent' | 'files' | 'workflow';
  label: string;           // "Plan", "Explore", "Agent", "Files", "Workflow"
  subagentType: string;    // "Plan", "Explore", "general-purpose", "Workflow"
  description: string;
  model?: string;
  content: string | null;
  prompt: string;
  streaming: boolean;
  status: 'running' | 'complete' | 'error';
  startedAt: number;       // Date.now()
  completedAt?: number;
  isError?: boolean;
  blocks: MessageBlock[];  // live sub-agent activity (same types as main chat)
  /** For type==='workflow': the live phase/agent progress tree. */
  workflow?: WorkflowSnapshot;
  /** True for a sub-agent spawned with run_in_background. The Agent tool returns
   *  immediately (a task id) while the sub-agent keeps streaming, so the panel
   *  must stay open + running until the background task settles — otherwise its
   *  later tools/thoughts spill into the main chat instead of this panel. */
  background?: boolean;
}

// --- Session modified files & diff types ---

/** Mirrors MAX_DIFF_LINES in nerve/gateway/diff.py — diffs are truncated past this. */
export const MAX_DIFF_LINES = 2000;

export interface DiffLine {
  type: 'addition' | 'deletion' | 'context' | 'info';
  content: string;
  old_line?: number;
  new_line?: number;
}

export interface DiffHunk {
  old_start: number;
  old_count: number;
  new_start: number;
  new_count: number;
  header: string;
  lines: DiffLine[];
}

export interface FileDiff {
  path: string;
  short_path: string;
  status: 'created' | 'modified' | 'deleted' | 'unchanged';
  binary: boolean;
  stats: { additions: number; deletions: number };
  hunks: DiffHunk[];
  /** Raw git-style unified-diff string for the @pierre/diffs renderer. */
  patch: string;
  truncated: boolean;
  /** Markdown files only: post-change file content (original for deleted
   *  files) for the rendered-preview toggle. Null for non-markdown files. */
  markdown_content?: string | null;
  /** True when markdown_content was cut at MAX_DIFF_LINES. */
  markdown_truncated?: boolean;
}

export interface ModifiedFileSummary {
  path: string;
  short_path: string;
  status: 'created' | 'modified' | 'deleted';
  stats: { additions: number; deletions: number };
  created_at: string;
}
