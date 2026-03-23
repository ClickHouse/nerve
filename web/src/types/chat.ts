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
}

export type MessageBlock = ThinkingBlockData | TextBlockData | ToolCallBlockData;

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
  // Real-time running status (set by backend + WS updates)
  is_running?: boolean;
  starred?: boolean;
}

export type AgentStatus =
  | { state: 'idle' }
  | { state: 'thinking' }
  | { state: 'tool'; toolName: string }
  | { state: 'writing' };

export interface PanelTab {
  id: string;              // toolUseId
  type: 'plan' | 'subagent' | 'files';
  label: string;           // "Plan", "Explore", "Agent", "Files"
  subagentType: string;    // "Plan", "Explore", "general-purpose"
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
}

// --- Session modified files & diff types ---

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
  truncated: boolean;
}

export interface ModifiedFileSummary {
  path: string;
  short_path: string;
  status: 'created' | 'modified' | 'deleted';
  stats: { additions: number; deletions: number };
  created_at: string;
}
