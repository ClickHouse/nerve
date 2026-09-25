import { useState } from 'react';
import { ChevronRight, ChevronDown, Terminal, FileText, Search, Globe, Loader2, Download } from '../ui/icons';
import { Button } from '../ui';
import { getToolSummary } from '../../utils/toolSummary';
import type { ToolCallBlockData } from '../../types/chat';
import { getToken } from '../../api/client';
import { EditToolBlock } from './tools/EditToolBlock';
import { BashToolBlock } from './tools/BashToolBlock';
import { FileToolBlock } from './tools/FileToolBlock';
import { MemoryToolBlock } from './tools/MemoryToolBlock';
import { TaskToolBlock } from './tools/TaskToolBlock';
import { CCTaskToolBlock } from './tools/CCTaskToolBlock';
import { ScheduleWakeupBlock } from './tools/ScheduleWakeupBlock';
import { SourceToolBlock } from './tools/SourceToolBlock';
import { SubagentToolBlock } from './tools/SubagentToolBlock';
import { WorkflowToolBlock } from './tools/WorkflowToolBlock';
import { QuestionBlock } from './tools/QuestionBlock';
import { PlanApprovalBlock } from './tools/PlanApprovalBlock';
import { PlanToolBlock } from './tools/PlanToolBlock';
import { SkillToolBlock } from './tools/SkillToolBlock';
import { NotificationToolBlock } from './tools/NotificationToolBlock';
import { WorkflowRunToolBlock } from './tools/WorkflowRunToolBlock';

const WORKFLOW_RUN_ID_RE = /wfr-[0-9a-f]{8}/;

/**
 * First wfr-xxxxxxxx id in a workflow_run_* tool result. Both tools embed it:
 *   workflow_run_start  → "Workflow run wfr-xxxxxxxx created (running). ..."
 *   workflow_run_status → "wfr-xxxxxxxx [status] title — spent $X / budget $Y"
 * MCP results may arrive as JSON content-block arrays — unwrap text blocks
 * first. Returns null while the call has no result yet.
 */
function extractWorkflowRunId(result?: string): string | null {
  if (!result) return null;
  let text = result;
  try {
    const parsed: unknown = JSON.parse(result);
    if (Array.isArray(parsed)) {
      text = parsed
        .filter(b => b && b.type === 'text')
        .map(b => String(b.text))
        .join('\n');
    }
  } catch { /* not JSON */ }
  const match = text.match(WORKFLOW_RUN_ID_RE);
  return match ? match[0] : null;
}

const TOOL_ICONS: Record<string, typeof Terminal> = {
  Bash: Terminal,
  Read: FileText,
  Write: FileText,
  Edit: FileText,
  Grep: Search,
  Glob: Search,
  WebSearch: Globe,
  WebFetch: Globe,
};

export function ToolCallBlock({ block }: { block: ToolCallBlockData }) {
  // Route to specialized renderers
  switch (block.tool) {
    case 'Edit':
      return <EditToolBlock block={block} />;
    case 'Bash':
      return <BashToolBlock block={block} />;
    case 'Read':
    case 'Write':
      return <FileToolBlock block={block} />;
    // Sub-agent spawn: Claude Code 2.1.x renamed "Task" → "Agent". Match both
    // so old chat history keeps rendering with the sub-agent card.
    case 'Agent':
    case 'Task':
      return <SubagentToolBlock block={block} />;
    // Dynamic workflow: compact card in chat; live phase/agent tree in panel.
    case 'Workflow':
      return <WorkflowToolBlock block={block} />;
    // Claude Code 2.1+ task tools (replacement for TodoWrite). Compact card
    // in chat; full list shown by the TaskPanel below the message stream.
    case 'TaskCreate':
    case 'TaskUpdate':
    case 'TaskList':
    case 'TaskGet':
    case 'TaskStop':
    case 'TaskOutput':
      return <CCTaskToolBlock block={block} />;
    // Claude Code 2.1+ self-paced ``/loop`` wakeup. Nerve has no timer
    // for it, but we render the call so the user sees what was scheduled.
    case 'ScheduleWakeup':
      return <ScheduleWakeupBlock block={block} />;
    case 'AskUserQuestion':
      return <QuestionBlock block={block} />;
    case 'ExitPlanMode':
    case 'EnterPlanMode':
      return <PlanApprovalBlock block={block} />;
  }

  // send_file — render as inline download card, no tool chrome
  if (block.tool.includes('send_file')) {
    return <SendFileBlock block={block} />;
  }

  // Notification tools
  if (block.tool.includes('notify') || block.tool.includes('ask_user')) {
    return <NotificationToolBlock block={block} />;
  }

  // Workflow runs (workflow_run_start / workflow_run_status) — live run card
  // fed by the runs store (global workflow_run_update WS event). The run id
  // only exists in the tool result, so while the call is still running (or
  // errored, e.g. "no such workflow run") fall through to the generic block.
  if (block.tool.includes('workflow_run_start') || block.tool.includes('workflow_run_status')) {
    const runId = extractWorkflowRunId(block.result);
    if (runId && !block.isError) {
      return <WorkflowRunToolBlock block={block} runId={runId} />;
    }
  }

  // MCP tool routing by name pattern
  if (block.tool.includes('list_sources') || block.tool.includes('poll_source') || block.tool.includes('poll_all') || block.tool.includes('read_source')) {
    return <SourceToolBlock block={block} />;
  }
  if (block.tool.includes('memory') || block.tool.includes('memorize') || block.tool.includes('recall') || block.tool.includes('conversation_history') || block.tool.includes('sync_status')) {
    return <MemoryToolBlock block={block} />;
  }
  if (block.tool.includes('plan_')) {
    return <PlanToolBlock block={block} />;
  }
  if (block.tool.includes('skill_list') || block.tool.includes('skill_get') || block.tool.includes('skill_read_reference') || block.tool.includes('skill_run_script') || block.tool.includes('skill_create') || block.tool.includes('skill_update')) {
    return <SkillToolBlock block={block} />;
  }
  if (block.tool.includes('task_')) {
    return <TaskToolBlock block={block} />;
  }

  // Generic fallback
  return <GenericToolBlock block={block} />;
}

function SendFileBlock({ block }: { block: ToolCallBlockData }) {
  const filePath = block.input?.file_path as string || '';
  const filename = filePath.split('/').pop() || 'file';
  const downloadUrl = `/api/files/download?path=${encodeURIComponent(filePath)}`;
  const isRunning = block.status === 'running';

  if (isRunning) {
    return (
      <div className="my-1.5 inline-flex items-center gap-2 px-3 py-2 text-sm text-text-muted">
        <Loader2 size={14} className="animate-spin" /> Sending file...
      </div>
    );
  }

  if (block.isError) {
    return (
      <div className="my-1.5 inline-flex items-center gap-2 px-3 py-2 text-sm text-error">
        <FileText size={14} /> Failed to send file
      </div>
    );
  }

  return (
    <div className="my-2">
      <a
        href={`${downloadUrl}&token=${getToken()}`}
        download={filename}
        className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-surface hover:bg-surface-hover transition-colors text-sm text-text-secondary"
      >
        <FileText size={14} />
        <span className="truncate max-w-[200px]">{filename}</span>
        <Download size={12} className="text-text-muted" />
      </a>
    </div>
  );
}

function GenericToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const Icon = TOOL_ICONS[block.tool] || Terminal;
  const summary = getToolSummary(block.tool, block.input);
  const isRunning = block.status === 'running';

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <Button
        variant="subtle"
        size="md"
        fullWidth
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
        className="justify-start text-left rounded-none"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-hue-red' : 'text-text-muted'}`} />
        }
        <span className="text-sm font-mono font-medium text-text-secondary">{block.tool}</span>
        {summary && <span className="text-xs text-text-dim truncate font-mono">{summary}</span>}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </Button>

      {expanded && (
        <div className="border-t border-border">
          {/* Input */}
          <div className="px-3 py-2">
            <div className="text-2xs uppercase tracking-wider text-text-faint mb-1">Input</div>
            <pre className="text-xs text-text-muted font-mono whitespace-pre-wrap overflow-x-auto max-h-60 overflow-y-auto bg-bg rounded p-2 border border-border-subtle">
              {JSON.stringify(block.input, null, 2)}
            </pre>
          </div>

          {/* Result */}
          {block.result !== undefined && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <div className="text-2xs uppercase tracking-wider text-text-faint mb-1">
                {block.isError ? 'Error' : 'Result'}
              </div>
              <pre className={`text-xs font-mono whitespace-pre-wrap overflow-x-auto max-h-80 overflow-y-auto bg-bg rounded p-2 border border-border-subtle ${block.isError ? 'text-hue-red' : 'text-text-muted'}`}>
                {block.result}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-xs text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Running...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
