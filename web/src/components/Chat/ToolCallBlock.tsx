import { useState } from 'react';
import { ChevronRight, ChevronDown, Terminal, FileText, Search, Globe, Loader2, Download } from 'lucide-react';
import { getToolSummary } from '../../utils/toolSummary';
import type { ToolCallBlockData } from '../../types/chat';
import { getToken } from '../../api/client';
import { EditToolBlock } from './tools/EditToolBlock';
import { BashToolBlock } from './tools/BashToolBlock';
import { FileToolBlock } from './tools/FileToolBlock';
import { MemoryToolBlock } from './tools/MemoryToolBlock';
import { TaskToolBlock } from './tools/TaskToolBlock';
import { SourceToolBlock } from './tools/SourceToolBlock';
import { SubagentToolBlock } from './tools/SubagentToolBlock';
import { HoAToolBlock } from './tools/HoAToolBlock';
import { QuestionBlock } from './tools/QuestionBlock';
import { PlanApprovalBlock } from './tools/PlanApprovalBlock';
import { PlanToolBlock } from './tools/PlanToolBlock';
import { SkillToolBlock } from './tools/SkillToolBlock';
import { NotificationToolBlock } from './tools/NotificationToolBlock';

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
    case 'Task':
      return <SubagentToolBlock block={block} />;
    case 'AskUserQuestion':
      return <QuestionBlock block={block} />;
    case 'ExitPlanMode':
    case 'EnterPlanMode':
      return <PlanApprovalBlock block={block} />;
  }

  // houseofagents
  if (block.tool.includes('hoa_execute')) {
    return <HoAToolBlock block={block} />;
  }

  // send_file — render as inline download card, no tool chrome
  if (block.tool.includes('send_file')) {
    return <SendFileBlock block={block} />;
  }

  // Notification tools
  if (block.tool.includes('notify') || block.tool.includes('ask_user')) {
    return <NotificationToolBlock block={block} />;
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
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-hue-red' : 'text-text-muted'}`} />
        }
        <span className="text-[13px] font-mono font-medium text-text-secondary">{block.tool}</span>
        {summary && <span className="text-[12px] text-text-dim truncate font-mono">{summary}</span>}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Input */}
          <div className="px-3 py-2">
            <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">Input</div>
            <pre className="text-[12px] text-text-muted font-mono whitespace-pre-wrap overflow-x-auto max-h-60 overflow-y-auto bg-bg rounded p-2 border border-border-subtle">
              {JSON.stringify(block.input, null, 2)}
            </pre>
          </div>

          {/* Result */}
          {block.result !== undefined && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">
                {block.isError ? 'Error' : 'Result'}
              </div>
              <pre className={`text-[12px] font-mono whitespace-pre-wrap overflow-x-auto max-h-80 overflow-y-auto bg-bg rounded p-2 border border-border-subtle ${block.isError ? 'text-hue-red' : 'text-text-muted'}`}>
                {block.result}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Running...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
