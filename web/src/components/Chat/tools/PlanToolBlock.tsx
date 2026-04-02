import { useState } from 'react';
import { ChevronRight, ChevronDown, Lightbulb, ListTodo, FileText, Check, X, MessageSquare, Loader2, ExternalLink } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { MarkdownContent } from '../MarkdownContent';
import type { ToolCallBlockData } from '../../../types/chat';

/** Extract readable text from MCP content blocks. */
function extractText(result: string): string {
  try {
    const parsed = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter((b: any) => b.type === 'text')
        .map((b: any) => b.text)
        .join('\n');
    }
  } catch { /* not JSON */ }
  return result;
}

interface ParsedPlan {
  status: string;
  taskTitle: string;
  planId: string;
  version: string;
  date: string;
}

/** Parse plan list lines: "- [status] title — plan plan-xxx vN (date)" */
function parsePlanList(text: string): ParsedPlan[] {
  const items: ParsedPlan[] = [];
  for (const line of text.split('\n')) {
    const match = line.match(/^-\s*\[(\w+)\]\s*(.+?)\s*—\s*plan\s+(plan-\S+)\s+v(\d+)\s*\(([^)]+)\)/);
    if (match) {
      items.push({
        status: match[1],
        taskTitle: match[2].trim(),
        planId: match[3],
        version: match[4],
        date: match[5],
      });
    }
  }
  return items;
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/15 text-yellow-400',
  approved: 'bg-green-500/15 text-green-400',
  implementing: 'bg-blue-500/15 text-blue-400',
  declined: 'bg-red-500/15 text-red-400',
  superseded: 'bg-border-subtle text-text-muted',
};

type PlanTool = 'plan_propose' | 'plan_list' | 'plan_read' | 'plan_approve' | 'plan_decline' | 'plan_revise';

const TOOL_CONFIG: Record<PlanTool, { label: string; icon: typeof Lightbulb; runningLabel: string }> = {
  plan_propose: { label: 'Propose Plan', icon: Lightbulb, runningLabel: 'Proposing...' },
  plan_list:    { label: 'List Plans', icon: ListTodo, runningLabel: 'Loading...' },
  plan_read:    { label: 'Read Plan', icon: FileText, runningLabel: 'Reading...' },
  plan_approve: { label: 'Approve Plan', icon: Check, runningLabel: 'Approving...' },
  plan_decline: { label: 'Decline Plan', icon: X, runningLabel: 'Declining...' },
  plan_revise:  { label: 'Revise Plan', icon: MessageSquare, runningLabel: 'Requesting revision...' },
};

export function PlanToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const navigate = useNavigate();
  const isRunning = block.status === 'running';

  const toolName = (block.tool.split('__').pop() || block.tool) as PlanTool;
  const config = TOOL_CONFIG[toolName] || { label: 'Plan', icon: Lightbulb, runningLabel: 'Working...' };
  const Icon = config.icon;

  const planId = String(block.input.plan_id || '');
  const feedback = String(block.input.feedback || '');
  const resultText = block.result ? extractText(block.result) : '';

  // plan_list parsing
  const planList = toolName === 'plan_list' ? parsePlanList(resultText) : [];

  // plan_propose: extract proposed plan ID
  const proposedPlanId = toolName === 'plan_propose'
    ? resultText.match(/Plan proposed:\s*(plan-\S+)/)?.[1]
    : null;

  // plan_approve: extract impl session ID
  const implSessionId = toolName === 'plan_approve'
    ? resultText.match(/impl[_ ]session[_ ](?:id)?:?\s*(\S+)/i)?.[1]
    : null;

  // plan_read: split header from content at the --- separator
  const readParts = toolName === 'plan_read' && resultText
    ? resultText.split(/\n---\n(.*)$/s)
    : null;
  const readHeader = readParts?.[0] || '';
  const readContent = readParts?.[1] || '';

  // Collapsed summary text
  let summary = '';
  if (toolName === 'plan_propose') summary = String(block.input.task_id || '');
  else if (toolName === 'plan_list' && planList.length > 0) summary = `${planList.length} plans`;
  else if (planId) summary = planId;

  // Icon color
  const iconColor = block.isError ? 'text-red-400'
    : toolName === 'plan_approve' ? 'text-emerald-400'
    : toolName === 'plan_decline' ? 'text-red-400'
    : 'text-amber-400';

  return (
    <div className="my-1.5 border border-amber-500/20 rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-amber-400 animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${iconColor}`} />
        }
        <span className="text-[13px] font-medium text-amber-300">{config.label}</span>
        {summary && <span className="text-[12px] text-text-dim truncate">{summary}</span>}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-amber-500/10">

          {/* ── plan_propose ── */}
          {toolName === 'plan_propose' && (
            <div className="px-3 py-2">
              {block.input.content ? (
                <div className="text-[12px] text-[#999] max-h-40 overflow-y-auto whitespace-pre-wrap">
                  {String(block.input.content).slice(0, 500)}
                  {String(block.input.content).length > 500 ? '...' : null}
                </div>
              ) : null}
              {proposedPlanId && !block.isError && (
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/plans/${proposedPlanId}`); }}
                  className="mt-2 flex items-center gap-1 text-[11px] text-amber-400 hover:text-amber-300 cursor-pointer"
                >
                  <ExternalLink size={10} /> Review plan
                </button>
              )}
              {proposedPlanId && !block.isError && (
                <div className="mt-1 text-[11px] text-green-400/70">
                  Plan proposed — awaiting review
                </div>
              )}
            </div>
          )}

          {/* ── plan_list ── */}
          {toolName === 'plan_list' && (planList.length > 0 ? (
            <div className="px-3 py-2 space-y-1 max-h-60 overflow-y-auto">
              {planList.map((p, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 text-[12px] cursor-pointer hover:bg-surface-hover rounded px-1 py-0.5"
                  onClick={() => navigate(`/plans/${p.planId}`)}
                >
                  <span className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${STATUS_COLORS[p.status] || 'bg-border-subtle text-text-muted'}`}>
                    {p.status}
                  </span>
                  <span className="text-[#bbb] truncate">{p.taskTitle}</span>
                  <span className="text-[10px] text-text-faint shrink-0">v{p.version}</span>
                </div>
              ))}
            </div>
          ) : resultText ? (
            <pre className={`px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
              {resultText}
            </pre>
          ) : null)}

          {/* ── plan_read ── */}
          {toolName === 'plan_read' && resultText && !block.isError && (
            <div className="px-3 py-2">
              {/* Header metadata */}
              {readHeader && (
                <pre className="text-[12px] text-text-muted whitespace-pre-wrap mb-2">{readHeader}</pre>
              )}
              {/* Plan content */}
              {readContent && (
                <div className="max-h-96 overflow-y-auto bg-bg rounded-lg p-4 border border-amber-500/10">
                  <MarkdownContent content={readContent} />
                </div>
              )}
              {planId && (
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/plans/${planId}`); }}
                  className="mt-2 flex items-center gap-1 text-[11px] text-amber-400 hover:text-amber-300 cursor-pointer"
                >
                  <ExternalLink size={10} /> Open plan
                </button>
              )}
            </div>
          )}

          {/* ── plan_approve ── */}
          {toolName === 'plan_approve' && resultText && !block.isError && (
            <div className="px-3 py-2">
              <div className="flex items-center gap-2 text-[12px] text-emerald-400">
                <Check size={12} />
                <span>Plan approved</span>
              </div>
              {implSessionId && (
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/chat/${implSessionId}`); }}
                  className="mt-2 flex items-center gap-1 text-[11px] text-blue-400 hover:text-blue-300 cursor-pointer"
                >
                  <MessageSquare size={10} /> Watch implementation
                </button>
              )}
              {planId && (
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/plans/${planId}`); }}
                  className="mt-1 flex items-center gap-1 text-[11px] text-amber-400 hover:text-amber-300 cursor-pointer"
                >
                  <ExternalLink size={10} /> View plan
                </button>
              )}
            </div>
          )}

          {/* ── plan_decline ── */}
          {toolName === 'plan_decline' && resultText && !block.isError && (
            <div className="px-3 py-2">
              <div className="flex items-center gap-2 text-[12px] text-red-400">
                <X size={12} />
                <span>Plan declined</span>
              </div>
              {feedback && (
                <div className="mt-2 flex gap-0">
                  <div className="w-0.5 bg-red-400/30 rounded-full shrink-0" />
                  <p className="pl-2 text-[12px] text-text-muted whitespace-pre-wrap">{feedback}</p>
                </div>
              )}
            </div>
          )}

          {/* ── plan_revise ── */}
          {toolName === 'plan_revise' && resultText && !block.isError && (
            <div className="px-3 py-2">
              <div className="flex items-center gap-2 text-[12px] text-amber-400">
                <MessageSquare size={12} />
                <span>Revision requested</span>
              </div>
              {feedback && (
                <div className="mt-2 flex gap-0">
                  <div className="w-0.5 bg-amber-400/30 rounded-full shrink-0" />
                  <p className="pl-2 text-[12px] text-[#999] whitespace-pre-wrap">{feedback}</p>
                </div>
              )}
            </div>
          )}

          {/* ── Error fallback ── */}
          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-amber-500/10">
              {resultText}
            </pre>
          )}

          {/* ── Running spinner ── */}
          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> {config.runningLabel}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
