import { useState } from 'react';
import { ChevronRight, ChevronDown, Terminal, FileText, Search, Globe, Loader2 } from '../ui/icons';
import { Button } from '../ui';
import type { ToolCallBlockData } from '../../types/chat';
import type { ToolCallGroup } from '../../types/renderBlocks';
import { workflowRunCardId } from '../../utils/workflowRunCards';
import { ToolCallBlock } from './ToolCallBlock';

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

/** How many items to always show at the bottom of a collapsed group. */
const VISIBLE_TAIL = 3;

export function ToolCallGroupBlock({
  group,
  repeatRunCalls,
}: {
  group: ToolCallGroup;
  /** Tool-use ids to render as repeat workflow-run rows (see ToolCallBlock). */
  repeatRunCalls?: ReadonlySet<string>;
}) {
  const [expanded, setExpanded] = useState(false);
  const { tool, blocks } = group;

  const total = blocks.length;
  const tailStart = Math.max(0, total - VISIBLE_TAIL);

  // A call that holds a workflow-run card stays visible when the group is
  // collapsed. The card is the only live view of its run (status, spend, Kill),
  // and the repeat rows for that run refer to it. So a group of calls for
  // different runs shows every card instead of hiding the older ones.
  const holdsRunCard = (block: ToolCallBlockData) =>
    repeatRunCalls !== undefined
    && workflowRunCardId(block) !== null
    && !repeatRunCalls.has(block.toolUseId);
  const isShown = (block: ToolCallBlockData, i: number) =>
    expanded || i >= tailStart || holdsRunCard(block);

  const hiddenCount = blocks.filter((b, i) => i < tailStart && !holdsRunCard(b)).length;
  const needsCollapsing = hiddenCount > 0;

  const Icon = TOOL_ICONS[tool] || Terminal;
  const hasRunning = blocks.some(b => b.status === 'running');
  const hasError = blocks.some(b => b.isError);

  return (
    <div className="my-0.5">
      {/* Collapse bar — only shown for groups of 4+ that hide at least one call */}
      {needsCollapsing && (
        <Button
          variant="subtle"
          size="sm"
          fullWidth
          onClick={() => setExpanded(!expanded)}
          aria-expanded={expanded}
          className="justify-start gap-2 rounded-md text-left leading-tight"
        >
          {hasRunning
            ? <Loader2 size={12} className="text-accent animate-spin shrink-0" />
            : <Icon size={12} className={`shrink-0 ${hasError ? 'text-hue-red' : 'text-text-faint'}`} />
          }
          <span className="font-mono font-medium">
            {expanded ? 'Collapse' : `Show ${hiddenCount} more`}
          </span>
          <span className="text-text-faint">·</span>
          <span className="text-text-faint">{total} {tool} calls</span>
          <div className="ml-auto shrink-0">
            {expanded
              ? <ChevronDown size={12} className="text-text-faint" />
              : <ChevronRight size={12} className="text-text-faint" />
            }
          </div>
        </Button>
      )}

      {/* In order: every call when expanded; else the last 3 plus any
          workflow-run card holders before them. */}
      {blocks.map((block, i) => isShown(block, i) && (
        <ToolCallBlock key={block.toolUseId} block={block} repeatRunCard={repeatRunCalls?.has(block.toolUseId)} />
      ))}
    </div>
  );
}
