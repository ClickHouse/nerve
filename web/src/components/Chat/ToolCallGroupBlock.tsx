import { useState } from 'react';
import { ChevronRight, ChevronDown, Terminal, FileText, Search, Globe, Loader2 } from '../ui/icons';
import { Button } from '../ui';
import type { ToolCallGroup } from '../../types/renderBlocks';
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

export function ToolCallGroupBlock({ group }: { group: ToolCallGroup }) {
  const [expanded, setExpanded] = useState(false);
  const { tool, blocks } = group;

  const total = blocks.length;
  const hiddenCount = Math.max(0, total - VISIBLE_TAIL);
  const needsCollapsing = hiddenCount > 0;

  const Icon = TOOL_ICONS[tool] || Terminal;
  const hasRunning = blocks.some(b => b.status === 'running');
  const hasError = blocks.some(b => b.isError);

  const hiddenBlocks = needsCollapsing ? blocks.slice(0, hiddenCount) : [];
  const visibleBlocks = needsCollapsing ? blocks.slice(hiddenCount) : blocks;

  return (
    <div className="my-0.5">
      {/* Collapse bar — only shown for groups of 4+ */}
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

      {/* Expanded hidden items */}
      {expanded && hiddenBlocks.map((block) => (
        <ToolCallBlock key={block.toolUseId} block={block} />
      ))}

      {/* Always-visible tail (last 3, or all if total <= 3) */}
      {visibleBlocks.map((block) => (
        <ToolCallBlock key={block.toolUseId} block={block} />
      ))}
    </div>
  );
}
