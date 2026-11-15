import { memo, useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, Loader2 } from '../ui/icons';
import { Button } from '../ui';
import type { BurstItem, ToolBurst } from '../../types/renderBlocks';
import { ThinkingBlock } from './ThinkingBlock';
import { ToolCallBlock } from './ToolCallBlock';
import { ToolCallGroupBlock } from './ToolCallGroupBlock';
import { burstSummary } from './toolBursts';

/** A burst folds to its summary line from this many blocks (one a tool call). */
const MIN_FOLD_BLOCKS = 2;

/** Holds an icon at the height of the first text line, also when the summary wraps. */
const FIRST_LINE = 'flex h-[1lh] shrink-0 items-center';

/**
 * A run of thinking and tool calls (see `toolBursts.ts`). While it is the
 * live end of a running turn it stays open, so the newest rows show; after
 * that it folds to one line that names the work, and the line opens it.
 */
export const ToolBurstBlock = memo(function ToolBurstBlock({
  burst,
  live,
  repeatRunCalls,
}: {
  burst: ToolBurst;
  /** The burst ends a turn that still runs. */
  live: boolean;
  /** Tool-use ids to render as repeat workflow-run rows (see ToolCallBlock). */
  repeatRunCalls: ReadonlySet<string>;
}) {
  const { items, calls } = burst;
  // null: the person has not toggled it, so it follows `live`.
  const [toggled, setToggled] = useState<boolean | null>(null);
  const summary = useMemo(() => burstSummary(calls), [calls]);

  const thinkingCount = items.filter(item => item.type === 'thinking').length;
  const folds = calls.length > 0 && calls.length + thinkingCount >= MIN_FOLD_BLOCKS;
  const open = !folds || (toggled ?? live);
  const failed = calls.filter(call => call.isError).length;
  const running = calls.some(call => call.status === 'running');

  // A folded burst mounts no rows: a long transcript renders one line per burst.
  const rows = open && items.map((item, i) => (
    <BurstRow
      key={i}
      item={item}
      streaming={live && i === items.length - 1}
      repeatRunCalls={repeatRunCalls}
    />
  ));
  if (!folds) return <>{rows}</>;

  return (
    <div className="my-1">
      <Button
        variant="subtle"
        size="sm"
        onClick={() => setToggled(!open)}
        aria-expanded={open}
        className="-mx-1.5 max-w-full px-1.5 py-1 items-start justify-start text-left text-sm leading-snug text-text-muted"
      >
        <span className={FIRST_LINE} aria-hidden="true">
          {open
            ? <ChevronDown size={14} className="text-text-faint" />
            : <ChevronRight size={14} className="text-text-faint" />}
        </span>
        {/* A long MCP server name wraps inside the line on a phone. */}
        <span className="min-w-0 break-words">
          {summary}
          {failed > 0 && (
            <>
              <span className="text-text-faint"> · </span>
              <span className="text-error">{failed} failed</span>
            </>
          )}
        </span>
        {running && (
          <>
            <span className={FIRST_LINE} aria-hidden="true">
              <Loader2 size={12} className="text-accent animate-spin" />
            </span>
            <span className="sr-only">Running</span>
          </>
        )}
      </Button>
      {/* The rail sits under the centre of the 14 px chevron. */}
      {open && <div className="ml-[7px] pl-3 border-l border-border-subtle">{rows}</div>}
    </div>
  );
});

function BurstRow({ item, streaming, repeatRunCalls }: {
  item: BurstItem;
  streaming: boolean;
  repeatRunCalls: ReadonlySet<string>;
}) {
  switch (item.type) {
    case 'thinking':
      return <ThinkingBlock content={item.content} streaming={streaming} />;
    case 'tool_call':
      return <ToolCallBlock block={item} repeatRunCard={repeatRunCalls.has(item.toolUseId)} />;
    case 'tool_call_group':
      return <ToolCallGroupBlock group={item} repeatRunCalls={repeatRunCalls} />;
  }
}
