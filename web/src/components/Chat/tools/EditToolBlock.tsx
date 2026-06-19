import { useState, lazy, Suspense } from 'react';
import { ChevronRight, ChevronDown, FileEdit, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

// The diff renderer pulls in @pierre/diffs + Shiki — only loaded when an Edit
// block is expanded. Shares the lazy chunk with FileChangesPanel's DiffView.
const EditDiff = lazy(() => import('../DiffView').then((m) => ({ default: m.EditDiff })));

export function EditToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || '');
  const oldString = String(block.input.old_string || '');
  const newString = String(block.input.new_string || '');

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <FileEdit size={14} className={`shrink-0 ${block.isError ? 'text-hue-red' : 'text-hue-amber'}`} />
        }
        <span className="text-[13px] font-mono font-medium text-text-secondary">Edit</span>
        <span className="text-[12px] text-text-dim truncate font-mono">{filePath}</span>
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Diff view */}
          <div className="max-h-80 overflow-y-auto">
            <Suspense
              fallback={
                <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
                  <Loader2 size={12} className="animate-spin" /> Loading diff…
                </div>
              }
            >
              <EditDiff fileName={filePath} oldString={oldString} newString={newString} />
            </Suspense>
          </div>

          {/* Error */}
          {block.isError && block.result && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <pre className="text-[12px] font-mono text-hue-red whitespace-pre-wrap">{block.result}</pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Applying edit...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
