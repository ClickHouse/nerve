import { useState } from 'react';
import { ChevronRight, ChevronDown, FileEdit, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

export function EditToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || '');
  const oldString = String(block.input.old_string || '');
  const newString = String(block.input.new_string || '');

  const oldLines = oldString.split('\n');
  const newLines = newString.split('\n');

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-[#6366f1] animate-spin shrink-0" />
          : <FileEdit size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : 'text-amber-400'}`} />
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
          <div className="font-mono text-[12px] overflow-x-auto max-h-80 overflow-y-auto">
            {oldLines.map((line, i) => (
              <div key={`old-${i}`} className="px-3 py-0.5 bg-red-900/15 text-red-300/80">
                <span className="select-none text-red-500/50 mr-2">-</span>{line}
              </div>
            ))}
            {newLines.map((line, i) => (
              <div key={`new-${i}`} className="px-3 py-0.5 bg-green-900/15 text-green-300/80">
                <span className="select-none text-green-500/50 mr-2">+</span>{line}
              </div>
            ))}
          </div>

          {/* Error */}
          {block.isError && block.result && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <pre className="text-[12px] font-mono text-red-400 whitespace-pre-wrap">{block.result}</pre>
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
