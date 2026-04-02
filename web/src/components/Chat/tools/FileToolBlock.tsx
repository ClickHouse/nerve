import { useState } from 'react';
import { ChevronRight, ChevronDown, FileText, FilePlus, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

export function FileToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || block.input.path || '');
  const isWrite = block.tool === 'Write';
  const Icon = isWrite ? FilePlus : FileText;

  // For Read results, show line count
  const lineCount = block.result ? block.result.split('\n').length : null;

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-[#6366f1] animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : 'text-blue-400'}`} />
        }
        <span className="text-[13px] font-mono font-medium text-text-secondary">{block.tool}</span>
        <span className="text-[12px] text-text-dim truncate font-mono">{filePath}</span>
        {lineCount && !isWrite && (
          <span className="text-[10px] text-text-faint shrink-0">{lineCount} lines</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {block.result !== undefined && (
            <pre className={`px-3 py-2 text-[12px] font-mono whitespace-pre-wrap max-h-80 overflow-y-auto bg-bg ${block.isError ? 'text-red-400' : 'text-text-muted'}`}>
              {block.result}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> {isWrite ? 'Writing...' : 'Reading...'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
