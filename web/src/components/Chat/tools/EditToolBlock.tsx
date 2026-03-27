import { FileEdit, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { CollapsibleToolBlock } from './CollapsibleToolBlock';

export function EditToolBlock({ block }: { block: ToolCallBlockData }) {
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || '');
  const oldString = String(block.input.old_string || '');
  const newString = String(block.input.new_string || '');

  const oldLines = oldString.split('\n');
  const newLines = newString.split('\n');

  return (
    <CollapsibleToolBlock
      isRunning={isRunning}
      isError={block.isError}
      icon={FileEdit}
      iconClassName="text-amber-400"
      label="Edit"
      labelClassName="text-[#ccc] font-mono"
      headerExtra={
        <span className="text-[12px] text-[#666] truncate font-mono">{filePath}</span>
      }
    >
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
        <div className="px-3 py-2 border-t border-[#222]">
          <pre className="text-[12px] font-mono text-red-400 whitespace-pre-wrap">{block.result}</pre>
        </div>
      )}

      {isRunning && block.result === undefined && (
        <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2 border-t border-[#222]">
          <Loader2 size={12} className="animate-spin" /> Applying edit...
        </div>
      )}
    </CollapsibleToolBlock>
  );
}
