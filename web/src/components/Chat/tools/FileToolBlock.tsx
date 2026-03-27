import { FileText, FilePlus, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { CollapsibleToolBlock } from './CollapsibleToolBlock';

export function FileToolBlock({ block }: { block: ToolCallBlockData }) {
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || block.input.path || '');
  const isWrite = block.tool === 'Write';
  const Icon = isWrite ? FilePlus : FileText;

  // For Read results, show line count
  const lineCount = block.result ? block.result.split('\n').length : null;

  return (
    <CollapsibleToolBlock
      isRunning={isRunning}
      isError={block.isError}
      icon={Icon}
      iconClassName="text-blue-400"
      label={block.tool}
      labelClassName="text-[#ccc] font-mono"
      headerExtra={<>
        <span className="text-[12px] text-[#666] truncate font-mono">{filePath}</span>
        {lineCount && !isWrite && (
          <span className="text-[10px] text-[#444] shrink-0">{lineCount} lines</span>
        )}
      </>}
    >
      {block.result !== undefined && (
        <pre className={`px-3 py-2 text-[12px] font-mono whitespace-pre-wrap max-h-80 overflow-y-auto bg-[#0f0f0f] ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
          {block.result}
        </pre>
      )}

      {isRunning && block.result === undefined && (
        <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2">
          <Loader2 size={12} className="animate-spin" /> {isWrite ? 'Writing...' : 'Reading...'}
        </div>
      )}
    </CollapsibleToolBlock>
  );
}
