import { Terminal, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { CollapsibleToolBlock } from './CollapsibleToolBlock';

export function BashToolBlock({ block }: { block: ToolCallBlockData }) {
  const isRunning = block.status === 'running';
  const command = String(block.input.command || '');
  const truncatedCmd = command.length > 80 ? command.slice(0, 80) + '...' : command;

  return (
    <CollapsibleToolBlock
      isRunning={isRunning}
      isError={block.isError}
      icon={Terminal}
      iconClassName="text-emerald-400"
      label=""
      theme="default"
      headerExtra={<>
        <span className="text-emerald-500 text-[13px] font-mono select-none">$</span>
        <span className="text-[13px] font-mono text-[#ccc] truncate">{truncatedCmd}</span>
      </>}
    >
      {/* Full command */}
      {command.length > 80 && (
        <div className="px-3 py-2 border-b border-[#1a1a1a]">
          <pre className="text-[12px] font-mono text-[#ccc] whitespace-pre-wrap">{command}</pre>
        </div>
      )}

      {/* Output */}
      {block.result !== undefined && (
        <pre className={`px-3 py-2 text-[12px] font-mono whitespace-pre-wrap max-h-80 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-[#888]'}`}>
          {block.result}
        </pre>
      )}

      {isRunning && block.result === undefined && (
        <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2">
          <Loader2 size={12} className="animate-spin" /> Running...
        </div>
      )}
    </CollapsibleToolBlock>
  );
}
