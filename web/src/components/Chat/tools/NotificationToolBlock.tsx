import { Bell, HelpCircle, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractText } from '../../../utils/extractResultText';
import { CollapsibleToolBlock } from './CollapsibleToolBlock';

export function NotificationToolBlock({ block }: { block: ToolCallBlockData }) {
  const isRunning = block.status === 'running';
  const isNotify = block.tool === 'notify';
  const isAsk = block.tool === 'ask_user';

  const title = String(block.input.title || '');
  const priority = String(block.input.priority || 'normal');
  const optionsRaw = String(block.input.options || '');
  const options = optionsRaw ? optionsRaw.split(',').map(o => o.trim()).filter(Boolean) : [];
  const wait = String(block.input.wait || 'false').toLowerCase() === 'true';
  const body = String(block.input.body || '');

  const Icon = isAsk ? HelpCircle : Bell;
  const iconColor = isAsk ? 'text-blue-400' : 'text-amber-400';
  const label = isNotify ? 'Notify' : 'Ask User';

  const resultText = block.result ? extractText(block.result) : '';
  const isSent = resultText.includes('sent') || resultText.includes('Sent');

  return (
    <CollapsibleToolBlock
      isRunning={isRunning}
      isError={block.isError}
      icon={Icon}
      iconClassName={iconColor}
      label={label}
      headerExtra={<>
        {title && <span className="text-[12px] text-[#777] truncate">{title}</span>}
        {priority !== 'normal' && (
          <span className={`text-[10px] px-1.5 py-0.5 rounded shrink-0 ${
            priority === 'urgent' ? 'bg-red-500/15 text-red-400' :
            priority === 'high' ? 'bg-orange-400/15 text-orange-400' :
            'bg-[#333] text-[#888]'
          }`}>
            {priority}
          </span>
        )}
        {wait && isAsk && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-400 shrink-0">blocking</span>
        )}
        {isSent && !isRunning && (
          <span className="text-[10px] text-emerald-400/70 shrink-0">sent</span>
        )}
      </>}
    >
      <div className="px-3 py-2">
        {title && <p className="text-[13px] text-[#e0e0e0] font-medium">{title}</p>}
        {body && <p className="text-[12px] text-[#888] mt-0.5">{body}</p>}

        {/* Options for questions */}
        {isAsk && options.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            {options.map(opt => (
              <span key={opt} className="px-2 py-0.5 text-[11px] bg-[#1a1a2e] text-blue-300/80 rounded border border-blue-500/20">
                {opt}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Result */}
      {resultText && (
        <div className="px-3 py-2 border-t border-[#222]">
          <pre className={`text-[12px] font-mono whitespace-pre-wrap ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
            {resultText}
          </pre>
        </div>
      )}

      {isRunning && block.result === undefined && (
        <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2 border-t border-[#222]">
          <Loader2 size={12} className="animate-spin" /> Sending...
        </div>
      )}
    </CollapsibleToolBlock>
  );
}
