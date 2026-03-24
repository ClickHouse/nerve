import { useState } from 'react';
import { Users, Loader2, ChevronDown, ChevronRight, Check, X } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

const PROVIDER_COLORS: Record<string, string> = {
  anthropic: 'bg-orange-400/20 text-orange-300 border-orange-400/30',
  openai: 'bg-emerald-400/20 text-emerald-300 border-emerald-400/30',
  gemini: 'bg-blue-400/20 text-blue-300 border-blue-400/30',
};

function getProviderStyle(kind?: string) {
  if (!kind) return 'bg-[#2a2a2a] text-[#888] border-[#333]';
  return PROVIDER_COLORS[kind.toLowerCase()] || PROVIDER_COLORS.anthropic;
}

interface HoAEvent {
  event?: string;     // event type: block_started, block_log, block_finished, etc.
  agent?: string;
  provider?: string;  // anthropic, openai, gemini
  label?: string;     // pipeline block name (e.g. "Implement", "Review")
  block_id?: number;
  iteration?: number;
  loop_pass?: number;
  message?: string;
  [key: string]: unknown;
}

export function HoAToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(true);
  const isRunning = block.status === 'running';
  const events: HoAEvent[] = (block.hoaEvents as HoAEvent[] | undefined) || [];

  // Extract state from latest events
  const lastEvent = events.length > 0 ? events[events.length - 1] : null;
  const activeAgent = lastEvent?.agent ?? undefined;
  const activeProvider = lastEvent?.provider ?? undefined;
  // Find the latest block label from block_started events
  const activeLabel = [...events].reverse().find(e => e.label)?.label;
  const mode = String(block.input?.mode || 'relay');
  const agents = String(block.input?.agents || '');

  return (
    <div className="my-1.5 border border-amber-400/20 rounded-lg bg-[#141414] overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-[#1a1a1a] transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-amber-400 animate-spin shrink-0" />
          : block.isError
            ? <X size={14} className="text-red-400 shrink-0" />
            : <Check size={14} className="text-emerald-400 shrink-0" />
        }
        <Users size={14} className="text-amber-400 shrink-0" />
        <span className="text-[13px] font-mono font-medium text-[#ccc]">hoa_execute</span>
        <span className="text-[12px] text-amber-400/60 font-mono">{mode}</span>
        {activeLabel && isRunning && <span className="text-[12px] text-[#888]">· {activeLabel}</span>}
        {agents && !activeLabel && <span className="text-[12px] text-[#555] font-mono truncate">{agents}</span>}

        {isRunning && activeAgent ? (
          <span className={`ml-2 px-1.5 py-0.5 text-[10px] rounded border ${getProviderStyle(activeProvider)}`}>
            {activeAgent}
          </span>
        ) : null}

        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-[#555]" /> : <ChevronRight size={14} className="text-[#555]" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-amber-400/10">
          {/* Progress events */}
          {events.length > 0 && (
            <div className="px-3 py-2 max-h-48 overflow-y-auto">
              <div className="space-y-1">
                {events.slice(-20).map((event, i) => (
                  <div key={i} className="flex items-center gap-2 text-[11px]">
                    {event.label && (
                      <span className="px-1.5 py-0.5 rounded bg-[#2a2a2a] text-[#aaa] border border-[#333] text-[10px]">
                        {event.label}
                      </span>
                    )}
                    {event.agent && (
                      <span className={`px-1.5 py-0.5 rounded border text-[10px] ${getProviderStyle(event.provider)}`}>
                        {event.agent}
                      </span>
                    )}
                    {event.iteration !== undefined && (
                      <span className="text-[#555]">iter {event.iteration}</span>
                    )}
                    {event.message && (
                      <span className="text-[#777] truncate">{event.message}</span>
                    )}
                    {!event.message && !event.agent && !event.label && (
                      <span className="text-[#555] font-mono truncate">
                        {JSON.stringify(event).slice(0, 80)}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Running indicator when no events yet */}
          {isRunning && events.length === 0 && (
            <div className="px-3 py-3 text-[12px] text-[#666] flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> Starting multi-agent execution...
            </div>
          )}

          {/* Result */}
          {block.result !== undefined && (
            <div className="px-3 py-2 border-t border-[#222]">
              <div className="text-[10px] uppercase tracking-wider text-[#555] mb-1">
                {block.isError ? 'Error' : 'Result'}
              </div>
              <pre className={`text-[12px] font-mono whitespace-pre-wrap overflow-x-auto max-h-80 overflow-y-auto bg-[#0f0f0f] rounded p-2 border border-[#222] ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
                {block.result}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
