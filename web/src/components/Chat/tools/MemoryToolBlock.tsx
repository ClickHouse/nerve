import { useState } from 'react';
import { ChevronRight, ChevronDown, Brain, BookOpen, Search, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

/** Extract readable text from MCP content blocks or plain text. */
function extractText(result: string): string {
  try {
    const parsed = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter((b: any) => b.type === 'text')
        .map((b: any) => b.text)
        .join('\n');
    }
  } catch {
    // Not JSON — return as-is
  }
  return result;
}

interface MemoryItem {
  type: string;  // event, profile, knowledge, behavior
  id?: string;
  text: string;
}

/** Parse "- [type] (id:...) description" lines from recall result text. */
function parseMemoryItems(text: string): MemoryItem[] {
  const items: MemoryItem[] = [];
  const lines = text.split('\n');
  for (const line of lines) {
    const match = line.match(/^-\s*\[(\w+)\]\s*(?:\(id:([^)]+)\)\s*)?(.+)/);
    if (match) {
      items.push({ type: match[1], id: match[2], text: match[3].trim() });
    }
  }
  return items;
}

const TYPE_COLORS: Record<string, string> = {
  event: 'text-blue-400 bg-blue-500/10',
  profile: 'text-green-400 bg-green-500/10',
  knowledge: 'text-amber-400 bg-amber-500/10',
  behavior: 'text-purple-400 bg-purple-500/10',
};

export function MemoryToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const isRecall = block.tool.includes('recall');
  const isHistory = block.tool.includes('conversation_history');
  const isMemorize = block.tool.includes('memorize');
  const isSyncStatus = block.tool.includes('sync_status');

  // Derive label and icon
  let label: string;
  let Icon = Brain;
  if (isRecall) { label = 'Recall'; Icon = Search; }
  else if (isHistory) { label = 'History'; Icon = BookOpen; }
  else if (isMemorize) { label = 'Memorize'; Icon = Brain; }
  else if (isSyncStatus) { label = 'Sync Status'; Icon = BookOpen; }
  else { label = block.tool.split('__').pop() || block.tool; }

  // Extract summary for collapsed view
  const query = String(block.input.query || block.input.date || block.input.content || '');
  const truncatedQuery = query.length > 60 ? query.slice(0, 60) + '...' : query;

  // Parse result
  const resultText = block.result ? extractText(block.result) : '';
  const memoryItems = (isRecall || isHistory) ? parseMemoryItems(resultText) : [];

  // Count from result text (e.g. "Recalled 3 memories:")
  const countMatch = resultText.match(/(\d+)\s+(memories|items)/);
  const count = countMatch ? countMatch[1] : memoryItems.length > 0 ? String(memoryItems.length) : null;

  return (
    <div className="my-1.5 border border-purple-500/20 rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-purple-400 animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : 'text-purple-400'}`} />
        }
        <span className="text-[13px] font-medium text-purple-300">{label}</span>
        {truncatedQuery && <span className="text-[12px] text-text-dim truncate">{truncatedQuery}</span>}
        {count && !isRunning && (
          <span className="text-[10px] text-purple-400/60 shrink-0">{count} items</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-purple-500/10">
          {/* Memorize: show what was memorized */}
          {isMemorize && query && (
            <div className="px-3 py-2 text-[12px] text-text-secondary">
              <div className="flex items-center gap-1.5 mb-1">
                <Brain size={11} className="text-purple-400" />
                <span className="text-[10px] uppercase tracking-wider text-purple-400/60">Memorized</span>
              </div>
              <p className="leading-relaxed">{String(query)}</p>
              {block.input.memory_type ? (
                <span className={`inline-block mt-1.5 text-[10px] px-1.5 py-0.5 rounded ${TYPE_COLORS[String(block.input.memory_type)] || 'text-text-muted bg-border-subtle'}`}>
                  {String(block.input.memory_type)}
                </span>
              ) : null}
            </div>
          )}

          {/* Recall / History: show parsed memory items */}
          {(isRecall || isHistory) && memoryItems.length > 0 ? (
            <div className="px-3 py-2 space-y-1.5 max-h-80 overflow-y-auto">
              {memoryItems.map((item, i) => (
                <div key={i} className="flex gap-2 text-[12px] leading-relaxed">
                  <span className={`shrink-0 text-[10px] px-1 py-0.5 rounded mt-0.5 ${TYPE_COLORS[item.type] || 'text-text-muted bg-border-subtle'}`}>
                    {item.type}
                  </span>
                  <span className="text-text-secondary">{item.text}</span>
                </div>
              ))}
            </div>
          ) : resultText && !isMemorize ? (
            <pre className={`px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-text-muted'}`}>
              {resultText}
            </pre>
          ) : null}

          {/* Success/error feedback for memorize */}
          {isMemorize && resultText && !block.isError && (
            <div className="px-3 py-1.5 text-[11px] text-green-400/70 border-t border-purple-500/10">
              Saved to memory
            </div>
          )}
          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-purple-500/10">
              {resultText}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> {isRecall || isHistory ? 'Searching...' : 'Saving...'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
