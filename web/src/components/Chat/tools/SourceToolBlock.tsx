import { useState } from 'react';
import { ChevronRight, ChevronDown, Inbox, Radio, BookOpen, Loader2, Mail, Github, MessageCircle } from 'lucide-react';
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

function sourceIcon(source: string) {
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return <Mail size={12} className="text-red-400" />;
    case 'github': return <Github size={12} className="text-purple-400" />;
    case 'telegram': return <MessageCircle size={12} className="text-blue-400" />;
    default: return <Inbox size={12} className="text-text-dim" />;
  }
}

/** Parse source list output into structured entries. */
interface SourceEntry {
  name: string;
  messageCount?: string;
  unread?: string;
  details: string;
}

function parseSourceList(text: string): SourceEntry[] {
  const entries: SourceEntry[] = [];
  for (const line of text.split('\n')) {
    const match = line.match(/^-\s+\*\*([^*]+)\*\*:\s*(.+)/);
    if (match) {
      const name = match[1];
      const rest = match[2];
      const msgMatch = rest.match(/(\d+)\s+messages/);
      const unreadMatch = rest.match(/\*\*\w+\*\*:\s*(\d+)\s+unread/);
      entries.push({
        name,
        messageCount: msgMatch?.[1],
        unread: unreadMatch?.[1],
        details: rest,
      });
    }
  }
  return entries;
}

/** Parse message records from poll/read output. */
interface SourceMessage {
  index: string;
  source: string;
  summary: string;
  type: string;
  time: string;
  relativeTime: string;
  seq: string;
  metadata?: string;
  content: string;
}

function parseSourceMessages(text: string): { messages: SourceMessage[]; messageCount: number } {
  const messages: SourceMessage[] = [];
  const sections = text.split(/^### \[/m).filter(Boolean);

  for (const section of sections) {
    const full = '### [' + section;
    const headerMatch = full.match(/^### \[(\d+\/\d+)\] ([^:]+):\s*(.+)/);
    if (!headerMatch) continue;

    const lines = full.split('\n');
    const typeMatch = lines[1]?.match(/\*\*Type:\*\*\s*(\S+)\s*\|\s*\*\*Time:\*\*\s*([^\s(]+)\s*\(([^)]+)\)\s*\|\s*\*\*seq:\*\*\s*(\S+)/);
    const metaMatch = lines.find(l => l.startsWith('**Metadata:**'));

    // Content is everything after the header lines, trimmed
    const contentStart = lines.findIndex((l, i) => i > 1 && !l.startsWith('**'));
    const contentLines = contentStart >= 0
      ? lines.slice(contentStart).join('\n').replace(/\n---\s*$/, '').trim()
      : '';

    messages.push({
      index: headerMatch[1],
      source: headerMatch[2].trim(),
      summary: headerMatch[3].trim(),
      type: typeMatch?.[1] || '',
      time: typeMatch?.[2] || '',
      relativeTime: typeMatch?.[3] || '',
      seq: typeMatch?.[4] || '',
      metadata: metaMatch?.replace('**Metadata:** ', ''),
      content: contentLines,
    });
  }

  const countMatch = text.match(/^## (\d+) message/m);
  return { messages, messageCount: countMatch ? parseInt(countMatch[1]) : messages.length };
}

export function SourceToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const isList = block.tool.includes('list_sources');
  const isPoll = block.tool.includes('poll_source') || block.tool.includes('poll_all');
  const isRead = block.tool.includes('read_source');

  let label: string;
  let Icon = Inbox;
  if (isList) { label = 'Sources'; Icon = Inbox; }
  else if (isPoll) { label = 'Poll'; Icon = Radio; }
  else { label = 'Browse'; Icon = BookOpen; }

  const source = String(block.input.source || block.input.consumer || '');
  const consumer = String(block.input.consumer || '');

  // Parse result
  const resultText = block.result ? extractText(block.result) : '';
  const isNoMessages = resultText.includes('No new messages') || resultText.includes('No messages found');

  // Parse structured data
  const sourceEntries = isList ? parseSourceList(resultText) : [];
  const { messages: parsedMessages, messageCount } = (isPoll || isRead) ? parseSourceMessages(resultText) : { messages: [], messageCount: 0 };

  // Build summary for collapsed view
  let summary = '';
  if (isList) {
    summary = consumer ? `consumer="${consumer}"` : '';
    if (sourceEntries.length > 0) {
      const totalUnread = sourceEntries.reduce((sum, e) => sum + (parseInt(e.unread || '0') || 0), 0);
      if (totalUnread > 0) summary += ` · ${totalUnread} unread`;
      else if (!isRunning && block.result) summary += ' · all caught up';
    }
  } else if (isPoll) {
    if (isNoMessages) summary = source ? `${source} · no new` : 'no new messages';
    else if (messageCount > 0) summary = source ? `${source} · ${messageCount} new` : `${messageCount} new`;
    else summary = source || consumer;
  } else if (isRead) {
    summary = source;
  }

  return (
    <div className="my-1.5 border border-cyan-500/20 rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-cyan-400 animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : 'text-cyan-400'}`} />
        }
        <span className="text-[13px] font-medium text-cyan-300">{label}</span>
        {summary && <span className="text-[12px] text-text-dim truncate">{summary}</span>}
        {(isPoll || isRead) && messageCount > 0 && !isRunning && (
          <span className="text-[10px] text-cyan-400/60 shrink-0">{messageCount} msg</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-cyan-500/10">
          {/* list_sources: structured source list */}
          {isList && sourceEntries.length > 0 && (
            <div className="px-3 py-2 space-y-1">
              {sourceEntries.map((entry, i) => (
                <div key={i} className="flex items-center gap-2 text-[12px]">
                  {sourceIcon(entry.name)}
                  <span className="text-text-secondary font-mono">{entry.name}</span>
                  {entry.messageCount && <span className="text-text-dim">{entry.messageCount} msgs</span>}
                  {entry.unread && parseInt(entry.unread) > 0 && (
                    <span className="text-amber-400 font-medium">{entry.unread} unread</span>
                  )}
                  {entry.unread === '0' && (
                    <span className="text-text-faint">0 unread</span>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* poll/read: message list */}
          {(isPoll || isRead) && parsedMessages.length > 0 && (
            <div className="max-h-96 overflow-y-auto">
              {parsedMessages.map((msg, i) => (
                <div key={i} className="px-3 py-2 border-t border-surface-raised first:border-t-0">
                  <div className="flex items-center gap-2 mb-1">
                    {sourceIcon(msg.source)}
                    <span className="text-[12px] text-text-secondary font-medium truncate flex-1">{msg.summary}</span>
                    <span className="text-[10px] text-text-dim shrink-0">{msg.relativeTime}</span>
                  </div>
                  <div className="text-[11px] text-text-faint flex items-center gap-2 mb-1">
                    <span>{msg.type}</span>
                    <span>seq:{msg.seq}</span>
                    {msg.time && <span>{msg.time}</span>}
                  </div>
                  {msg.content && (
                    <pre className="text-[11px] text-[#999] whitespace-pre-wrap leading-relaxed max-h-32 overflow-y-auto">
                      {msg.content.length > 500 ? msg.content.slice(0, 500) + '...' : msg.content}
                    </pre>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* No messages state */}
          {isNoMessages && !isList && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Inbox size={12} className="text-text-faint" /> No new messages
            </div>
          )}

          {/* Fallback: raw text for unparsed results */}
          {!isList && parsedMessages.length === 0 && !isNoMessages && resultText && (
            <pre className={`px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto ${block.isError ? 'text-red-400' : 'text-[#999]'}`}>
              {resultText}
            </pre>
          )}

          {/* list_sources fallback */}
          {isList && sourceEntries.length === 0 && resultText && (
            <pre className="px-3 py-2 text-[12px] text-[#999] whitespace-pre-wrap max-h-60 overflow-y-auto">
              {resultText}
            </pre>
          )}

          {/* Error */}
          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-cyan-500/10">
              {resultText}
            </pre>
          )}

          {/* Running state */}
          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> {isPoll ? 'Polling...' : isList ? 'Loading sources...' : 'Browsing...'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
