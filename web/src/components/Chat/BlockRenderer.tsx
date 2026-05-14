import { useMemo } from 'react';
import { Download, FileText } from 'lucide-react';
import type { MessageBlock } from '../../types/chat';
import { ThinkingBlock } from './ThinkingBlock';
import { ToolCallBlock } from './ToolCallBlock';
import { ToolCallGroupBlock } from './ToolCallGroupBlock';
import { MarkdownContent } from './MarkdownContent';
import { groupToolCalls } from '../../utils/groupToolCalls';
import { getToken } from '../../api/client';

interface BlockRendererProps {
  blocks: MessageBlock[];
  /** Show streaming cursor on last block + streaming prop on last ThinkingBlock. */
  streaming?: boolean;
  /** Tailwind bg class for text cursor (default: 'bg-accent'). */
  cursorColor?: string;
  /** Optional wrapper class for text blocks (e.g. 'text-[13px] my-1'). */
  textClassName?: string;
}

export function BlockRenderer({
  blocks,
  streaming = false,
  cursorColor = 'bg-accent',
  textClassName,
}: BlockRendererProps) {
  const renderItems = useMemo(() => groupToolCalls(blocks), [blocks]);

  return (
    <>
      {renderItems.map((item, i) => {
        const isLast = streaming && i === renderItems.length - 1;

        switch (item.type) {
          case 'thinking':
            return <ThinkingBlock key={i} content={item.content} streaming={isLast} />;
          case 'tool_call':
            return <ToolCallBlock key={i} block={item} />;
          case 'tool_call_group':
            return <ToolCallGroupBlock key={i} group={item} />;
          case 'text': {
            const inner = (
              <>
                <MarkdownContent content={item.content} />
                {isLast && (
                  <span
                    className={`streaming-cursor inline-block w-1.5 h-4 ${cursorColor} ml-0.5 align-text-bottom`}
                  />
                )}
              </>
            );
            return textClassName ? (
              <div key={i} className={textClassName}>{inner}</div>
            ) : (
              <div key={i}>{inner}</div>
            );
          }
          case 'image': {
            const imgUrl = `${item.url}${item.url.includes('?') ? '&' : '?'}token=${getToken()}`;
            return (
              <div key={i} className="my-2">
                <a href={imgUrl} target="_blank" rel="noopener noreferrer" className="inline-block rounded-lg overflow-hidden border border-border hover:border-accent/50 transition-colors">
                  <img src={imgUrl} alt={item.filename} className="max-w-[300px] max-h-[300px] object-contain" />
                </a>
              </div>
            );
          }
          case 'file':
            return (
              <div key={i} className="my-2">
                <a
                  href={`${item.url}${item.url.includes('?') ? '&' : '?'}token=${getToken()}`}
                  download={item.filename}
                  className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-surface hover:bg-surface-hover transition-colors text-sm text-text-secondary"
                >
                  <FileText size={14} />
                  <span className="truncate max-w-[200px]">{item.filename}</span>
                  {item.size != null && <span className="text-text-muted text-xs">({(item.size / 1024).toFixed(1)}KB)</span>}
                  <Download size={12} className="text-text-muted" />
                </a>
              </div>
            );
          default:
            return null;
        }
      })}
    </>
  );
}
