import { useState } from 'react';
import { ChevronRight, ChevronDown, Brain } from '../ui/icons';
import { Button } from '../ui';

export function ThinkingBlock({ content, streaming }: { content: string; streaming?: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const preview = content.split('\n')[0].slice(0, 100);

  return (
    <div className="my-2 border-l-2 border-border-subtle bg-bg-sunken rounded-r-md">
      <Button
        variant="subtle"
        size="md"
        fullWidth
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
        className="justify-start text-left rounded-l-none rounded-r-md hover:bg-surface-hover"
      >
        <Brain size={14} className="text-accent shrink-0" />
        {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        <span className="text-sm leading-tight text-text-muted italic truncate">
          {expanded ? 'Thinking' : preview || 'Thinking...'}
        </span>
        {streaming && <span className="streaming-cursor inline-block w-1.5 h-3.5 bg-accent ml-1 shrink-0" />}
      </Button>
      {expanded && (
        <div className="px-4 pb-3 text-sm text-text-muted italic whitespace-pre-wrap leading-relaxed">
          {content}
          {streaming && <span className="streaming-cursor inline-block w-1.5 h-3.5 bg-accent ml-0.5 align-text-bottom" />}
        </div>
      )}
    </div>
  );
}
