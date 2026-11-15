import { useEffect, useState, type ReactNode } from 'react';
import { copyToClipboard } from '../../../utils/clipboard';
import { IconButton } from '../../ui';
import { Check, Copy } from '../../ui/icons';
import { highlightWidgetSource } from './highlight';

/**
 * The source of a widget as a code block: what a ```widget block is when it
 * is not drawn (the feature is off, the sandbox is out of reach, the source
 * is too large), and what its Source button shows.
 *
 * It looks like the chat's other code blocks: the language in a bar with a
 * Copy button, the text below, highlighted as HTML once the highlighter has
 * loaded (`highlight.tsx`). The source is text here, never markup.
 */
export function WidgetSource({ source, note, warning = false, actions }: {
  source: string;
  /** One quiet line after the label: why the widget is not drawn. */
  note?: ReactNode;
  /** The note is about something that went wrong. */
  warning?: boolean;
  /** Controls before Copy: back to the widget, try again. */
  actions?: ReactNode;
}) {
  const [copied, setCopied] = useState(false);
  const [highlighted, setHighlighted] = useState<{ source: string; nodes: ReactNode[] } | null>(null);

  useEffect(() => {
    let cancelled = false;
    void highlightWidgetSource(source).then((nodes) => {
      if (!cancelled && nodes) setHighlighted({ source, nodes });
    });
    return () => {
      cancelled = true;
    };
  }, [source]);

  const handleCopy = async () => {
    if (!(await copyToClipboard(source))) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <>
      <div className="rich-source-bar flex items-center justify-between gap-2 bg-surface-raised border border-border rounded-t-md px-3 py-1">
        <span className="min-w-0 text-xs text-text-dim font-mono truncate">
          widget
          {note && (
            <span role="status" className={`ml-2 font-sans ${warning ? 'text-warning' : 'text-text-faint'}`}>{note}</span>
          )}
        </span>
        <span className="flex shrink-0 items-center gap-1">
          {actions}
          <IconButton label={copied ? 'Copied' : 'Copy source'} size="xs" onClick={handleCopy}>
            {copied ? <Check size={14} className="text-hue-emerald" /> : <Copy size={14} />}
          </IconButton>
        </span>
      </div>
      {/* The `!` overrides graft the header above onto `.markdown-content pre`. */}
      <pre className="!mt-0 !rounded-t-none !border-t-0">
        <code className="hljs language-html">
          {highlighted && highlighted.source === source ? highlighted.nodes : source}
        </code>
      </pre>
    </>
  );
}
