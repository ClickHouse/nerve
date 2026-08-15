import { useState, useRef, type ReactNode } from 'react';
import { Copy, Check } from '../ui/icons';
import { IconButton } from '../ui';
import { copyToClipboard } from '../../utils/clipboard';

export function CodeBlock({ className, children }: { className?: string; children: ReactNode }) {
  const [copied, setCopied] = useState(false);
  const codeRef = useRef<HTMLElement>(null);
  const language = className?.replace(/^.*?language-/, '').replace(/\s.*$/, '') || '';

  const handleCopy = async () => {
    const text = codeRef.current?.textContent || '';
    const ok = await copyToClipboard(text);
    if (!ok) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-2">
      <div className="flex items-center justify-between bg-surface-raised border border-border rounded-t-md px-3 py-1">
        <span className="text-xs text-text-dim font-mono">{language}</span>
        <IconButton label={copied ? 'Copied' : 'Copy'} size="xs" onClick={handleCopy}>
          {copied ? <Check size={14} className="text-hue-emerald" /> : <Copy size={14} />}
        </IconButton>
      </div>
      {/* The `!` overrides graft this header onto the `.markdown-content pre`
          rule in index.css — load-bearing, do not drop them. */}
      <pre className="!mt-0 !rounded-t-none !border-t-0">
        <code ref={codeRef} className={className}>{children}</code>
      </pre>
    </div>
  );
}
