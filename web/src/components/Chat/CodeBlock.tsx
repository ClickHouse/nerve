import { useState, useRef, type ReactNode } from 'react';
import { Copy, Check } from 'lucide-react';

export function CodeBlock({ className, children }: { className?: string; children: ReactNode }) {
  const [copied, setCopied] = useState(false);
  const codeRef = useRef<HTMLElement>(null);
  const language = className?.replace(/^.*?language-/, '').replace(/\s.*$/, '') || '';

  const handleCopy = () => {
    const text = codeRef.current?.textContent || '';
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-2">
      <div className="flex items-center justify-between bg-surface-raised border border-border rounded-t-md px-3 py-1">
        <span className="text-[11px] text-text-dim font-mono">{language}</span>
        <button
          onClick={handleCopy}
          className="text-text-dim hover:text-text-muted cursor-pointer p-1"
          title="Copy"
        >
          {copied ? <Check size={14} className="text-emerald-400" /> : <Copy size={14} />}
        </button>
      </div>
      <pre className="!mt-0 !rounded-t-none !border-t-0">
        <code ref={codeRef} className={className}>{children}</code>
      </pre>
    </div>
  );
}
