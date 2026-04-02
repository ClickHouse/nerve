import { useEffect, useRef, useState, useCallback } from 'react';
import { Plus, Trash2, Sparkles, HelpCircle, StickyNote } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import type { QuoteAction } from '../../stores/chatStore';

interface ToolbarPosition {
  x: number;
  y: number;
  text: string;
}

const ACTIONS: { action: QuoteAction; icon: typeof Plus; label: string }[] = [
  { action: 'add', icon: Plus, label: 'Add' },
  { action: 'remove', icon: Trash2, label: 'Remove' },
  { action: 'improve', icon: Sparkles, label: 'Improve' },
  { action: 'question', icon: HelpCircle, label: 'Ask' },
  { action: 'note', icon: StickyNote, label: 'Note' },
];

export function SelectionToolbar({ containerRef }: { containerRef: React.RefObject<HTMLDivElement | null> }) {
  const [position, setPosition] = useState<ToolbarPosition | null>(null);
  const toolbarRef = useRef<HTMLDivElement>(null);

  const checkSelection = useCallback(() => {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed || !sel.toString().trim()) {
      setPosition(null);
      return;
    }

    const text = sel.toString().trim();
    const container = containerRef.current;
    if (!container || !text) return;

    const anchorNode = sel.anchorNode;
    if (!anchorNode || !container.contains(anchorNode)) {
      setPosition(null);
      return;
    }

    // Only activate inside assistant messages or plan panel
    const anchorEl = anchorNode instanceof Element ? anchorNode : anchorNode.parentElement;
    if (!anchorEl?.closest('[data-role="assistant"], [data-role="plan"]')) {
      setPosition(null);
      return;
    }

    const range = sel.getRangeAt(0);
    const rect = range.getBoundingClientRect();
    const containerRect = container.getBoundingClientRect();

    setPosition({
      x: Math.round(rect.left + rect.width / 2 - containerRect.left),
      y: Math.round(rect.top - containerRect.top + container.scrollTop - 10),
      text,
    });
  }, [containerRef]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleMouseUp = () => {
      // Wait for browser to finalize selection
      requestAnimationFrame(() => checkSelection());
    };

    const handleMouseDown = (e: MouseEvent) => {
      if (toolbarRef.current && !toolbarRef.current.contains(e.target as Node)) {
        setPosition(null);
      }
    };

    const handleScroll = () => setPosition(null);

    container.addEventListener('mouseup', handleMouseUp);
    document.addEventListener('mousedown', handleMouseDown);
    container.addEventListener('scroll', handleScroll, { passive: true });

    return () => {
      container.removeEventListener('mouseup', handleMouseUp);
      document.removeEventListener('mousedown', handleMouseDown);
      container.removeEventListener('scroll', handleScroll);
    };
  }, [containerRef, checkSelection]);

  const handleAction = (action: QuoteAction) => {
    if (!position) return;
    useChatStore.getState().addQuote(position.text, action);
    window.getSelection()?.removeAllRanges();
    setPosition(null);
  };

  if (!position) return null;

  return (
    <div
      ref={toolbarRef}
      className="selection-toolbar absolute z-50"
      style={{
        left: `${position.x}px`,
        top: `${position.y}px`,
        transform: 'translate(-50%, -100%)',
      }}
    >
      <div className="flex items-center bg-surface-raised border border-border rounded-lg shadow-xl shadow-black/50 overflow-hidden">
        {ACTIONS.map(({ action, icon: Icon, label }) => (
          <button
            key={action}
            onClick={() => handleAction(action)}
            title={label}
            className="flex items-center gap-1.5 px-3 py-2 text-[12px] text-text-muted hover:text-white hover:bg-surface-hover transition-colors cursor-pointer border-r border-border last:border-r-0"
          >
            <Icon size={13} />
            <span>{label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
