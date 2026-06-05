import { useEffect, useRef, useState, useCallback } from 'react';
import { Plus, Trash2, Sparkles, HelpCircle, StickyNote } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import type { QuoteAction } from '../../stores/chatStore';

interface ToolbarPosition {
  x: number;
  y: number;
  text: string;
}

interface FoundSelection {
  text: string;
  rect: DOMRect;
}

const QUOTE_SCOPE = '[data-role="assistant"], [data-role="plan"]';

function readSelection(sel: Selection | null): { text: string; range: Range } | null {
  if (!sel || sel.isCollapsed || sel.rangeCount === 0) return null;
  const text = sel.toString().trim();
  if (!text) return null;
  return { text, range: sel.getRangeAt(0) };
}

/**
 * Resolve the active text selection, looking in both the light DOM (chat /
 * plan text) and inside any `<diffs-container>` shadow roots. @pierre/diffs
 * renders the diff into a shadow root whose selection is not surfaced through
 * `window.getSelection()` in Chromium, so we read it via the non-standard
 * `ShadowRoot.getSelection()` and fall back gracefully where it's absent.
 */
function findSelection(container: HTMLElement): FoundSelection | null {
  // 1) Light DOM selection.
  const docSel = window.getSelection();
  const light = readSelection(docSel);
  if (light) {
    const anchor = docSel!.anchorNode;
    const anchorEl = anchor instanceof Element ? anchor : anchor?.parentElement ?? null;
    if (anchor && container.contains(anchor) && anchorEl?.closest(QUOTE_SCOPE)) {
      return { text: light.text, rect: light.range.getBoundingClientRect() };
    }
  }

  // 2) Shadow DOM selections (syntax-highlighted diffs).
  const hosts = container.querySelectorAll<HTMLElement>('diffs-container');
  for (const host of hosts) {
    if (!host.closest(QUOTE_SCOPE)) continue;
    const root = host.shadowRoot as (ShadowRoot & { getSelection?: () => Selection | null }) | null;
    const found = readSelection(root?.getSelection?.() ?? null);
    if (found) return { text: found.text, rect: found.range.getBoundingClientRect() };
  }

  return null;
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
    const container = containerRef.current;
    if (!container) {
      setPosition(null);
      return;
    }

    const found = findSelection(container);
    if (!found) {
      setPosition(null);
      return;
    }

    const { text, rect } = found;
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
