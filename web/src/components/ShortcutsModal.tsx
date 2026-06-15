import { useEffect } from 'react';
import { X } from 'lucide-react';
import { useUIStore } from '../stores/uiStore';
import { formatCombo, type ShortcutCombo } from '../utils/keyboard';

interface DisplayShortcut {
  combo: ShortcutCombo;
  description: string;
}

interface Section {
  title: string;
  items: DisplayShortcut[];
}

/**
 * Static display of every keyboard binding. The runtime handlers live in
 * App.tsx (global) and ChatPage.tsx (chat-scoped) — keep this list in sync
 * with those when bindings change.
 */
const SECTIONS: Section[] = [
  {
    title: 'General',
    items: [
      { combo: { mod: true, shift: true, key: 'o' }, description: 'New chat' },
      { combo: { mod: true, key: 'k' }, description: 'Focus session search' },
      { combo: { mod: true, key: '/' }, description: 'Show keyboard shortcuts' },
      { combo: { key: 'Escape' }, description: 'Close dialog · clear search · stop generation' },
    ],
  },
  {
    title: 'Chat',
    items: [
      { combo: { mod: true, shift: true, key: 's' }, description: 'Toggle session sidebar' },
      { combo: { mod: true, shift: true, key: ';' }, description: 'Focus message input' },
      { combo: { mod: true, shift: true, key: 'c' }, description: 'Copy last response' },
      { combo: { mod: true, shift: true, key: 'Backspace' }, description: 'Delete current conversation' },
      { combo: { mod: true, key: '\\' }, description: 'Toggle side panel' },
    ],
  },
  {
    title: 'Message input',
    items: [
      { combo: { key: 'Enter' }, description: 'Send message' },
      { combo: { shift: true, key: 'Enter' }, description: 'New line' },
    ],
  },
];

export function ShortcutsModal() {
  const open = useUIStore((s) => s.shortcutsModalOpen);
  const close = useUIStore((s) => s.closeShortcutsModal);

  // Local Esc handler — runs *before* the document-level shortcut listeners
  // because modal mount captures it first when focus is inside.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        close();
      }
    };
    document.addEventListener('keydown', onKey, true); // capture phase = wins
    return () => document.removeEventListener('keydown', onKey, true);
  }, [open, close]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 bg-black/60 flex items-center justify-center z-50"
      onClick={close}
    >
      <div
        className="bg-surface-raised border border-border-subtle rounded-xl w-[520px] max-w-[90vw] max-h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h2 className="text-[15px] font-semibold">Keyboard shortcuts</h2>
          <button
            onClick={close}
            className="text-text-faint hover:text-text-muted cursor-pointer p-1"
            title="Close"
          >
            <X size={18} />
          </button>
        </div>

        <div className="overflow-y-auto p-5 space-y-5">
          {SECTIONS.map((section) => (
            <div key={section.title}>
              <h3 className="text-[11px] uppercase tracking-wider text-text-faint font-medium mb-2">
                {section.title}
              </h3>
              <div className="space-y-1.5">
                {section.items.map((item, idx) => (
                  <div
                    key={idx}
                    className="flex items-center justify-between gap-4 py-1"
                  >
                    <span className="text-[13px] text-text-secondary">{item.description}</span>
                    <Kbd combo={item.combo} />
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Kbd({ combo }: { combo: ShortcutCombo }) {
  return (
    <kbd className="px-2 py-1 text-[11px] font-mono text-text-secondary bg-surface border border-border-subtle rounded shrink-0 tabular-nums">
      {formatCombo(combo)}
    </kbd>
  );
}
