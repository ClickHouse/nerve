import { useUIStore } from '../stores/uiStore';
import { formatCombo, type ShortcutCombo } from '../utils/keyboard';
import { Modal } from './ui/Modal';

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
  {
    title: 'Tasks',
    items: [
      { combo: { key: 'b' }, description: 'Board view' },
      { combo: { key: 'l' }, description: 'List view' },
      { combo: { key: 'n' }, description: 'New task' },
      { combo: { key: '/' }, description: 'Focus task search' },
      { combo: { key: 'Space' }, description: 'Pick up / drop a focused card' },
      { combo: { key: 'ArrowUp' }, description: 'Move a picked-up card (with arrows)' },
    ],
  },
];

export function ShortcutsModal() {
  const open = useUIStore((s) => s.shortcutsModalOpen);
  const close = useUIStore((s) => s.closeShortcutsModal);

  // The capture-phase Escape handling this component used to own is now
  // the Modal's job, along with the focus trap it never had.
  return (
    <Modal open={open} onClose={close} title="Keyboard shortcuts" size="lg">
      <div className="p-5 space-y-5">
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
    </Modal>
  );
}

function Kbd({ combo }: { combo: ShortcutCombo }) {
  return (
    <kbd className="px-2 py-1 text-[11px] font-mono text-text-secondary bg-surface border border-border-subtle rounded shrink-0 tabular-nums">
      {formatCombo(combo)}
    </kbd>
  );
}
