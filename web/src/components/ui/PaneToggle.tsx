import { PanelLeftOpen, PanelLeftClose } from 'lucide-react';

/**
 * Opens/closes a page's side pane once it has collapsed into a drawer.
 *
 * Same icon pair and position as the chat header's sidebar toggle, so the
 * control for "show me the list" is in the same corner on every page that
 * has a list.
 */
export function PaneToggle({ open, onToggle, label }: {
  open: boolean;
  onToggle: () => void;
  /** Names the pane, e.g. "job list" — used for the title/aria-label. */
  label: string;
}) {
  const action = `${open ? 'Hide' : 'Show'} ${label}`;
  return (
    <button
      onClick={onToggle}
      title={action}
      aria-label={action}
      aria-expanded={open}
      className="w-8 h-8 -ml-1 shrink-0 flex items-center justify-center rounded text-text-faint hover:text-text-muted hover:bg-surface-raised cursor-pointer transition-colors"
    >
      {open ? <PanelLeftClose size={16} /> : <PanelLeftOpen size={16} />}
    </button>
  );
}
