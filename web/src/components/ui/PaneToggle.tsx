import { PanelLeftClose, PanelLeftOpen } from './icons';
import { IconButton } from './IconButton';

/**
 * Opens/closes a page's side pane once it has collapsed into a drawer.
 *
 * Same icon pair and position as the chat header's sidebar toggle, so the
 * control for "show me the list" is in the same corner on every page that
 * has a list.
 *
 * The pair is Click UI's `slide-out` plus its mirror image, drawn in
 * `icons.tsx`: Click UI ships `slide-in`/`slide-out` with the bar on opposite
 * edges, which would have made the panel edge jump sides as the toggle flipped.
 */
export function PaneToggle({ open, onToggle, label }: {
  open: boolean;
  onToggle: () => void;
  /** Names the pane, e.g. "job list" — used for the title/aria-label. */
  label: string;
}) {
  const action = `${open ? 'Hide' : 'Show'} ${label}`;
  return (
    <IconButton
      label={action}
      onClick={onToggle}
      aria-expanded={open}
      // Pulls the button's own box back out of the header's leading gap, so the
      // glyph lines up with the title beside it rather than the button's edge.
      className="-ml-1"
    >
      {open ? <PanelLeftClose size={16} /> : <PanelLeftOpen size={16} />}
    </IconButton>
  );
}
