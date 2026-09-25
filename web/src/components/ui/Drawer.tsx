import type { ReactNode } from 'react';
import { useModalSurface } from '../../hooks/useModalSurface';
import { safeAreaInsets } from '../../utils/safeArea';

/**
 * Off-canvas panel over a tap-to-dismiss scrim.
 *
 * Stays mounted while closed so the slide has something to animate, and
 * carries `inert` in that state so a panel parked off-screen cannot be
 * reached by tab or read by a screen reader.
 *
 * While open it is a modal, and `useModalSurface` gives it the matching
 * keyboard contract: focus moves in, Tab cycles inside instead of walking onto
 * the page behind it, Escape closes it — claimed here, so it stops short of the
 * global Escape shortcut that halts a streaming response — and focus goes back
 * to whatever opened it. Callers should still put a visible close control in
 * their header; Escape alone is not discoverable.
 *
 * `side` distinguishes the two jobs navigation does on a phone: `left` for
 * "which item within this section" (the chat session list), `right` for
 * "which section of the app" (the nav overflow behind More).
 *
 * **Why this is not Click UI's `Flyout`.** Four reasons, and each is one of the
 * behaviours above:
 *
 * 1. `Flyout` defaults to `modal={false}` — a non-modal Radix dialog, so no
 *    focus trap, no `aria-modal`, and nothing stopping Tab from walking onto
 *    the transcript behind it. On a phone this surface *is* covering the page.
 * 2. It unmounts its content when closed. The slide has to animate in both
 *    directions, which is why this one stays mounted and parked off-screen.
 * 3. Radix dismisses on Escape with `preventDefault()` and nothing more, so the
 *    global Escape shortcut ("stop the agent") would still fire behind it.
 *    `useModalSurface` claims the key with `stopPropagation()`.
 * 4. Its sizes are the desktop side-panel set (`narrow`/`wide`/`widest`) over a
 *    relative or absolute positioning strategy — not a 85vw-capped-at-320px
 *    sheet pinned to the viewport, and nothing in it pays the safe-area inset
 *    that a viewport-pinned surface owes.
 *
 * `Flyout` is the right component for a desktop detail panel. This is a phone
 * sheet. Click UI reaches it through the surface and border tokens below.
 */
export function Drawer({ open, onClose, side = 'left', label, children }: {
  open: boolean;
  onClose: () => void;
  side?: 'left' | 'right';
  /** Accessible name for the panel — it is a dialog with no visible title. */
  label: string;
  children: ReactNode;
}) {
  const closedTransform = side === 'left' ? '-translate-x-full' : 'translate-x-full';
  const { dialogProps } = useModalSurface<HTMLDivElement>(open, onClose);

  return (
    <>
      {open && (
        <div
          onClick={onClose}
          className="fixed inset-0 z-40 bg-black/60 transition-opacity duration-200"
          aria-hidden="true"
        />
      )}
      <div
        {...dialogProps}
        aria-label={label}
        inert={open ? undefined : true}
        className={`fixed inset-y-0 z-50 flex w-[85vw] max-w-[320px] flex-col overflow-hidden bg-surface outline-none transition-transform duration-200
          ${side === 'left' ? 'left-0 border-r' : 'right-0 border-l'} border-border-subtle
          ${open ? 'translate-x-0' : closedTransform}`}
        // Fixed, so the shell's safe-area padding does not reach it — including
        // the side inset for the edge it is anchored to.
        style={safeAreaInsets(side)}
      >
        {children}
      </div>
    </>
  );
}
