import type { ReactNode } from 'react';
import { useModalSurface } from '../../hooks/useModalSurface';

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
        // The panel spans the full height, so it owns both insets itself.
        style={{
          paddingTop: 'env(safe-area-inset-top)',
          paddingBottom: 'env(safe-area-inset-bottom)',
        }}
      >
        {children}
      </div>
    </>
  );
}
