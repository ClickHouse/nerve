import { useCallback, useEffect, useId, useRef, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';

/**
 * The app's one dialog primitive.
 *
 * Five dialogs were hand-rolled before this: each re-implemented the
 * backdrop, one handled Escape, none trapped focus, none locked body
 * scroll, and none carried dialog semantics for a screen reader. Behaviour
 * that every dialog needs belongs in one place.
 *
 * Three details are load-bearing:
 *
 * **Escape is captured, not bubbled.** The app installs document-level
 * shortcut handlers (App.tsx, ChatPage.tsx) that also claim Escape — to
 * clear a search box, to stop generation. A bubble-phase listener here
 * would fire *after* them, so closing a dialog could also stop a running
 * generation behind it. Capture phase runs outermost-first, so the dialog
 * sees the key and stops it before anything else does.
 *
 * **Only the topmost dialog reacts.** Every open Modal has a capture-phase
 * listener, so without the stack below, one Escape would close a
 * confirmation *and* the dialog that raised it.
 *
 * **Focus is trapped and restored.** Tab must not walk into the page
 * behind the backdrop, and dismissing a dialog has to put focus back where
 * the user left it, or keyboard navigation restarts from the top of the
 * document.
 */

/**
 * Open dialogs, oldest first. Module-level rather than context so a dialog
 * rendered anywhere in the tree participates without a provider.
 */
const modalStack: string[] = [];

/**
 * Body scroll lock, refcounted so stacked dialogs don't unlock early.
 *
 * The count is deliberately its own thing rather than a read of
 * `modalStack.length`. React runs effect cleanups in declaration order, so
 * this cleanup fires while the dialog is still in the stack — reading the
 * stack here would see a length of 1 on the last dialog out and never
 * restore scrolling at all.
 */
let scrollLockCount = 0;
let scrollLockPrevious = '';

function useScrollLock(active: boolean) {
  useEffect(() => {
    if (!active) return;
    if (scrollLockCount === 0) {
      scrollLockPrevious = document.body.style.overflow;
      document.body.style.overflow = 'hidden';
    }
    scrollLockCount += 1;
    return () => {
      scrollLockCount -= 1;
      // Only the last dialog out restores the original value — an inner
      // dialog closing must not re-enable scrolling under an outer one.
      if (scrollLockCount === 0) {
        document.body.style.overflow = scrollLockPrevious;
      }
    };
  }, [active]);
}

const FOCUSABLE = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

export type ModalSize = 'sm' | 'md' | 'lg' | 'xl';

const SIZES: Record<ModalSize, string> = {
  sm: 'w-[360px] max-w-[90vw]',
  md: 'w-[480px] max-w-[90vw]',
  lg: 'w-[520px] max-w-[90vw]',
  xl: 'w-[620px] max-w-[92vw]',
};

export interface ModalProps {
  open: boolean;
  onClose: () => void;
  /** Renders the header bar with a close button. Omit for a bare panel. */
  title?: ReactNode;
  size?: ModalSize;
  children: ReactNode;
  /** Pinned below the scrolling body, for action buttons. */
  footer?: ReactNode;
  /**
   * Layout for the footer region. Defaults to a right-aligned button row;
   * override for footers that are a form rather than a set of actions.
   * The separator and `shrink-0` are always applied.
   */
  footerClassName?: string;
  /** Set false for dialogs where a stray click shouldn't discard input. */
  closeOnBackdrop?: boolean;
  /** Extra classes on the panel (e.g. a taller max-height). */
  className?: string;
  /** Accessible name when there's no visible `title`. */
  ariaLabel?: string;
}

export function Modal({
  open,
  onClose,
  title,
  size = 'md',
  children,
  footer,
  footerClassName = 'px-5 py-3 flex justify-end gap-2',
  closeOnBackdrop = true,
  className = '',
  ariaLabel,
}: ModalProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const restoreFocusRef = useRef<HTMLElement | null>(null);
  const titleId = useId();
  const id = useId();

  useScrollLock(open);

  // Register in the stack for the lifetime of the open state, so the
  // Escape handler and the scroll lock both know who is on top.
  useEffect(() => {
    if (!open) return;
    modalStack.push(id);
    return () => {
      const at = modalStack.indexOf(id);
      if (at !== -1) modalStack.splice(at, 1);
    };
  }, [open, id]);

  const isTopmost = useCallback(
    () => modalStack[modalStack.length - 1] === id,
    [id],
  );

  // Escape + focus trap. Capture phase so this beats the global handlers.
  useEffect(() => {
    if (!open) return;

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (!isTopmost()) return;
        e.preventDefault();
        e.stopPropagation();
        onClose();
        return;
      }

      if (e.key !== 'Tab' || !isTopmost()) return;
      const panel = panelRef.current;
      if (!panel) return;

      const focusable = Array.from(
        panel.querySelectorAll<HTMLElement>(FOCUSABLE),
      ).filter((el) => el.offsetParent !== null || el === document.activeElement);
      if (focusable.length === 0) {
        // Nothing tabbable inside — keep focus on the panel rather than
        // letting Tab escape to the page behind the backdrop.
        e.preventDefault();
        panel.focus();
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement as HTMLElement | null;

      // Wrap at both ends, and pull focus back in if it has drifted out
      // (which happens when the previously focused node is unmounted).
      if (e.shiftKey && (active === first || !panel.contains(active))) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && (active === last || !panel.contains(active))) {
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', onKeyDown, true);
    return () => document.removeEventListener('keydown', onKeyDown, true);
  }, [open, onClose, isTopmost]);

  // Move focus in on open, and put it back on close.
  useEffect(() => {
    if (!open) return;
    restoreFocusRef.current = document.activeElement as HTMLElement | null;

    // After paint, so children that autoFocus have already claimed it.
    const raf = requestAnimationFrame(() => {
      const panel = panelRef.current;
      if (!panel || panel.contains(document.activeElement)) return;
      const target = panel.querySelector<HTMLElement>(FOCUSABLE);
      (target ?? panel).focus();
    });

    return () => {
      cancelAnimationFrame(raf);
      restoreFocusRef.current?.focus?.();
    };
  }, [open]);

  if (!open) return null;

  return createPortal(
    <div
      className="modal-backdrop fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4"
      onMouseDown={(e) => {
        // mousedown, not click: a click fires on the backdrop when a drag
        // that *started* inside the panel (selecting text, dragging a
        // slider) is released outside it, which would discard the dialog.
        if (closeOnBackdrop && e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? titleId : undefined}
        aria-label={title ? undefined : ariaLabel}
        tabIndex={-1}
        className={`modal-panel bg-surface-raised border border-border-subtle rounded-xl flex flex-col max-h-[85vh] outline-none ${SIZES[size]} ${className}`}
      >
        {title && (
          <div className="flex items-center justify-between px-5 py-3 border-b border-border shrink-0">
            <h2 id={titleId} className="text-[15px] font-semibold">
              {title}
            </h2>
            <button
              onClick={onClose}
              className="text-text-faint hover:text-text-muted cursor-pointer p-1"
              aria-label="Close dialog"
              title="Close"
            >
              <X size={18} />
            </button>
          </div>
        )}

        <div className="flex-1 overflow-y-auto min-h-0">{children}</div>

        {footer && (
          <div className={`border-t border-border shrink-0 ${footerClassName}`}>
            {footer}
          </div>
        )}
      </div>
    </div>,
    document.body,
  );
}
