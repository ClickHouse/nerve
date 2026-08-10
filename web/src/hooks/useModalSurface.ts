import { useCallback, useEffect, useRef } from 'react';

/**
 * Controls that can hold keyboard focus. Excludes what the browser has already
 * taken out of the tab order (`disabled`, `tabindex="-1"`).
 */
const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

function focusableWithin(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR))
    // `offsetParent === null` drops `display:none` subtrees (e.g. a panel
    // hidden with Tailwind's `hidden`): the selector still matches them, but
    // they cannot take focus.
    .filter(el => el.offsetParent !== null && !el.closest('[inert]'));
}

/**
 * Modal plumbing for an overlay that covers the page on a phone — a drawer or
 * a full-screen panel.
 *
 * Covering the page visually is not enough. Left alone, the transcript and the
 * navigation underneath stay in the tab order, so Tab walks focus onto controls
 * the user cannot see. This gives the surface the four things a modal owes the
 * keyboard:
 *
 * 1. focus moves into it when it opens,
 * 2. Tab cycles inside it instead of escaping behind it,
 * 3. Escape dismisses it — and is claimed here, so it does not *also* reach the
 *    global Escape shortcut and stop a streaming response,
 * 4. focus returns to whatever opened it when it closes.
 *
 * `role="dialog"` + `aria-modal` cover the same ground for assistive
 * technology, which treats everything outside an aria-modal dialog as inert.
 *
 * Spread `dialogProps` onto the surface element; add an `aria-label` there so
 * the dialog has a name.
 */
export function useModalSurface<T extends HTMLElement>(active: boolean, onClose?: () => void) {
  const ref = useRef<T>(null);
  // Held in a ref so a new `onClose` identity cannot re-run the focus effect,
  // which would yank focus back to the top of the surface mid-interaction.
  const onCloseRef = useRef(onClose);
  useEffect(() => { onCloseRef.current = onClose; }, [onClose]);

  useEffect(() => {
    const surface = ref.current;
    if (!active || !surface) return;

    const restoreTo = document.activeElement as HTMLElement | null;
    // Focus the surface itself rather than its first control: that control is
    // usually a search box or a close button, and starting there skips the
    // surface's own heading for a screen reader.
    surface.focus();

    return () => {
      if (!restoreTo?.isConnected) return;
      const current = document.activeElement;
      // Hand focus back only if it is still inside the surface, or adrift on
      // <body> because the surface was dismissed by tapping the scrim. If the
      // user has already moved focus elsewhere, leave it there.
      const adrift = !current || current === document.body;
      if (adrift || surface.contains(current)) restoreTo.focus();
    };
  }, [active]);

  const onKeyDown = useCallback((e: React.KeyboardEvent<T>) => {
    const surface = ref.current;
    if (!surface) return;

    if (e.key === 'Escape') {
      const close = onCloseRef.current;
      if (!close) return;
      // Claim it before it bubbles to the document-level shortcut handler,
      // where Escape means "stop the agent".
      e.preventDefault();
      e.stopPropagation();
      close();
      return;
    }

    if (e.key !== 'Tab') return;
    const focusables = focusableWithin(surface);
    if (focusables.length === 0) {
      // Nothing to move to — better to hold focus on the surface than to let
      // it land on something hidden behind the overlay.
      e.preventDefault();
      return;
    }
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    const current = document.activeElement;
    // Shift+Tab off the top would leave through the start of the surface;
    // Tab off the bottom would leave through the end. Wrap both.
    if (e.shiftKey && (current === first || current === surface)) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && current === last) {
      e.preventDefault();
      first.focus();
    }
  }, []);

  return {
    dialogProps: {
      ref,
      role: 'dialog' as const,
      'aria-modal': active,
      // Lets the surface hold focus without joining the tab order itself.
      tabIndex: -1,
      onKeyDown,
    },
  };
}
