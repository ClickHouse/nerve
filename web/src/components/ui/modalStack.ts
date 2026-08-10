/**
 * Open dialogs, oldest first. Module-level rather than context so a dialog
 * rendered anywhere in the tree participates without a provider.
 *
 * Its own module rather than an export from Modal.tsx so that reading the
 * stack from a page doesn't turn that file into a mixed component/util
 * module — which react-refresh treats as an error.
 */
export const modalStack: string[] = [];

/**
 * Is any dialog currently open?
 *
 * Page-level keyboard shortcuts gate on this. A page behind a backdrop stays
 * mounted and keeps its `document` listener, so without the gate a printable
 * key like `n` opens a second dialog on top of the first, and `/` pulls focus
 * out of an `aria-modal` dialog into a box the user can no longer see.
 */
export function isModalOpen(): boolean {
  return modalStack.length > 0;
}
