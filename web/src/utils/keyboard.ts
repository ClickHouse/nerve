/**
 * Keyboard shortcut helpers.
 *
 * Conventions used by the shortcut system:
 * - `mod` = Cmd on macOS, Ctrl elsewhere.
 * - `key` is matched case-insensitively against `event.key`. For symbols like
 *   `;` or `/` we match the literal character; for letters we match lowercase.
 * - `Backspace` and `Delete` are treated as aliases so Mac (⌘Shift⌫) and
 *   Linux/Windows (Ctrl+Shift+Delete) both work for "delete current".
 */

/** Detect macOS for both label rendering and to know that Cmd ≠ Ctrl. */
export const isMac =
  typeof navigator !== 'undefined' &&
  /Mac|iPod|iPhone|iPad/.test(navigator.platform);

/** Is the platform's "mod" key (Cmd on Mac, Ctrl elsewhere) pressed? */
export function isMod(e: KeyboardEvent): boolean {
  return isMac ? e.metaKey && !e.ctrlKey : e.ctrlKey && !e.metaKey;
}

/** Focus is in an editable element — most shortcuts should bail out. */
export function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
  if (target.isContentEditable) return true;
  return false;
}

export interface ShortcutCombo {
  /** Requires Cmd (Mac) / Ctrl (other). */
  mod?: boolean;
  shift?: boolean;
  alt?: boolean;
  /**
   * `event.key` value. Examples: "k", "/", ";", "o", "\\", "Backspace",
   * "Escape". For letters use lowercase. "Backspace" also matches "Delete"
   * so Mac and Linux bindings coexist.
   */
  key: string;
}

export interface ShortcutDef {
  id: string;
  combo: ShortcutCombo;
  /** Section + description shown in the modal. */
  description: string;
  section: 'global' | 'chat' | 'input';
  /** Only fire when this returns true (e.g. only on /chat). */
  when?: () => boolean;
  /** Default false: skipped when focus is in an editable element. */
  allowInInput?: boolean;
  action: (e: KeyboardEvent) => void;
}

/** Does this keydown event match the combo? */
export function matchesCombo(e: KeyboardEvent, combo: ShortcutCombo): boolean {
  if (!!combo.mod !== isMod(e)) return false;
  if (!!combo.shift !== e.shiftKey) return false;
  if (!!combo.alt !== e.altKey) return false;

  const eventKey = e.key;
  const wanted = combo.key;
  if (wanted === 'Backspace') {
    return eventKey === 'Backspace' || eventKey === 'Delete';
  }
  return eventKey.toLowerCase() === wanted.toLowerCase();
}

/** Human-readable label for the shortcuts modal. */
export function formatCombo(combo: ShortcutCombo): string {
  const parts: string[] = [];
  if (combo.mod) parts.push(isMac ? '⌘' : 'Ctrl');
  if (combo.shift) parts.push(isMac ? '⇧' : 'Shift');
  if (combo.alt) parts.push(isMac ? '⌥' : 'Alt');
  parts.push(formatKey(combo.key));
  return parts.join(isMac ? ' ' : '+');
}

function formatKey(key: string): string {
  switch (key) {
    case 'Backspace':
      return isMac ? '⌫' : 'Backspace';
    case 'Escape':
      return 'Esc';
    case 'Enter':
      return isMac ? '↵' : 'Enter';
    case '\\':
      return '\\';
    default:
      // Single letters → uppercase; symbols stay as-is.
      return key.length === 1 ? key.toUpperCase() : key;
  }
}
