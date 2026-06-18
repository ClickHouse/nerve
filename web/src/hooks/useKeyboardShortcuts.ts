import { useEffect } from 'react';
import { isSafeInInputCombo, isTypingTarget, matchesCombo, type ShortcutDef } from '../utils/keyboard';

/**
 * Attach a `document`-level keydown listener that fires the first shortcut
 * whose combo matches the event. Order matters: more specific shortcuts
 * should come first.
 *
 * When focus is inside an editable element, shortcuts still fire if either:
 * - the shortcut sets `allowInInput: true`, or
 * - the combo is "safe in input" by default (Cmd/Ctrl combos and Escape) —
 *   pressing it can't be confused with typing.
 *
 * Set `allowInInput: false` explicitly to opt out of the safe-by-default
 * behavior. Skips when the shortcut's `when()` predicate is defined and
 * returns false.
 *
 * Pass the same array reference across renders if you can — otherwise we
 * re-register the listener on every render. In practice the callers below
 * build a fresh array each render but the cost is one removeEventListener +
 * addEventListener, which is negligible.
 */
export function useKeyboardShortcuts(shortcuts: ShortcutDef[]): void {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const typing = isTypingTarget(e.target);
      for (const sc of shortcuts) {
        if (typing) {
          const allowed = sc.allowInInput ?? isSafeInInputCombo(sc.combo);
          if (!allowed) continue;
        }
        if (sc.when && !sc.when()) continue;
        if (!matchesCombo(e, sc.combo)) continue;
        e.preventDefault();
        sc.action(e);
        return;
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [shortcuts]);
}
