import { useEffect } from 'react';
import { isTypingTarget, matchesCombo, type ShortcutDef } from '../utils/keyboard';

/**
 * Attach a `document`-level keydown listener that fires the first shortcut
 * whose combo matches the event. Order matters: more specific shortcuts
 * should come first.
 *
 * Skips when focus is inside an editable element unless the shortcut sets
 * `allowInInput: true`. Skips when the shortcut's `when()` predicate is
 * defined and returns false.
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
        if (typing && !sc.allowInInput) continue;
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
