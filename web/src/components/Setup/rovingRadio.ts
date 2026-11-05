/**
 * The ARIA radiogroup keyboard pattern for a row of `role="radio"` controls
 * (the choice cards, the effort and theme pills): one tab stop — the checked
 * option, else the first enabled one — and arrow keys that move both the
 * selection and the focus, wrapping at the ends and skipping disabled
 * options. Home and End jump to the first and last.
 *
 * Pure helpers rather than a hook, so a group can keep rendering whatever
 * element it already renders and only add `tabIndex` and `onKeyDown`.
 */

/** The `tabIndex` of the option `value` inside a group. */
export function radioTabIndex<V>(
  values: readonly V[], checked: V | null, value: V, disabled: (v: V) => boolean,
): 0 | -1 {
  const enabled = values.filter((v) => !disabled(v));
  const stop = checked !== null && enabled.includes(checked) ? checked : enabled[0];
  return value === stop ? 0 : -1;
}

/**
 * The option an arrow key moves to from `current`, or `null` for any other
 * key (so the caller leaves the event alone).
 */
export function radioTarget<V>(
  values: readonly V[], current: V, key: string, disabled: (v: V) => boolean,
): V | null {
  const enabled = values.filter((v) => !disabled(v));
  if (enabled.length === 0) return null;
  const at = enabled.indexOf(current);
  switch (key) {
    case 'ArrowDown':
    case 'ArrowRight':
      return at < 0 ? enabled[0] : enabled[(at + 1) % enabled.length];
    case 'ArrowUp':
    case 'ArrowLeft':
      return at < 0 ? enabled[enabled.length - 1] : enabled[(at - 1 + enabled.length) % enabled.length];
    case 'Home':
      return enabled[0];
    case 'End':
      return enabled[enabled.length - 1];
    default:
      return null;
  }
}

/**
 * Focus the `index`-th radio of the group that contains `from`. Indices
 * count every radio in DOM order, disabled ones included, so callers pass
 * the index in their full option list.
 */
export function focusRadio(from: HTMLElement, index: number): void {
  const group = from.closest('[role="radiogroup"]');
  const radios = group?.querySelectorAll<HTMLElement>('[role="radio"]');
  radios?.[index]?.focus();
}
