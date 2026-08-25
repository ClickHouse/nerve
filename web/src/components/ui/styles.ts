/**
 * Shared bits for the primitives in this folder.
 *
 * Its own module rather than an export from one of the components so that
 * importing a class string from a sibling doesn't turn that file into a mixed
 * component/util module — which react-refresh treats as an error.
 */

import { extendTailwindMerge } from 'tailwind-merge';

/** Join class names, dropping the falsy ones. */
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}

/**
 * tailwind-merge, taught the two font-size steps this app adds to Tailwind's
 * scale (`--text-2xs` and `--text-display`, in index.css).
 *
 * `2xs` already matches tailwind-merge's t-shirt-size validator, so it needs no
 * help. `display` does not, and `text-<anything not a known size>` falls
 * through to the *colour* group — so without this line a caller's
 * `text-display` would leave a primitive's `text-sm` standing and would displace
 * its text colour instead. Add any further `--text-*` step here at the same
 * time as index.css.
 */
const twMerge = extendTailwindMerge({
  extend: { theme: { text: ['display'] } },
});

/**
 * One entry per (defaults, override) pair actually rendered — a few hundred at
 * most, since both halves come from a fixed set of tables and literals. Cleared
 * wholesale rather than evicted one at a time: reaching the limit means
 * something is generating class strings dynamically, and an LRU would then
 * thrash rather than fail visibly.
 */
const MERGE_CACHE_LIMIT = 1000;
const mergeCache = new Map<string, string>();

/**
 * A primitive's own classes, with the caller's `className` allowed to win.
 *
 * Tailwind v4 emits same-property utilities in alphabetical/numeric order of
 * class name, so between two of them the later-*sorting* name wins — not the one
 * written last in the `class` attribute. Every primitive in this folder
 * therefore used to beat its own callers at whatever it happened to set:
 * `<Button size="xs" className="px-0">` rendered `px-2`, and `size="sm"` plus
 * `text-sm` rendered `text-xs`. Several migrated call sites asked for exactly
 * those and silently did not get them.
 *
 * This drops the defaults that the override contradicts, so the escape hatch the
 * props cannot cover behaves the way its name implies.
 *
 * **It merges the override against the defaults, and nothing else.** Passing the
 * whole string through `twMerge` in one go would be shorter, and would also
 * quietly resolve collisions *between* the variant and size tables — which is
 * the bug class `Button.test.tsx` exists to catch, and which has already shipped
 * four times on this branch. Those tables must still never emit two of the same
 * property, and a duplicate inside them must still reach the DOM where the test
 * can see it. Filtering per default class keeps both properties: internal
 * duplicates survive untouched, and only the caller displaces anything.
 */
export function overridable(
  base: Array<string | false | null | undefined>,
  override?: string | false | null,
): string {
  const defaults = cx(...base);
  if (!override) return defaults;

  const key = `${defaults}\u0000${override}`;
  const cached = mergeCache.get(key);
  if (cached !== undefined) return cached;

  const verbatim = new Set(override.split(/\s+/).filter(Boolean));
  const kept = defaults.split(' ').filter((c) => {
    // Already spelled out by the caller — keeping it too would just duplicate it.
    if (verbatim.has(c)) return false;
    // tailwind-merge preserves input order for survivors, so `c` survived iff it
    // is still the first token.
    const merged = twMerge(c, override);
    return merged === c || merged.startsWith(`${c} `);
  });

  const result = kept.length > 0 ? `${kept.join(' ')} ${override}` : override;
  if (mergeCache.size >= MERGE_CACHE_LIMIT) mergeCache.clear();
  mergeCache.set(key, result);
  return result;
}

/**
 * The one focus treatment in the app: the border takes the accent colour and
 * nothing else moves. There is no focus ring anywhere in this codebase, so
 * adding one here would make wrapped controls look unlike their neighbours
 * until every last one is converted.
 */
export const FIELD_BASE =
  'bg-surface-raised border border-border-subtle text-text outline-none ' +
  'transition-colors focus:border-accent/50 placeholder:text-text-faint ' +
  'disabled:opacity-50 disabled:cursor-not-allowed';

/**
 * A field with no chrome: the shared focus, placeholder and disabled behaviour,
 * but no background, border, radius or padding — the caller owns the surface.
 *
 * For full-bleed editing panes (the markdown editor, the chat composer) that sit
 * directly on the page rather than in a form. It sets no colour or spacing at
 * all, so a caller that wants a `p-5` and a `bg-bg-sunken` states them once
 * rather than stating them *against* a form field's chrome. `overridable` would
 * now let the caller win either way — this stays a named mode because "bare
 * editing surface" is a different thing from "form field with the padding
 * changed", not because the cascade forces it.
 */
export const FIELD_BARE =
  'bg-transparent border-0 text-text outline-none ' +
  'placeholder:text-text-faint disabled:opacity-50 disabled:cursor-not-allowed';

/** Padding/type-scale steps the app actually uses on inputs. */
export const FIELD_SIZES = {
  sm: 'px-2 py-1 text-xs rounded',
  md: 'px-3 py-2 text-sm rounded-lg',
} as const;

export type FieldSize = keyof typeof FIELD_SIZES;
