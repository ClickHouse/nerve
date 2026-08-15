/**
 * Shared bits for the primitives in this folder.
 *
 * Its own module rather than an export from one of the components so that
 * importing a class string from a sibling doesn't turn that file into a mixed
 * component/util module — which react-refresh treats as an error.
 */

/** Join class names, dropping the falsy ones. */
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
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

/** Padding/type-scale steps the app actually uses on inputs. */
export const FIELD_SIZES = {
  sm: 'px-2 py-1 text-xs rounded',
  md: 'px-3 py-2 text-sm rounded-lg',
} as const;

export type FieldSize = keyof typeof FIELD_SIZES;
