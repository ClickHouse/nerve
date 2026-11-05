import type { SetupProfileDetail, SetupStep } from '../../api/client';

/** Structural equality good enough for wire-shaped values. */
export function same(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

/**
 * Whether Continue on this step must send its *decision payload* even when
 * nothing differs from the server's detail: a pending or skipped step is
 * answered by the act of continuing, and the server records `answered`.
 * A done step sends only what changed.
 */
export function decides(step: Pick<SetupStep, 'status'>): boolean {
  return step.status === 'pending' || step.status === 'skipped';
}

/** The subset of `form` whose values differ from `base`, over `keys`. */
export function changedFields<T extends Record<string, unknown>>(
  form: T, base: Partial<Record<keyof T, unknown>>, keys: (keyof T)[],
): Partial<T> {
  const out: Partial<T> = {};
  for (const key of keys) {
    if (!same(form[key], base[key])) out[key] = form[key];
  }
  return out;
}

/**
 * How a `SecretField` value lands in a payload: `''` is omitted (never
 * written), a string is the new value, `null` removes.
 */
export function secretPayload(value: string | null): string | null | undefined {
  if (value === null) return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

export function codePoints(s: string): number {
  return [...s].length;
}

/**
 * Whether the server's quiet hours are on. Off is both times empty; an
 * older backend sends no `quiet_enabled` and always has both times.
 */
export function quietEnabled(
  detail: Pick<SetupProfileDetail, 'quiet_enabled' | 'quiet_start' | 'quiet_end'>,
): boolean {
  return detail.quiet_enabled ?? Boolean(detail.quiet_start && detail.quiet_end);
}
