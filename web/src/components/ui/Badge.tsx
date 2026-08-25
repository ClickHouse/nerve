import { forwardRef, type HTMLAttributes, type ReactNode } from 'react';
import { cx } from './styles';

/**
 * A status chip.
 *
 * There were 68 of these in the app under 57 distinct class strings — the least
 * consolidated thing in the codebase. They agree on the shape (`text-2xs
 * px-1.5 py-0.5 rounded`) and disagree on everything else.
 *
 * `tone` is the part that is worth having. Roughly 150 of the app's colour
 * classes are stock Tailwind (`text-emerald-400`, `bg-red-500/15`), which are
 * dark-theme values that do not move when the light theme is on. The `hue-*`
 * tokens do, so every tone here is theme-adaptive by construction.
 *
 * Not Click UI's `Badge`: that one takes its content as a `text` prop with a
 * `state` from a fixed semantic set, and several of these badges are coloured
 * from a user-configured hex — task statuses are editable in the UI. `style`
 * stays open for exactly that case.
 */

export type BadgeTone =
  | 'neutral'
  | 'accent'
  | 'success'
  | 'warning'
  | 'danger'
  | 'info'
  | 'purple';

/**
 * The four tones that mean *outcome* use the status tokens; the three that mean
 * *identity* use hue tokens.
 *
 * Two agents independently asked which system chips belong to, having been told
 * to convert status colour to `bg-success-bg`/`text-success` while `Badge`'s own
 * tones resolved to `hue-*`. The answer is that a chip saying "failed" and a
 * banner saying "failed" must be the same red, so the status tones follow the
 * tokens. They are also the pairs Click UI actually designed and that we have
 * measured — success 10.5:1 dark / 7.3:1 light, warning 5.7 / 5.3, danger 8.6 /
 * 5.1, info 7.2 / 6.2 — whereas a hue over a 15% tint of itself was never
 * checked against anything.
 *
 * `accent`, `purple` and `neutral` stay as they are: they label a *kind* of
 * thing (a plan type, a skill, a transport), not how something went, and there
 * is no status token that means "purple".
 *
 * `purple` does pair a hue with a 15% tint of itself, which is the combination
 * the note above says was never checked — and it did not hold: `hue-purple` on
 * its own tint measures 3.93:1 in dark (3.68 on a raised surface). The
 * foreground is therefore `hue-violet`, which is the same ramp two steps
 * lighter — the step index.css already keeps lighter precisely because it is
 * the one used under an alpha modifier. Over the same tint that reads
 * 5.72 / 5.37 dark and 5.00 / 4.59 light (surface / raised).
 */
const TONES: Record<BadgeTone, string> = {
  neutral: 'text-text-muted bg-border-subtle',
  accent: 'text-accent bg-accent/15',
  success: 'text-success bg-success-bg',
  warning: 'text-warning bg-warning-bg',
  danger: 'text-error bg-error-bg',
  info: 'text-info bg-info-bg',
  purple: 'text-hue-violet bg-hue-purple/15',
};

export type BadgeSize = 'xs' | 'sm';

const SIZES: Record<BadgeSize, string> = {
  xs: 'text-2xs px-1.5 py-0.5 gap-1',
  sm: 'text-xs px-2 py-0.5 gap-1',
};

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone;
  size?: BadgeSize;
  /** Fully rounded rather than the default small radius. */
  pill?: boolean;
  /** Hairline border in the tone's colour, for badges on a busy background. */
  outline?: boolean;
  children?: ReactNode;
}

export const Badge = forwardRef<HTMLSpanElement, BadgeProps>(function Badge(
  { tone = 'neutral', size = 'xs', pill = false, outline = false, className, children, ...rest },
  ref,
) {
  return (
    <span
      ref={ref}
      className={cx(
        'inline-flex items-center font-medium whitespace-nowrap',
        SIZES[size],
        TONES[tone],
        pill ? 'rounded-full' : 'rounded',
        outline && 'border border-current/25',
        className,
      )}
      {...rest}
    >
      {children}
    </span>
  );
});
