import { forwardRef, type HTMLAttributes, type ReactNode } from 'react';
import { overridable } from './styles';

/**
 * A status chip.
 *
 * `tone` uses the `hue-*` and status tokens rather than stock Tailwind colours
 * (`text-emerald-400`, `bg-red-500/15`): stock colours are dark-theme values
 * that do not move when the light theme is on, and the tokens do, so every tone
 * here is theme-adaptive by construction.
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
 * A chip saying "failed" and a banner saying "failed" must be the same red, so
 * the outcome tones follow the status tokens. Those are also the pairs Click UI
 * designed and the pairs we have measured — success 10.5:1 dark / 7.3:1 light,
 * warning 5.7 / 5.3, danger 8.6 / 5.1, info 7.2 / 6.2.
 *
 * `accent`, `purple` and `neutral` label a *kind* of thing (a plan type, a
 * skill, a transport), not how something went, and there is no status token
 * that means "purple".
 *
 * `purple` pairs a hue with a 15% tint of itself. `hue-purple` over its own
 * tint measures 3.93:1 in dark (3.68 on a raised surface), so the foreground is
 * `hue-violet`: the same ramp two steps lighter — the step index.css keeps
 * lighter precisely because it is the one used under an alpha modifier. Over
 * the same tint that reads 5.72 / 5.37 dark and 5.00 / 4.59 light (surface /
 * raised).
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
      className={overridable(
        [
          'inline-flex items-center font-medium whitespace-nowrap',
          SIZES[size],
          TONES[tone],
          pill ? 'rounded-full' : 'rounded',
          outline && 'border border-current/25',
        ],
        className,
      )}
      {...rest}
    >
      {children}
    </span>
  );
});
