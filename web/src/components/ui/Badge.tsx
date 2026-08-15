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

const TONES: Record<BadgeTone, string> = {
  neutral: 'text-text-muted bg-border-subtle',
  accent: 'text-accent bg-accent/15',
  success: 'text-hue-emerald bg-hue-emerald/15',
  warning: 'text-hue-amber bg-hue-amber/15',
  danger: 'text-hue-red bg-hue-red/15',
  info: 'text-hue-blue bg-hue-blue/15',
  purple: 'text-hue-purple bg-hue-purple/15',
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
