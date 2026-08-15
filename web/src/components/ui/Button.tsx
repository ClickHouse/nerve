import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { cx } from './styles';

/**
 * The app's button.
 *
 * There were 239 hand-styled `<button>` elements across 74 files before this,
 * carrying 195 distinct class strings between them. Nearly all of those were
 * one of six shapes with the spacing or the disabled opacity drifting by a step
 * — `disabled:opacity-30`, `-40` and `-50` all appear, on buttons that do the
 * same job. The variants below are those six shapes, named.
 *
 * Deliberately **not** Click UI's `Button`: that one renders its content from a
 * `label` string plus an `iconLeft`/`iconRight` icon name, and takes no
 * children. Most buttons in this app hold real markup — an icon element, a
 * truncating span, a count badge — so wrapping it would have meant rewriting
 * every call site's content rather than just its classes. Click UI's `Button`
 * is still the right thing for a plain labelled button; import it directly.
 *
 * Everything the design system does contribute reaches this through the
 * semantic tokens (`bg-accent`, `text-text-muted`, `border-border-subtle`),
 * which is where Click UI's palette is bound.
 */

export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'ghost'
  | 'subtle'
  | 'danger'
  | 'dangerSolid'
  | 'pill'
  | 'tab';

export type ButtonSize = 'xs' | 'sm' | 'md';

const VARIANTS: Record<ButtonVariant, string> = {
  /** The page's one committing action. */
  primary: 'text-white bg-accent hover:bg-accent-hover rounded-lg font-medium',
  /** Sits beside a primary without competing with it. */
  secondary:
    'text-text-secondary bg-surface-raised hover:bg-surface-hover border border-border rounded-lg',
  /** Bare label; only the text colour moves. For "Cancel", "Show more". */
  ghost: 'text-text-muted hover:text-text-secondary rounded',
  /** The menu/list-row shape: no chrome at rest, a surface on hover. */
  subtle: 'text-text-secondary hover:bg-surface-raised rounded',
  /**
   * Destructive, tinted. Uses `hue-red` rather than a stock `red-*` so it
   * follows the light theme — the hand-rolled `bg-red-600` buttons this
   * replaces are pinned to dark-theme values.
   */
  danger:
    'text-hue-red bg-hue-red/15 hover:bg-hue-red/25 rounded-md font-medium',
  /** Destructive and unmissable — for the confirm step, not the trigger. */
  dangerSolid: 'text-white bg-hue-red hover:bg-hue-red/90 rounded-md font-medium',
  /** Filter chip. Pair with `active`. */
  pill: 'rounded-full border whitespace-nowrap',
  /** Underlined tab. Pair with `active`. */
  tab: 'font-medium border-b-2 border-transparent rounded-none',
};

/**
 * The selected treatment, per variant. The app was already consistent about
 * this — an active control is accent-on-accent-tint — it just spelled the tint
 * three different ways (`/10`, `/15`, `/20`).
 */
const ACTIVE: Partial<Record<ButtonVariant, string>> = {
  pill: 'bg-accent/15 text-accent border-accent/30',
  tab: 'text-accent border-accent',
  subtle: 'bg-accent/15 text-accent',
  ghost: 'text-accent',
};

const INACTIVE: Partial<Record<ButtonVariant, string>> = {
  pill: 'text-text-dim border-border hover:text-text-muted',
  tab: 'text-text-dim hover:text-text-muted',
};

const SIZES: Record<ButtonSize, string> = {
  xs: 'px-2 py-1 text-xs gap-1',
  sm: 'px-3 py-1.5 text-xs gap-1.5',
  md: 'px-3 py-2 text-sm gap-2',
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  /** Selected/current. Only `pill`, `tab`, `subtle` and `ghost` render it. */
  active?: boolean;
  /** Stretch to the container. Off by default; a bare `w-full` in the base
   *  classes would fight every caller that wants an intrinsic width. */
  fullWidth?: boolean;
  children?: ReactNode;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  {
    variant = 'secondary',
    size = 'sm',
    active = false,
    fullWidth = false,
    className,
    type,
    children,
    ...rest
  },
  ref,
) {
  const state = active ? ACTIVE[variant] : INACTIVE[variant];
  return (
    <button
      ref={ref}
      // Buttons inside a form default to `submit` and will submit it. Almost
      // none of these are submit buttons, and the ones that are say so.
      type={type ?? 'button'}
      className={cx(
        'inline-flex items-center justify-center shrink-0 cursor-pointer',
        'transition-colors disabled:opacity-50 disabled:cursor-not-allowed',
        SIZES[size],
        VARIANTS[variant],
        state,
        fullWidth && 'w-full',
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
});
