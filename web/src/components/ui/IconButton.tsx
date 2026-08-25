import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { overridable } from './styles';

/**
 * A button whose whole content is an icon.
 *
 * `label` is required and is spent twice — as `title`, so a pointer gets a
 * tooltip, and as `aria-label`, so the control has a name at all. That is the
 * point of the component: the app has ~100 icon-only buttons and only 27
 * `aria-label`s between them, so most of them announce as "button" and nothing
 * else. Making the label non-optional means a converted button cannot be
 * nameless.
 *
 * Not Click UI's `IconButton`, which picks its glyph from a closed union of
 * Click UI icon names. A quarter of this app's icons are drawn locally (see
 * `icons.tsx`) and have no such name, and several of these buttons swap their
 * icon by state — a spinner while busy, the action's glyph otherwise. Taking
 * the icon as children covers both.
 */

export type IconButtonSize = 'xs' | 'sm' | 'md';

/**
 * Square hit areas. `sm` matches the pane and sidebar toggles, `md` the chat
 * composer's controls; `xs` is the inline affordance that sits inside a row of
 * text and cannot afford to set the row's height.
 */
const SIZES: Record<IconButtonSize, string> = {
  xs: 'p-1 rounded',
  sm: 'w-8 h-8 rounded',
  md: 'w-10 h-10 rounded-xl',
};

export type IconButtonVariant =
  | 'ghost'
  | 'subtle'
  | 'primary'
  | 'danger'
  | 'dangerGhost';

/**
 * The colour a variant carries **when it is not active**.
 *
 * Split out of VARIANTS because Tailwind v4 emits same-property utilities in
 * alphabetical order of class name, so between two colour classes on one
 * element the later-*sorting* name wins rather than the one written last.
 * `.text-accent` sorts before every other colour token in this app (offset
 * 49085 in the built stylesheet, against `.text-hue-red` 51903, `.text-on-accent`
 * 52978, `.text-text-dim` 53924, `.text-text-faint` 53967, `.text-text-muted`
 * 54167), so appending the accent `active` treatment after a coloured base lost
 * every time — `active` did nothing on any of these four. Emitting exactly one
 * of REST/ACTIVE makes the result independent of that order.
 */
const REST: Record<IconButtonVariant, string> = {
  /** Quiet until pointed at — the default, and what most of these are. */
  ghost: 'text-text-faint hover:text-text-muted hover:bg-surface-raised',
  /** Carries a surface at rest, for controls sitting on the page background. */
  subtle: 'text-text-muted bg-surface-raised hover:bg-surface-hover',
  /**
   * `text-on-accent`, never `text-white`: the accent is ClickHouse yellow in
   * dark mode, where white is unreadable on it. See the same note on `Button`'s
   * `primary` — it has to be right here, not corrected per call site.
   */
  primary: 'text-on-accent bg-accent hover:bg-accent-hover',
  /** Destructive and already red at rest, for a control that sits alone. */
  danger: 'text-hue-red hover:bg-hue-red/15',
  /**
   * Destructive but quiet until pointed at. This is the row-level
   * delete/remove/purge treatment — inside a list, `danger` reads as an alarm
   * on every row.
   */
  dangerGhost: 'text-text-dim hover:text-hue-red hover:bg-hue-red/10',
};

/** The selected treatment. Mutually exclusive with REST — see the note above. */
const ACTIVE: Record<IconButtonVariant, string> = {
  ghost: 'text-accent bg-accent/15',
  subtle: 'text-accent bg-accent/15',
  // Already the loudest thing on screen; staying lit is the whole point.
  primary: 'text-on-accent bg-accent-hover',
  danger: 'text-hue-red bg-hue-red/15',
  dangerGhost: 'text-hue-red bg-hue-red/10',
};

/** Non-colour classes a variant always carries. */
const SHAPE: Partial<Record<IconButtonVariant, string>> = {
  subtle: 'border border-border',
};

export interface IconButtonProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'title' | 'aria-label'> {
  /** Names the action. Becomes both the tooltip and the accessible name. */
  label: string;
  size?: IconButtonSize;
  variant?: IconButtonVariant;
  /** Selected/current — for toggles that stay lit while their pane is open. */
  active?: boolean;
  /** The icon element. */
  children: ReactNode;
}

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(
  function IconButton(
    {
      label,
      size = 'sm',
      variant = 'ghost',
      active = false,
      className,
      type,
      children,
      ...rest
    },
    ref,
  ) {
    return (
      <button
        ref={ref}
        type={type ?? 'button'}
        title={label}
        aria-label={label}
        className={overridable(
          [
            'inline-flex items-center justify-center shrink-0 cursor-pointer',
            'transition-colors disabled:opacity-50 disabled:cursor-not-allowed',
            SIZES[size],
            SHAPE[variant],
            active ? ACTIVE[variant] : REST[variant],
          ],
          className,
        )}
        {...rest}
      >
        {children}
      </button>
    );
  },
);
