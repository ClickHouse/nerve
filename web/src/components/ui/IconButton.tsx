import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { cx } from './styles';

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

export type IconButtonVariant = 'ghost' | 'subtle' | 'primary' | 'danger';

const VARIANTS: Record<IconButtonVariant, string> = {
  /** Quiet until pointed at — the default, and what most of these are. */
  ghost: 'text-text-faint hover:text-text-muted hover:bg-surface-raised',
  /** Carries a surface at rest, for controls sitting on the page background. */
  subtle:
    'text-text-muted bg-surface-raised hover:bg-surface-hover border border-border',
  primary: 'text-white bg-accent hover:bg-accent-hover',
  danger: 'text-hue-red hover:bg-hue-red/15',
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
        className={cx(
          'inline-flex items-center justify-center shrink-0 cursor-pointer',
          'transition-colors disabled:opacity-50 disabled:cursor-not-allowed',
          SIZES[size],
          VARIANTS[variant],
          active && 'bg-accent/15 text-accent',
          className,
        )}
        {...rest}
      >
        {children}
      </button>
    );
  },
);
