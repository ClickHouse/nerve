import { forwardRef, type InputHTMLAttributes, type ReactNode } from 'react';
import { cx } from './styles';

/**
 * A checkbox, with its label.
 *
 * Native, and keeping the native `onChange(e)` with `e.target.checked` — Click
 * UI's `Checkbox` is Radix's, whose `onCheckedChange` hands back a
 * `boolean | 'indeterminate'` instead. Radix's is the right one when a checkbox
 * needs a tri-state or has to be styled beyond `accent-color`; this one is for
 * converting the plain ones.
 *
 * The label is rendered as a `<label>` wrapping both, so the text is part of
 * the hit area — the app's one hand-rolled checkbox put its label in a sibling
 * span, which made a 13px box the only place you could click.
 */
export interface CheckboxProps
  extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type' | 'size'> {
  label?: ReactNode;
  /** Classes for the `<label>` wrapper; `className` goes to the input. */
  labelClassName?: string;
}

export const Checkbox = forwardRef<HTMLInputElement, CheckboxProps>(
  function Checkbox({ label, className, labelClassName, disabled, ...rest }, ref) {
    const input = (
      <input
        ref={ref}
        type="checkbox"
        disabled={disabled}
        // `accent-accent` is the whole styling budget: it tints the native
        // control with the theme's accent and leaves the platform's own check,
        // focus ring and touch target alone.
        className={cx(
          'accent-accent cursor-pointer disabled:cursor-not-allowed',
          className,
        )}
        {...rest}
      />
    );

    if (label === undefined) return input;

    return (
      <label
        className={cx(
          'inline-flex items-center gap-2 text-sm text-text-secondary',
          disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
          labelClassName,
        )}
      >
        {input}
        {label}
      </label>
    );
  },
);
