import { forwardRef, type InputHTMLAttributes, type ReactNode } from 'react';
import { overridable } from './styles';

/**
 * A checkbox, with its label.
 *
 * Native, and keeping the native `onChange(e)` with `e.target.checked` — Click
 * UI's `Checkbox` is Radix's, whose `onCheckedChange` hands back a
 * `boolean | 'indeterminate'` instead. Radix's is the right one when a checkbox
 * needs a tri-state or has to be styled beyond `accent-color`; this one is for
 * the plain ones.
 *
 * The label is rendered as a `<label>` wrapping both, so the text is part of
 * the hit area and not just the 13px box.
 */
/**
 * The label's typography, as props rather than as classes on `labelClassName`.
 *
 * These are the only two properties the app's checkbox labels vary, and naming
 * them keeps the choice closed and type-checked — `labelTone="dim"` is checked
 * at compile time where `labelClassName="text-text-dim"` is a string nobody
 * validates. `labelClassName` takes anything else, and wins over these props.
 */
export type CheckboxLabelSize = 'xs' | 'sm';
export type CheckboxLabelTone = 'secondary' | 'muted' | 'dim';

const LABEL_SIZES: Record<CheckboxLabelSize, string> = {
  xs: 'text-xs',
  sm: 'text-sm',
};

const LABEL_TONES: Record<CheckboxLabelTone, string> = {
  secondary: 'text-text-secondary',
  muted: 'text-text-muted',
  dim: 'text-text-dim',
};

export interface CheckboxProps
  extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type' | 'size'> {
  label?: ReactNode;
  /**
   * Classes for the `<label>` wrapper; `className` goes to the input.
   * Use `labelSize`/`labelTone` for typography — see the note above.
   */
  labelClassName?: string;
  labelSize?: CheckboxLabelSize;
  labelTone?: CheckboxLabelTone;
}

export const Checkbox = forwardRef<HTMLInputElement, CheckboxProps>(
  function Checkbox(
    {
      label,
      className,
      labelClassName,
      labelSize = 'sm',
      labelTone = 'secondary',
      disabled,
      ...rest
    },
    ref,
  ) {
    const input = (
      <input
        ref={ref}
        type="checkbox"
        disabled={disabled}
        // `accent-accent` is the whole styling budget: it tints the native
        // control with the theme's accent and leaves the platform's own check,
        // focus ring and touch target alone.
        className={overridable(
          ['accent-accent cursor-pointer disabled:cursor-not-allowed'],
          className,
        )}
        {...rest}
      />
    );

    if (label === undefined) return input;

    return (
      <label
        className={overridable(
          [
            'inline-flex items-center gap-2',
            LABEL_SIZES[labelSize],
            LABEL_TONES[labelTone],
            disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
          ],
          labelClassName,
        )}
      >
        {input}
        {label}
      </label>
    );
  },
);
