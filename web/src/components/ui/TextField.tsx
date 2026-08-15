import {
  forwardRef,
  type InputHTMLAttributes,
  type TextareaHTMLAttributes,
} from 'react';
import { FIELD_BASE, FIELD_SIZES, cx, type FieldSize } from './styles';

/**
 * Text inputs.
 *
 * These keep the **native** event shape — `onChange(e)` with `e.target.value` —
 * which is what all 31 inputs and 17 textareas in the app already use, so a
 * call site converts by deleting its class string and nothing else. Click UI's
 * `TextField` takes `onChange(value, e?)` with the event optional, and narrows
 * `type` to text/email/tel/url; the app needs `password`, `number`, `date`,
 * `color` and `file` as well. It is the better component for a *new* form —
 * import it directly — but not a drop-in for these.
 *
 * `fullWidth` is a prop rather than a `w-full` in the base classes because four
 * of the number inputs are deliberately narrow (`w-16`, `w-24`) and a base
 * `w-full` would win the cascade against them.
 */

export interface TextFieldProps
  extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
  fieldSize?: FieldSize;
  /** Stretch to the container. */
  fullWidth?: boolean;
}

export const TextField = forwardRef<HTMLInputElement, TextFieldProps>(
  function TextField(
    { fieldSize = 'md', fullWidth = true, className, type = 'text', ...rest },
    ref,
  ) {
    return (
      <input
        ref={ref}
        type={type}
        className={cx(
          FIELD_BASE,
          FIELD_SIZES[fieldSize],
          fullWidth && 'w-full',
          className,
        )}
        {...rest}
      />
    );
  },
);

export interface TextAreaProps
  extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  fieldSize?: FieldSize;
  fullWidth?: boolean;
  /**
   * Let the user drag the bottom edge. Off by default: 15 of the app's 17
   * textareas suppress it, because they sit in a dialog whose height is already
   * pinned and a resize handle just pushes the footer off screen.
   */
  resizable?: boolean;
}

export const TextArea = forwardRef<HTMLTextAreaElement, TextAreaProps>(
  function TextArea(
    {
      fieldSize = 'md',
      fullWidth = true,
      resizable = false,
      className,
      rows = 3,
      ...rest
    },
    ref,
  ) {
    return (
      <textarea
        ref={ref}
        rows={rows}
        className={cx(
          FIELD_BASE,
          FIELD_SIZES[fieldSize],
          fullWidth && 'w-full',
          resizable ? 'resize-y' : 'resize-none',
          className,
        )}
        {...rest}
      />
    );
  },
);
