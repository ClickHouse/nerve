import {
  forwardRef,
  type InputHTMLAttributes,
  type TextareaHTMLAttributes,
} from 'react';
import {
  FIELD_BARE,
  FIELD_BASE,
  FIELD_SIZES,
  overridable,
  type FieldSize,
} from './styles';

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
 * of the number inputs are deliberately narrow (`w-16`, `w-24`), and defaulting
 * it on with a named opt-out states that better than four `w-auto` overrides.
 */

/**
 * Drop the field chrome — background, border, radius and padding — keeping only
 * the focus and placeholder behaviour.
 *
 * For the full-bleed editing surfaces — the markdown editor pane, the chat
 * composer — which want the shared focus and disabled handling but own their own
 * background. One flag rather than the five negating classes it would otherwise
 * take at each call site.
 */
export interface FieldChromeProps {
  /** Render as a bare editing surface rather than a bordered form field. */
  bare?: boolean;
}

export interface TextFieldProps
  extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'>,
    FieldChromeProps {
  fieldSize?: FieldSize;
  /** Stretch to the container. */
  fullWidth?: boolean;
}

export const TextField = forwardRef<HTMLInputElement, TextFieldProps>(
  function TextField(
    {
      fieldSize = 'md',
      fullWidth = true,
      bare = false,
      className,
      type = 'text',
      ...rest
    },
    ref,
  ) {
    return (
      <input
        ref={ref}
        type={type}
        className={overridable(
          [
            bare ? FIELD_BARE : FIELD_BASE,
            !bare && FIELD_SIZES[fieldSize],
            fullWidth && 'w-full',
          ],
          className,
        )}
        {...rest}
      />
    );
  },
);

export interface TextAreaProps
  extends TextareaHTMLAttributes<HTMLTextAreaElement>,
    FieldChromeProps {
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
      bare = false,
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
        className={overridable(
          [
            bare ? FIELD_BARE : FIELD_BASE,
            !bare && FIELD_SIZES[fieldSize],
            fullWidth && 'w-full',
            resizable ? 'resize-y' : 'resize-none',
          ],
          className,
        )}
        {...rest}
      />
    );
  },
);
