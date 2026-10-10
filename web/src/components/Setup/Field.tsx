import { cloneElement, isValidElement, useId, type ReactElement, type ReactNode } from 'react';
import { cx } from '../ui/styles';
import { CONTROL_CLASS } from './controlStyle';

/**
 * A labelled form field: `<label htmlFor>`, optional help text, and an error
 * tied to the control through `aria-describedby` and `aria-invalid`.
 *
 * The app's `TextField`/`Select` carry no label slot of their own — pages use
 * `aria-label`. The wizard wants visible labels and errors a screen reader
 * announces with the field, which is what this wrapper adds. The child must
 * be a single form control; its `id`, `aria-describedby`, `aria-invalid` and
 * the wizard's control box (`CONTROL_CLASS`, a visible box on any surface)
 * are set here.
 */
export function Field({ label, help, error, children, trailing, id: givenId, className }: {
  label: ReactNode;
  help?: ReactNode;
  error?: string | null;
  children: ReactNode;
  /** A control beside the field (an eye toggle); outside the label association. */
  trailing?: ReactNode;
  id?: string;
  className?: string;
}) {
  const generated = useId();
  const id = givenId ?? `field-${generated}`;
  const helpId = `${id}-help`;
  const errorId = `${id}-error`;
  const describedBy = [help ? helpId : null, error ? errorId : null].filter(Boolean).join(' ') || undefined;

  const child = isValidElement(children) ? children as ReactElement<Record<string, unknown>> : null;
  const control = child
    ? cloneElement(child, {
      id,
      'aria-describedby': describedBy,
      'aria-invalid': error ? true : undefined,
      className: cx(typeof child.props.className === 'string' ? child.props.className : undefined, CONTROL_CLASS),
    })
    : children;

  return (
    <div className={`flex flex-col gap-1 ${className ?? ''}`}>
      <label htmlFor={id} className="text-sm font-medium text-text">{label}</label>
      {trailing ? <div className="flex items-center gap-1">{control}{trailing}</div> : control}
      {help && <p id={helpId} className="text-xs text-text-dim">{help}</p>}
      {/* `role="alert"`: announced when it appears, even while focus is
          elsewhere (a save that fails leaves focus on the submit button, or
          on nothing at all). Focusable so the frame can land on it when no
          control carries `aria-invalid`. */}
      {error && <p id={errorId} role="alert" tabIndex={-1} className="text-xs text-error outline-none">{error}</p>}
    </div>
  );
}
