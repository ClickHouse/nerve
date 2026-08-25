import { forwardRef, type ReactNode, type SelectHTMLAttributes } from 'react';
import { FIELD_BASE, FIELD_SIZES, overridable, type FieldSize } from './styles';

/**
 * A dropdown, on the native `<select>`.
 *
 * Click UI's `Select` is the richer control — it is searchable, groupable and
 * styleable — but it is not a drop-in here, for two reasons beyond the obvious
 * one that its API is `onSelect(value)` rather than `onChange(e)`:
 *
 * **It portals.** Radix renders the open list into `document.body`. Three of
 * this app's selects live inside a `Modal`, whose focus trap only knows about
 * nodes inside its own panel and whose Escape handler runs on the capture phase
 * — so an open Click UI list inside a dialog would be untabbable, and Escape
 * would close the dialog instead of the list. Making that work means teaching
 * the Modal about Radix's layer stack, which is a bigger change than the one
 * this refactor is making.
 *
 * **It is not a native control.** This app is used on a phone (the whole
 * `PageHeader` breakpoint story exists for that reason), and a native `<select>`
 * gets the platform's own picker there.
 *
 * So: use this for the existing selects. Reach for Click UI's `Select`
 * directly when a new one needs search or option groups — and keep it out of
 * dialogs until the Modal knows about Radix layers.
 */

export interface SelectOption {
  value: string;
  label: ReactNode;
  disabled?: boolean;
}

export interface SelectProps
  extends Omit<SelectHTMLAttributes<HTMLSelectElement>, 'size' | 'children'> {
  fieldSize?: FieldSize;
  fullWidth?: boolean;
  /** Options, if you would rather not write `<option>`s. */
  options?: SelectOption[];
  /**
   * Rendered first with an empty value, for the `<option value="">All …
   * </option>` idiom the filter bars use.
   */
  emptyLabel?: ReactNode;
  /**
   * Keep showing a `value` that isn't in `options` rather than silently
   * snapping the control to the first option. Statuses are user-configurable
   * and a task can hold one that has since been renamed away.
   */
  children?: ReactNode;
}

export const Select = forwardRef<HTMLSelectElement, SelectProps>(function Select(
  {
    fieldSize = 'md',
    fullWidth = false,
    options,
    emptyLabel,
    className,
    value,
    children,
    ...rest
  },
  ref,
) {
  const known =
    options === undefined ||
    value === undefined ||
    value === '' ||
    options.some((o) => o.value === value);

  return (
    <select
      ref={ref}
      value={value}
      className={overridable(
        [FIELD_BASE, FIELD_SIZES[fieldSize], 'cursor-pointer', fullWidth && 'w-full'],
        className,
      )}
      {...rest}
    >
      {emptyLabel !== undefined && <option value="">{emptyLabel}</option>}
      {/* A value with no matching option would otherwise make the control
          display — and on the next change, submit — some other option. */}
      {!known && typeof value === 'string' && <option value={value}>{value}</option>}
      {options?.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled}>
          {o.label}
        </option>
      ))}
      {children}
    </select>
  );
});
