import type { ButtonHTMLAttributes, ReactNode } from 'react';
import { cx } from './styles';

export interface SwitchProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'onChange' | 'role' | 'aria-checked'> {
  checked: boolean;
  onChange: (checked: boolean) => void;
  /** Visible text after the track. It is also the switch's accessible name. */
  label?: ReactNode;
}

/**
 * An on/off switch: a `button` with `role="switch"`, so Space and Enter work
 * and assistive technology reads it as on or off. Use it for a setting that
 * takes effect at once; use `Checkbox` for a choice in a form.
 */
export function Switch({ checked, onChange, label, disabled, className, ...rest }: SwitchProps) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={cx(
        'group inline-flex items-center gap-2 rounded text-sm text-text-secondary outline-none',
        'cursor-pointer disabled:cursor-not-allowed disabled:opacity-50',
        className,
      )}
      {...rest}
    >
      <span
        aria-hidden
        className={cx(
          'relative inline-flex h-5 w-9 shrink-0 rounded-full transition-colors',
          'group-focus-visible:ring-2 group-focus-visible:ring-accent/60 group-focus-visible:ring-offset-1',
          'group-focus-visible:ring-offset-bg',
          checked ? 'bg-accent' : 'bg-text-faint',
        )}
      >
        <span
          className={cx(
            'absolute top-0.5 left-0.5 h-4 w-4 rounded-full bg-white shadow-sm transition-transform',
            checked && 'translate-x-4',
          )}
        />
      </span>
      {label}
    </button>
  );
}
