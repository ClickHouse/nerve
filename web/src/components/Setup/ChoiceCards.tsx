import type { KeyboardEvent, ReactNode } from 'react';
import { Check } from '../ui/icons';
import { focusRadio, radioTabIndex, radioTarget } from './rovingRadio';

export interface Choice<V extends string> {
  value: V;
  title: ReactNode;
  description?: ReactNode;
  icon?: ReactNode;
  disabled?: boolean;
}

/**
 * A choice among a few options, each big enough to read and to tap.
 *
 * `role="radiogroup"` of `role="radio"` buttons with the keyboard pattern a
 * screen reader announces ("radio, 1 of 2") leads people to expect: one tab
 * stop for the group, Up/Down/Left/Right to move the selection and the
 * focus, Home/End for the ends. Each card is a 44 px+ target and the
 * checked state is announced, not just coloured.
 */
export function ChoiceCards<V extends string>({
  label, options, value, onChange, disabled, columns = 1,
}: {
  label: string;
  options: Choice<V>[];
  value: V | null;
  onChange: (value: V) => void;
  disabled?: boolean;
  columns?: 1 | 2;
}) {
  const values = options.map((o) => o.value);
  const isOff = (v: V) => !!disabled || !!options.find((o) => o.value === v)?.disabled;

  const onKeyDown = (e: KeyboardEvent<HTMLButtonElement>, from: V) => {
    const target = radioTarget(values, from, e.key, isOff);
    if (target === null) return;
    e.preventDefault();
    if (target !== value) onChange(target);
    focusRadio(e.currentTarget, values.indexOf(target));
  };

  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={`grid gap-2 ${columns === 2 ? 'sm:grid-cols-2' : ''}`}
    >
      {options.map((option) => {
        const checked = option.value === value;
        const off = isOff(option.value);
        return (
          <button
            key={option.value}
            type="button"
            role="radio"
            aria-checked={checked}
            disabled={off}
            tabIndex={radioTabIndex(values, value, option.value, isOff)}
            onClick={() => onChange(option.value)}
            onKeyDown={(e) => onKeyDown(e, option.value)}
            className={`text-left flex items-start gap-3 rounded-lg border p-3 min-h-[44px] transition-colors
              disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer
              ${checked
                ? 'border-accent bg-accent/10'
                : 'border-border bg-surface-raised hover:border-border hover:bg-surface-hover'}`}
          >
            {option.icon && <span className="mt-0.5 shrink-0 text-text-muted">{option.icon}</span>}
            <span className="min-w-0 flex-1">
              <span className="block text-sm font-medium text-text">{option.title}</span>
              {option.description && (
                <span className="block text-xs text-text-muted mt-0.5">{option.description}</span>
              )}
            </span>
            <span
              aria-hidden="true"
              className={`mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full border
                ${checked ? 'border-accent bg-accent text-on-accent' : 'border-border'}`}
            >
              {checked && <Check size={10} />}
            </span>
          </button>
        );
      })}
    </div>
  );
}
