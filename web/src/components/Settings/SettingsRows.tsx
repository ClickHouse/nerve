import { useId, useState, type KeyboardEvent, type ReactNode } from 'react';
import type { NotificationLevel, SettingField } from '../../api/settings';
import { useSettingsStore } from '../../stores/settingsStore';
import { Badge, Button, TextField } from '../ui';
import { Check, Lock, X } from '../ui/icons';
import { Field } from '../Setup/Field';
import { focusRadio, radioTabIndex, radioTarget } from '../Setup/rovingRadio';
import { canEdit } from './sections';

/** A titled list of rows in one bordered card, with an optional footer. */
export function SettingsGroup({ title, description, children, footer }: {
  title: string;
  description?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
}) {
  const headingId = useId();
  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-2">
      <div>
        <h3 id={headingId} className="text-sm font-semibold text-text">{title}</h3>
        {description && <p className="mt-0.5 text-xs text-text-dim">{description}</p>}
      </div>
      <div className="rounded-lg border border-border bg-surface-raised">
        <ul className="divide-y divide-border">{children}</ul>
        {footer && <div className="border-t border-border px-4 py-3">{footer}</div>}
      </div>
    </section>
  );
}

/**
 * One row: an icon, a title with a line under it, the row's main control on
 * the right, and its details below. `titleId` names the control
 * (`aria-labelledby`).
 */
export function SettingRow({ icon, title, titleId, description, control, children }: {
  icon: ReactNode;
  title: string;
  titleId?: string;
  description?: ReactNode;
  control?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <li className="flex gap-3 px-4 py-3">
      <span className="mt-0.5 shrink-0 text-text-muted" aria-hidden="true">{icon}</span>
      <div className="flex min-w-0 flex-1 flex-col gap-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p id={titleId} className="text-sm font-medium text-text">{title}</p>
            {description && <div className="mt-0.5 text-xs text-text-dim">{description}</div>}
          </div>
          {control && <div className="flex shrink-0 items-center gap-2">{control}</div>}
        </div>
        {children}
      </div>
    </li>
  );
}

const FILE_LABELS: Record<string, string> = {
  'settings.yaml': 'settings.yaml',
  'config.yaml': 'config.yaml on this machine',
  'config.local.yaml': 'config.local.yaml on this machine',
};

/** Where the changes of a row are saved, in one small line. */
export function FileNote({ fields }: { fields: SettingField<unknown>[] }) {
  const files = [...new Set(fields.filter(canEdit).map((f) => f.file).filter((f): f is string => !!f))];
  if (files.length === 0) return null;
  return (
    <p className="text-2xs text-text-faint">
      Saved in {files.map((f) => FILE_LABELS[f] ?? f).join(' and ')}
    </p>
  );
}

/** Why a field set elsewhere cannot change here. */
export function ManagedNote({ field }: { field: SettingField<unknown> }) {
  if (field.state !== 'managed' || !field.reason) return null;
  return (
    <p className="flex items-start gap-1.5 text-xs text-text-dim">
      <Lock size={12} className="mt-0.5 shrink-0" aria-hidden="true" />
      <span>{field.reason}</span>
    </p>
  );
}

/** "Test arrived" or "Test did not arrive" on a row, after a test. */
export function TestResult({ channel }: { channel: string }) {
  const entry = useSettingsStore((s) => s.testResult?.results.find((r) => r.channel === channel));
  if (!entry) return null;
  return (
    <div>
      {entry.delivered
        ? <Badge tone="success"><Check size={11} aria-hidden="true" />Test arrived</Badge>
        : <Badge tone="danger"><X size={11} aria-hidden="true" />Test did not arrive</Badge>}
    </div>
  );
}

const LEVELS: readonly NotificationLevel[] = ['all', 'important'];
const LEVEL_LABELS: Record<NotificationLevel, string> = {
  all: 'Everything',
  important: 'Questions and important',
};
const never = () => false;

/** "Everything" or "Questions and important", as a radio group of pills. */
export function LevelChoice({ value, disabled, busy, onChange }: {
  value: NotificationLevel;
  disabled: boolean;
  busy: boolean;
  onChange: (level: NotificationLevel) => void;
}) {
  const labelId = useId();
  const choose = (level: NotificationLevel) => {
    if (!busy && level !== value) onChange(level);
  };
  return (
    <div className="flex flex-col gap-1.5">
      <span id={labelId} className="text-xs font-medium text-text-muted">What to send</span>
      <div role="radiogroup" aria-labelledby={labelId} aria-busy={busy || undefined} className="flex flex-wrap gap-1.5">
        {LEVELS.map((level) => (
          <Button
            key={level}
            variant="pill"
            size="sm"
            role="radio"
            aria-checked={value === level}
            active={value === level}
            disabled={disabled}
            tabIndex={radioTabIndex(LEVELS, value, level, never)}
            onClick={() => choose(level)}
            onKeyDown={(e) => {
              const target = radioTarget(LEVELS, level, e.key, never);
              if (target === null) return;
              e.preventDefault();
              choose(target);
              focusRadio(e.currentTarget, LEVELS.indexOf(target));
            }}
          >
            {LEVEL_LABELS[level]}
          </Button>
        ))}
      </div>
    </div>
  );
}

/**
 * A text field that saves on Enter or when it loses focus, if the text
 * changed. Escape puts the saved value back. Remount it (`key`) when the
 * saved value changes, so the field shows the new one.
 */
export function CommitField({ label, value, help, error, disabled, busy, placeholder, inputMode, type, unit, onCommit }: {
  label: string;
  value: string;
  help?: ReactNode;
  error: string | null;
  disabled: boolean;
  busy: boolean;
  placeholder?: string;
  inputMode?: 'text' | 'numeric';
  /** `time` gives the browser's time picker; its value is `HH:MM`. */
  type?: 'text' | 'time';
  /** A word after the field, such as "hours". */
  unit?: string;
  onCommit: (text: string) => void;
}) {
  const [draft, setDraft] = useState(value);
  const commit = () => {
    if (!busy && draft.trim() !== value) onCommit(draft.trim());
  };
  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') { e.preventDefault(); commit(); }
    if (e.key === 'Escape') setDraft(value);
  };
  return (
    <Field
      label={<span className="text-xs text-text-muted">{label}</span>}
      help={help}
      error={error}
      trailing={unit ? <span className="ml-1 text-sm text-text-muted">{unit}</span> : undefined}
    >
      <TextField
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={onKeyDown}
        disabled={disabled}
        aria-busy={busy || undefined}
        placeholder={placeholder}
        inputMode={inputMode}
        type={type}
        className={unit ? 'w-24' : type === 'time' ? 'w-32' : 'w-full max-w-xs'}
      />
    </Field>
  );
}
