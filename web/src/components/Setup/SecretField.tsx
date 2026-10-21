import { useId, useState, type ReactNode } from 'react';
import type { SecretOut } from '../../api/client';
import { Button, IconButton, TextField } from '../ui';
import { Eye, EyeOff } from '../ui/icons';
import { MANAGED_SOURCE_HINT, MANAGED_SOURCE_TEXT } from './copy';
import { secretManaged } from './credentialSource';

/** Secondary controls reach the 44 px phone target without changing the desktop density. */
const TOUCH = 'min-h-[44px] md:min-h-0';

/**
 * A secret the server reports as `{configured, last4, source}` and never
 * reads back.
 *
 * Four states. Configured and untouched: "Configured · …ab12 — leave blank
 * to keep", with *Replace* and *Remove*. Replacing (or not configured): a
 * password input with an eye toggle. Removed: a note, with *Undo*. Managed
 * (`source` is the environment, a configuration repository or the channel
 * gateway): the line says who provides it and where it is changed, with no
 * Replace, no Remove and no input — the server would refuse the write.
 *
 * The value model: `''` means "untouched/blank" (omitted from the payload),
 * a string is a new value, and `null` means "remove" (sent as `null`).
 */
export function SecretField({
  label, help, error, secret, value, onChange, placeholder, autoComplete = 'off',
  disabled, removable = true, replaceable = true, startReplacing = false, onReplace, id: givenId,
}: {
  label: ReactNode;
  help?: ReactNode;
  error?: string | null;
  secret: SecretOut | undefined;
  value: string | null;
  onChange: (value: string | null) => void;
  placeholder?: string;
  autoComplete?: string;
  disabled?: boolean;
  removable?: boolean;
  /** Offer Replace on a configured value; off where a new value could not be stored anyway. */
  replaceable?: boolean;
  /** Open on the input even though a value is configured (a Replace already pressed elsewhere). */
  startReplacing?: boolean;
  /** Called when Replace is pressed, for a parent that reveals more on it. */
  onReplace?: () => void;
  id?: string;
}) {
  const generated = useId();
  const id = givenId ?? `secret-${generated}`;
  const helpId = `${id}-help`;
  const errorId = `${id}-error`;
  const [replacing, setReplacing] = useState(startReplacing);
  const [shown, setShown] = useState(false);
  const configured = !!secret?.configured;
  const managed = secretManaged(secret);
  const describedBy = [help ? helpId : null, error ? errorId : null].filter(Boolean).join(' ') || undefined;

  const showInput = !configured || replacing || (value !== null && value !== '');
  const removed = value === null;

  if (managed) {
    return (
      <div className="flex flex-col gap-1">
        <label htmlFor={id} className="text-sm font-medium text-text">{label}</label>
        <p id={id} className="text-sm text-text-muted" tabIndex={-1}>
          {MANAGED_SOURCE_TEXT[managed]}
          {secret?.last4 ? <> · <span className="font-mono">…{secret.last4}</span></> : null}
        </p>
        <p id={helpId} className="text-xs text-text-dim">{MANAGED_SOURCE_HINT[managed]}</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-1">
      <label htmlFor={id} className="text-sm font-medium text-text">{label}</label>
      {removed ? (
        <div className="flex items-center gap-2 text-sm text-text-muted" role="status">
          <span id={id} tabIndex={-1}>Will be removed when you continue.</span>
          <Button variant="link" size="sm" onClick={() => onChange('')} disabled={disabled} className={TOUCH}>Undo</Button>
        </div>
      ) : showInput ? (
        <div className="flex items-center gap-1">
          <TextField
            id={id}
            type={shown ? 'text' : 'password'}
            value={value ?? ''}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            autoComplete={autoComplete}
            aria-describedby={describedBy}
            aria-invalid={error ? true : undefined}
            disabled={disabled}
            spellCheck={false}
          />
          <IconButton
            label={shown ? 'Hide value' : 'Show value'}
            onClick={() => setShown((s) => !s)}
            disabled={disabled}
          >
            {shown ? <EyeOff size={16} /> : <Eye size={16} />}
          </IconButton>
          {configured && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => { setReplacing(false); onChange(''); }}
              disabled={disabled}
              className={TOUCH}
            >
              Keep current
            </Button>
          )}
        </div>
      ) : (
        <div className="flex flex-wrap items-center gap-2">
          <p id={id} className="text-sm text-text-muted" tabIndex={-1}>
            Configured{secret?.last4 ? <> · <span className="font-mono">…{secret.last4}</span></> : null}
            {replaceable ? ' — leave blank to keep' : null}
          </p>
          {replaceable && (
            <Button
              variant="secondary"
              size="xs"
              onClick={() => { setReplacing(true); onReplace?.(); }}
              disabled={disabled}
              className={TOUCH}
            >
              Replace
            </Button>
          )}
          {removable && (
            <Button variant="dangerGhost" size="xs" onClick={() => onChange(null)} disabled={disabled} className={TOUCH}>
              Remove
            </Button>
          )}
        </div>
      )}
      {help && <p id={helpId} className="text-xs text-text-dim">{help}</p>}
      {/* Announced when it appears, wherever focus is; focusable as the frame's fallback. */}
      {error && <p id={errorId} role="alert" tabIndex={-1} className="text-xs text-error outline-none">{error}</p>}
    </div>
  );
}
