import { useRef, useState, type KeyboardEvent } from 'react';
import { Check, Send } from '../ui/icons';
import { Badge, IconButton, TextField } from '../ui';
import { isTypingTarget } from '../../utils/keyboard';
import type { OptionItem } from './askOptions';

const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
/** Digits 1 to 9 choose the first nine rows, as the letters do. */
const DIGIT_ROWS = 9;
const OTHER_LABEL = 'Other…';
/** The Other… row of a question that has no options. */
const OWN_WORDS_LABEL = 'Answer in your own words…';

const LIST_CLASS = 'rounded-lg border border-border bg-bg-sunken divide-y divide-border overflow-hidden';
const ROW_CLASS = 'flex items-center gap-2.5 w-full min-h-10 px-3 py-2 text-left text-sm';
const BUTTON_ROW_CLASS = `${ROW_CLASS} cursor-pointer transition-colors hover:bg-surface `
  + 'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent/60 '
  + 'disabled:cursor-default disabled:hover:bg-transparent';

/** The row that a key chooses: a letter by its place in the alphabet, or a digit 1 to 9. -1 for none. */
function rowForKey(key: string): number {
  if (/^[a-z]$/i.test(key)) return key.toUpperCase().charCodeAt(0) - LETTERS.charCodeAt(0);
  if (/^[1-9]$/.test(key)) return Number(key) - 1;
  return -1;
}

/** The value of `aria-keyshortcuts` for the row at `index`. */
function shortcutsFor(index: number): string | undefined {
  return [LETTERS[index], index < DIGIT_ROWS ? String(index + 1) : ''].filter(Boolean).join(' ') || undefined;
}

function KeyHint({ index }: { index: number }) {
  const letter = LETTERS[index];
  if (!letter) return <span aria-hidden="true" className="shrink-0 w-5" />;
  return (
    <span
      aria-hidden="true"
      className="shrink-0 w-5 h-5 rounded border border-border-subtle bg-surface text-2xs font-medium text-text-muted flex items-center justify-center"
    >
      {letter}
    </span>
  );
}

function RowText({ option, chosen }: { option: OptionItem; chosen: boolean }) {
  return (
    <>
      <span className="flex-1 min-w-0 break-words">{option.label}</span>
      {option.recommended && <Badge tone="accent" className="shrink-0">Recommended</Badge>}
      {chosen && (
        <>
          <Check size={14} className="shrink-0 text-success" aria-hidden="true" />
          <span className="sr-only">(your answer)</span>
        </>
      )}
    </>
  );
}

export interface OptionListProps {
  options: OptionItem[];
  /** The accessible name of the list, for example the question. */
  label: string;
  /** Receives the chosen value. Without it the list is read-only. */
  onChoose?: (value: string) => void;
  /** A last row, "Other…", that opens a field for an answer in the person's own words. */
  allowOther?: boolean;
  /** The rows do not respond, for example while an answer is on its way. */
  disabled?: boolean;
  /** The answer, marked with a check. */
  chosen?: string | null;
}

/**
 * Answer options as full-width rows, each with a letter key hint (A, B, C).
 * While focus is in the list, a letter or a digit 1 to 9 chooses its row.
 * The options come in display order (see `questionOptions`).
 */
export function OptionList({ options, label, onChoose, allowOther = false, disabled = false, chosen = null }: OptionListProps) {
  const [otherOpen, setOtherOpen] = useState(false);
  const [text, setText] = useState('');
  const otherRef = useRef<HTMLButtonElement>(null);

  if (!onChoose) {
    return (
      <ul aria-label={label} className={LIST_CLASS}>
        {options.map((option, i) => (
          <li key={i} className={`${ROW_CLASS} ${option.value === chosen ? 'text-text' : 'text-text-muted'}`}>
            <KeyHint index={i} />
            <RowText option={option} chosen={option.value === chosen} />
          </li>
        ))}
      </ul>
    );
  }

  const send = () => {
    const answer = text.trim();
    if (answer && !disabled) onChoose(answer);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (disabled || e.ctrlKey || e.metaKey || e.altKey || isTypingTarget(e.target)) return;
    const row = rowForKey(e.key);
    if (row < 0 || row >= options.length + (allowOther ? 1 : 0)) return;
    // Handled here: no letter reaches the field that Other… opens, and no
    // page shortcut runs.
    e.preventDefault();
    e.stopPropagation();
    if (row < options.length) onChoose(options[row].value);
    else setOtherOpen(true);
  };

  return (
    <div role="group" aria-label={label} onKeyDown={onKeyDown} className={LIST_CLASS}>
      {options.map((option, i) => (
        <button
          key={i}
          type="button"
          disabled={disabled}
          aria-keyshortcuts={shortcutsFor(i)}
          onClick={() => onChoose(option.value)}
          className={`${BUTTON_ROW_CLASS} text-text`}
        >
          <KeyHint index={i} />
          <RowText option={option} chosen={option.value === chosen} />
        </button>
      ))}
      {allowOther && (
        <div>
          <button
            ref={otherRef}
            type="button"
            disabled={disabled}
            aria-expanded={otherOpen}
            aria-keyshortcuts={shortcutsFor(options.length)}
            onClick={() => setOtherOpen(open => !open)}
            className={`${BUTTON_ROW_CLASS} text-text-muted`}
          >
            <KeyHint index={options.length} />
            <span className="flex-1 min-w-0">{options.length ? OTHER_LABEL : OWN_WORDS_LABEL}</span>
          </button>
          {otherOpen && (
            <div className="flex items-center gap-2 px-3 pb-2.5">
              <TextField
                value={text}
                onChange={e => setText(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter' && !e.nativeEvent.isComposing) {
                    e.preventDefault();
                    send();
                  } else if (e.key === 'Escape') {
                    // Kept here: the app-wide Escape stops the running turn.
                    e.preventDefault();
                    e.stopPropagation();
                    setOtherOpen(false);
                    otherRef.current?.focus();
                  }
                }}
                disabled={disabled}
                aria-label="Your answer"
                placeholder="Type your answer"
                autoComplete="off"
                // Opened on purpose a moment ago, by Other….
                autoFocus
                className="flex-1 min-w-0"
              />
              <IconButton label="Send answer" variant="primary" onClick={send} disabled={disabled || !text.trim()}>
                <Send size={14} />
              </IconButton>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
