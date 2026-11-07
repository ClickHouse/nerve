import { useRef, useState, type KeyboardEvent } from 'react';
import { Check, Moon, Send, X, type Icon } from '../ui/icons';
import { Badge, IconButton, TextField } from '../ui';
import { isTypingTarget } from '../../utils/keyboard';
import type { OptionItem, OptionMark } from './askOptions';

const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
/** Digits 1 to 9 choose the first nine rows, as the letters do. */
const DIGIT_ROWS = 9;
const OTHER_LABEL = 'Other…';
/** The Other… row of a question that has no options. */
const OWN_WORDS_LABEL = 'Answer in your own words…';

const LIST_CLASS = 'rounded-lg border border-border bg-bg-sunken divide-y divide-border overflow-hidden';
const ROW_CLASS = 'flex items-center gap-2.5 w-full min-h-10 px-3 py-2 text-left text-sm';
const BUTTON_ROW_CLASS = `${ROW_CLASS} cursor-pointer transition-colors `
  + 'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent/60 '
  + 'disabled:cursor-default disabled:hover:bg-transparent';
const HINT_CLASS = 'shrink-0 w-5 h-5 rounded border flex items-center justify-center';
const PLAIN_HINT_CLASS = 'border-border-subtle bg-surface text-text-muted';

/** The look of an approval row: its icon, the label colour, the box of the icon and the hover fill. */
const MARKS: Record<OptionMark, { icon: Icon; text: string; hint: string; hover: string }> = {
  approve: {
    icon: Check, text: 'text-success', hint: 'border-success-border bg-success-bg text-success', hover: 'hover:bg-success-bg/50',
  },
  decline: {
    icon: X, text: 'text-error', hint: 'border-error-border bg-error-bg text-error', hover: 'hover:bg-error-bg/50',
  },
  snooze: { icon: Moon, text: 'text-text', hint: PLAIN_HINT_CLASS, hover: 'hover:bg-surface' },
};

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

/** The tooltip of an approval row, which shows its icon where other rows show their letter. */
function keyTitle(option: OptionItem, index: number): string | undefined {
  return option.mark && LETTERS[index] ? `Press ${LETTERS[index]}` : undefined;
}

/**
 * The box at the start of a row: the row's letter, or the icon of an approval
 * row. `muted` shows the icon without its colour.
 */
function KeyHint({ index, mark, muted = false }: { index: number; mark?: OptionMark; muted?: boolean }) {
  if (mark) {
    const { icon: MarkIcon, hint } = MARKS[mark];
    return (
      <span aria-hidden="true" className={`${HINT_CLASS} ${muted ? PLAIN_HINT_CLASS : hint}`}>
        <MarkIcon size={12} strokeWidth={2.5} />
      </span>
    );
  }
  const letter = LETTERS[index];
  if (!letter) return <span aria-hidden="true" className="shrink-0 w-5" />;
  return <span aria-hidden="true" className={`${HINT_CLASS} ${PLAIN_HINT_CLASS} text-2xs font-medium`}>{letter}</span>;
}

/** The icon of a given answer: the icon of an approval row in its colour, or a check. */
export function AnswerIcon({ mark }: { mark?: OptionMark }) {
  const { icon: MarkIcon, text } = mark ? MARKS[mark] : { icon: Check, text: 'text-success' };
  return <MarkIcon size={14} className={`shrink-0 mt-0.5 ${text}`} aria-hidden="true" />;
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
 * An approval row (Approve, Decline, Snooze) shows its icon and colour in
 * place of the letter. While focus is in the list, a letter or a digit 1 to 9
 * chooses its row.
 * The options come in display order (see `questionOptions`).
 */
export function OptionList({ options, label, onChoose, allowOther = false, disabled = false, chosen = null }: OptionListProps) {
  const [otherOpen, setOtherOpen] = useState(false);
  const [text, setText] = useState('');
  const otherRef = useRef<HTMLButtonElement>(null);

  if (!onChoose) {
    return (
      <ul aria-label={label} className={LIST_CLASS}>
        {options.map((option, i) => {
          const isChosen = option.value === chosen;
          const text = isChosen ? (option.mark ? MARKS[option.mark].text : 'text-text') : 'text-text-muted';
          return (
            <li key={i} className={`${ROW_CLASS} ${text}`}>
              <KeyHint index={i} mark={option.mark} muted={!isChosen} />
              <RowText option={option} chosen={isChosen} />
            </li>
          );
        })}
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
          title={keyTitle(option, i)}
          onClick={() => onChoose(option.value)}
          className={`${BUTTON_ROW_CLASS} ${option.mark ? `${MARKS[option.mark].text} ${MARKS[option.mark].hover}` : 'text-text hover:bg-surface'}`}
        >
          <KeyHint index={i} mark={option.mark} />
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
            className={`${BUTTON_ROW_CLASS} text-text-muted hover:bg-surface`}
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
