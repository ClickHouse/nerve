import { useId, useState, type ButtonHTMLAttributes, type ReactNode, type Ref } from 'react';
import { Check, ChevronDown, ChevronUp, MessageCircleQuestion } from '../../ui/icons';
import { Badge, Button } from '../../ui';
import { InlineMarkdown } from '../MarkdownContent';

/**
 * The parts the question cards share: `QuestionBlock` (the CLI's
 * `AskUserQuestion`) and `FormBlock` (Nerve's `ask_form`).
 */

function optionClass(resolved: boolean, selected: boolean): string {
  const state = resolved
    ? selected
      ? 'border-accent/40 bg-accent/10 cursor-default'
      : 'border-surface-raised bg-bg-sunken opacity-40 cursor-default'
    : selected
      ? 'border-accent/50 bg-accent/10 cursor-pointer'
      : 'border-border-subtle bg-bg-sunken hover:border-border hover:bg-surface cursor-pointer';
  return `question-option w-full text-left px-3.5 py-2.5 rounded-md border transition-all duration-150 ${state}`;
}

function Indicator({ multiSelect, selected }: { multiSelect: boolean; selected: boolean }) {
  return (
    <div className={`mt-0.5 shrink-0 w-4 h-4 ${multiSelect ? 'rounded-sm' : 'rounded-full'} border flex items-center justify-center transition-colors duration-150 ${
      selected ? 'border-accent bg-accent' : 'border-text-faint bg-transparent'
    }`}>
      {selected && <Check size={10} className="text-on-accent" />}
    </div>
  );
}

export interface OptionButtonProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children' | 'className' | 'type' | 'disabled'> {
  /** A square indicator (pick any) rather than a round one (pick one). */
  multiSelect: boolean;
  selected: boolean;
  /** The question is answered or closed: the row only shows the answer. */
  resolved: boolean;
  label: ReactNode;
  /** Inline markdown, without links (a link in a button is a control in a control). */
  description?: string;
  ref?: Ref<HTMLButtonElement>;
}

/**
 * One option: a row that toggles (`aria-pressed`), with a round or square
 * indicator. Buttons rather than radios and checkboxes: there is no roving
 * focus or arrow-key model here, and every row is its own tab stop.
 */
export function OptionButton({ multiSelect, selected, resolved, label, description, ref, ...rest }: OptionButtonProps) {
  return (
    <button
      ref={ref}
      type="button"
      disabled={resolved}
      aria-pressed={selected}
      className={optionClass(resolved, selected)}
      {...rest}
    >
      <div className="flex items-start gap-3">
        <Indicator multiSelect={multiSelect} selected={selected} />
        <div className="flex-1 min-w-0">
          <div className={`text-sm font-medium ${selected ? 'text-accent-text' : 'text-text-secondary'}`}>
            {label}
          </div>
          {description && (
            <div className="text-xs text-text-muted mt-0.5 leading-relaxed">
              <InlineMarkdown content={description} links={false} />
            </div>
          )}
        </div>
      </div>
    </button>
  );
}

/** A question's header line: its chip, a required mark, a note on the right. */
export function QuestionHeader({ label, note, required = false }: {
  label: string;
  note?: string;
  required?: boolean;
}) {
  return (
    <div className="flex items-center gap-2 mb-2">
      <MessageCircleQuestion size={15} className="text-accent shrink-0" />
      <Badge tone="accent" className="px-2 font-semibold uppercase tracking-wider">
        {label}
      </Badge>
      {required && <span className="text-xs leading-none text-error" aria-hidden="true" title="Required">*</span>}
      {note && <span className="text-2xs text-text-faint ml-auto">{note}</span>}
    </div>
  );
}

/** No answer: a dash to the eye, words to a screen reader. */
export function NoAnswer() {
  return (
    <span className="text-text-faint">
      <span aria-hidden="true">—</span>
      <span className="sr-only">No answer</span>
    </span>
  );
}

export interface AnswerRow {
  key: string;
  /** The question's chip. */
  label: string;
  /** A list shows as chips; null (or empty) as "—". */
  answer: string | string[] | null;
}

function AnswerValue({ answer }: { answer: AnswerRow['answer'] }) {
  if (answer === null || (Array.isArray(answer) ? answer.length === 0 : !answer.trim())) return <NoAnswer />;
  if (Array.isArray(answer)) {
    return (
      <span className="min-w-0 flex flex-wrap gap-1">
        {answer.map((item, i) => (
          <Badge key={i} size="sm" className="max-w-full whitespace-normal break-all">{item}</Badge>
        ))}
      </span>
    );
  }
  return <span className="min-w-0 truncate" title={answer}>{answer}</span>;
}

/**
 * An answered (or closed) question card, kept small in the transcript: one
 * line per question, `LABEL → answer`. "Show questions" opens the full card
 * (`children`, read-only) in its place; "Hide" folds it again.
 */
export function AnsweredCard({ rows, closed, closedReason, children }: {
  rows: AnswerRow[];
  /** It ended without an answer: timed out, closed, the session stopped. */
  closed: boolean;
  /** Why, as a tooltip on "Closed". */
  closedReason?: string;
  children: ReactNode;
}) {
  const [expanded, setExpanded] = useState(false);
  const bodyId = useId();
  return (
    <div className="my-2 border border-border rounded-lg bg-surface overflow-hidden">
      <div id={bodyId}>
        {expanded ? children : (
          <dl className="px-3 py-2.5 space-y-1.5">
            {rows.map(row => (
              <div key={row.key} className="flex items-baseline gap-2 min-w-0">
                <dt className="shrink-0">
                  <Badge tone="accent" className="font-semibold uppercase tracking-wider">{row.label}</Badge>
                </dt>
                <dd className="flex-1 min-w-0 flex items-baseline gap-2 text-sm text-text-secondary">
                  <span aria-hidden="true" className="shrink-0 text-text-faint">→</span>
                  <AnswerValue answer={row.answer} />
                </dd>
              </div>
            ))}
          </dl>
        )}
      </div>
      <div className="px-3 py-1.5 border-t border-border-subtle flex items-center gap-2">
        <Check size={12} className={closed ? 'text-text-faint' : 'text-success'} aria-hidden="true" />
        <span className={`text-xs ${closed ? 'text-text-faint' : 'text-success'}`} title={closed ? closedReason : undefined}>
          {closed ? 'Closed' : 'Answered'}
        </span>
        <Button
          variant="ghost"
          size="xs"
          className="ml-auto"
          aria-expanded={expanded}
          aria-controls={bodyId}
          onClick={() => setExpanded(open => !open)}
        >
          {expanded ? <ChevronUp size={12} aria-hidden="true" /> : <ChevronDown size={12} aria-hidden="true" />}
          {expanded ? 'Hide' : 'Show questions'}
        </Button>
      </div>
    </div>
  );
}
