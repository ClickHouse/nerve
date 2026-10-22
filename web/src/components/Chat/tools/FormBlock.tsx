import {
  useId, useLayoutEffect, useMemo, useRef, useState, type ClipboardEvent, type KeyboardEvent, type ReactNode,
} from 'react';
import { ListChecks, Send, X } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { useChatStore } from '../../../stores/chatStore';
import { Badge, Button, TextArea, TextField } from '../../ui';
import { InlineMarkdown } from '../MarkdownContent';
import { formatCombo } from '../../../utils/keyboard';
import { AnsweredCard, NoAnswer, OptionButton, QuestionHeader, type AnswerRow } from './QuestionCard';
import {
  addItems, answersOf, couldBecomeUrl, draftFor, firstLine, formQuestions, formTitle, isValidRepo, isValidUrl,
  MAX_ANSWER_LENGTH, parseFormResult, problemOf, resultText, sameIds, splitItems,
  type Draft, type FormAnswers, type FormQuestion,
} from './formQuestions';

/** The form's name when the call gives it no title. */
const UNTITLED = 'Questions';
const OTHER_LABEL = 'Other…';
const SUBMIT_KEYS = formatCombo({ mod: true, key: 'Enter' });
const URL_PROBLEM = 'A full address, starting with https:// or http://, without spaces.';
const REPOS_PROBLEM = 'Write each repository as owner/name, like org/repo-a.';

/**
 * Nerve's `ask_form` in the chat: a few questions of mixed kinds, answered
 * together (see `formQuestions.ts` for the kinds and the answer shape).
 *
 * - The card takes answers only while this form waits (a `form`
 *   interaction asking the same questions). It locks when the wait ends —
 *   answered here, in another tab, timed out — and after a reload it shows
 *   the answers from the tool result.
 * - Submit is enabled once every required question has an answer and none
 *   is invalid: an address that is not http(s), a repository that is not
 *   owner/name, an Other… ticked beside options with nothing typed.
 *   Cmd/Ctrl+Enter submits from anywhere in the card. Escape never leaves
 *   the card: the app-wide Escape stops a running turn, and the turn is
 *   running — it waits for this form.
 * - Answered, the card folds to one line per question; "Show questions"
 *   opens it, read-only.
 */
export function FormBlock({ block, fallback = null }: {
  block: ToolCallBlockData;
  /** Shown instead when the call holds no question to show. */
  fallback?: ReactNode;
}) {
  const own = useMemo(() => formQuestions(block.input), [block.input]);
  const pendingInput = useChatStore(s => (
    s.pendingInteraction?.interactionType === 'form' ? s.pendingInteraction.toolInput : null
  ));
  const live = useMemo(() => (pendingInput ? formQuestions(pendingInput) : []), [pendingInput]);
  // The waiting form is this card's when the call still runs and asks the
  // same questions (the server passes ids through verbatim), so a card left
  // over from a stopped turn cannot answer a newer form. A running call
  // whose input does not parse takes the server's copy, which is the one
  // that waits (with its defaults filled in).
  const running = block.status === 'running' && block.result === undefined;
  const mine = running && live.length > 0 && (own.length === 0 || sameIds(own, live));
  // Latch the waiting form once seen: when the wait ends (answered here or in
  // another tab) the card locks and keeps these questions. Set during render,
  // React's pattern for keeping something from an earlier render.
  const [seen, setSeen] = useState<FormQuestion[] | null>(null);
  if (mine && seen === null) setSeen(live);
  const [submitted, setSubmitted] = useState<{ questions: FormQuestion[]; answers: FormAnswers } | null>(null);
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [touched, setTouched] = useState<ReadonlySet<string>>(() => new Set());
  const [openedOther, setOpenedOther] = useState<string | null>(null);

  const questions = submitted?.questions ?? (mine ? live : seen ?? own);
  if (questions.length === 0) return <>{fallback}</>;
  const title = formTitle(block.input) || (mine ? formTitle(pendingInput) : '');

  // Finished (history), a result in, answered here, or the wait seen and over.
  const resolved = submitted !== null || block.status === 'complete' || block.result !== undefined
    || (seen !== null && !mine);
  if (resolved) {
    // An error (timed out, closed, the session stopped, an invalid call), or
    // a finished call with no result at all, holds no answers.
    const closed = !!block.isError || (block.status === 'complete' && block.result === undefined && !submitted);
    const answers: FormAnswers = closed ? {} : parseFormResult(block.result, questions)?.answers ?? submitted?.answers ?? {};
    const rows: AnswerRow[] = questions.map(q => ({ key: q.id, label: q.label, answer: answers[q.id] ?? null }));
    return (
      <AnsweredCard
        rows={rows}
        closed={closed}
        closedReason={block.isError ? firstLine(resultText(block.result ?? '')) : undefined}
      >
        <FormFields title={title} questions={questions} draftOf={q => draftFor(q, answers[q.id], true)} />
      </AnsweredCard>
    );
  }

  const draftOf = (q: FormQuestion) => drafts[q.id] ?? draftFor(q, q.defaultValue);
  const editor: Editor = {
    update: (q, change) => setDrafts(prev => ({ ...prev, [q.id]: change(prev[q.id] ?? draftFor(q, q.defaultValue)) })),
    touched,
    touch: id => setTouched(prev => (prev.has(id) ? prev : new Set(prev).add(id))),
    openedOther,
    openOther: setOpenedOther,
  };
  const canSubmit = mine && questions.every(q => problemOf(q, draftOf(q)) === null);

  const submit = () => {
    // Only while this form waits. Without a wait it is answered already
    // (reload, another tab), and the answer must not go anywhere else.
    if (!canSubmit || useChatStore.getState().pendingInteraction?.interactionType !== 'form') return;
    const answers = answersOf(questions, draftOf);
    setSubmitted({ questions, answers });
    useChatStore.getState().answerInteraction({ answers });
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key === 'Escape') {
      // Kept in the card, whatever has the focus: the turn waits for it.
      e.preventDefault();
      e.stopPropagation();
    } else if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault();
      e.stopPropagation();
      submit();
    }
  };

  return (
    <div className="question-block my-2">
      <div
        role="form"
        aria-label={title || UNTITLED}
        onKeyDown={onKeyDown}
        className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden"
      >
        <FormFields title={title} questions={questions} draftOf={draftOf} editor={editor} />
        <div className="px-3 pb-3">
          <Button
            variant="primary"
            size="md"
            fullWidth
            disabled={!canSubmit}
            onClick={submit}
            aria-keyshortcuts="Meta+Enter Control+Enter"
          >
            <Send size={13} />
            Submit
            <kbd aria-hidden="true" className="ml-1 font-sans text-2xs opacity-70">{SUBMIT_KEYS}</kbd>
          </Button>
        </div>
      </div>
    </div>
  );
}

/** What the fields of a live form change through; absent, they are read-only. */
interface Editor {
  update: (q: FormQuestion, change: (d: Draft) => Draft) => void;
  /** Questions whose field was left once (an address is checked from then on). */
  touched: ReadonlySet<string>;
  touch: (id: string) => void;
  /** The question whose Other… was just chosen: its field takes the focus. */
  openedOther: string | null;
  openOther: (id: string) => void;
}

function FormFields({ title, questions, draftOf, editor }: {
  title: string;
  questions: FormQuestion[];
  draftOf: (q: FormQuestion) => Draft;
  editor?: Editor;
}) {
  return (
    <>
      {title && (
        <div className="px-4 pt-3.5 pb-1 flex items-center gap-2">
          <ListChecks size={15} className="text-accent shrink-0" aria-hidden="true" />
          <span className="text-sm leading-tight font-medium text-text">{title}</span>
        </div>
      )}
      {questions.map((q, i) => (
        <QuestionField key={q.id} q={q} draft={draftOf(q)} editor={editor} divider={i > 0} />
      ))}
    </>
  );
}

interface FieldProps {
  q: FormQuestion;
  draft: Draft;
  editor?: Editor;
  /** The question sentence: the field's name. */
  questionId: string;
  helpId?: string;
  /** For the field's problem message, when it shows one. */
  problemId: string;
}

function describedBy(...ids: Array<string | false | undefined>): string | undefined {
  return ids.filter(Boolean).join(' ') || undefined;
}

function QuestionField({ q, draft, editor, divider }: {
  q: FormQuestion;
  draft: Draft;
  editor?: Editor;
  divider: boolean;
}) {
  const idBase = useId();
  const questionId = `${idBase}-question`;
  const helpId = q.help ? `${idBase}-help` : undefined;
  const note = [q.kind === 'multi' && 'Select multiple', !q.required && 'Optional'].filter(Boolean).join(' · ');
  const field: FieldProps = { q, draft, editor, questionId, helpId, problemId: `${idBase}-problem` };
  return (
    <div
      role="group"
      aria-labelledby={questionId}
      aria-describedby={helpId}
      className={divider ? 'border-t border-border-subtle' : ''}
    >
      <div className="px-4 pt-3.5 pb-2">
        <QuestionHeader label={q.label} note={note || undefined} required={q.required && !!editor} />
        <p id={questionId} className="text-sm text-text-secondary leading-relaxed">
          <InlineMarkdown content={q.question} />
        </p>
        {q.help && (
          <p id={helpId} className="mt-1 text-xs text-text-muted leading-relaxed">
            <InlineMarkdown content={q.help} />
          </p>
        )}
      </div>
      <div className="px-3 pb-3">
        {q.kind === 'choice' || q.kind === 'multi' ? <OptionsField {...field} />
          : q.kind === 'list' || q.kind === 'repos' ? <ItemsField {...field} />
            : q.kind === 'url' ? <UrlField {...field} />
              : <TextAnswerField {...field} />}
      </div>
    </div>
  );
}

/** A typed answer, read-only. */
function ReadOnlyText({ text }: { text: string }) {
  return (
    <p className="mx-1 text-sm text-text-secondary whitespace-pre-wrap break-words">
      {text.trim() ? text : <NoAnswer />}
    </p>
  );
}

/** choice: option rows, one answer; multi: any number. Other… unless the call turns it off. */
function OptionsField({ q, draft, editor }: FieldProps) {
  const multi = q.kind === 'multi';
  const otherRef = useRef<HTMLButtonElement>(null);
  const otherChosen = draft.other !== null;

  const choose = (i: number) => editor?.update(q, d => (multi
    ? { ...d, chosen: d.chosen.includes(i) ? d.chosen.filter(x => x !== i) : [...d.chosen, i].sort((a, b) => a - b) }
    // One answer: an option replaces the person's own words.
    : { ...d, chosen: [i], other: null }));

  const toggleOther = () => {
    if (!editor) return;
    if (!otherChosen) editor.openOther(q.id);
    editor.update(q, d => (multi
      // Own words are one more answer, on or off.
      ? { ...d, other: d.other === null ? '' : null }
      // Own words replace the options.
      : { ...d, chosen: [], other: d.other ?? '' }));
  };

  return (
    <div className="space-y-1.5">
      {q.options.map((option, i) => (
        <OptionButton
          key={i}
          multiSelect={multi}
          selected={draft.chosen.includes(i)}
          resolved={!editor}
          label={option.label}
          description={option.description}
          onClick={() => choose(i)}
        />
      ))}
      {(q.allowOther || otherChosen) && (
        <div>
          <OptionButton
            ref={otherRef}
            multiSelect={multi}
            selected={otherChosen}
            resolved={!editor}
            label={OTHER_LABEL}
            description={!otherChosen && editor ? 'Answer in your own words' : undefined}
            onClick={toggleOther}
          />
          {otherChosen && (editor ? (
            <TextField
              value={draft.other ?? ''}
              onChange={e => {
                const value = e.target.value;
                editor.update(q, d => ({ ...d, other: value }));
              }}
              onKeyDown={e => {
                // Back to Other…, the text kept; the card keeps the key in.
                if (e.key === 'Escape') otherRef.current?.focus();
              }}
              aria-label="Your answer"
              placeholder="Type your answer"
              // Beside options (multi) it must say something; on a choice it
              // is the answer.
              required={multi || q.required}
              maxLength={MAX_ANSWER_LENGTH}
              autoComplete="off"
              // Opened on purpose a moment ago, by Other… — not a prefilled default.
              autoFocus={editor.openedOther === q.id}
              className="mt-1.5"
            />
          ) : (
            <p className="mx-2 mt-1.5 text-sm text-text-secondary whitespace-pre-wrap break-words">{draft.other}</p>
          ))}
        </div>
      )}
    </div>
  );
}

/** text: a two-row box that grows with what is typed. Enter is a new line. */
function TextAnswerField({ q, draft, editor, questionId, helpId }: FieldProps) {
  const ref = useRef<HTMLTextAreaElement>(null);
  const value = draft.text;
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    // Fit the text; the class caps the height, and past it the box scrolls.
    el.style.height = 'auto';
    el.style.height = `${el.scrollHeight + el.offsetHeight - el.clientHeight}px`;
  }, [value]);
  if (!editor) return <ReadOnlyText text={value} />;
  return (
    <TextArea
      ref={ref}
      value={value}
      onChange={e => {
        const text = e.target.value;
        editor.update(q, d => ({ ...d, text }));
      }}
      rows={2}
      aria-labelledby={questionId}
      aria-describedby={helpId}
      required={q.required}
      placeholder={q.placeholder || undefined}
      maxLength={MAX_ANSWER_LENGTH}
      className="max-h-60 overflow-y-auto"
    />
  );
}

/** url: an http(s) address, checked as it is typed. */
function UrlField({ q, draft, editor, questionId, helpId, problemId }: FieldProps) {
  if (!editor) return <ReadOnlyText text={draft.text} />;
  const value = draft.text;
  // Not while it is still on its way to "https://", unless the field was left.
  const invalid = value.trim() !== '' && !isValidUrl(value) && (editor.touched.has(q.id) || !couldBecomeUrl(value));
  return (
    <div>
      <TextField
        type="url"
        inputMode="url"
        value={value}
        onChange={e => {
          const text = e.target.value;
          editor.update(q, d => ({ ...d, text }));
        }}
        onBlur={() => editor.touch(q.id)}
        aria-labelledby={questionId}
        aria-describedby={describedBy(helpId, invalid && problemId)}
        aria-invalid={invalid || undefined}
        required={q.required}
        placeholder={q.placeholder || 'https://'}
        maxLength={MAX_ANSWER_LENGTH}
        autoComplete="off"
        spellCheck={false}
        className={invalid ? 'border-error focus:border-error' : undefined}
      />
      {invalid && <p id={problemId} className="mt-1 px-1 text-xs text-error">{URL_PROBLEM}</p>}
    </div>
  );
}

/** Items as chips; on `repos`, one that is not owner/name is marked. */
function Chips({ items, repos, onRemove }: {
  items: string[];
  repos: boolean;
  /** Absent: read-only. */
  onRemove?: (index: number) => void;
}) {
  return items.map((item, i) => {
    const bad = repos && !isValidRepo(item);
    return (
      <Badge
        key={`${i}-${item}`}
        tone={bad ? 'danger' : 'neutral'}
        size="sm"
        className="max-w-full whitespace-normal break-all"
        title={bad ? 'Not owner/name' : undefined}
      >
        {item}
        {bad && <span className="sr-only"> (not owner/name)</span>}
        {onRemove && (
          <button
            type="button"
            aria-label={`Remove ${item}`}
            onClick={() => onRemove(i)}
            className="-mr-0.5 rounded opacity-60 hover:opacity-100 focus-visible:opacity-100 cursor-pointer"
          >
            <X size={11} aria-hidden="true" />
          </button>
        )}
      </Badge>
    );
  });
}

/** Text that ends items: a comma, or a line break (pasted). */
const ITEM_BREAK = /[,\r\n]/;

/**
 * list / repos: items as chips. A comma or Enter ends an item, Backspace in
 * the empty field takes the last one back, a pasted list is split at commas
 * and line breaks, and leaving the field keeps what is typed as an item.
 */
function ItemsField({ q, draft, editor, questionId, helpId, problemId }: FieldProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const repos = q.kind === 'repos';
  if (!editor) {
    return draft.items.length > 0
      ? <div className="flex flex-wrap gap-1.5 px-1"><Chips items={draft.items} repos={repos} /></div>
      : <ReadOnlyText text="" />;
  }
  const invalid = repos && draft.items.some(item => !isValidRepo(item));

  const commitTyped = () => editor.update(q, d => (
    d.text.trim() ? { ...d, items: addItems(d.items, splitItems(d.text), q.kind), text: '' } : d
  ));

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    // Not while an input method composes: its keys belong to the composition.
    if (e.nativeEvent.isComposing) return;
    if (e.key === 'Enter' && !e.metaKey && !e.ctrlKey) {
      // Enter ends the item; Cmd/Ctrl+Enter goes on to submit the form.
      e.preventDefault();
      commitTyped();
    } else if (e.key === 'Backspace' && draft.text === '' && draft.items.length > 0) {
      e.preventDefault();
      editor.update(q, d => ({ ...d, items: d.items.slice(0, -1) }));
    }
  };

  const onPaste = (e: ClipboardEvent<HTMLInputElement>) => {
    const pasted = e.clipboardData.getData('text');
    if (!ITEM_BREAK.test(pasted)) return;
    // A pasted list: every part an item at once, the last one too.
    e.preventDefault();
    const input = e.currentTarget;
    const text = draft.text.slice(0, input.selectionStart ?? draft.text.length)
      + pasted + draft.text.slice(input.selectionEnd ?? draft.text.length);
    editor.update(q, d => ({ ...d, items: addItems(d.items, splitItems(text), q.kind), text: '' }));
  };

  return (
    <div>
      <div
        onClick={e => { if (e.target === e.currentTarget) inputRef.current?.focus(); }}
        className={`flex flex-wrap items-center gap-1.5 px-2 py-1.5 rounded-lg border bg-surface-raised transition-colors cursor-text ${
          invalid ? 'border-error' : 'border-border-subtle focus-within:border-accent/50'
        }`}
      >
        <Chips
          items={draft.items}
          repos={repos}
          onRemove={i => editor.update(q, d => ({ ...d, items: d.items.filter((_, j) => j !== i) }))}
        />
        <input
          ref={inputRef}
          value={draft.text}
          onChange={e => {
            const value = e.target.value;
            if (ITEM_BREAK.test(value)) {
              // A comma ends an item; what follows it is the next one, typed on.
              const parts = value.split(ITEM_BREAK);
              const rest = parts.pop() ?? '';
              editor.update(q, d => ({ ...d, items: addItems(d.items, parts, q.kind), text: rest.trimStart() }));
            } else {
              editor.update(q, d => ({ ...d, text: value }));
            }
          }}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
          onBlur={commitTyped}
          aria-labelledby={questionId}
          aria-describedby={describedBy(helpId, invalid && problemId)}
          aria-invalid={invalid || undefined}
          // The question is: the field itself is empty once its text is an item.
          aria-required={q.required || undefined}
          placeholder={draft.items.length === 0 ? q.placeholder || (repos ? 'owner/name, then Enter' : 'Type, then Enter') : undefined}
          maxLength={MAX_ANSWER_LENGTH}
          autoComplete="off"
          spellCheck={!repos}
          className="flex-1 min-w-32 px-1 py-0.5 bg-transparent border-0 outline-none text-sm text-text placeholder:text-text-faint"
        />
      </div>
      {invalid && <p id={problemId} className="mt-1 px-1 text-xs text-error">{REPOS_PROBLEM}</p>}
    </div>
  );
}
