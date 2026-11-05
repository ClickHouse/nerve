import { useId, useState } from 'react';
import { Send } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { InlineMarkdown, MarkdownContent } from '../MarkdownContent';
import { useChatStore } from '../../../stores/chatStore';
import { Button, TextField } from '../../ui';
import { AnsweredCard, OptionButton, QuestionHeader, type AnswerRow } from './QuestionCard';

interface QuestionOption {
  label: string;
  description: string;
  markdown?: string;
}

interface Question {
  question: string;
  header: string;
  options: QuestionOption[];
  multiSelect: boolean;
}

/** The free-text choice every question gets, as the terminal offers "Other". */
const OTHER_LABEL = 'Other…';

interface ParsedAnswers {
  /** Per question, the options chosen. */
  chosen: Map<number, Set<number>>;
  /** Per question, the person's own words (text that matches no option). */
  own: Map<number, string>;
}

// Recover the answers from the persisted tool_result, whose shape is
// `... "question"="label, label" ...`, so a reloaded poll re-highlights the
// answer. Labels are matched whole (longest first, so a label that itself
// holds ", " still matches); what is left is the person's own answer.
function parseAnswers(result: string, questions: Question[]): ParsedAnswers {
  const chosen = new Map<number, Set<number>>();
  const own = new Map<number, string>();
  questions.forEach((q, qIdx) => {
    const needle = `"${q.question}"="`;
    const start = result.indexOf(needle);
    if (start === -1) return;
    const valStart = start + needle.length;
    const valEnd = result.indexOf('"', valStart);
    if (valEnd === -1) return;
    let rest = `, ${result.slice(valStart, valEnd)}, `;
    const set = new Set<number>();
    const byLength = q.options.map((opt, oIdx) => ({ label: opt.label, oIdx }))
      .sort((a, b) => b.label.length - a.label.length);
    for (const { label, oIdx } of byLength) {
      const token = `, ${label}, `;
      if (rest.includes(token)) {
        set.add(oIdx);
        rest = rest.replace(token, ', ');
      }
    }
    const text = rest.slice(2, -2).trim();
    if (set.size > 0) chosen.set(qIdx, set);
    if (text) own.set(qIdx, text);
  });
  return { chosen, own };
}

export function QuestionBlock({ block }: { block: ToolCallBlockData }) {
  // A question may come without options (a free-text kind): then Other… is
  // the whole answer.
  const questions = ((block.input.questions as Question[]) || []).map(q => ({ ...q, options: q.options ?? [] }));
  // Per-question selections: Map<questionIndex, Set<optionIndex>>
  const [selections, setSelections] = useState<Map<number, Set<number>>>(new Map());
  // Per question answered with "Other…": the person's own words ('' until typed).
  const [own, setOwn] = useState<Map<number, string>>(new Map());
  const [submitted, setSubmitted] = useState(false);
  const [hoveredOption, setHoveredOption] = useState<{ q: number; o: number } | null>(null);
  const questionPending = useChatStore(s => s.pendingInteraction?.interactionType === 'question');
  // Latch that a live question prompt was seen; once it clears (answered here,
  // by a parallel client, or before reconnect replay) the form must lock.
  // Set during render, React's pattern for keeping something from an
  // earlier render.
  const [seenPending, setSeenPending] = useState(false);
  if (questionPending && !seenPending) setSeenPending(true);
  const idBase = useId();

  if (questions.length === 0) return null;

  const isSingleSimple = questions.length === 1 && !questions[0].multiSelect;

  // A persisted tool_result means the interaction is already resolved — render
  // read-only so a reloaded session shows the answer instead of re-prompting.
  const parsed = block.result !== undefined ? parseAnswers(block.result, questions) : null;
  const isResolved = submitted || parsed !== null || (seenPending && !questionPending);
  // Prefer parsed answers, but fall back to live ones when the result string
  // didn't parse (e.g. a quote in a label) so the highlight isn't lost.
  const hasParsed = parsed !== null && (parsed.chosen.size > 0 || parsed.own.size > 0);
  const effSelections = hasParsed ? parsed.chosen : selections;
  const effOwn = hasParsed ? parsed.own : own;

  const submitAnswers = (sel = selections, words = own) => {
    // Answer only when a live question interaction is pending. Without one the
    // poll is already resolved (reload, or answered by a parallel client), so
    // never fall back to posting the answer as a fresh chat message.
    const state = useChatStore.getState();
    if (state.pendingInteraction?.interactionType !== 'question') return;

    setSubmitted(true);
    // Build answers dict for the SDK: { questionText: "label, label, own words" }
    const answers: Record<string, string> = {};
    questions.forEach((q, i) => {
      const parts = Array.from(sel.get(i) ?? []).map(o => q.options[o].label);
      const text = words.get(i)?.trim();
      if (text) parts.push(text);
      if (parts.length > 0) answers[q.question] = parts.join(', ');
    });
    state.answerInteraction(answers);
  };

  const handleSelect = (qIdx: number, oIdx: number) => {
    if (isResolved) return;
    // Single question + single select: the click is the answer.
    if (isSingleSimple) {
      const only = new Map([[qIdx, new Set([oIdx])]]);
      setSelections(only);
      setOwn(new Map());
      submitAnswers(only, new Map());
      return;
    }
    const q = questions[qIdx];
    setSelections(prev => {
      const next = new Map(prev);
      if (q.multiSelect) {
        const current = new Set(prev.get(qIdx) || []);
        if (current.has(oIdx)) current.delete(oIdx);
        else current.add(oIdx);
        next.set(qIdx, current);
      } else {
        next.set(qIdx, new Set([oIdx]));
      }
      return next;
    });
    // Single select: an option replaces the person's own answer.
    if (!q.multiSelect) {
      setOwn(prev => {
        const next = new Map(prev);
        next.delete(qIdx);
        return next;
      });
    }
  };

  const handleOther = (qIdx: number) => {
    if (isResolved) return;
    if (questions[qIdx].multiSelect) {
      // Multi select: own words are one more answer, on or off.
      setOwn(prev => {
        const next = new Map(prev);
        if (next.has(qIdx)) next.delete(qIdx);
        else next.set(qIdx, '');
        return next;
      });
      return;
    }
    // Single select: own words replace the options.
    setSelections(prev => {
      const next = new Map(prev);
      next.delete(qIdx);
      return next;
    });
    setOwn(prev => (prev.has(qIdx) ? prev : new Map(prev).set(qIdx, '')));
  };

  const setOwnText = (qIdx: number, text: string) => {
    setOwn(prev => new Map(prev).set(qIdx, text));
  };

  // Answered: an option, or own words — and own words, once chosen, must
  // say something.
  const isAnswered = (i: number) => {
    const text = own.get(i);
    if (text !== undefined) return text.trim().length > 0;
    return (selections.get(i)?.size ?? 0) > 0;
  };
  const allAnswered = questions.every((_q, i) => isAnswered(i));

  // The questions with their options: the live poll, or (resolved) the
  // full card behind the summary, showing what was answered.
  const questionList = questions.map((q, qIdx) => {
    const questionId = `${idBase}-q${qIdx}`;
    const ownText = effOwn.get(qIdx);
    const otherChosen = ownText !== undefined;
    return (
      <div
        key={qIdx}
        role="group"
        aria-labelledby={questionId}
        className={qIdx > 0 ? 'border-t border-border-subtle' : ''}
      >
        {/* Question header */}
        <div className="px-4 pt-3.5 pb-2">
          <QuestionHeader label={q.header} note={q.multiSelect ? 'Select multiple' : undefined} />
          <p id={questionId} className="text-sm text-text-secondary leading-relaxed">
            <InlineMarkdown content={q.question} />
          </p>
        </div>

        {/* Options */}
        <div className="px-3 pb-3 space-y-1.5">
          {q.options.map((opt, oIdx) => {
            const isSelected = effSelections.get(qIdx)?.has(oIdx) || false;
            const isHovered = hoveredOption?.q === qIdx && hoveredOption?.o === oIdx;

            return (
              <div key={oIdx}>
                <OptionButton
                  multiSelect={q.multiSelect}
                  selected={isSelected}
                  resolved={isResolved}
                  label={opt.label}
                  description={opt.description}
                  onClick={() => handleSelect(qIdx, oIdx)}
                  onMouseEnter={() => setHoveredOption({ q: qIdx, o: oIdx })}
                  onMouseLeave={() => setHoveredOption(null)}
                />

                {opt.markdown && (isHovered || (isSelected && !isResolved)) && (
                  <div className="mx-2 mt-1 mb-0.5 px-3 py-2 bg-bg border border-border-subtle rounded text-xs max-h-48 overflow-y-auto">
                    <MarkdownContent content={opt.markdown} />
                  </div>
                )}
              </div>
            );
          })}

          {/* Other…: the person's own words */}
          <div>
            <OptionButton
              multiSelect={q.multiSelect}
              selected={otherChosen}
              resolved={isResolved}
              label={OTHER_LABEL}
              description={!otherChosen && !isResolved ? 'Answer in your own words' : undefined}
              onClick={() => handleOther(qIdx)}
            />
            {otherChosen && (isResolved ? (
              <p className="mx-2 mt-1.5 text-sm text-text-secondary whitespace-pre-wrap">{ownText}</p>
            ) : (
              <TextField
                value={ownText}
                onChange={e => setOwnText(qIdx, e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Escape') {
                    // Escape stays here: the app-wide Escape stops a
                    // running turn, and the turn is running — it waits
                    // for this answer. It only leaves the field, back to
                    // Other… (the button above it); the text stays.
                    e.preventDefault();
                    e.stopPropagation();
                    e.currentTarget.parentElement?.querySelector('button')?.focus();
                  } else if (e.key === 'Enter' && !e.nativeEvent.isComposing && allAnswered) {
                    // Not while an input method composes: its Enter
                    // confirms the composition, not the answer.
                    e.preventDefault();
                    submitAnswers();
                  }
                }}
                aria-label="Your answer"
                placeholder="Type your answer"
                required
                autoComplete="off"
                // Opened on purpose a moment ago, by Other….
                autoFocus
                className="mt-1.5"
              />
            ))}
          </div>
        </div>
      </div>
    );
  });

  // Answered, or closed without an answer: one line per question; the full
  // card opens on request. "Closed" when the interaction ended without an
  // answer (timeout / cancel / deny).
  if (isResolved) {
    const rows: AnswerRow[] = questions.map((q, i) => {
      const parts = [...(effSelections.get(i) ?? [])].sort((a, b) => a - b)
        .flatMap(o => (q.options[o] ? [q.options[o].label] : []));
      const words = effOwn.get(i)?.trim();
      if (words) parts.push(words);
      const answer = parts.length === 0 ? null : q.multiSelect ? parts : parts.join(', ');
      return { key: String(i), label: q.header, answer };
    });
    return <AnsweredCard rows={rows} closed={!!block.isError}>{questionList}</AnsweredCard>;
  }

  return (
    <div className="question-block my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        {questionList}

        {/* Submit button — shown for multi-question or multiSelect, and for a
            single simple question answered in the person's own words. */}
        {(!isSingleSimple || own.has(0)) && (
          <div className="px-3 pb-3">
            <Button
              variant="primary"
              size="md"
              fullWidth
              disabled={!allAnswered}
              onClick={() => submitAnswers()}
            >
              <Send size={13} />
              Submit
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
