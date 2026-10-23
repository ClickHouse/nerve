import { useId, useState } from 'react';
import { MessageCircleQuestion, Check, Send } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { InlineMarkdown, MarkdownContent } from '../MarkdownContent';
import { useChatStore } from '../../../stores/chatStore';
import { Badge, Button, TextField } from '../../ui';

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

  return (
    <div className="question-block my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        {questions.map((q, qIdx) => {
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
                <div className="flex items-center gap-2 mb-2">
                  <MessageCircleQuestion size={15} className="text-accent shrink-0" />
                  <Badge tone="accent" className="px-2 font-semibold uppercase tracking-wider">
                    {q.header}
                  </Badge>
                  {q.multiSelect && (
                    <span className="text-2xs text-text-faint ml-auto">Select multiple</span>
                  )}
                </div>
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
                      <button
                        type="button"
                        onClick={() => handleSelect(qIdx, oIdx)}
                        onMouseEnter={() => setHoveredOption({ q: qIdx, o: oIdx })}
                        onMouseLeave={() => setHoveredOption(null)}
                        disabled={isResolved}
                        aria-pressed={isSelected}
                        className={optionClass(isResolved, isSelected)}
                      >
                        <div className="flex items-start gap-3">
                          <Indicator multiSelect={q.multiSelect} selected={isSelected} />
                          <div className="flex-1 min-w-0">
                            <div className={`text-sm font-medium ${isSelected ? 'text-accent-text' : 'text-text-secondary'}`}>
                              {opt.label}
                            </div>
                            {opt.description && (
                              <div className="text-xs text-text-muted mt-0.5 leading-relaxed">
                                {/* Inside the option's button: no links. */}
                                <InlineMarkdown content={opt.description} links={false} />
                              </div>
                            )}
                          </div>
                        </div>
                      </button>

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
                  <button
                    type="button"
                    onClick={() => handleOther(qIdx)}
                    disabled={isResolved}
                    aria-pressed={otherChosen}
                    className={optionClass(isResolved, otherChosen)}
                  >
                    <div className="flex items-start gap-3">
                      <Indicator multiSelect={q.multiSelect} selected={otherChosen} />
                      <div className="flex-1 min-w-0">
                        <div className={`text-sm font-medium ${otherChosen ? 'text-accent-text' : 'text-text-secondary'}`}>
                          {OTHER_LABEL}
                        </div>
                        {!otherChosen && !isResolved && (
                          <div className="text-xs text-text-muted mt-0.5 leading-relaxed">Answer in your own words</div>
                        )}
                      </div>
                    </div>
                  </button>
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
        })}

        {/* Submit button — shown for multi-question or multiSelect, and for a
            single simple question answered in the person's own words. */}
        {!isResolved && (!isSingleSimple || own.has(0)) && (
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

        {/* Resolution confirmation — "Answered", or "Closed" when the
            interaction ended without an answer (timeout / cancel / deny). */}
        {isResolved && (
          <div className="px-4 py-2 border-t border-accent/10 flex items-center gap-2">
            <Check size={12} className={block.isError ? 'text-text-faint' : 'text-success'} />
            <span className={`text-xs ${block.isError ? 'text-text-faint' : 'text-success'}`}>
              {block.isError ? 'Closed' : 'Answered'}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
