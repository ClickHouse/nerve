import { useState, useRef } from 'react';
import { MessageCircleQuestion, Check, Send } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { MarkdownContent } from '../MarkdownContent';
import { useChatStore } from '../../../stores/chatStore';

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

// Recover the chosen option labels from the persisted tool_result, whose shape is
// `... "question"="label, label" ...`, so a reloaded poll re-highlights the answer.
function parseChosenSelections(result: string, questions: Question[]): Map<number, Set<number>> {
  const chosen = new Map<number, Set<number>>();
  questions.forEach((q, qIdx) => {
    const needle = `"${q.question}"="`;
    const start = result.indexOf(needle);
    if (start === -1) return;
    const valStart = start + needle.length;
    const valEnd = result.indexOf('"', valStart);
    if (valEnd === -1) return;
    // Recover the chosen labels as exact ", "-joined tokens. (A label that
    // itself contains ", " won't re-highlight — cosmetic, reload-only.)
    const chosenLabels = new Set(result.slice(valStart, valEnd).split(', '));
    const set = new Set<number>();
    q.options.forEach((opt, oIdx) => {
      if (chosenLabels.has(opt.label)) set.add(oIdx);
    });
    if (set.size > 0) chosen.set(qIdx, set);
  });
  return chosen;
}

export function QuestionBlock({ block }: { block: ToolCallBlockData }) {
  const questions = (block.input.questions as Question[]) || [];
  // Per-question selections: Map<questionIndex, Set<optionIndex>>
  const [selections, setSelections] = useState<Map<number, Set<number>>>(new Map());
  const [submitted, setSubmitted] = useState(false);
  const [hoveredOption, setHoveredOption] = useState<{ q: number; o: number } | null>(null);
  const questionPending = useChatStore(s => s.pendingInteraction?.interactionType === 'question');
  // Latch that a live question prompt was seen; once it clears (answered here,
  // by a parallel client, or before reconnect replay) the form must lock.
  const seenPending = useRef(false);
  if (questionPending) seenPending.current = true;

  if (questions.length === 0) return null;

  const isSingleSimple = questions.length === 1 && !questions[0].multiSelect;

  // A persisted tool_result means the interaction is already resolved — render
  // read-only so a reloaded session shows the answer instead of re-prompting.
  const parsedSelections = block.result !== undefined ? parseChosenSelections(block.result, questions) : null;
  const isResolved = submitted || parsedSelections !== null || (seenPending.current && !questionPending);
  // Prefer parsed answers, but fall back to live selections when the result
  // string didn't parse (e.g. a quote in a label) so the highlight isn't lost.
  const effSelections = parsedSelections && parsedSelections.size > 0 ? parsedSelections : selections;

  const handleSelect = (qIdx: number, oIdx: number) => {
    if (isResolved) return;
    setSelections(prev => {
      const next = new Map(prev);
      const q = questions[qIdx];
      if (q.multiSelect) {
        const current = new Set(prev.get(qIdx) || []);
        current.has(oIdx) ? current.delete(oIdx) : current.add(oIdx);
        next.set(qIdx, current);
      } else {
        next.set(qIdx, new Set([oIdx]));
      }
      return next;
    });
    // Single question + single select: submit immediately
    if (isSingleSimple) {
      submitAnswers(new Map([[qIdx, new Set([oIdx])]]));
    }
  };

  const submitAnswers = (sel?: Map<number, Set<number>>) => {
    const s = sel || selections;

    // Answer only when a live question interaction is pending. Without one the
    // poll is already resolved (reload, or answered by a parallel client), so
    // never fall back to posting the answer as a fresh chat message.
    const state = useChatStore.getState();
    if (state.pendingInteraction?.interactionType !== 'question') return;

    setSubmitted(true);
    // Build answers dict for the SDK: { questionText: selectedLabel }
    const answers: Record<string, string> = {};
    for (let i = 0; i < questions.length; i++) {
      const chosen = s.get(i);
      if (!chosen || chosen.size === 0) continue;
      const labels = Array.from(chosen).map(o => questions[i].options[o].label);
      answers[questions[i].question] = labels.join(', ');
    }
    state.answerInteraction(answers);
  };

  const allAnswered = questions.every((_q, i) => {
    const sel = selections.get(i);
    return sel && sel.size > 0;
  });

  return (
    <div className="question-block my-2">
      <div className="border border-accent/20 rounded-lg bg-bg-sunken overflow-hidden">
        {questions.map((q, qIdx) => (
          <div key={qIdx} className={qIdx > 0 ? 'border-t border-border-subtle' : ''}>
            {/* Question header */}
            <div className="px-4 pt-3.5 pb-2">
              <div className="flex items-center gap-2 mb-2">
                <MessageCircleQuestion size={15} className="text-accent shrink-0" />
                <span className="text-[10px] font-semibold uppercase tracking-wider text-accent/70 bg-accent/10 px-2 py-0.5 rounded">
                  {q.header}
                </span>
                {q.multiSelect && (
                  <span className="text-[10px] text-text-faint ml-auto">Select multiple</span>
                )}
              </div>
              <p className="text-[14px] text-text-secondary leading-relaxed">{q.question}</p>
            </div>

            {/* Options */}
            <div className="px-3 pb-3 space-y-1.5">
              {q.options.map((opt, oIdx) => {
                const isSelected = effSelections.get(qIdx)?.has(oIdx) || false;
                const isHovered = hoveredOption?.q === qIdx && hoveredOption?.o === oIdx;

                return (
                  <div key={oIdx}>
                    <button
                      onClick={() => handleSelect(qIdx, oIdx)}
                      onMouseEnter={() => setHoveredOption({ q: qIdx, o: oIdx })}
                      onMouseLeave={() => setHoveredOption(null)}
                      disabled={isResolved}
                      className={`question-option w-full text-left px-3.5 py-2.5 rounded-md border transition-all duration-150 ${
                        isResolved
                          ? isSelected
                            ? 'border-accent/40 bg-accent/10 cursor-default'
                            : 'border-surface-raised bg-bg-sunken opacity-40 cursor-default'
                          : isSelected
                            ? 'border-accent/50 bg-accent/10 cursor-pointer'
                            : 'border-border-subtle bg-bg-sunken hover:border-border hover:bg-surface cursor-pointer'
                      }`}
                    >
                      <div className="flex items-start gap-3">
                        <div className={`mt-0.5 shrink-0 w-4 h-4 ${q.multiSelect ? 'rounded-sm' : 'rounded-full'} border flex items-center justify-center transition-colors duration-150 ${
                          isSelected ? 'border-accent bg-accent' : 'border-text-faint bg-transparent'
                        }`}>
                          {isSelected && <Check size={10} className="text-white" strokeWidth={3} />}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className={`text-[13px] font-medium ${isSelected ? 'text-accent-text' : 'text-text-secondary'}`}>
                            {opt.label}
                          </div>
                          {opt.description && (
                            <div className="text-[12px] text-text-muted mt-0.5 leading-relaxed">{opt.description}</div>
                          )}
                        </div>
                      </div>
                    </button>

                    {opt.markdown && (isHovered || (isSelected && !isResolved)) && (
                      <div className="mx-2 mt-1 mb-0.5 px-3 py-2 bg-bg border border-border-subtle rounded text-[12px] max-h-48 overflow-y-auto">
                        <MarkdownContent content={opt.markdown} />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        ))}

        {/* Submit button — shown for multi-question or multiSelect, hidden for single simple question */}
        {!isSingleSimple && !isResolved && (
          <div className="px-3 pb-3">
            <button
              onClick={() => submitAnswers()}
              disabled={!allAnswered}
              className={`w-full py-2 rounded-md text-[13px] font-medium transition-all duration-150 flex items-center justify-center gap-2 ${
                allAnswered
                  ? 'bg-accent hover:bg-accent-hover text-white cursor-pointer'
                  : 'bg-surface text-text-faint cursor-not-allowed'
              }`}
            >
              <Send size={13} />
              Submit
            </button>
          </div>
        )}

        {/* Resolution confirmation — "Answered", or "Closed" when the
            interaction ended without an answer (timeout / cancel / deny). */}
        {isResolved && (
          <div className="px-4 py-2 border-t border-accent/10 flex items-center gap-2">
            <Check size={12} className={block.isError ? 'text-text-faint' : 'text-hue-green'} />
            <span className={`text-[11px] ${block.isError ? 'text-text-faint' : 'text-hue-green/70'}`}>
              {block.isError ? 'Closed' : 'Answered'}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
