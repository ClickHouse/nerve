import { useState } from 'react';
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

export function QuestionBlock({ block }: { block: ToolCallBlockData }) {
  const questions = (block.input.questions as Question[]) || [];
  // Per-question selections: Map<questionIndex, Set<optionIndex>>
  const [selections, setSelections] = useState<Map<number, Set<number>>>(new Map());
  const [submitted, setSubmitted] = useState(false);
  const [hoveredOption, setHoveredOption] = useState<{ q: number; o: number } | null>(null);

  if (questions.length === 0) return null;

  const isSingleSimple = questions.length === 1 && !questions[0].multiSelect;

  const handleSelect = (qIdx: number, oIdx: number) => {
    if (submitted) return;
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
    setSubmitted(true);

    // Check store at call time (not closure) — the interaction event
    // may arrive after the component rendered but before the user clicks.
    const state = useChatStore.getState();
    const pending = state.pendingInteraction;
    const hasInteraction = pending?.interactionType === 'question';

    if (hasInteraction) {
      // Build answers dict for the SDK: { questionText: selectedLabel }
      const answers: Record<string, string> = {};
      for (let i = 0; i < questions.length; i++) {
        const chosen = s.get(i);
        if (!chosen || chosen.size === 0) continue;
        const labels = Array.from(chosen).map(o => questions[i].options[o].label);
        answers[questions[i].question] = labels.join(', ');
      }
      state.answerInteraction(answers);
    } else {
      // Fallback: send as a regular message (tool already completed / non-interactive)
      const parts: string[] = [];
      for (let i = 0; i < questions.length; i++) {
        const chosen = s.get(i);
        if (!chosen || chosen.size === 0) continue;
        const labels = Array.from(chosen).map(o => questions[i].options[o].label);
        if (questions.length > 1) {
          parts.push(`**${questions[i].header}**: ${labels.join(', ')}`);
        } else {
          parts.push(labels.join(', '));
        }
      }
      if (parts.length > 0) {
        state.sendMessage(parts.join('\n'));
      }
    }
  };

  const allAnswered = questions.every((_q, i) => {
    const sel = selections.get(i);
    return sel && sel.size > 0;
  });

  return (
    <div className="question-block my-2">
      <div className="border border-indigo-500/20 rounded-lg bg-[#111118] overflow-hidden">
        {questions.map((q, qIdx) => (
          <div key={qIdx} className={qIdx > 0 ? 'border-t border-[#1a1a2a]' : ''}>
            {/* Question header */}
            <div className="px-4 pt-3.5 pb-2">
              <div className="flex items-center gap-2 mb-2">
                <MessageCircleQuestion size={15} className="text-indigo-400 shrink-0" />
                <span className="text-[10px] font-semibold uppercase tracking-wider text-indigo-400/70 bg-indigo-500/10 px-2 py-0.5 rounded">
                  {q.header}
                </span>
                {q.multiSelect && (
                  <span className="text-[10px] text-text-faint ml-auto">Select multiple</span>
                )}
              </div>
              <p className="text-[14px] text-[#ddd] leading-relaxed">{q.question}</p>
            </div>

            {/* Options */}
            <div className="px-3 pb-3 space-y-1.5">
              {q.options.map((opt, oIdx) => {
                const isSelected = selections.get(qIdx)?.has(oIdx) || false;
                const isHovered = hoveredOption?.q === qIdx && hoveredOption?.o === oIdx;

                return (
                  <div key={oIdx}>
                    <button
                      onClick={() => handleSelect(qIdx, oIdx)}
                      onMouseEnter={() => setHoveredOption({ q: qIdx, o: oIdx })}
                      onMouseLeave={() => setHoveredOption(null)}
                      disabled={submitted}
                      className={`question-option w-full text-left px-3.5 py-2.5 rounded-md border transition-all duration-150 ${
                        submitted
                          ? isSelected
                            ? 'border-indigo-500/40 bg-indigo-500/10 cursor-default'
                            : 'border-surface-raised bg-[#0e0e12] opacity-40 cursor-default'
                          : isSelected
                            ? 'border-indigo-500/50 bg-indigo-500/10 cursor-pointer'
                            : 'border-border-subtle bg-[#0c0c10] hover:border-[#3a3a4a] hover:bg-[#141420] cursor-pointer'
                      }`}
                    >
                      <div className="flex items-start gap-3">
                        <div className={`mt-0.5 shrink-0 w-4 h-4 ${q.multiSelect ? 'rounded-sm' : 'rounded-full'} border flex items-center justify-center transition-colors duration-150 ${
                          isSelected ? 'border-indigo-500 bg-indigo-500' : 'border-text-faint bg-transparent'
                        }`}>
                          {isSelected && <Check size={10} className="text-white" strokeWidth={3} />}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className={`text-[13px] font-medium ${isSelected ? 'text-indigo-300' : 'text-text-secondary'}`}>
                            {opt.label}
                          </div>
                          {opt.description && (
                            <div className="text-[12px] text-[#777] mt-0.5 leading-relaxed">{opt.description}</div>
                          )}
                        </div>
                      </div>
                    </button>

                    {opt.markdown && (isHovered || (isSelected && !submitted)) && (
                      <div className="mx-2 mt-1 mb-0.5 px-3 py-2 bg-[#0a0a0e] border border-[#1e1e28] rounded text-[12px] max-h-48 overflow-y-auto">
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
        {!isSingleSimple && !submitted && (
          <div className="px-3 pb-3">
            <button
              onClick={() => submitAnswers()}
              disabled={!allAnswered}
              className={`w-full py-2 rounded-md text-[13px] font-medium transition-all duration-150 flex items-center justify-center gap-2 ${
                allAnswered
                  ? 'bg-indigo-600 hover:bg-indigo-500 text-white cursor-pointer'
                  : 'bg-[#1a1a22] text-text-faint cursor-not-allowed'
              }`}
            >
              <Send size={13} />
              Submit
            </button>
          </div>
        )}

        {/* Answered confirmation */}
        {submitted && (
          <div className="px-4 py-2 border-t border-indigo-500/10 flex items-center gap-2">
            <Check size={12} className="text-green-400" />
            <span className="text-[11px] text-green-400/70">Answered</span>
          </div>
        )}
      </div>
    </div>
  );
}
