import { useEffect, useMemo, useState } from 'react';
import { ExternalLink, MessageCircleQuestion, Send, X } from '../ui/icons';
import { Button, TextField } from '../ui';
import { useChatStore } from '../../stores/chatStore';

interface QuestionOption {
  label: string;
  description?: string;
  value?: string;
}

interface Question {
  id?: string;
  question: string;
  header?: string;
  options?: QuestionOption[];
  multiSelect?: boolean;
  freeText?: boolean;
  allowOther?: boolean;
  isSecret?: boolean;
  required?: boolean;
}

/** Out-of-band Codex request_user_input / MCP elicitation prompt. */
export function InteractiveQuestionCard() {
  const pending = useChatStore(s => s.pendingInteraction);
  const answer = useChatStore(s => s.answerInteraction);
  const deny = useChatStore(s => s.denyInteraction);
  const [selected, setSelected] = useState<Record<string, string[]>>({});
  const [text, setText] = useState<Record<string, string>>({});

  const input = pending?.toolInput ?? {};
  const visible = pending?.interactionType === 'question' && input.outOfBand === true;
  const questions = useMemo(
    () => (Array.isArray(input.questions) ? input.questions as Question[] : []),
    [input.questions],
  );
  useEffect(() => {
    setSelected({});
    setText({});
  }, [pending?.interactionId]);
  if (!visible) return null;

  const keyFor = (q: Question) => q.id || q.question;
  const complete = questions.every((q) => {
    if (q.required === false) return true;
    const key = keyFor(q);
    return Boolean(text[key]?.trim() || selected[key]?.length);
  });

  const choose = (q: Question, value: string) => {
    const key = keyFor(q);
    setSelected((current) => {
      const prior = current[key] ?? [];
      const next = q.multiSelect
        ? (prior.includes(value) ? prior.filter(v => v !== value) : [...prior, value])
        : [value];
      return { ...current, [key]: next };
    });
    if (!q.multiSelect) setText(current => ({ ...current, [key]: '' }));
  };

  const submit = () => {
    const result: Record<string, string> = {};
    for (const q of questions) {
      const key = keyFor(q);
      const free = text[key]?.trim();
      const choices = selected[key] ?? [];
      if (free) result[key] = free;
      else if (choices.length) result[key] = choices.join(', ');
    }
    answer(result);
  };

  const url = typeof input.url === 'string' ? input.url : '';
  const message = typeof input.message === 'string' ? input.message : '';

  return (
    <div className="mx-4 mb-2 border border-accent/30 rounded-lg bg-surface shadow-lg overflow-hidden">
      <div className="px-3 py-2 flex items-center gap-2 bg-accent/10">
        <MessageCircleQuestion size={15} className="text-accent" />
        <span className="text-sm font-medium text-text">
          {message || 'Codex needs your input'}
        </span>
        <span className="ml-auto text-xs text-text-muted">waiting</span>
      </div>
      <div className="px-3 py-3 space-y-3">
        {url && (
          <a
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-xs text-accent hover:underline break-all"
          >
            <ExternalLink size={12} />
            {url}
          </a>
        )}
        {questions.map((q) => {
          const key = keyFor(q);
          const options = q.options ?? [];
          const showText = q.freeText || q.allowOther;
          return (
            <div key={key} className="space-y-2">
              <div>
                {q.header && <div className="text-2xs uppercase tracking-wide text-accent/70">{q.header}</div>}
                <div className="text-sm text-text-secondary">{q.question}</div>
              </div>
              {options.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {options.map((option) => {
                    const value = option.value ?? option.label;
                    const active = (selected[key] ?? []).includes(value);
                    return (
                      <Button
                        key={value}
                        variant="pill"
                        size="sm"
                        active={active}
                        role={q.multiSelect ? 'checkbox' : 'radio'}
                        aria-checked={active}
                        onClick={() => choose(q, value)}
                        title={option.description}
                        className="rounded px-2.5 py-1.5"
                      >
                        {option.label}
                      </Button>
                    );
                  })}
                </div>
              )}
              {showText && (
                <TextField
                  type={q.isSecret ? 'password' : 'text'}
                  value={text[key] ?? ''}
                  onChange={(event) => setText(current => ({ ...current, [key]: event.target.value }))}
                  onKeyDown={(event) => { if (event.key === 'Enter' && complete) submit(); }}
                  placeholder={q.allowOther && options.length ? 'Other…' : 'Type your answer…'}
                  autoComplete="off"
                  className="px-2.5 rounded"
                />
              )}
            </div>
          );
        })}
      </div>
      <div className="px-3 py-2 border-t border-border-subtle flex justify-end gap-2">
        <Button variant="subtle" size="sm" onClick={() => deny('Declined by user.')}>
          <X size={12} /> Decline
        </Button>
        <Button variant="primary" size="sm" onClick={submit} disabled={!complete} className="rounded">
          <Send size={12} /> Continue
        </Button>
      </div>
    </div>
  );
}
