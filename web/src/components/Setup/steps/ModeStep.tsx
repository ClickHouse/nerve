import { useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type SetupMode, type SetupModeDetail, type SetupState, type SetupStep } from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { Badge, Button, TextArea } from '../../ui';
import { ChoiceCards } from '../ChoiceCards';
import {
  MODE_CARDS, ONBOARDING_ENDED, ONBOARDING_SENTENCES, STEP_HEADINGS, WORKER_FIRST_BOOT, WORKER_TEMPLATES,
} from '../copy';
import { Field } from '../Field';
import { Notice } from '../Notice';
import { StepFrame } from '../StepFrame';

const TASK_MIN = 20;
const TASK_MAX = 4000;

/**
 * Personal assistant or Worker. Chosen once: the templates it writes are the
 * workspace's root files. Afterwards the step is read-only with the reason;
 * a worker whose templates arrived without a `TASK.md` may still write that.
 *
 * Personal assistant is preselected (and tagged Recommended) while the
 * choice is still open, so Continue is live on arrival; the preselection
 * is the form's opening value, not an edit, so Back never asks about it.
 */
export function ModeStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupModeDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const [initialMode] = useState<SetupMode | null>(
    () => detail.mode ?? (step.status === 'pending' ? 'personal' : null),
  );
  const [mode, setMode] = useState<SetupMode | null>(initialMode);
  const [task, setTask] = useState('');
  const [clientError, setClientError] = useState<string | null>(null);

  const chosen = detail.mode !== null;
  const taskOnly = chosen && detail.mode === 'worker' && !detail.task_present && step.status !== 'blocked';
  const readOnly = (step.read_only || chosen) && !taskOnly;
  const taskError = clientError ?? (fieldError?.field === 'task_description' ? fieldError.detail : null);

  const submit = async () => {
    setClientError(null);
    if (!mode) { setClientError('Choose one to continue.'); return false; }
    const body: Record<string, unknown> = { mode };
    if (mode === 'worker') {
      const text = task.trim();
      if (text.length < TASK_MIN) {
        setClientError(`Describe the task in at least one full sentence (${TASK_MIN}+ characters).`);
        return false;
      }
      if (text.length > TASK_MAX) {
        setClientError(`Keep the description under ${TASK_MAX} characters.`);
        return false;
      }
      body.task_description = text;
    }
    return save('mode', () => api.setupStep('mode', body));
  };

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.mode.heading}
      intro={STEP_HEADINGS.mode.intro}
      onSubmit={submit}
      readOnly={readOnly}
      dirty={mode !== initialMode || task !== ''}
      canContinue={mode !== null}
    >
      <ChoiceCards
        label="Mode"
        options={MODE_CARDS.map((c) => ({
          value: c.value,
          title: c.recommended && !chosen
            ? <>{c.title} <Badge tone="accent" className="ml-1 align-middle">Recommended</Badge></>
            : c.title,
          description: c.description,
        }))}
        value={mode}
        onChange={setMode}
        disabled={readOnly || chosen}
        columns={2}
      />

      {chosen && !readOnly && taskOnly && (
        <Notice tone="info">The workspace templates are in place. This worker still needs a task description.</Notice>
      )}

      {mode === 'worker' && (!chosen || taskOnly) && (
        <>
          <Field
            label="Task description"
            help={`${TASK_MIN}–${TASK_MAX} characters. Say what to watch, what to produce, and what it must never do on its own.`}
            error={taskError}
          >
            <TextArea
              rows={5}
              value={task}
              onChange={(e) => setTask(e.target.value)}
              placeholder="Watch the nightly build for example/repo and propose fixes before changing anything."
              disabled={readOnly}
            />
          </Field>
          <div className="flex flex-wrap gap-2" aria-label="Starter templates">
            {WORKER_TEMPLATES.map((t) => (
              <Button key={t.label} variant="pill" size="xs" onClick={() => setTask(t.text)} disabled={readOnly}>
                {t.label}
              </Button>
            ))}
          </div>
          <Notice tone="info">{WORKER_FIRST_BOOT}</Notice>
        </>
      )}

      {/* The onboarding in sentences, never the enum or the session id: the
          link carries the id. Without a link the sentence simply ends. */}
      {chosen && detail.mode === 'worker' && detail.onboarding !== 'not_applicable' && (
        <p className="text-xs text-text-dim" role="status">
          {detail.onboarding === 'ended' || detail.onboarding === 'running' ? (
            <>
              {detail.onboarding === 'ended' ? ONBOARDING_ENDED : ONBOARDING_SENTENCES.running}.
              {state.mode === 'worker' && detail.onboarding_session_id ? (
                <>
                  {' '}<Link to={`/chat/${detail.onboarding_session_id}`} className="underline underline-offset-2 hover:text-text">Open it</Link>
                  {detail.onboarding === 'ended' ? ' to see why.' : '.'}
                </>
              ) : null}
            </>
          ) : ONBOARDING_SENTENCES[detail.onboarding]}
        </p>
      )}
      {clientError && mode !== 'worker' && <p role="alert" className="text-sm text-error">{clientError}</p>}
    </StepFrame>
  );
}
