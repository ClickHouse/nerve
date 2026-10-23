import { useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type SetupMode, type SetupModeDetail, type SetupStep } from '../../../api/client';
import { onboardingCanStart, useSetupStore } from '../../../stores/setupStore';
import { Badge, TextArea } from '../../ui';
import { ChoiceCards } from '../ChoiceCards';
import {
  MODE_CARDS, ONBOARDING_OPEN, ONBOARDING_OPEN_SESSION, STEP_HEADINGS, WORKER_NOTE_HELP, WORKER_NOTE_LABEL,
  onboardingSentence,
} from '../copy';
import { Field } from '../Field';
import { Notice } from '../Notice';
import { StepFrame } from '../StepFrame';

const NOTE_MAX = 4000;

/**
 * Personal assistant or Worker. Chosen once: the templates it writes are the
 * workspace's root files. Afterwards the step is read-only with the reason;
 * a worker whose templates arrived without a `TASK.md` may still write that.
 *
 * A worker takes an optional note, which goes into `TASK.md`. What the agent
 * does is decided in the onboarding conversation after setup.
 *
 * Personal assistant is preselected (and tagged Recommended) while the
 * choice is still open, so Continue is live on arrival; the preselection
 * is the form's opening value, not an edit, so Back never asks about it.
 */
export function ModeStep({ step }: { step: SetupStep }) {
  const detail = step.detail as SetupModeDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const canStart = useSetupStore((s) => onboardingCanStart(s.state));
  const [initialMode] = useState<SetupMode | null>(
    () => detail.mode ?? (step.status === 'pending' ? 'personal' : null),
  );
  const [mode, setMode] = useState<SetupMode | null>(initialMode);
  const [note, setNote] = useState('');
  const [clientError, setClientError] = useState<string | null>(null);

  const chosen = detail.mode !== null;
  const taskOnly = chosen && detail.mode === 'worker' && !detail.task_present && step.status !== 'blocked';
  const readOnly = (step.read_only || chosen) && !taskOnly;
  const noteError = clientError ?? (fieldError?.field === 'task_description' ? fieldError.detail : null);

  const submit = async () => {
    setClientError(null);
    if (!mode) { setClientError('Choose one to continue.'); return false; }
    const body: Record<string, unknown> = { mode };
    if (mode === 'worker') {
      const text = note.trim();
      if (text.length > NOTE_MAX) {
        setClientError(`Keep the note under ${NOTE_MAX} characters.`);
        return false;
      }
      if (text) body.task_description = text;
    }
    return save('mode', () => api.setupStep('mode', body));
  };

  const onboarding = detail.onboarding;
  const sessionId = detail.onboarding_session_id;
  const kind = detail.onboarding_kind ?? 'conversation';

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.mode.heading}
      intro={STEP_HEADINGS.mode.intro}
      onSubmit={submit}
      readOnly={readOnly}
      dirty={mode !== initialMode || note !== ''}
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
        <Notice tone="info">The workspace templates are in place. Continue to create this worker's TASK.md.</Notice>
      )}

      {mode === 'worker' && (!chosen || taskOnly) && (
        <Field label={WORKER_NOTE_LABEL} help={WORKER_NOTE_HELP} error={noteError}>
          <TextArea
            rows={3}
            value={note}
            onChange={(e) => setNote(e.target.value)}
            placeholder="For example: the repositories it works in, or what it must never do on its own."
            disabled={readOnly}
          />
        </Field>
      )}

      {/* The onboarding in sentences, never the enum or the session id: the
          link carries the id. */}
      {chosen && onboarding !== 'not_applicable' && (
        <p className="text-xs text-text-dim" role="status">
          {onboardingSentence(onboarding, kind, canStart)}
          {onboarding === 'running' && sessionId && (
            <>
              {' '}
              <Link to={`/chat/${sessionId}`} className="underline underline-offset-2 hover:text-text">
                {kind === 'autonomous' ? ONBOARDING_OPEN_SESSION : ONBOARDING_OPEN}
              </Link>
            </>
          )}
        </p>
      )}
      {clientError && mode !== 'worker' && <p role="alert" className="text-sm text-error">{clientError}</p>}
    </StepFrame>
  );
}
