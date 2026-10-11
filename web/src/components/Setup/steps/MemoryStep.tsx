import { useState } from 'react';
import { api, type SetupMemoryDetail, type SetupState, type SetupStep } from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { ChoiceCards } from '../ChoiceCards';
import { MEMORY_WITHOUT_EMBEDDINGS, MEMORY_WITH_EMBEDDINGS, STEP_HEADINGS } from '../copy';
import { secretManaged } from '../credentialSource';
import { Notice } from '../Notice';
import { decides, secretPayload } from '../payload';
import { SecretField } from '../SecretField';
import { StepFrame } from '../StepFrame';
import { TestButton, type TestOutcome } from '../TestButton';

type Choice = 'embeddings' | 'without';

/**
 * Optional OpenAI key for embeddings, with the trade-off spelled out.
 * Sends `{openai_api_key}` or `{openai_api_key: null, embeddings: false}`.
 *
 * A key the operator provided (environment, configuration repository) is
 * shown as managed: the choice is fixed on embeddings, the field says who
 * provides the key, and Continue sends nothing.
 */
export function MemoryStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupMemoryDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const locked = step.read_only || step.status === 'blocked';
  const configured = detail.openai_api_key.configured;
  const managed = secretManaged(detail.openai_api_key) !== null;

  const [choice, setChoice] = useState<Choice>(() => (configured ? 'embeddings' : 'without'));
  const [key, setKey] = useState<string | null>('');
  const [clientError, setClientError] = useState<string | null>(null);
  const [weakKeyWarning, setWeakKeyWarning] = useState(false);

  const keyError = clientError ?? (fieldError?.field === 'openai_api_key' ? fieldError.detail : null);

  const submit = async () => {
    setClientError(null);
    if (managed) return true;   // the operator's key; nothing to send
    if (choice === 'embeddings') {
      const typed = secretPayload(key);
      if (typed === undefined || typed === null) {
        if (configured && key !== null) return true;  // keep what is there
        setClientError('Paste an OpenAI key, or choose No embeddings.');
        return false;
      }
      if (!typed.startsWith('sk-') && !weakKeyWarning) { setWeakKeyWarning(true); return false; }
      return save('memory', () => api.setupStep('memory', { openai_api_key: typed }));
    }
    // Without embeddings: a decision on a pending step, or a change on a
    // done one that previously had a key.
    if (!decides(step) && !configured && !detail.embeddings_chosen) return true;
    return save('memory', () => api.setupStep('memory', { openai_api_key: null, embeddings: false }));
  };

  const runTest = async (): Promise<TestOutcome> => {
    const result = await api.setupMemoryTest({ openai_api_key: (key ?? '').trim() });
    return { ok: result.ok, detail: result.detail, hint: result.hint };
  };

  const oauthOnly = state.steps.find((s) => s.id === 'provider')?.detail;
  const providerWarning = oauthOnly && 'warning' in oauthOnly ? oauthOnly.warning : null;

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.memory.heading}
      intro={STEP_HEADINGS.memory.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={(key ?? '') !== '' || key === null || choice !== (configured ? 'embeddings' : 'without')}
      continueLabel={weakKeyWarning ? 'Save anyway' : undefined}
    >
      <ChoiceCards
        label="Memory recall"
        options={[
          { value: 'embeddings', title: 'Use embeddings', description: MEMORY_WITH_EMBEDDINGS },
          { value: 'without', title: 'No embeddings', description: MEMORY_WITHOUT_EMBEDDINGS },
        ]}
        value={choice}
        onChange={(v) => { setChoice(v); setClientError(null); setWeakKeyWarning(false); }}
        disabled={locked || managed}
      />

      {choice === 'embeddings' && (
        <>
          <SecretField
            label="OpenAI API key"
            help="Used only for embeddings. Stored in config.local.yaml on the server."
            secret={detail.openai_api_key}
            value={key}
            onChange={(v) => { setKey(v); setClientError(null); setWeakKeyWarning(false); }}
            placeholder="sk-…"
            error={keyError}
            disabled={locked}
          />
          {weakKeyWarning && (
            <Notice tone="warning">That does not look like an OpenAI key (they start with sk-). Select Save anyway to keep it.</Notice>
          )}
          {!locked && !managed && <TestButton label="Test key" run={runTest} disabled={!(key ?? '').trim()} />}
        </>
      )}

      {providerWarning === 'oauth_background_jobs' && (
        <Notice tone="warning">
          Memory runs in the background and needs an Anthropic API key. The Model step has only a subscription
          token, so an OpenAI key alone will not turn memory on.
        </Notice>
      )}
    </StepFrame>
  );
}
