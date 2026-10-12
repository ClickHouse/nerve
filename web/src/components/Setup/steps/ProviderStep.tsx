import { useEffect, useState, type ReactNode } from 'react';
import {
  api, type SetupProviderDetail, type SetupProviderType, type SetupState, type SetupStep,
} from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { Button, Select, TextField } from '../../ui';
import { Cloud, KeyRound, Monitor, Sparkles } from '../../ui/icons';
import { ChoiceCards } from '../ChoiceCards';
import {
  COMMON_AWS_REGIONS, EFFORT_GUIDANCE, EFFORT_SERVER_DEFAULT, HOST_LOGIN_FOUND,
  HOST_LOGIN_NONE, HOST_LOGIN_UNREADABLE, HOST_LOGIN_UNREADABLE_HINT, MANAGED_SOURCE_HINT, MANAGED_SOURCE_TEXT,
  MODEL_DEFAULT_LINE, MODEL_FAMILY_GUIDANCE, MODEL_SERVER_DEFAULT, NO_LOCAL_SECRETS_TEXT,
  OAUTH_BACKGROUND_WARNING, PROVIDER_CARDS, PROVIDER_SAVED_PICK_MODEL, PROVIDER_TEST_FAILED_WARNING,
  STEP_HEADINGS, SUBSCRIPTION_NOTICE,
} from '../copy';
import { providerManaged } from '../credentialSource';
import { CodeText, Field } from '../Field';
import { keepsLocalSecrets } from '../navigation';
import { Notice } from '../Notice';
import { changedFields, decides, same, secretPayload } from '../payload';
import { focusRadio, radioTabIndex, radioTarget } from '../rovingRadio';
import { SecretField } from '../SecretField';
import { StepFrame } from '../StepFrame';
import { TestButton, type TestOutcome } from '../TestButton';

const REGION_RE = /^[a-z]{2}(-[a-z]+)+-\d$/;
const TYPES: SetupProviderType[] = ['anthropic', 'oauth', 'host_login', 'bedrock'];
const ICONS: Record<SetupProviderType, ReactNode> = {
  anthropic: <KeyRound size={16} />,
  oauth: <Sparkles size={16} />,
  host_login: <Monitor size={16} />,
  bedrock: <Cloud size={16} />,
};
/** Secondary controls reach the 44 px phone target without changing the desktop density. */
const TOUCH = 'min-h-[44px] md:min-h-0';

type SecretKey = 'anthropic_api_key' | 'claude_oauth_token';
const SECRET_LABELS: Record<SecretKey, string> = {
  anthropic_api_key: 'Anthropic API key',
  claude_oauth_token: 'Claude subscription token',
};
/** Rendered through `CodeText`: a backticked part shows as a command. */
const SECRET_HELP: Record<SecretKey, string> = {
  anthropic_api_key: 'Get one at console.anthropic.com. It is stored in config.local.yaml on the server, readable only by the account Nerve runs as.',
  claude_oauth_token: 'Run `claude setup-token` on any computer where you are signed in to Claude, then paste the token it prints.',
};
const SECRET_PLACEHOLDER: Record<SecretKey, string> = {
  anthropic_api_key: 'sk-ant-…',
  claude_oauth_token: 'sk-ant-oat01-…',
};

interface Form {
  type: SetupProviderType;
  anthropic_api_key: string | null;
  claude_oauth_token: string | null;
  aws_region: string;
  aws_profile: string;
  model: string;
  effort: string;
}

/**
 * How this server reaches Claude. Four cards; a `SecretField` per type; a
 * live Test that runs on the server and says exactly what failed; and, once
 * a credential works, the model picker from `/api/setup/models`.
 *
 * On a pending step the first Continue saves the credential and **stays**:
 * the server's answer makes the step `done` (and remounts this form), the
 * picker appears with the defaults the server applied, and a notice says
 * so. The next Continue sends `{model, effort}` if they were changed, else
 * just advances. The brief's "after success: model picker" is otherwise
 * never seen in the forward flow.
 *
 * A done step with a saved credential opens **compact**: the configured
 * line with Replace, Test, Remove and the picker — the cards and the key
 * field wait behind Replace, so the primary button is a handful of Tabs
 * away rather than sixteen.
 *
 * Two things make Continue ask twice, both with a warning and a **Save
 * anyway** label: a key that does not look like an Anthropic key, and a
 * key whose live test failed a moment ago (it would be saved unverified).
 *
 * A credential provided outside the wizard (the environment, a
 * configuration repository) is shown as managed: no cards, no key field,
 * no Replace, Remove or Test. The model and effort stay this instance's to
 * choose, so the picker still renders and a PUT can only carry those two.
 *
 * On an instance that keeps no credentials on disk (`local_secrets: false`)
 * a pasted key or token could not be stored, so the cards for them are not
 * offered: the sentence stands where the key field would be, AWS Bedrock
 * (an IAM role, no secret) is the one card, a credential from the
 * environment reads as managed exactly as above, and the picker is open.
 */
export function ProviderStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupProviderDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const hostLogin = useSetupStore((s) => s.hostLogin);
  const loadHostLogin = useSetupStore((s) => s.loadHostLogin);
  const models = useSetupStore((s) => s.models);
  const loadModels = useSetupStore((s) => s.loadModels);
  const savedNotice = useSetupStore((s) => s.savedNotice);
  const noteSaved = useSetupStore((s) => s.noteSaved);

  const managed = providerManaged(detail);
  const managedCredential = managed !== null;
  const proxy = detail.credential === 'proxy';
  const locked = step.read_only || step.status === 'blocked';
  const done = step.status === 'done';
  const keyless = !keepsLocalSecrets(state);

  const [initial] = useState<Form>(() => ({
    // Nothing configured on an instance that stores no key: Bedrock is the
    // one card left, so it opens selected rather than on a card that is gone.
    type: keyless && detail.credential === 'none' ? 'bedrock'
      : TYPES.includes(detail.type as SetupProviderType) ? detail.type as SetupProviderType : 'anthropic',
    anthropic_api_key: '',
    claude_oauth_token: '',
    aws_region: detail.aws_region || 'us-east-1',
    aws_profile: detail.aws_profile || '',
    model: detail.model || '',
    effort: detail.effort || '',
  }));
  const [form, setForm] = useState<Form>(initial);
  const [clientError, setClientError] = useState<{ field: string; detail: string } | null>(null);
  const [weakKeyWarning, setWeakKeyWarning] = useState(false);
  const [testFailed, setTestFailed] = useState(false);
  const [testFailedWarning, setTestFailedWarning] = useState(false);
  const [testedOk, setTestedOk] = useState(false);
  const [justSaved, setJustSaved] = useState(false);
  const [changing, setChanging] = useState(false);
  const [hostAnnounce, setHostAnnounce] = useState<string | null>(null);

  // The secret the saved credential lives in, when it is one of the two.
  const savedSecret: SecretKey | null = detail.credential === 'api_key' ? 'anthropic_api_key'
    : detail.credential === 'oauth_token' ? 'claude_oauth_token' : null;
  const compact = done && !locked && !managedCredential && !proxy && !changing
    && (savedSecret !== null || detail.credential === 'bedrock');

  // Host-login detection is lazy and rate-limited on the server; it is
  // asked for only when this step opens, never from the overview.
  useEffect(() => { void loadHostLogin(); }, [loadHostLogin]);
  useEffect(() => { if (done) void loadModels(); }, [done, loadModels]);

  // After the first save the server's `done` answer remounts this form and
  // focus would land on <body>. The picker is what comes next, so its
  // heading takes focus — on the remounted form (the notice is in the
  // store, so it is set when this mounts) or, if the server did not
  // remount, when the notice is set.
  useEffect(() => {
    if (savedNotice === 'provider') document.getElementById('setup-model-picker-heading')?.focus();
  }, [savedNotice]);

  // "Look again" is the one explicit re-detection; when it finds a login
  // the card appears and a status line says so, otherwise nothing moves.
  const lookAgain = async () => {
    await loadHostLogin(true);
    const found = useSetupStore.getState().hostLogin;
    setHostAnnounce(found?.available ? HOST_LOGIN_FOUND(found.source ?? 'detected') : null);
  };

  const set = <K extends keyof Form>(key: K, value: Form[K]) => {
    setForm((f) => ({ ...f, [key]: value }));
    setClientError(null);
    setWeakKeyWarning(false);
    // A failed test was about the credential; the model choice is not.
    if (key !== 'model' && key !== 'effort') {
      setTestFailed(false);
      setTestFailedWarning(false);
    }
  };

  const errFor = (field: string) =>
    clientError?.field === field ? clientError.detail
      : fieldError?.field === field ? fieldError.detail : null;

  /** The body for a Test, and the credential part of a save. */
  const candidate = (): Record<string, unknown> => {
    const body: Record<string, unknown> = { type: form.type };
    if (form.type === 'anthropic') {
      const key = secretPayload(form.anthropic_api_key);
      if (key !== undefined) body.anthropic_api_key = key;
    } else if (form.type === 'oauth') {
      const token = secretPayload(form.claude_oauth_token);
      if (token !== undefined) body.claude_oauth_token = token;
    } else if (form.type === 'bedrock') {
      body.aws_region = form.aws_region.trim();
      if (form.aws_profile.trim()) body.aws_profile = form.aws_profile.trim();
    }
    return body;
  };

  /** Whether the save would carry a credential the server has not verified. */
  const carriesCredential = (): boolean => {
    const body = candidate();
    return typeof body.anthropic_api_key === 'string' || typeof body.claude_oauth_token === 'string'
      || form.type === 'bedrock';
  };

  const hostLoginMissing = hostLogin?.lookup === 'unreadable' ? HOST_LOGIN_UNREADABLE : HOST_LOGIN_NONE;

  const validate = (): boolean => {
    const typedKey = (form.anthropic_api_key ?? '').trim();
    if (form.type === 'anthropic' && !typedKey && !detail.anthropic_api_key.configured && !managedCredential) {
      setClientError({ field: 'anthropic_api_key', detail: 'Paste an API key to continue.' });
      return false;
    }
    if (form.type === 'oauth') {
      const token = (form.claude_oauth_token ?? '').trim();
      if (!token && !detail.claude_oauth_token.configured && !managedCredential) {
        setClientError({ field: 'claude_oauth_token', detail: 'Paste a subscription token to continue.' });
        return false;
      }
    }
    if (form.type === 'bedrock' && !REGION_RE.test(form.aws_region.trim())) {
      setClientError({ field: 'aws_region', detail: 'Use an AWS region id such as us-east-1.' });
      return false;
    }
    if (form.type === 'host_login' && !hostLogin?.available) {
      setClientError({ field: 'type', detail: hostLoginMissing });
      return false;
    }
    // The two-step saves: warn once, with the label switched to "Save
    // anyway"; the next Continue goes through. Both warnings show at once
    // when both apply, so it never takes three presses.
    const weak = form.type === 'anthropic' && !!typedKey && !typedKey.startsWith('sk-ant-');
    const unverified = testFailed && carriesCredential();
    if ((weak && !weakKeyWarning) || (unverified && !testFailedWarning)) {
      if (weak) setWeakKeyWarning(true);
      if (unverified) setTestFailedWarning(true);
      return false;
    }
    return true;
  };

  const submit = async () => {
    if (!validate()) return false;
    let body: Record<string, unknown>;
    if (decides(step) && !managedCredential) {
      body = candidate();
      if (form.model && form.model !== detail.model) body.model = form.model;
      if (form.effort && form.effort !== detail.effort) body.effort = form.effort;
      const ok = await save('provider', () => api.setupStep('provider', body));
      if (!ok) return false;
      // Stay: show the picker with what the server applied. (The server's
      // answer usually remounts this form as `done`; the store carries the
      // notice across that, and `justSaved` covers the case it does not.)
      setJustSaved(true);
      noteSaved('provider');
      void loadModels();
      return false;
    } else {
      // A done step sends only what differs from the server's detail. The
      // region/profile fields only exist for Bedrock; for any other type the
      // form's `us-east-1` default is not a change.
      const bedrock = form.type === 'bedrock';
      const diff = changedFields(
        {
          type: form.type,
          aws_region: bedrock ? form.aws_region.trim() : detail.aws_region,
          aws_profile: bedrock ? form.aws_profile.trim() : detail.aws_profile,
          model: form.model, effort: form.effort,
        },
        { type: detail.type, aws_region: detail.aws_region, aws_profile: detail.aws_profile, model: detail.model, effort: detail.effort },
        ['type', 'aws_region', 'aws_profile', 'model', 'effort'],
      ) as Record<string, unknown>;
      const key = secretPayload(form.anthropic_api_key);
      const token = secretPayload(form.claude_oauth_token);
      const credentialChanged = !managedCredential
        && (diff.type !== undefined || key !== undefined || token !== undefined
          || diff.aws_region !== undefined || diff.aws_profile !== undefined);
      if (managedCredential) {
        delete diff.type; delete diff.aws_region; delete diff.aws_profile;
      } else if (credentialChanged) {
        // The server resolves a credential change against the type, so the
        // type (and, for Bedrock, the region) always rides along.
        Object.assign(diff, candidate());
      }
      if (Object.keys(diff).length === 0) return true;
      body = diff;
    }
    return save('provider', () => api.setupStep('provider', body));
  };

  const runTest = async (): Promise<TestOutcome> => {
    const result = await api.setupProviderTest(candidate());
    return { ok: result.ok, detail: result.detail, hint: result.hint };
  };

  const onTestResult = (r: TestOutcome) => {
    if (r.ok) {
      setTestedOk(true);
      setTestFailed(false);
      setTestFailedWarning(false);
      void loadModels();
    } else {
      setTestFailed(true);
    }
  };

  const cards = PROVIDER_CARDS
    .filter((c) => c.value !== 'host_login' || hostLogin?.available)
    // A key, a token and the server's login would all be stored here.
    .filter((c) => !keyless || c.value === 'bedrock')
    .map((c) => ({
      value: c.value,
      title: c.title,
      description: c.value === 'host_login' && hostLogin?.source ? `${c.description} Found: ${hostLogin.source}.` : c.description,
      icon: ICONS[c.value],
    }));

  const showPicker = done || testedOk || justSaved;
  // Every model the API returned, the configured one marked. When the
  // instance was changed away from Nerve's own default, the help says which
  // is which, so "recommended" is never mistaken for "Nerve's default".
  const modelOptions = (models?.models ?? []).map((m) => ({
    value: m.id, label: `${m.label}${m.recommended ? ' · recommended' : ''}`,
  }));
  const selectedFamily = models?.models.find((m) => m.id === form.model)?.family ?? 'other';
  const labelOf = (id: string) => models?.models.find((m) => m.id === id)?.label ?? id;
  const defaultLine = models?.default_model && models.default_model !== models.current.model
    ? MODEL_DEFAULT_LINE(labelOf(models.default_model), labelOf(models.current.model))
    : null;
  const modelHelp = `${MODEL_FAMILY_GUIDANCE[selectedFamily] ?? MODEL_FAMILY_GUIDANCE.other}${defaultLine ? ` ${defaultLine}` : ''}`;
  const efforts = models?.efforts ?? ['max', 'xhigh', 'high', 'medium', 'low'];
  const dirty = !same(form, initial);
  const effortOff = () => !!locked;

  const secretField = (
    key: SecretKey,
    extra: { removable?: boolean; replaceable?: boolean; onReplace?: () => void; startReplacing?: boolean } = {},
  ) => (
    <SecretField
      label={SECRET_LABELS[key]}
      help={<CodeText text={SECRET_HELP[key]} />}
      secret={detail[key]}
      value={form[key]}
      onChange={(v) => set(key, v)}
      placeholder={SECRET_PLACEHOLDER[key]}
      error={errFor(key)}
      disabled={locked}
      {...extra}
    />
  );

  const testFailedNotice = testFailedWarning && <Notice tone="warning">{PROVIDER_TEST_FAILED_WARNING}</Notice>;

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.provider.heading}
      intro={STEP_HEADINGS.provider.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
      continueLabel={weakKeyWarning || testFailedWarning ? 'Save anyway' : undefined}
    >
      {managed && <Notice tone="locked">{MANAGED_SOURCE_TEXT[managed]}. {MANAGED_SOURCE_HINT[managed]}</Notice>}
      {/* Where a key could otherwise be entered or replaced, the reason it
          cannot be; a managed credential has already said who provides it. */}
      {keyless && !managed && <Notice tone="locked">{NO_LOCAL_SECRETS_TEXT}</Notice>}
      {proxy && <Notice tone="info">Nerve reaches Claude through the local proxy that <code>nerve init --terminal</code> set up. You can change the model and effort here.</Notice>}
      {detail.warning === 'oauth_background_jobs' && <Notice tone="warning"><CodeText text={OAUTH_BACKGROUND_WARNING} /></Notice>}
      {savedNotice === 'provider' && <Notice tone="info">{PROVIDER_SAVED_PICK_MODEL}</Notice>}

      {compact && (
        <div className="flex flex-col gap-3">
          {savedSecret ? (
            // A replacement key could not be stored on an instance without
            // local secrets; removing the saved one still can.
            secretField(savedSecret, { removable: false, replaceable: !keyless, onReplace: () => setChanging(true) })
          ) : (
            <div className="flex flex-wrap items-center gap-2">
              <p className="text-sm text-text-muted">
                AWS Bedrock · <span className="font-mono">{detail.aws_region}</span>
                {detail.aws_profile ? <> · profile <span className="font-mono">{detail.aws_profile}</span></> : null}
              </p>
              <Button variant="secondary" size="xs" onClick={() => setChanging(true)} className={TOUCH}>Replace</Button>
            </div>
          )}
          {!(savedSecret && form[savedSecret] === null) && <TestButton run={runTest} onResult={onTestResult} />}
          {testFailedNotice}
          {/* Destructive, so it comes after Test rather than one Tab after
              Replace; removal is confirmed by "Will be removed when you
              continue" + Undo, and happens on Continue. */}
          {savedSecret && form[savedSecret] !== null && (
            <div>
              <Button variant="dangerGhost" size="xs" onClick={() => set(savedSecret, null)} className={TOUCH}>
                Remove
              </Button>
            </div>
          )}
        </div>
      )}

      {!compact && !managedCredential && !proxy && (
        <>
          <ChoiceCards
            label="Model provider"
            options={cards}
            value={form.type}
            onChange={(v) => set('type', v)}
            disabled={locked}
            columns={2}
          />
          {errFor('type') && <p role="alert" tabIndex={-1} className="text-sm text-error outline-none">{errFor('type')}</p>}
          {hostLogin && !hostLogin.available && (
            <p className="text-xs text-text-dim flex flex-wrap items-center gap-x-2">
              <span>{hostLoginMissing}</span>
              {/* The server's own reason when it sends one (it names the
                  terminal fallback); else, for an unreadable Keychain, the
                  likely cause and the ways around it. */}
              {hostLogin.reason
                ? <span>{hostLogin.reason}</span>
                : hostLogin.lookup === 'unreadable' ? <span><CodeText text={HOST_LOGIN_UNREADABLE_HINT} /></span> : null}
              <Button variant="link" size="sm" onClick={() => void lookAgain()} disabled={locked} className={TOUCH}>
                Look again
              </Button>
            </p>
          )}
          {hostAnnounce && hostLogin?.available && (
            <p role="status" className="text-xs text-text-muted">{hostAnnounce}</p>
          )}

          {form.type === 'anthropic' && secretField('anthropic_api_key', {
            startReplacing: changing && savedSecret === 'anthropic_api_key',
          })}
          {weakKeyWarning && (
            <Notice tone="warning">That does not look like an Anthropic API key (they start with sk-ant-). Select Save anyway to keep it.</Notice>
          )}

          {form.type === 'oauth' && (
            <>
              {secretField('claude_oauth_token', { startReplacing: changing && savedSecret === 'claude_oauth_token' })}
              <Notice tone="info"><CodeText text={SUBSCRIPTION_NOTICE} /></Notice>
            </>
          )}

          {form.type === 'host_login' && (
            <Notice tone="info">
              When you continue, Nerve copies the login found on this server ({hostLogin?.source ?? 'detected'})
              into its own configuration. The login itself is never shown here.
            </Notice>
          )}

          {form.type === 'bedrock' && (
            <div className="grid gap-3 sm:grid-cols-2">
              <Field label="AWS region" help="Where Claude is enabled for your account." error={errFor('aws_region')}>
                <TextField
                  value={form.aws_region}
                  onChange={(e) => set('aws_region', e.target.value)}
                  list="setup-aws-regions"
                  autoComplete="off"
                  disabled={locked}
                />
              </Field>
              <datalist id="setup-aws-regions">
                {COMMON_AWS_REGIONS.map((r) => <option key={r} value={r} />)}
              </datalist>
              <Field label="AWS profile (optional)" help="A named profile from the server's AWS config." error={errFor('aws_profile')}>
                <TextField
                  value={form.aws_profile}
                  onChange={(e) => set('aws_profile', e.target.value)}
                  autoComplete="off"
                  disabled={locked}
                />
              </Field>
            </div>
          )}

          {!locked && <TestButton run={runTest} onResult={onTestResult} />}
          {testFailedNotice}
        </>
      )}

      {showPicker && (
        <div className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-3">
          <h2 id="setup-model-picker-heading" tabIndex={-1} className="text-sm font-medium text-text outline-none">Default model</h2>
          <Field
            label="Model"
            help={modelHelp}
            error={errFor('model')}
          >
            {modelOptions.length > 0 ? (
              <Select
                value={form.model}
                onChange={(e) => set('model', e.target.value)}
                options={modelOptions}
                emptyLabel={MODEL_SERVER_DEFAULT(detail.model)}
                fullWidth
                disabled={locked}
              />
            ) : (
              <TextField
                value={form.model}
                onChange={(e) => set('model', e.target.value)}
                placeholder="claude-…"
                autoComplete="off"
                disabled={locked}
              />
            )}
          </Field>
          <div className="flex flex-col gap-1">
            <span className="text-sm font-medium text-text" id="setup-effort-label">Effort</span>
            {/* One tab stop; arrow keys move the choice (the radiogroup pattern). */}
            <div className="flex flex-wrap gap-1.5" role="radiogroup" aria-labelledby="setup-effort-label">
              {efforts.map((level) => (
                <Button
                  key={level}
                  variant="pill"
                  size="sm"
                  role="radio"
                  aria-checked={form.effort === level}
                  active={form.effort === level}
                  tabIndex={radioTabIndex(efforts, form.effort || null, level, effortOff)}
                  onClick={() => set('effort', level)}
                  onKeyDown={(e) => {
                    const target = radioTarget(efforts, level, e.key, effortOff);
                    if (target === null) return;
                    e.preventDefault();
                    set('effort', target);
                    focusRadio(e.currentTarget, efforts.indexOf(target));
                  }}
                  disabled={locked}
                >
                  {level}
                </Button>
              ))}
            </div>
            <p className="text-xs text-text-dim">
              {EFFORT_GUIDANCE[form.effort] ?? 'Pick how long the model may think per answer.'}
              {form.effort && form.effort === detail.effort ? EFFORT_SERVER_DEFAULT : ''}
            </p>
            {errFor('effort') && <p role="alert" tabIndex={-1} className="text-xs text-error outline-none">{errFor('effort')}</p>}
          </div>
        </div>
      )}
    </StepFrame>
  );
}
