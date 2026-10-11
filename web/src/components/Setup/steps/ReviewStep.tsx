import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import type { SetupFinishResponse, SetupProviderDetail, SetupState, SetupStep } from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { copyToClipboard } from '../../../utils/clipboard';
import { Badge, Button } from '../../ui';
import { Check, Copy, Loader2, RefreshCw } from '../../ui/icons';
import {
  ACCOUNT_ROW_LINK, ACCOUNT_ROW_TEXT, HTTPS_HINT, PASSWORDLESS_LAN_HINT, PROVIDER_LAST_TEST_FAILED,
  RESTART_EXPLAINER, RESTART_HEADLINE, RESTART_HEADLINE_FIRST_RUN, REVIEW_DECIDE_FIRST, REVIEW_HEADING,
  describeRestartReason, isFirstRunRestart,
} from '../copy';
import { stepTitle, stopPath } from '../navigation';
import { Notice } from '../Notice';
import { ReadyScreen } from '../ReadyScreen';
import { StatusBadge } from '../StatusBadge';
import { useWizardNav } from '../wizardContext';

/** Where a step opened from here comes back to, on Save and on Back. */
const RETURN_HERE = { returnTo: '/setup/review' };

/** The server's last live test of the provider failed: say so beside its summary. */
function lastTestFailed(step: SetupStep): boolean {
  return step.id === 'provider' && (step.detail as SetupProviderDetail).last_test?.ok === false;
}

/**
 * Review and finish. The apply card comes **first** — it is what this
 * step is for, and the primary button must be above the fold — with the
 * restart reasons in words (the raw `path: change` lines as secondary
 * text), anything still to do (with a one-click Skip), and then the
 * details: the checklist, the server check-up and what was written where.
 *
 * **Finish** waits until nothing is pending: the list above it has an Open
 * and a Skip per step, so "N steps left" on the next page can never follow
 * "setup is complete" on this one. It calls `POST /api/setup/finish`; when
 * that answers `restart_required`, `restart()` follows and the overlay
 * waits for the new boot generation. Either way the ending is `ReadyScreen`.
 */
export function ReviewStep({ state }: { state: SetupState }) {
  const nav = useWizardNav();
  const busy = useSetupStore((s) => s.busy);
  const error = useSetupStore((s) => s.error);
  const environment = useSetupStore((s) => s.environment);
  const loadEnvironment = useSetupStore((s) => s.loadEnvironment);
  const finish = useSetupStore((s) => s.finish);
  const restart = useSetupStore((s) => s.restart);
  const skip = useSetupStore((s) => s.skip);
  const reconnecting = useSetupStore((s) => s.reconnecting);
  const latest = useSetupStore((s) => s.state) ?? state;
  const [ready, setReady] = useState(false);
  const [onboarding, setOnboarding] = useState<SetupFinishResponse['onboarding'] | null>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => { void loadEnvironment(); }, [loadEnvironment]);

  const apply = async () => {
    const result = await finish();
    if (!result) return;
    setOnboarding(result.onboarding);
    if (result.action === 'reloaded') { setReady(true); return; }
    const back = await restart();
    if (back) setReady(true);
  };

  if (ready) return <ReadyScreen state={latest} onboarding={onboarding} />;

  const files = [...new Set(state.steps.flatMap((s) => s.status === 'done' ? s.files : []))].sort();
  // The operator's command, on screen before anyone clicks: the server's
  // `restart_command` when it sends one, else lifted from the sentence it
  // already sends ("… On the server run: systemctl restart nerve.service — …").
  const commandFrom = (text: string | undefined) => text?.match(/run: (\S.*?)(?: —|$)/)?.[1] ?? null;
  const command = state.restart_mode === 'unsupported'
    ? (state.restart_command ?? commandFrom(state.restart_message) ?? commandFrom(environment?.restart_message))
    : null;
  // Before the click the card speaks in the future tense; the server's
  // present-tense "Restarting. This page reconnects on its own." is for the
  // overlay. A container or a supervisor has its own sentence, kept as is.
  const cardMessage = state.restart_mode === 'restart' && !environment?.in_container
    ? RESTART_EXPLAINER
    : state.restart_message;
  const anyBusy = busy !== null || reconnecting;
  const pendingSteps = state.steps.filter((s) => s.status === 'pending');
  const canFinish = pendingSteps.length === 0;

  // A fresh install always carries "memory: changed" from the Mode step and
  // nothing else; the headline says what that restart is, rather than
  // making it look like an accident. Derived from the reasons, so a page
  // reload between Mode and Review does not change the sentence.
  const firstRun = isFirstRunRestart(state.restart_reasons);
  const reasons = new Map<string, string[]>();
  for (const raw of state.restart_reasons) {
    const sentence = describeRestartReason(raw);
    reasons.set(sentence, [...(reasons.get(sentence) ?? []), raw]);
  }

  return (
    <div className="flex flex-col gap-5">
      <div>
        <h1 id="setup-step-heading" tabIndex={-1} className="text-xl font-semibold text-text outline-none">
          {REVIEW_HEADING.heading}
        </h1>
        <p className="text-sm text-text-muted mt-1">{REVIEW_HEADING.intro}</p>
      </div>

      <section aria-label="Apply" className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-4">
        {state.restart_pending ? (
          <>
            <h2 className="text-sm font-medium text-text flex items-center gap-2">
              <RefreshCw size={14} />{firstRun ? RESTART_HEADLINE_FIRST_RUN : RESTART_HEADLINE}
            </h2>
            <ul className="text-sm text-text-muted flex flex-col gap-1" aria-label="Restart reasons">
              {[...reasons.entries()].map(([sentence, raws]) => (
                <li key={sentence} className="flex flex-col">
                  <span>{sentence}</span>
                  {raws[0] !== sentence && (
                    <span className="font-mono text-xs text-text-dim">{raws.join(' · ')}</span>
                  )}
                </li>
              ))}
            </ul>
            <p className="text-sm text-text-muted">{cardMessage}</p>
            {state.restart_mode === 'unsupported' && command && (
              <div className="flex items-center gap-2">
                <code className="flex-1 font-mono text-xs rounded border border-border bg-code-bg px-2 py-1.5 overflow-x-auto">{command}</code>
                <Button variant="secondary" size="sm" onClick={() => void copyToClipboard(command).then(setCopied)} className="min-h-[44px] md:min-h-0">
                  <Copy size={14} />{copied ? 'Copied' : 'Copy'}
                </Button>
              </div>
            )}
          </>
        ) : (
          <h2 className="text-sm font-medium text-text flex items-center gap-2"><Check size={14} className="text-success" />Everything applies without a restart.</h2>
        )}

        {pendingSteps.length > 0 && (
          <>
            <ul className="flex flex-col gap-1" aria-label="Still to do">
              {pendingSteps.map((step) => (
                <li key={step.id} className="flex items-center justify-between gap-3 rounded-lg border border-border-subtle px-3 py-1.5 text-sm">
                  <span className="text-text">{stepTitle(step)} — still to do</span>
                  <span className="flex items-center gap-1 shrink-0">
                    <Link
                      to={stopPath(step.id)}
                      state={RETURN_HERE}
                      className="text-xs text-text-muted hover:text-text underline-offset-2 hover:underline px-1.5 min-h-[44px] md:min-h-[32px] inline-flex items-center"
                    >
                      Open
                    </Link>
                    {step.can_skip && (
                      <Button variant="ghost" size="xs" onClick={() => void skip(step.id, true)} disabled={anyBusy} className="min-h-[44px] md:min-h-0">
                        Skip
                      </Button>
                    )}
                  </span>
                </li>
              ))}
            </ul>
            <p id="setup-review-decide-first" className="text-xs text-text-muted">{REVIEW_DECIDE_FIRST}</p>
          </>
        )}

        {state.warning && <Notice tone="warning">{state.warning}</Notice>}
        {error && <p role="alert" className="text-sm text-error">{error}</p>}
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="ghost" size="md" onClick={() => nav.back('review')} disabled={anyBusy}>Back</Button>
          <span className="flex-1" />
          <Button
            variant="primary"
            size="md"
            onClick={() => void apply()}
            disabled={anyBusy || !canFinish}
            aria-describedby={canFinish ? undefined : 'setup-review-decide-first'}
            className="min-h-[44px]"
          >
            {busy === 'finish' || busy === 'restart' ? <Loader2 size={14} className="animate-spin" /> : null}
            {state.restart_pending
              ? (state.restart_mode === 'unsupported' ? 'Finish and wait for the restart' : 'Restart Nerve')
              : 'Finish'}
          </Button>
        </div>
      </section>

      <section aria-label="Checklist" className="flex flex-col gap-1">
        <h2 className="text-sm font-medium text-text">Checklist</h2>
        <ul className="flex flex-col gap-1">
          {state.steps.map((step) => (
            <li key={step.id} className="flex items-center justify-between gap-3 rounded-lg border border-border-subtle px-3 py-2">
              <div className="min-w-0">
                <p className="text-sm text-text">{stepTitle(step)}</p>
                {step.summary && (
                  <p className="text-xs text-text-dim truncate">
                    {step.summary}
                    {lastTestFailed(step) && <span className="text-hue-amber"> · {PROVIDER_LAST_TEST_FAILED}</span>}
                  </p>
                )}
                {step.id === 'claim' && (
                  <p className="text-xs text-text-dim">
                    {ACCOUNT_ROW_TEXT}{' '}
                    <Link to="/accounts" className="underline underline-offset-2 hover:text-text">{ACCOUNT_ROW_LINK}</Link>
                  </p>
                )}
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <StatusBadge step={step} />
                {!step.read_only && step.status !== 'blocked' && step.id !== 'claim' && (
                  <Link
                    to={stopPath(step.id)}
                    state={RETURN_HERE}
                    className="text-xs text-text-muted hover:text-text underline-offset-2 hover:underline"
                  >
                    {step.status === 'done' ? 'Change' : 'Open'}
                  </Link>
                )}
              </div>
            </li>
          ))}
        </ul>
      </section>

      <section aria-label="Server check-up" className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-text">Server check-up</h2>
        {!environment ? (
          <p className="text-xs text-text-dim" role="status">Checking the server…</p>
        ) : (
          <ul className="flex flex-col gap-1">
            {environment.checks.map((check) => (
              <li key={check.id} className="flex items-start justify-between gap-3 rounded-lg border border-border-subtle px-3 py-2">
                <div className="min-w-0">
                  <p className="text-sm text-text">{check.label}{check.detail ? <span className="text-text-dim"> · {check.detail}</span> : null}</p>
                  {check.hint && check.status !== 'ok' && <p className="text-xs text-text-muted">{check.hint}</p>}
                </div>
                <Badge tone={check.status === 'ok' ? 'success' : check.status === 'warn' ? 'warning' : 'neutral'}>
                  {check.status === 'ok' ? 'ok' : check.status === 'warn' ? 'check' : 'missing'}
                </Badge>
              </li>
            ))}
          </ul>
        )}
        {environment?.served.https_hint && <Notice tone="warning">{HTTPS_HINT}</Notice>}
        {environment && environment.passwordless && environment.bind_host !== '127.0.0.1' && (
          <Notice tone="warning">{PASSWORDLESS_LAN_HINT}</Notice>
        )}
      </section>

      {files.length > 0 && (
        <section aria-label="Files written" className="flex flex-col gap-2">
          <h2 className="text-sm font-medium text-text">Files written</h2>
          <ul className="flex flex-wrap gap-1.5">
            {files.map((f) => <li key={f} className="font-mono text-xs rounded border border-border bg-code-bg px-1.5 py-0.5">{f}</li>)}
          </ul>
          <p className="text-xs text-text-dim">Keys and tokens are in config.local.yaml, readable only by the account Nerve runs as.</p>
        </section>
      )}
    </div>
  );
}
