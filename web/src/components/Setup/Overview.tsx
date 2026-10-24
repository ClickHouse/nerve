import { Link, useNavigate } from 'react-router-dom';
import type { SetupState } from '../../api/client';
import { Button } from '../ui';
import { ArrowRight, RefreshCw } from '../ui/icons';
import { actorName } from '../../stores/actorStore';
import { useAuthStore } from '../../stores/authStore';
import {
  ACCOUNT_ROW_LINK, ACCOUNT_ROW_TEXT, ALL_STEPS_OPERATOR, OPERATOR_SECTION_HEADING, OVERVIEW_SUBLINE,
  OVERVIEW_SUBLINE_FINISHED,
} from './copy';
import { firstPendingStop, operatorSteps, ownSteps, stepTitle, stopPath } from './navigation';
import { Notice } from './Notice';
import { StatusBadge } from './StatusBadge';
import { useWizardNav } from './wizardContext';

/**
 * `/setup`: orientation before the first step, and the place to come back to.
 *
 * Who you are (from the actor the auth store confirmed — in external mode
 * that is the name your organisation's sign-in supplied), what the wizard
 * does, the person's own steps, and a Continue that lands on the first
 * pending step. "Change" opens a done step to edit it: Save and Back come
 * straight back here. Under the list, "Your operator has configured" names
 * the done steps that the person cannot change here, with their summaries.
 * A step that a lockdown or the instance's policy closed is not shown. When
 * no step is left for the person (external mode under lockdown, before a
 * mode is chosen), the page says so in place of the count and the list.
 */
export function Overview({ state }: { state: SetupState }) {
  const nav = useWizardNav();
  const navigate = useNavigate();
  const viewer = useAuthStore((s) => s.viewer);
  const account = useAuthStore((s) => s.account);
  const external = state.auth_mode === 'external';
  const name = viewer ? actorName(viewer) : (account?.username ?? 'you');
  const own = ownSteps(state);
  const operator = operatorSteps(state);
  const done = own.filter((s) => s.status === 'done');
  const skipped = own.filter((s) => s.status === 'skipped');
  const next = firstPendingStop(state);
  const count = state.finished && skipped.length > 0
    ? `${done.length} done · ${skipped.length} skipped`
    : `${done.length} of ${own.length} steps done${skipped.length > 0 ? ` · ${skipped.length} skipped` : ''}`;

  return (
    <div className="flex flex-col gap-4">
      <div>
        {/* The page header above already says "Set up Nerve"; the heading
            greets the person instead (in external mode, by the name their
            organisation's sign-in supplied). The line under it says where
            the name came from in external mode; locally the heading has
            already said who, so the line moves the story forward. */}
        <h1 id="setup-step-heading" tabIndex={-1} className="text-xl font-semibold text-text outline-none">
          {state.finished ? 'Everything is set up' : `Welcome, ${name}`}
        </h1>
        <p className="text-sm text-text-muted mt-1">
          {external
            ? <>Signed in as <strong className="text-text">{name}</strong> through your organisation's sign-in.</>
            : (state.finished ? OVERVIEW_SUBLINE_FINISHED : OVERVIEW_SUBLINE)}
        </p>
      </div>

      <div className="rounded-lg border border-border bg-surface-raised p-4 flex flex-col gap-2 text-sm text-text-muted">
        {own.length === 0 ? <p>{ALL_STEPS_OPERATOR}</p> : (
          <>
            <p>
              Each step saves when you continue.
              {external && ' These settings apply to everyone who uses this instance, not only to you.'}
            </p>
            <p><strong className="text-text">{count}</strong>.</p>
          </>
        )}
      </div>

      {state.lockdown && (
        <Notice tone="locked">
          {state.read_only_reason ?? 'Configuration is managed from the config repository on this instance.'}
          {' '}Steps that write configuration are locked. The other steps still work.
        </Notice>
      )}

      {state.restart_pending && (
        <Notice tone="warning" icon={<RefreshCw size={14} />}>
          Some changes take effect after a restart. <Link to="/setup/review" className="underline">Review and restart</Link>.
        </Notice>
      )}

      {state.warning && <Notice tone="warning">{state.warning}</Notice>}

      {own.length > 0 && (
        <ol className="flex flex-col gap-1" aria-label="Steps">
          {own.map((step) => (
            <li key={step.id} className="flex items-center justify-between gap-3 rounded-lg border border-border-subtle px-3 py-2">
              <div className="min-w-0">
                <Link to={stopPath(step.id)} className="text-sm text-text hover:underline underline-offset-2">
                  {stepTitle(step)}
                </Link>
                {step.summary && <p className="text-xs text-text-dim truncate">{step.summary}</p>}
                {/* The account's page is read-only; what can be changed lives in Accounts. */}
                {step.id === 'claim' && (
                  <p className="text-xs text-text-dim">
                    {ACCOUNT_ROW_TEXT}{' '}
                    <Link to="/accounts" className="underline underline-offset-2 hover:text-text">{ACCOUNT_ROW_LINK}</Link>
                  </p>
                )}
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <StatusBadge step={step} />
                {step.status === 'done' && !step.read_only && step.id !== 'claim' && (
                  <Link
                    to={stopPath(step.id)}
                    state={{ returnTo: '/setup' }}
                    className="text-xs text-text-muted hover:text-text underline-offset-2 hover:underline"
                  >
                    Change
                  </Link>
                )}
              </div>
            </li>
          ))}
        </ol>
      )}

      {operator.length > 0 && (
        <section aria-labelledby="setup-operator-heading" className="flex flex-col gap-1">
          <h2 id="setup-operator-heading" className="text-sm font-medium text-text">{OPERATOR_SECTION_HEADING}</h2>
          <ul className="flex flex-col gap-1">
            {operator.map((step) => (
              <li key={step.id} className="rounded-lg border border-border-subtle px-3 py-2">
                <p className="text-sm text-text">{stepTitle(step)}</p>
                {step.summary && <p className="text-xs text-text-dim truncate">{step.summary}</p>}
              </li>
            ))}
          </ul>
        </section>
      )}

      {/* Finished: a person who is done wants the chat, so that is the
          primary action; the review stays one click away. */}
      <div className="flex items-center gap-3">
        {state.finished ? (
          <>
            <Button variant="primary" size="md" onClick={() => navigate('/chat')} className="min-h-[44px]">
              Back to chat<ArrowRight size={14} />
            </Button>
            <Link to={stopPath('review')} className="text-sm text-text-muted hover:text-text underline-offset-2 hover:underline min-h-[44px] inline-flex items-center">
              Open review
            </Link>
          </>
        ) : (
          <Button variant="primary" size="md" onClick={() => nav.goTo(next)} className="min-h-[44px]">
            Continue<ArrowRight size={14} />
          </Button>
        )}
      </div>
    </div>
  );
}
