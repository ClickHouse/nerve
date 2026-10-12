import { Link, useNavigate } from 'react-router-dom';
import type { SetupState, SetupStep } from '../../api/client';
import { Button } from '../ui';
import { ArrowRight, RefreshCw } from '../ui/icons';
import { actorName } from '../../stores/actorStore';
import { useAuthStore } from '../../stores/authStore';
import { ACCOUNT_ROW_LINK, ACCOUNT_ROW_TEXT, NOT_AVAILABLE_HERE, OVERVIEW_SUBLINE, OVERVIEW_SUBLINE_FINISHED } from './copy';
import { stepManaged } from './credentialSource';
import { closedByPolicy, firstPendingStop, stepTitle, stopPath, waitsForMode } from './navigation';
import { Notice } from './Notice';
import { StatusBadge } from './StatusBadge';
import { useWizardNav } from './wizardContext';

/**
 * A done step whose configuration provably did not come from this wizard:
 * a credential the operator provided (the environment, a configuration
 * repository, the channel gateway), or any config-writing step on a
 * lockdown instance (its files arrive from the repository). Anything else
 * may well be the person's own work from a minute ago, or another
 * person's, and is not called "already configured".
 */
function preconfigured(step: SetupStep, state: SetupState): boolean {
  if (step.status !== 'done') return false;
  if (stepManaged(step) !== null) return true;
  return state.lockdown && step.id !== 'claim' && step.id !== 'mode';
}

/**
 * `/setup`: orientation before the first step, and the place to come back to.
 *
 * Who you are (from the actor the auth store confirmed — in external mode
 * that is the name your organisation's sign-in supplied), what the wizard
 * does, what was provably configured outside it, and a Continue that lands
 * on the first pending step. "Change" opens a done step to edit it: Save
 * and Back come straight back here.
 */
export function Overview({ state }: { state: SetupState }) {
  const nav = useWizardNav();
  const navigate = useNavigate();
  const viewer = useAuthStore((s) => s.viewer);
  const account = useAuthStore((s) => s.account);
  const external = state.auth_mode === 'external';
  const name = viewer ? actorName(viewer) : (account?.username ?? 'you');
  const done = state.steps.filter((s) => s.status === 'done');
  const skipped = state.steps.filter((s) => s.status === 'skipped');
  const total = state.steps.length;
  const next = firstPendingStop(state);
  const preset = state.steps.filter((s) => preconfigured(s, state)).map((s) => stepTitle(s));
  const count = state.finished && skipped.length > 0
    ? `${done.length} done · ${skipped.length} skipped`
    : `${done.length} of ${total} steps done${skipped.length > 0 ? ` · ${skipped.length} skipped` : ''}`;

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
        <p>
          Each step saves when you continue.
          {external && ' These settings apply to everyone who uses this instance, not only to you.'}
        </p>
        <p>
          <strong className="text-text">{count}</strong>.
          {preset.length > 0 && !state.finished && (
            <> Already configured: {preset.join(', ')}.</>
          )}
        </p>
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

      <ol className="flex flex-col gap-1" aria-label="Steps">
        {state.steps.map((step) => (
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
              {step.status === 'blocked' && step.blocked_reason && !waitsForMode(step) && (
                <p className="text-xs text-text-dim">{step.blocked_reason}</p>
              )}
            </div>
            <div className="flex items-center gap-2 shrink-0">
              {/* A step the instance has closed (it keeps no credentials on
                  disk) is not a "blocked" chore: the row says so in words,
                  and the reason above it says why. */}
              {closedByPolicy(step)
                ? <span className="text-xs text-text-muted">{NOT_AVAILABLE_HERE}</span>
                : <StatusBadge step={step} />}
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
