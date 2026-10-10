import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { Navigate, useLocation, useNavigate, useParams } from 'react-router-dom';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { useAuthStore } from '../../stores/authStore';
import { useSetupStore } from '../../stores/setupStore';
import { Button, Drawer, PageHeader, PaneToggle } from '../ui';
import { ShieldCheck } from '../ui/icons';
import {
  REVIEW, nextStop, previousStop, stepOf, stepTitle, stopPath, stopPosition, type WizardStop,
} from './navigation';
import { Overview } from './Overview';
import { ProgressRail } from './ProgressRail';
import { RestartOverlay } from './RestartOverlay';
import { AgentsStep } from './steps/AgentsStep';
import { AutomationStep } from './steps/AutomationStep';
import { ChannelsStep } from './steps/ChannelsStep';
import { MemoryStep } from './steps/MemoryStep';
import { ModeStep } from './steps/ModeStep';
import { ProfileStep } from './steps/ProfileStep';
import { ProviderStep } from './steps/ProviderStep';
import { ReviewStep } from './steps/ReviewStep';
import { SourcesStep } from './steps/SourcesStep';
import { ClaimDoneStep } from './steps/ClaimDoneStep';
import { WizardContext, type WizardNav } from './wizardContext';

/**
 * The wizard's frame: header, progress rail (a column on a desktop, a drawer
 * on a phone), and the current stop — Overview, a step form, or Review.
 *
 * The state is re-read on mount, on every step change and when the tab
 * becomes visible again, so another person's save (or a restart) is seen.
 * Forms are keyed by viewer and by the step's `etag`, which covers only the
 * persisted part of the detail — a Test result or a pairing poll never
 * remounts a form somebody is typing into.
 */
/**
 * Where an edit returns to. Only the wizard's own pages are accepted, so a
 * stray `history.state` can never send the person elsewhere.
 */
function returnToOf(state: unknown): string | null {
  if (!state || typeof state !== 'object' || !('returnTo' in state)) return null;
  const to = (state as { returnTo?: unknown }).returnTo;
  return typeof to === 'string' && to.startsWith('/setup') ? to : null;
}

export function WizardShell() {
  const { stepId } = useParams<{ stepId?: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const returnTo = returnToOf(location.state);
  const isMobile = useIsMobile();
  const state = useSetupStore((s) => s.state);
  const loading = useSetupStore((s) => s.loading);
  const error = useSetupStore((s) => s.error);
  const load = useSetupStore((s) => s.load);
  const viewerId = useAuthStore((s) => s.viewer?.id ?? 'nobody');
  const [railOpen, setRailOpen] = useState(false);

  useEffect(() => { void load(); }, [load, stepId]);
  useEffect(() => {
    const onVisible = () => { if (document.visibilityState === 'visible') void load(); };
    document.addEventListener('visibilitychange', onVisible);
    return () => document.removeEventListener('visibilitychange', onVisible);
  }, [load]);

  // A "Saved…" notice belongs to the step it was shown on, so it goes when
  // the stop changes — a navigation, not a re-render.
  const previousStep = useRef<string | undefined>(stepId);
  useEffect(() => {
    if (previousStep.current === stepId) return;
    previousStep.current = stepId;
    useSetupStore.getState().clearSavedNotice();
  }, [stepId]);

  // Focus the step heading once per stop, including the first mount (the
  // claim lands here on /setup/mode; a nav click lands on the Overview), so a
  // keyboard or screen-reader user starts on the page rather than on <body>.
  // The heading exists only once the state is loaded, so the effect also
  // runs when that happens and focuses the stop it has not focused yet.
  const focusedStop = useRef<string | null>(null);
  const ready = state !== null;
  useEffect(() => {
    const stop = stepId ?? '';
    if (focusedStop.current === stop) return;
    const headingEl = document.getElementById('setup-step-heading');
    if (!headingEl) return;
    focusedStop.current = stop;
    headingEl.focus();
  }, [stepId, ready]);

  const goTo = useCallback((stop: WizardStop | null) => {
    navigate(stop ? stopPath(stop) : '/setup');
  }, [navigate]);

  // A step opened to be *edited* (Change/Open on the Overview or the Review)
  // returns to where it was opened from, forwards and backwards; the forward
  // flow marches on.
  const nav = useMemo<WizardNav>(() => ({
    goTo,
    advance: (from) => {
      if (returnTo) { navigate(returnTo); return; }
      const current = useSetupStore.getState().state;
      goTo(current ? nextStop(current, from) : REVIEW);
    },
    back: (from) => {
      if (returnTo) { navigate(returnTo); return; }
      const current = useSetupStore.getState().state;
      goTo(current ? previousStop(current, from) : null);
    },
    returnTo,
    viewerId,
  }), [goTo, navigate, returnTo, viewerId]);

  const current: WizardStop | null = stepId ? (stepId as WizardStop) : null;

  let body: ReactNode;
  if (!state) {
    body = loading
      ? <p className="text-sm text-text-dim" role="status">Loading…</p>
      : (
        <div className="flex flex-col gap-3">
          <h1 id="setup-step-heading" tabIndex={-1} className="text-xl font-semibold text-text outline-none">Setup</h1>
          <p role={error ? 'alert' : 'status'} className={`text-sm ${error ? 'text-error' : 'text-text-muted'}`}>
            {error ?? 'This server does not offer the setup checklist.'}
          </p>
          {error && <div><Button variant="secondary" size="sm" onClick={() => void load()}>Retry</Button></div>}
        </div>
      );
  } else if (!current) {
    body = <Overview state={state} />;
  } else if (current === REVIEW) {
    body = <ReviewStep state={state} />;
  } else {
    const step = stepOf(state, current);
    if (!step) {
      body = <Navigate to="/setup" replace />;
    } else {
      const key = `${viewerId}:${step.etag}`;
      switch (step.id) {
        case 'claim': body = <ClaimDoneStep key={key} step={step} />; break;
        case 'mode': body = <ModeStep key={key} step={step} state={state} />; break;
        case 'provider': body = <ProviderStep key={key} step={step} />; break;
        case 'profile': body = <ProfileStep key={key} step={step} state={state} />; break;
        case 'memory': body = <MemoryStep key={key} step={step} state={state} />; break;
        case 'channels': body = <ChannelsStep key={key} step={step} state={state} />; break;
        case 'sources': body = <SourcesStep key={key} step={step} state={state} />; break;
        case 'automation': body = <AutomationStep key={key} step={step} state={state} />; break;
        case 'agents': body = <AgentsStep key={key} step={step} state={state} />; break;
        default: body = <Navigate to="/setup" replace />;
      }
    }
  }

  // The phone's position line. Steps count the server's steps only (the same
  // M as the Overview's "N of M"); Review is named, not numbered.
  const position = state && current ? stopPosition(state, current) : null;
  const currentStep = state && current ? stepOf(state, current) : undefined;
  const positionLine = state && current
    ? (current === REVIEW
      ? 'Review'
      : position ? `Step ${position.index} of ${position.total} · ${currentStep ? stepTitle(currentStep) : ''}` : null)
    : null;

  return (
    <WizardContext.Provider value={nav}>
      <div className="flex-1 flex flex-col h-full overflow-hidden">
        {/* The body owns the page's <h1> (the focusable step heading), so
            the header title is plain text here. */}
        <PageHeader
          leading={isMobile && state
            ? <PaneToggle open={railOpen} onToggle={() => setRailOpen((o) => !o)} label="setup steps" />
            : undefined}
          icon={<ShieldCheck size={18} className="text-hue-amber" />}
          title="Set up Nerve"
          titleAs="p"
        />
        {isMobile && positionLine && (
          <p className="px-4 py-1.5 text-xs text-text-dim border-b border-border-subtle shrink-0">
            {positionLine}
          </p>
        )}
        <div className="flex-1 flex min-h-0">
          {state && (isMobile ? (
            <Drawer open={railOpen} onClose={() => setRailOpen(false)} side="left" label="Setup steps">
              <div className="p-3 overflow-y-auto">
                <ProgressRail state={state} current={current} onNavigate={() => setRailOpen(false)} />
              </div>
            </Drawer>
          ) : (
            <aside className="hidden md:flex w-[220px] shrink-0 flex-col border-r border-border-subtle p-3 overflow-y-auto">
              <ProgressRail state={state} current={current} />
            </aside>
          ))}
          <div className="flex-1 overflow-auto p-4 lg:p-6 min-w-0 scroll-pb-20">
            <div className="max-w-2xl mx-auto min-h-full flex flex-col">
              {body}
            </div>
          </div>
        </div>
      </div>
      <RestartOverlay />
    </WizardContext.Provider>
  );
}
