import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { SetupState } from '../../api/client';
import { onboardingCanStart, onboardingOf, useSetupStore } from '../../stores/setupStore';
import { Button } from '../ui';
import { MessageSquare } from '../ui/icons';
import {
  ONBOARDING_OPEN, ONBOARDING_OPEN_SESSION, ONBOARDING_SKIP, ONBOARDING_SKIP_CONFIRM, ONBOARDING_SKIP_KEEP,
  ONBOARDING_START, onboardingSentence,
} from './copy';

/**
 * The onboarding on the setup page, while it is pending or runs: a way into
 * it (**Start the conversation** once setup is complete), and **Skip
 * onboarding** behind a confirmation. A skip keeps the session; it only
 * stops the reminders.
 */
export function OnboardingPanel({ state }: { state: SetupState }) {
  const navigate = useNavigate();
  const skip = useSetupStore((s) => s.skipOnboarding);
  const startOnboarding = useSetupStore((s) => s.startOnboarding);
  const busy = useSetupStore((s) => s.busy);
  const [confirming, setConfirming] = useState(false);
  const { status, sessionId, kind } = onboardingOf(state);
  const canStart = onboardingCanStart(state);
  if (status !== 'pending' && status !== 'running') return null;

  return (
    <section
      aria-label="Onboarding"
      className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-4"
    >
      <p className="flex items-start gap-2 text-sm text-text">
        <MessageSquare size={16} className="mt-0.5 shrink-0 text-accent" />
        {onboardingSentence(status, kind, canStart)}
      </p>
      {confirming ? (
        <div className="flex flex-col gap-2">
          <p className="text-sm text-text-muted">{ONBOARDING_SKIP_CONFIRM}</p>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="danger"
              size="sm"
              disabled={busy !== null}
              onClick={async () => { if (await skip()) setConfirming(false); }}
            >
              {ONBOARDING_SKIP}
            </Button>
            {/* The safe choice takes the focus that the Skip button had. */}
            <Button variant="ghost" size="sm" autoFocus onClick={() => setConfirming(false)}>
              {ONBOARDING_SKIP_KEEP}
            </Button>
          </div>
        </div>
      ) : (
        <div className="flex flex-wrap gap-2">
          {status === 'running' && sessionId && (
            <Button variant="primary" size="sm" onClick={() => navigate(`/chat/${sessionId}`)}>
              {kind === 'autonomous' ? ONBOARDING_OPEN_SESSION : ONBOARDING_OPEN}
            </Button>
          )}
          {canStart && (
            <Button
              variant="primary"
              size="sm"
              disabled={busy !== null}
              onClick={async () => {
                const id = await startOnboarding();
                if (id) navigate(`/chat/${id}`);
              }}
            >
              {ONBOARDING_START}
            </Button>
          )}
          <Button variant="secondary" size="sm" onClick={() => setConfirming(true)}>
            {ONBOARDING_SKIP}
          </Button>
        </div>
      )}
    </section>
  );
}
