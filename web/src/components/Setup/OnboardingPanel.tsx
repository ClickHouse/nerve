import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { SetupState } from '../../api/client';
import { onboardingOf, useSetupStore } from '../../stores/setupStore';
import { Button } from '../ui';
import { MessageSquare } from '../ui/icons';
import {
  ONBOARDING_OPEN, ONBOARDING_SENTENCES, ONBOARDING_SKIP, ONBOARDING_SKIP_CONFIRM, ONBOARDING_SKIP_KEEP,
} from './copy';

/**
 * The onboarding conversation on the setup page, while it is pending or
 * waits for the person: a way into it, and **Skip onboarding** behind a
 * confirmation. A skip keeps the session; it only stops the reminders.
 */
export function OnboardingPanel({ state }: { state: SetupState }) {
  const navigate = useNavigate();
  const skip = useSetupStore((s) => s.skipOnboarding);
  const busy = useSetupStore((s) => s.busy);
  const [confirming, setConfirming] = useState(false);
  const { status, sessionId } = onboardingOf(state);
  if (status !== 'pending' && status !== 'running') return null;

  return (
    <section
      aria-label="Onboarding"
      className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-4"
    >
      <p className="flex items-start gap-2 text-sm text-text">
        <MessageSquare size={16} className="mt-0.5 shrink-0 text-accent" />
        {ONBOARDING_SENTENCES[status]}
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
            <Button variant="ghost" size="sm" onClick={() => setConfirming(false)}>
              {ONBOARDING_SKIP_KEEP}
            </Button>
          </div>
        </div>
      ) : (
        <div className="flex flex-wrap gap-2">
          {status === 'running' && sessionId && (
            <Button variant="primary" size="sm" onClick={() => navigate(`/chat/${sessionId}`)}>
              {ONBOARDING_OPEN}
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
