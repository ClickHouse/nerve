import { useEffect, useState, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import {
  api, type PairingStatus, type SetupChannelsDetail,
  type SetupOnboarding, type SetupProfileDetail, type SetupProviderDetail, type SetupSourcesDetail,
  type SetupState,
} from '../../api/client';
import { useAuthStore } from '../../stores/authStore';
import { onboardingOf, useSetupStore } from '../../stores/setupStore';
import { Button } from '../ui';
import { Check, PartyPopper } from '../ui/icons';
import {
  MANAGED_SOURCE_SHORT, ONBOARDING_START, PROVIDER_LAST_TEST_FAILED, READY_ONBOARDING,
  READY_ONBOARDING_AFTER_RESTART, READY_STEPS_LEFT, READY_TELEGRAM_SYNC_LINK, READY_TELEGRAM_SYNC_UNSIGNED, SAY_HELLO,
} from './copy';
import { providerManaged, secretManaged } from './credentialSource';
import { stepOf, stopPath } from './navigation';
import { PairingWidget } from './PairingWidget';
import { personNameFor } from './personName';

type OnboardingAnswer = { status: SetupOnboarding; session_id: string | null };

/**
 * The ending. "Nerve is ready", what was set up, and the way into the
 * onboarding conversation, where the agent asks what the person wants it to
 * do and sets itself up. The conversation is the primary action while it
 * waits; when a restart comes first, the screen says it opens after the
 * restart. Without onboarding (done, skipped, or an older backend) a
 * personal install offers **Say hello**, which opens the chat with a
 * suggested first message, and a worker offers **Open chat**. The heading
 * takes focus on arrival: the Review's button is gone, and the overlay that
 * may have covered it has closed.
 */
export function ReadyScreen({ state, onboarding }: {
  state: SetupState;
  onboarding?: OnboardingAnswer | null;
}) {
  const navigate = useNavigate();
  const viewer = useAuthStore((s) => s.viewer);
  const error = useSetupStore((s) => s.error);
  const [pairing, setPairing] = useState<PairingStatus | null>(null);

  const profile = stepOf(state, 'profile')?.detail as SetupProfileDetail | undefined;
  const channels = stepOf(state, 'channels')?.detail as SetupChannelsDetail | undefined;
  const provider = stepOf(state, 'provider');
  const worker = state.mode === 'worker';
  const agentName = profile?.agent_name?.trim() || 'Nerve';
  const agentEmoji = profile?.agent_emoji?.trim() || '';
  const personName = personNameFor(state, profile, viewer);

  useEffect(() => {
    document.getElementById('setup-step-heading')?.focus();
  }, []);

  const telegramReady = !!channels?.telegram.bot_token.configured;
  const offerPairing = telegramReady && channels?.telegram.live && channels.telegram.allowed_users_count === 0;

  useEffect(() => {
    if (!offerPairing) return;
    let alive = true;
    api.setupPairing().then((s) => { if (alive) setPairing(s); }).catch(() => {});
    return () => { alive = false; };
  }, [offerPairing]);

  // finish() answered with the conversation it opened; the checklist read
  // afterwards says the same, and is the newer fact once it names a session.
  const live = onboardingOf(state);
  const conversation = live.status === 'running' && live.sessionId
    ? live.sessionId
    : (onboarding?.status === 'running' ? onboarding.session_id : null);
  const waitsForRestart = !conversation && live.status === 'pending' && state.restart_pending;

  // What was set up, one line each. A credential the operator provided
  // says so (the server's own summary already does for the model); the
  // Telegram sync that nobody has signed in to gets its line and the way
  // back, since the sync reads nothing until then.
  const summary: { key: string; text: ReactNode }[] = [];
  if (provider?.summary) {
    const providerDetail = provider.detail as SetupProviderDetail;
    const failed = providerDetail.last_test?.ok === false;
    const source = providerManaged(providerDetail);
    const sourceNote = source && !provider.summary.includes(MANAGED_SOURCE_SHORT[source]) ? ` · ${MANAGED_SOURCE_SHORT[source]}` : '';
    summary.push({ key: 'model', text: `Model: ${provider.summary}${sourceNote}${failed ? ` · ${PROVIDER_LAST_TEST_FAILED}` : ''}` });
  }
  if (telegramReady) {
    const botName = pairing?.bot_username ?? channels?.telegram.bot_username ?? null;
    const source = secretManaged(channels?.telegram.bot_token);
    summary.push({
      key: 'telegram',
      text: `${botName ? `Telegram: @${botName}` : 'Telegram bot ready to pair'}${source ? ` · ${MANAGED_SOURCE_SHORT[source]}` : ''}`,
    });
  }
  const sources = stepOf(state, 'sources')?.detail as SetupSourcesDetail | undefined;
  if (sources?.telegram.enabled && sources.telegram.session?.authorized === false) {
    summary.push({
      key: 'telegram-sync',
      text: (
        <>
          {READY_TELEGRAM_SYNC_UNSIGNED} ·{' '}
          <Link to={stopPath('sources')} className="underline underline-offset-2 hover:text-text">{READY_TELEGRAM_SYNC_LINK}</Link>
        </>
      ),
    });
  }
  const memory = stepOf(state, 'memory');
  if (memory?.summary) summary.push({ key: 'memory', text: `Memory: ${memory.summary}` });
  const automation = stepOf(state, 'automation');
  if (automation?.summary) summary.push({ key: 'automation', text: `Automation: ${automation.summary}` });

  const sayHello = () => {
    navigate('/chat', { state: { suggestedMessage: SAY_HELLO(personName) } });
  };

  return (
    <div className="flex flex-col items-center text-center gap-5 py-6">
      <div className="flex h-16 w-16 items-center justify-center rounded-full bg-success-bg text-success">
        <Check size={32} />
      </div>
      <div>
        <h1 id="setup-step-heading" tabIndex={-1} className="text-2xl font-semibold text-text outline-none flex items-center justify-center gap-2">
          <PartyPopper size={22} className="text-hue-amber" />
          {worker ? 'Your worker is ready' : 'Nerve is ready'}
        </h1>
        {/* No claim that everything is in effect: the summary below can
            list a Telegram sync nobody has signed in to or a provider whose
            last test failed, and a source can be on while its tool is
            missing on the server. */}
        <p className="text-sm text-text-muted mt-2 max-w-md">
          Setup is complete.
        </p>
        {/* Optional steps can still be open here (another tab, a reload):
            say so rather than let the banner contradict this page. */}
        {state.steps_left > 0 && (
          <p className="text-sm text-text-muted mt-2 max-w-md">{READY_STEPS_LEFT(state.steps_left)}</p>
        )}
      </div>

      {summary.length > 0 && (
        <ul className="text-sm text-text-muted flex flex-col gap-1" aria-label="Summary">
          {summary.map((line) => <li key={line.key}>{line.text}</li>)}
        </ul>
      )}

      {conversation ? (
        <div className="flex flex-col items-center gap-3">
          <p className="text-sm text-text-muted max-w-md">{READY_ONBOARDING}</p>
          <Button variant="primary" size="md" onClick={() => navigate(`/chat/${conversation}`)} className="min-h-[44px]">
            {ONBOARDING_START}
          </Button>
        </div>
      ) : waitsForRestart ? (
        <p role="status" className="text-sm text-text-muted max-w-md">{READY_ONBOARDING_AFTER_RESTART}</p>
      ) : worker ? (
        <Button variant="primary" size="md" onClick={() => navigate('/chat')} className="min-h-[44px]">Open chat</Button>
      ) : (
        <Button variant="primary" size="md" onClick={sayHello} className="min-h-[44px]">
          Say hello to {agentEmoji ? `${agentEmoji} ` : ''}{agentName}
        </Button>
      )}
      {error && <p role="alert" className="text-sm text-error">{error}</p>}

      {offerPairing && (
        <div className="w-full max-w-md text-left">
          <PairingWidget />
        </div>
      )}

      <Link to="/setup" className="text-sm text-text-muted hover:text-text underline-offset-2 hover:underline">
        Open the setup page
      </Link>
    </div>
  );
}
