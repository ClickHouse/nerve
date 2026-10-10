import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import {
  api, type PairingStatus, type SetupChannelsDetail, type SetupModeDetail,
  type SetupOnboarding, type SetupProfileDetail, type SetupProviderDetail, type SetupState,
} from '../../api/client';
import { useAuthStore } from '../../stores/authStore';
import { useSetupStore } from '../../stores/setupStore';
import { Button } from '../ui';
import { Check, Loader2, PartyPopper } from '../ui/icons';
import {
  ONBOARDING_OPEN, ONBOARDING_SENTENCES, ONBOARDING_TRY_AGAIN, PROVIDER_LAST_TEST_FAILED, READY_STEPS_LEFT,
  SAY_HELLO,
} from './copy';
import { stepOf } from './navigation';
import { PairingWidget } from './PairingWidget';
import { personNameFor } from './personName';

const ONBOARDING_POLL_MS = 5000;
const ONBOARDING_POLL_LIMIT = 120;  // 10 minutes

type OnboardingAnswer = { status: SetupOnboarding; session_id: string | null };

/**
 * The ending. "Nerve is ready", what was set up, and **Say hello**, which
 * opens the chat with a suggested first message. A worker ends with
 * "Onboarding session starting" and a link to that session — or, when the
 * session stopped without writing a plan (`ended`), with the link and a
 * **Try again** that asks the server to schedule it once more. The heading
 * takes focus on arrival: the Review's button is gone, and the overlay that
 * may have covered it has closed.
 */
export function ReadyScreen({ state, onboarding }: {
  state: SetupState;
  onboarding?: OnboardingAnswer | null;
}) {
  const navigate = useNavigate();
  const viewer = useAuthStore((s) => s.viewer);
  const load = useSetupStore((s) => s.load);
  const finish = useSetupStore((s) => s.finish);
  const busy = useSetupStore((s) => s.busy);
  const error = useSetupStore((s) => s.error);
  const [pairing, setPairing] = useState<PairingStatus | null>(null);
  // Try again's answer, and the session it is meant to replace.
  const [retry, setRetry] = useState<(OnboardingAnswer & { supersedes: string | null }) | null>(null);

  const profile = stepOf(state, 'profile')?.detail as SetupProfileDetail | undefined;
  const mode = stepOf(state, 'mode')?.detail as SetupModeDetail | undefined;
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

  // `onboarding` is what finish() answered once; `state` is re-read every
  // 5 s below. Once the live detail has moved past `pending` it is the
  // truth — the finish() result would otherwise pin "starting…" for ever.
  // One exception: right after Try again the checklist may still describe
  // the session that ended while the answer names the one just scheduled;
  // the answer is the newer fact until the checklist talks about that
  // session (running or, if it stopped again, ended).
  const live = mode?.onboarding;
  const liveSession = mode?.onboarding_session_id ?? null;
  let onboardingStatus: SetupOnboarding;
  let onboardingSession: string | null;
  if (retry && live === 'ended' && liveSession === retry.supersedes && liveSession !== retry.session_id) {
    onboardingStatus = retry.status;
    onboardingSession = retry.session_id;
  } else {
    onboardingStatus = live && live !== 'pending' ? live : (onboarding?.status ?? live ?? 'not_applicable');
    onboardingSession = liveSession ?? onboarding?.session_id ?? null;
  }

  // A worker's onboarding session is created by the server; poll until it
  // is running or done, bounded, so the link appears without a reload. An
  // ended session is not waited for: it has a link and a Try again.
  const polling = worker && (onboardingStatus === 'pending' || onboardingStatus === 'running');
  useEffect(() => {
    if (!polling) return;
    let count = 0;
    const timer = setInterval(() => {
      count += 1;
      if (count > ONBOARDING_POLL_LIMIT) { clearInterval(timer); return; }
      void load();
    }, ONBOARDING_POLL_MS);
    return () => clearInterval(timer);
  }, [polling, load]);

  // The server re-schedules the onboarding on an explicit finish.
  const tryAgain = async () => {
    const supersedes = onboardingSession;
    const result = await finish();
    if (result) setRetry({ ...result.onboarding, supersedes });
  };

  const summary: string[] = [];
  if (provider?.summary) {
    const failed = (provider.detail as SetupProviderDetail).last_test?.ok === false;
    summary.push(`Model: ${provider.summary}${failed ? ` · ${PROVIDER_LAST_TEST_FAILED}` : ''}`);
  }
  if (telegramReady) {
    summary.push(pairing?.bot_username || channels?.telegram.bot_username
      ? `Telegram: @${pairing?.bot_username ?? channels?.telegram.bot_username}`
      : 'Telegram bot ready to pair');
  }
  const memory = stepOf(state, 'memory');
  if (memory?.summary) summary.push(`Memory: ${memory.summary}`);
  const automation = stepOf(state, 'automation');
  if (automation?.summary) summary.push(`Automation: ${automation.summary}`);

  const sayHello = () => {
    navigate('/chat', { state: { suggestedMessage: SAY_HELLO(personName) } });
  };

  const starting = busy === 'finish';

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
        <p className="text-sm text-text-muted mt-2 max-w-md">
          {worker
            ? 'Setup is complete. The worker reads its task description on first boot and plans its work in an onboarding session.'
            : 'Setup is complete and everything you chose is in effect.'}
        </p>
        {/* Optional steps can still be open here (another tab, a reload):
            say so rather than let the banner contradict this page. */}
        {state.steps_left > 0 && (
          <p className="text-sm text-text-muted mt-2 max-w-md">{READY_STEPS_LEFT(state.steps_left)}</p>
        )}
      </div>

      {summary.length > 0 && (
        <ul className="text-sm text-text-muted flex flex-col gap-1" aria-label="Summary">
          {summary.slice(0, 3).map((line) => <li key={line}>{line}</li>)}
        </ul>
      )}

      {worker ? (
        <div className="flex flex-col items-center gap-3">
          {onboardingStatus === 'running' && onboardingSession ? (
            <>
              <p role="status" className="text-sm text-text-muted flex items-center gap-2">
                <Loader2 size={14} className="animate-spin" />{ONBOARDING_SENTENCES.running}
              </p>
              <Button variant="primary" size="md" onClick={() => navigate(`/chat/${onboardingSession}`)}>
                {ONBOARDING_OPEN}
              </Button>
            </>
          ) : onboardingStatus === 'done' ? (
            <Button variant="primary" size="md" onClick={() => navigate('/chat')}>Open chat</Button>
          ) : onboardingStatus === 'ended' ? (
            <>
              <p role="status" className="text-sm text-text-muted max-w-md">{ONBOARDING_SENTENCES.ended}</p>
              <div className="flex flex-wrap items-center justify-center gap-2">
                {onboardingSession && (
                  <Button variant="secondary" size="md" onClick={() => navigate(`/chat/${onboardingSession}`)} className="min-h-[44px]">
                    {ONBOARDING_OPEN}
                  </Button>
                )}
                <Button variant="primary" size="md" onClick={() => void tryAgain()} disabled={busy !== null} aria-busy={starting || undefined} className="min-h-[44px]">
                  {starting ? 'Starting again…' : ONBOARDING_TRY_AGAIN}
                </Button>
              </div>
            </>
          ) : (
            <p role="status" className="text-sm text-text-muted flex items-center gap-2">
              <Loader2 size={14} className="animate-spin" />Onboarding session starting…
            </p>
          )}
          {error && <p role="alert" className="text-sm text-error">{error}</p>}
        </div>
      ) : (
        <Button variant="primary" size="md" onClick={sayHello} className="min-h-[44px]">
          Say hello to {agentEmoji ? `${agentEmoji} ` : ''}{agentName}
        </Button>
      )}

      {offerPairing && (
        <div className="w-full max-w-md text-left">
          <PairingWidget />
        </div>
      )}

      <Link to="/setup" className="text-sm text-text-muted hover:text-text underline-offset-2 hover:underline">
        Open Setup later
      </Link>
    </div>
  );
}
