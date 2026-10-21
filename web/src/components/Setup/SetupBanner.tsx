import { useEffect, useState, type ReactNode } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { onboardingCanStart, onboardingOf, useSetupStore } from '../../stores/setupStore';
import { IconButton } from '../ui';
import { AlertTriangle, MessageSquare, X } from '../ui/icons';
import {
  BANNER_ONBOARDING, BANNER_ONBOARDING_AUTONOMOUS, BANNER_ONBOARDING_START, BANNER_RESTART, BANNER_STEPS_LEFT,
} from './copy';

/**
 * Where the banner sits, so that it covers nothing anyone needs.
 *
 * Desktop: bottom-left, but to the right of the 56 px `NavRail` (`w-14`),
 * whose bottom holds the connection dot, the theme toggle and Logout — a
 * `left-4` box would lie exactly on top of them. 4.5rem = the rail plus the
 * usual 1rem gap. (Bottom-right belongs to the notification toast.)
 *
 * Phone: under the page header rather than over it. Every header is
 * `py-2.5` around 32 px controls (≈ 53 px with its border), so the box
 * starts 3.5rem below the safe-area inset; the bottom belongs to the nav
 * bar and the composer.
 */
const BANNER_DESKTOP_CLASS = 'bottom-4 left-[4.5rem] max-w-sm';
const BANNER_PHONE_CLASS = 'inset-x-2 top-[calc(env(safe-area-inset-top)+3.5rem)]';
const BANNER_TEXT_CLASS = 'flex items-start gap-2 flex-1 min-w-0 text-sm text-text-muted hover:text-text';

/**
 * Two reminders, one place.
 *
 * **The onboarding** comes first. When setup is complete and the
 * conversation has not started (nobody pressed Finish, or an install from
 * before onboarding), it offers **Start**, which runs Finish (idempotent)
 * and opens the conversation. While the agent waits in the conversation,
 * every page except that chat says so, with **Continue**. While a worker
 * sets itself up without a person, it only links to that session. It is not
 * dismissible for good: the close button hides it until the next page. It
 * shows for everyone who uses the instance, because the conversation sets up
 * the shared agent and anyone can continue or skip it. The setup state is
 * read again on each page change while a session runs, so it goes away soon
 * after the agent marks onboarding done.
 *
 * **The steps left** (`BANNER_STEPS_LEFT`), for an abandoned wizard.
 * Hidden on `/setup` itself, when the server says setup is finished, while a
 * restart is in progress, and after a dismissal (per person, per tab
 * session).
 *
 * An overlay rather than a bar in the layout: every page is a full-height
 * flex box and a strip would cost each one its height. Nothing shows while
 * the state is unknown. It depends on the setup store only, so it renders
 * the same inside the hosted adapter as in local mode.
 */
export function SetupBanner() {
  const state = useSetupStore((s) => s.state);
  const dismissed = useSetupStore((s) => s.bannerDismissed);
  const reconnecting = useSetupStore((s) => s.reconnecting);
  const dismiss = useSetupStore((s) => s.dismissBanner);
  const load = useSetupStore((s) => s.load);
  const startOnboarding = useSetupStore((s) => s.startOnboarding);
  const busy = useSetupStore((s) => s.busy);
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const isMobile = useIsMobile();
  const [collapsedOn, setCollapsedOn] = useState<string | null>(null);

  const onboarding = onboardingOf(state);
  const waiting = onboarding.status === 'running' && onboarding.sessionId !== null;
  const canStart = onboardingCanStart(state);
  useEffect(() => {
    if (waiting) void load();
  }, [pathname, waiting, load]);

  if (!state || reconnecting) return null;
  if (waiting || canStart) {
    const chat = `/chat/${onboarding.sessionId}`;
    if ((waiting && pathname === chat) || collapsedOn === pathname) return null;
    const icon = <MessageSquare size={14} className="text-accent shrink-0 mt-0.5" />;
    const hide = { label: 'Hide until the next page', onClose: () => setCollapsedOn(pathname) };
    if (canStart) {
      const start = async () => {
        const sessionId = await startOnboarding();
        if (sessionId) navigate(`/chat/${sessionId}`);
      };
      return (
        <BannerBox isMobile={isMobile} onAction={start} disabled={busy !== null} icon={icon}
          text={BANNER_ONBOARDING_START} action="Start" {...hide} />
      );
    }
    const autonomous = onboarding.kind === 'autonomous';
    return (
      <BannerBox isMobile={isMobile} to={chat} icon={icon}
        text={autonomous ? BANNER_ONBOARDING_AUTONOMOUS : BANNER_ONBOARDING}
        action={autonomous ? 'Open' : 'Continue'} {...hide} />
    );
  }
  if (state.finished || dismissed) return null;
  if (pathname.startsWith('/setup')) return null;

  const stepsLeft = state.steps_left;
  const restartOnly = stepsLeft === 0;
  if (restartOnly && !state.restart_pending) return null;

  const text = restartOnly
    ? BANNER_RESTART
    : BANNER_STEPS_LEFT(stepsLeft) + (state.restart_pending ? ' · A restart is pending' : '');
  const action = restartOnly ? 'Restart' : 'Continue';
  const to = restartOnly ? '/setup/review' : '/setup';

  return (
    <BannerBox isMobile={isMobile} to={to} icon={<AlertTriangle size={14} className="text-hue-amber shrink-0 mt-0.5" />}
      text={text} action={action} label="Dismiss" onClose={dismiss} />
  );
}

/** `to` makes the text a link; `onAction` makes it a button. */
function BannerBox({ isMobile, to, onAction, disabled, icon, text, action, label, onClose }: {
  isMobile: boolean;
  to?: string;
  onAction?: () => void;
  disabled?: boolean;
  icon: ReactNode;
  text: string;
  action: string;
  label: string;
  onClose: () => void;
}) {
  const content = (
    <>
      {icon}
      <span>
        {text} · <span className="text-text underline underline-offset-2">{action}</span>
      </span>
    </>
  );
  return (
    <div
      className={`fixed z-40 flex items-start gap-2 rounded-lg border border-border-subtle bg-surface-raised shadow-xl p-3
        ${isMobile ? BANNER_PHONE_CLASS : BANNER_DESKTOP_CLASS}`}
      // The top inset is already in the `top` offset; a fixed box still has
      // to pay the side insets itself (the shell's padding never reaches it).
      style={isMobile ? { paddingLeft: 'env(safe-area-inset-left)', paddingRight: 'env(safe-area-inset-right)' } : undefined}
    >
      {to ? (
        <Link to={to} role="status" className={BANNER_TEXT_CLASS}>{content}</Link>
      ) : (
        <button type="button" onClick={onAction} disabled={disabled} className={`${BANNER_TEXT_CLASS} text-left`}>
          {content}
        </button>
      )}
      <IconButton label={label} size="xs" onClick={onClose}>
        <X size={14} />
      </IconButton>
    </div>
  );
}
