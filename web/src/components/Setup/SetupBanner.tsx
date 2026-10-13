import { Link, useLocation } from 'react-router-dom';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { useSetupStore } from '../../stores/setupStore';
import { IconButton } from '../ui';
import { AlertTriangle, X } from '../ui/icons';
import { BANNER_RESTART, BANNER_STEPS_LEFT } from './copy';

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

/**
 * "Finish setting up Nerve — N steps left", for an abandoned wizard.
 *
 * An overlay rather than a bar in the layout: every page is a full-height
 * flex box and a strip would cost each one its height. Hidden on `/setup`
 * itself, when the server says setup is finished, while the state is
 * unknown, while a restart is in progress, and after a dismissal (per
 * person, per tab session).
 *
 * It depends on the setup store only, so it renders the same inside the
 * hosted adapter as in local mode.
 */
export function SetupBanner() {
  const state = useSetupStore((s) => s.state);
  const dismissed = useSetupStore((s) => s.bannerDismissed);
  const reconnecting = useSetupStore((s) => s.reconnecting);
  const dismiss = useSetupStore((s) => s.dismissBanner);
  const { pathname } = useLocation();
  const isMobile = useIsMobile();

  if (!state || state.finished || dismissed || reconnecting) return null;
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
    <div
      className={`fixed z-40 flex items-start gap-2 rounded-lg border border-border-subtle bg-surface-raised shadow-xl p-3
        ${isMobile ? BANNER_PHONE_CLASS : BANNER_DESKTOP_CLASS}`}
      // The top inset is already in the `top` offset; a fixed box still has
      // to pay the side insets itself (the shell's padding never reaches it).
      style={isMobile ? { paddingLeft: 'env(safe-area-inset-left)', paddingRight: 'env(safe-area-inset-right)' } : undefined}
    >
      <Link
        to={to}
        role="status"
        className="flex items-start gap-2 flex-1 min-w-0 text-sm text-text-muted hover:text-text"
      >
        <AlertTriangle size={14} className="text-hue-amber shrink-0 mt-0.5" />
        <span>
          {text} · <span className="text-text underline underline-offset-2">{action}</span>
        </span>
      </Link>
      <IconButton label="Dismiss" size="xs" onClick={dismiss}>
        <X size={14} />
      </IconButton>
    </div>
  );
}
