import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../../stores/authStore';
import { useNotificationStore } from '../../stores/notificationStore';
import { ws } from '../../api/websocket';
import { api } from '../../api/client';
import { Drawer, IconButton } from '../ui';
import { MoreHorizontal, LogOut, X } from '../ui/icons';
import { ThemeToggle } from './ThemeToggle';
import { NAV_ITEMS, PRIMARY_PATHS, type NavItem } from './navItems';

/**
 * Phone navigation: four primary destinations plus More, pinned to the bottom.
 *
 * Bottom rather than top because of the notification badge — it counts
 * questions the agent is *blocked on*, which is the reason to open the panel
 * at all, so it has to be visible without going looking for it. Thumb reach
 * is the bonus.
 *
 * Replaces the 56px nav rail below `md`; the two are never mounted together
 * (AppShell picks one), so the notification poll and feature-flag fetch below
 * do not run twice.
 */
export function BottomNav() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuthStore();
  const pendingCount = useNotificationStore(s => s.pendingCount);
  const loadNotifications = useNotificationStore(s => s.loadNotifications);
  const [ultracodeEnabled, setUltracodeEnabled] = useState(false);
  const [moreOpen, setMoreOpen] = useState(false);

  useEffect(() => {
    loadNotifications();
    // A 404 is expected until the dashboard backend is loaded after restart.
    api.getUltracodeDashboardStatus().then(s => setUltracodeEnabled(s.enabled)).catch(() => {});
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const available = NAV_ITEMS.filter(i => !(i.feature === 'ultracode' && !ultracodeEnabled));
  const primary = PRIMARY_PATHS
    .map(p => available.find(i => i.path === p))
    .filter((i): i is NavItem => Boolean(i));
  const overflow = available.filter(i => !PRIMARY_PATHS.includes(i.path));

  // "More" counts as selected whenever the current route is one of the
  // destinations it hides, so the bar always shows where you are.
  const inOverflow = overflow.some(i => location.pathname.startsWith(i.path));

  const go = (path: string) => {
    navigate(path);
    setMoreOpen(false);
  };

  return (
    <>
      {/* `order-last` paints the bar at the bottom while leaving it first in
          the DOM, which is where the desktop nav rail already sits — so the
          reading and tab order is the same on both layouts. */}
      <nav
        className="order-last flex shrink-0 items-stretch border-t border-border-subtle bg-surface"
        style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}
        aria-label="Main"
      >
        {primary.map(({ path, icon: Icon, label }) => {
          const active = location.pathname.startsWith(path);
          const isNotifs = path === '/notifications';
          return (
            // Native, like the nav rail's cells and for the same reason: a tab
            // bar cell is a flexed column of glyph-over-label whose geometry is
            // the component, not the button. Keeping both navs on the same
            // shape means the phone and the desktop agree on what a current
            // destination looks like. See NavRail for the ordering detail.
            <button
              key={path}
              onClick={() => go(path)}
              aria-current={active ? 'page' : undefined}
              // min-h-14 keeps every target at/above the ~44px touch minimum.
              className={`relative flex min-h-14 flex-1 cursor-pointer flex-col items-center justify-center gap-0.5 transition-colors ${
                active ? 'text-accent' : 'text-text-dim'
              }`}
            >
              <Icon size={20} />
              {/* leading-none: the 14px line-height of text-2xs would otherwise
                  push the pair off-centre in the 56px cell. */}
              <span className="text-2xs leading-none">{label}</span>
              {/* bg-error-solid, not bg-error — see the matching badge in
                  NavRail. The bare token is a feedback *foreground*, pale in
                  dark mode, giving 1.62:1 under white text. */}
              {isNotifs && pendingCount > 0 && (
                <span className="absolute right-1/2 top-1.5 flex h-4 w-4 translate-x-3 items-center justify-center rounded-full bg-error-solid text-2xs font-medium leading-none text-white">
                  {pendingCount > 9 ? '9+' : pendingCount}
                </span>
              )}
            </button>
          );
        })}

        <button
          onClick={() => setMoreOpen(true)}
          aria-expanded={moreOpen}
          aria-haspopup="dialog"
          // While an overflow destination is active, the real current item is
          // inside a closed, inert drawer — so without this the main navigation
          // exposes no current item at all and the state is colour-only.
          aria-current={inOverflow ? true : undefined}
          className={`flex min-h-14 flex-1 cursor-pointer flex-col items-center justify-center gap-0.5 transition-colors ${
            inOverflow ? 'text-accent' : 'text-text-dim'
          }`}
        >
          <MoreHorizontal size={20} />
          <span className="text-2xs leading-none">More</span>
        </button>
      </nav>

      {/* Right-anchored so it reads as "which section of the app", distinct
          from the left drawer's "which item within this section". */}
      <Drawer open={moreOpen} onClose={() => setMoreOpen(false)} side="right" label="More destinations">
        <div className="flex items-center justify-between border-b border-border-subtle px-4 py-3">
          <span className="text-sm font-medium">More</span>
          <div className="flex items-center gap-3">
            <span
              className={`h-2 w-2 rounded-full ${ws.connected ? 'bg-success' : 'bg-error'}`}
              title={ws.connected ? 'Connected' : 'Disconnected'}
            />
            <ThemeToggle />
            {/* Escape closes it too, but only this is discoverable. */}
            <IconButton label="Close menu" size="xs" onClick={() => setMoreOpen(false)}>
              <X size={16} />
            </IconButton>
          </div>
        </div>

        {/* Native, for a different reason from the tab cells above: `Button`'s
            largest size is `px-3 py-2`, which on a phone drops these rows from
            a 44px touch target to ~36px. Restoring `px-4 py-3` from the call
            site does work today — but only because Tailwind happens to emit
            the larger value of a utility later, which is not a rule to build a
            touch target on. */}
        <div className="flex-1 overflow-y-auto py-1">
          {overflow.map(({ path, icon: Icon, label }) => {
            const active = location.pathname.startsWith(path);
            return (
              <button
                key={path}
                onClick={() => go(path)}
                aria-current={active ? 'page' : undefined}
                className={`flex w-full cursor-pointer items-center gap-3 px-4 py-3 text-left text-sm transition-colors ${
                  active ? 'bg-accent/10 text-accent' : 'text-text-muted hover:bg-surface-hover'
                }`}
              >
                <Icon size={18} />
                {label}
              </button>
            );
          })}
        </div>

        <button
          onClick={logout}
          className="flex w-full cursor-pointer items-center gap-3 border-t border-border-subtle px-4 py-3 text-left text-sm text-text-faint hover:bg-surface-hover"
        >
          <LogOut size={18} />
          Log out
        </button>
      </Drawer>
    </>
  );
}
