import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../../stores/authStore';
import { useNotificationStore } from '../../stores/notificationStore';
import { ws } from '../../api/websocket';
import { api } from '../../api/client';
import { IconButton } from '../ui';
import { LogOut } from '../ui/icons';
import { ThemeToggle } from './ThemeToggle';
import { NAV_ITEMS } from './navItems';

export function NavRail() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuthStore();
  const pendingCount = useNotificationStore(s => s.pendingCount);
  const loadNotifications = useNotificationStore(s => s.loadNotifications);
  const [ultracodeEnabled, setUltracodeEnabled] = useState(false);

  // Load notification count + feature flags on mount
  useEffect(() => {
    loadNotifications();
    // A 404 is expected until the dashboard backend is loaded after restart;
    // keep the currently-running UI unchanged in that case.
    api.getUltracodeDashboardStatus().then(s => setUltracodeEnabled(s.enabled)).catch(() => {});
  }, []);

  const visibleItems = NAV_ITEMS.filter(item => {
    if (item.feature === 'ultracode' && !ultracodeEnabled) return false;
    return true;
  });

  return (
    <div className="w-14 bg-surface border-r border-border flex flex-col items-center py-3 shrink-0">
      <div className="text-accent font-bold text-xs mb-4 tracking-wider">N</div>

      <div className="flex-1 flex flex-col gap-1">
        {visibleItems.map(({ path, icon: Icon, label }) => {
          const active = location.pathname.startsWith(path);
          const isNotifs = path === '/notifications';
          return (
            // A native button, not `Button`/`IconButton`: this is a 40×40 cell
            // holding an icon *above* a label, and every `Button` size carries
            // at least `px-2`. Tailwind emits the values of one utility in
            // ascending order, so a call site's `px-0` loses to the variant's
            // `px-2` at equal specificity — the padding cannot be given back,
            // and 40px minus 16px does not fit the word "Notifs".
            //
            // The active treatment below is `Button variant="subtle" active`'s,
            // spelled out, so the two still agree on what "current" looks like.
            <button
              key={path}
              onClick={() => navigate(path)}
              className={`relative w-10 h-10 rounded-lg flex flex-col items-center justify-center gap-0.5 cursor-pointer transition-colors
                ${active
                  ? 'bg-accent/15 text-accent'
                  : 'text-text-dim hover:text-text-muted hover:bg-surface-hover'
                }`}
              title={label}
            >
              <Icon size={18} />
              {/* 10px/14px: the label has to clear the 18px glyph inside 40px. */}
              <span className="text-2xs leading-none">{label}</span>
              {/* bg-error-solid, not bg-error. The bare status token is Click
                  UI's feedback *foreground* — pale #ffbaba in dark — so white
                  on it is 1.62:1. `-solid` is a theme-independent ramp entry
                  (#c10000, 6.43:1 both ways). The plain token is still right
                  for the connection dot, which carries no text. */}
              {isNotifs && pendingCount > 0 && (
                <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-error-solid rounded-full text-2xs leading-none text-white flex items-center justify-center font-medium">
                  {pendingCount > 9 ? '9+' : pendingCount}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div className="flex flex-col items-center gap-2">
        <div className={`w-2 h-2 rounded-full ${ws.connected ? 'bg-success' : 'bg-error'}`}
             title={ws.connected ? 'Connected' : 'Disconnected'} />
        <ThemeToggle />
        <IconButton label="Logout" size="md" onClick={logout}>
          <LogOut size={16} />
        </IconButton>
      </div>
    </div>
  );
}
