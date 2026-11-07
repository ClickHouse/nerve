import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { alertsOn, useBrowserAlertStore } from '../../stores/browserAlertStore';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { Button } from '../ui';
import { BellRing } from '../ui/icons';

export const ALERT_SETTINGS_PATH = '/settings/notifications';

/**
 * The offer on the first toast that waits for an answer: "Turn on" asks the
 * browser for permission, "Not now" closes the offer. Either answer closes
 * it in this browser for good. Shown only when the browser can show alerts
 * and the person did not choose yet.
 */
export function BrowserAlertOffer({ notif }: { notif: Notification }) {
  const first = useNotificationStore((s) => s.toastQueue.find((n) => n.type !== 'notify')?.id === notif.id);
  const alerts = useBrowserAlertStore();
  const [answered, setAnswered] = useState(false);

  if (answered) {
    if (!alertsOn(alerts) && alerts.support !== 'denied') return null;
    return (
      <p role="status" className="mt-2 border-t border-border-subtle pt-2 text-xs text-text-muted">
        {alertsOn(alerts) ? (
          <>
            Alerts are on. Change them in{' '}
            <Link to={ALERT_SETTINGS_PATH} className="text-link hover:text-link-hover hover:underline">Settings</Link>.
          </>
        ) : 'The browser blocked alerts. You can allow them in the site settings of the browser.'}
      </p>
    );
  }
  const canAsk = alerts.support === 'default' || alerts.support === 'granted';
  if (!first || alerts.offerDone || alerts.enabled || !canAsk) return null;

  return (
    <div className="mt-2 flex items-center gap-2 border-t border-border-subtle pt-2 text-xs">
      <BellRing size={12} className="shrink-0 text-text-muted" aria-hidden="true" />
      <span className="min-w-0 flex-1 text-text-muted">Alert me when this tab is in the background?</span>
      <Button
        variant="link"
        size="xs"
        className="font-medium"
        onClick={async () => {
          await alerts.turnOn();
          setAnswered(true);
        }}
      >
        Turn on
      </Button>
      <span className="text-text-faint" aria-hidden="true">·</span>
      <Button variant="ghost" size="xs" className="px-0 py-0" onClick={alerts.closeOffer}>Not now</Button>
    </div>
  );
}

/**
 * The Notifications page's way to the alert settings, with their state in
 * this browser. Not on a phone, where the page header has no room for it.
 */
export function BrowserAlertsLink() {
  const on = useBrowserAlertStore(alertsOn);
  const navigate = useNavigate();
  const mobile = useIsMobile();
  if (mobile) return null;
  return (
    <Button
      variant="pill"
      className="rounded-lg whitespace-nowrap"
      title="Browser alerts, in Settings"
      onClick={() => navigate(ALERT_SETTINGS_PATH)}
    >
      <BellRing size={13} aria-hidden="true" />
      {on ? 'Alerts on' : 'Alerts off'}
    </Button>
  );
}
