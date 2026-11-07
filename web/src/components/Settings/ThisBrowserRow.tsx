import { useEffect, useId } from 'react';
import { alertsOn, useBrowserAlertStore, type AlertSupport } from '../../stores/browserAlertStore';
import { useSettingsStore } from '../../stores/settingsStore';
import { Badge, Switch, type BadgeTone } from '../ui';
import { Globe } from '../ui/icons';
import { LevelChoice, SettingRow } from './SettingsRows';

/** In place of the switch, when this browser cannot show alerts for the panel. */
const UNAVAILABLE: Partial<Record<AlertSupport, { badge: string; tone: BadgeTone; help: string }>> = {
  insecure: {
    badge: 'Needs HTTPS',
    tone: 'warning',
    help: 'Browsers show alerts only for an HTTPS address. Open the panel over HTTPS to turn them on.',
  },
  denied: {
    badge: 'Blocked',
    tone: 'danger',
    help: 'The browser blocks alerts from this site. Allow notifications in the site settings of the browser.',
  },
  unsupported: {
    badge: 'Not supported',
    tone: 'neutral',
    help: 'This browser cannot show system alerts.',
  },
};

/**
 * The "This browser" row of the Notifications section: system alerts while
 * a panel tab is open but not in front, and what they include. Kept in this
 * browser (`useBrowserAlertStore`), not on the server, so the row reads and
 * writes none of the section's fields.
 */
export function ThisBrowserRow() {
  const titleId = useId();
  const alerts = useBrowserAlertStore();
  const quietHours = useSettingsStore((s) => Boolean(
    s.notifications?.fields.quiet_start.value && s.notifications.fields.quiet_end.value,
  ));
  useEffect(() => useBrowserAlertStore.getState().refresh(), []);

  const unavailable = UNAVAILABLE[alerts.support];
  const on = alertsOn(alerts);
  return (
    <SettingRow
      icon={<Globe size={16} />}
      title="This browser"
      titleId={titleId}
      description="System alerts while a panel tab is open but not in front."
      control={unavailable ? <Badge tone={unavailable.tone}>{unavailable.badge}</Badge> : (
        <Switch
          checked={on}
          aria-labelledby={titleId}
          onChange={(next) => void (next ? alerts.turnOn() : alerts.turnOff())}
        />
      )}
    >
      {unavailable && <p className="text-xs text-text-dim">{unavailable.help}</p>}
      {on && <LevelChoice value={alerts.level} disabled={false} busy={false} onChange={alerts.setLevel} />}
      {on && quietHours && (
        <p className="text-xs text-text-dim">During quiet hours, alerts make no sound unless urgent.</p>
      )}
      <p className="text-2xs text-text-faint">Applies to this browser only</p>
    </SettingRow>
  );
}
