import { useEffect, useId } from 'react';
import type { NotificationLevel } from '../../api/settings';
import { alertsOn, useBrowserAlertStore, type AlertSupport } from '../../stores/browserAlertStore';
import { thisDevice, usePushStore } from '../../stores/pushStore';
import { useSettingsStore } from '../../stores/settingsStore';
import { Badge, Switch, type BadgeTone } from '../ui';
import { Globe } from '../ui/icons';
import { OtherDevices, PushSwitch } from './PushSettings';
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
    help: 'This browser cannot show alerts.',
  },
};

/**
 * The "This browser" row of the Notifications section: system alerts while
 * a panel tab is open but not in front, push notifications when no tab is
 * open (`PushSwitch`), what both include, and the other browsers of
 * this person that get push (`OtherDevices`). The alert choices are kept
 * in this browser (`useBrowserAlertStore`); push keeps the level with the
 * browser's subscription. The row writes none of the section's fields.
 */
export function ThisBrowserRow() {
  const labelId = useId();
  const alerts = useBrowserAlertStore();
  const quietHours = useSettingsStore((s) => Boolean(
    s.notifications?.fields.quiet_start.value && s.notifications.fields.quiet_end.value,
  ));
  useEffect(() => {
    useBrowserAlertStore.getState().refresh();
    void usePushStore.getState().load();
  }, []);

  const pushOn = usePushStore((s) => thisDevice(s) !== undefined);
  const pushBusy = usePushStore((s) => s.busy);
  const unavailable = UNAVAILABLE[alerts.support];
  const on = alertsOn(alerts);
  // The choice changes after push took it, so a failed save keeps the level that push uses.
  const setLevel = async (level: NotificationLevel) => {
    if (await usePushStore.getState().setLevel(level)) alerts.setLevel(level);
  };
  return (
    <SettingRow
      icon={<Globe size={16} />}
      title="This browser"
      description={<span id={labelId}>Alerts while a tab is open</span>}
      control={unavailable ? <Badge tone={unavailable.tone}>{unavailable.badge}</Badge> : (
        <Switch
          checked={on}
          aria-labelledby={labelId}
          onChange={(next) => void (next ? alerts.turnOn() : alerts.turnOff())}
        />
      )}
    >
      {unavailable && <p className="text-xs text-text-dim">{unavailable.help}</p>}
      <PushSwitch />
      {(on || pushOn) && (
        <LevelChoice value={alerts.level} disabled={false} busy={pushBusy} onChange={(level) => void setLevel(level)} />
      )}
      {(on || pushOn) && quietHours && (
        <p className="text-xs text-text-dim">During quiet hours, alerts make no sound unless urgent.</p>
      )}
      <p className="text-2xs text-text-faint">Applies to this browser only</p>
      <OtherDevices />
    </SettingRow>
  );
}
