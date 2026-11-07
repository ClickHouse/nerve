import { useId } from 'react';
import type { PushDevice } from '../../api/push';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { thisDevice, usePushStore } from '../../stores/pushStore';
import { deviceLabel, needsHomeScreen, pushSupported } from '../../utils/webPush';
import { formatRelativeTime } from '../Cron/utils';
import { IconButton, Switch } from '../ui';
import { Trash2 } from '../ui/icons';

/**
 * The Web Push switch of the "This browser" row. Push uses the level of the
 * row ("What to send"). The row loads the push state (`usePushStore.load`).
 */
export function PushSwitch() {
  const switchId = useId();
  const push = usePushStore();
  const alerts = useBrowserAlertStore();
  const on = thisDevice(push) !== undefined;
  // The row says why alerts cannot show (no HTTPS, blocked); push needs the same.
  const blocked = alerts.support === 'insecure' || alerts.support === 'denied';
  const toggle = (next: boolean) => void (next
    ? push.turnOn(alerts.level).then(() => alerts.refresh())
    : push.turnOff());

  if (!pushSupported()) {
    return needsHomeScreen()
      ? <p className="text-xs text-text-dim">On iPhone and iPad, add the panel to the Home Screen to get notifications.</p>
      : null;
  }
  return (
    <>
      {!blocked && (
        <div className="flex items-center justify-between gap-3">
          <p id={switchId} className="min-w-0 text-xs font-medium text-text-muted">Get notifications when no tab is open</p>
          <Switch
            checked={on}
            disabled={push.busy}
            aria-busy={push.busy || undefined}
            aria-labelledby={switchId}
            onChange={toggle}
          />
        </div>
      )}
      {push.error && <p role="alert" className="text-xs text-error">{push.error}</p>}
    </>
  );
}

/** The other browsers of this person that get push, each with a remove button. */
export function OtherDevices() {
  const headingId = useId();
  const push = usePushStore();
  const current = thisDevice(push);
  const devices = push.devices.filter((device) => device !== current);
  if (devices.length === 0) return null;
  return (
    <div className="flex flex-col gap-1.5">
      <span id={headingId} className="text-xs font-medium text-text-muted">Your other devices</span>
      <ul aria-labelledby={headingId} className="divide-y divide-border rounded-md border border-border">
        {devices.map((device) => {
          const label = deviceLabel(device.user_agent);
          return (
            <li key={device.id} className="flex items-center justify-between gap-3 py-2 pl-3 pr-1.5">
              <div className="min-w-0">
                <p className="truncate text-xs text-text">{label}</p>
                <p className="text-2xs text-text-faint">{deviceDetail(device)}</p>
              </div>
              <IconButton label={`Remove ${label}`} disabled={push.busy} onClick={() => void push.remove(device.id)}>
                <Trash2 size={14} />
              </IconButton>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function deviceDetail(device: PushDevice): string {
  const added = new Date(device.created_at).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  const last = device.last_success_at
    ? `Last notification ${formatRelativeTime(device.last_success_at)}`
    : 'No notification yet';
  return `Added ${added} · ${last}`;
}
