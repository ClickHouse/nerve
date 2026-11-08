import { create } from 'zustand';
import type { NotificationLevel } from '../api/settings';
import { workerSupported } from '../utils/webPush';

/**
 * What this browser can do about system alerts: a permission of the
 * Notification API, or `insecure` (a plain-HTTP address, where browsers
 * refuse alerts) or `unsupported` (no Notification API).
 */
export type AlertSupport = 'insecure' | 'unsupported' | NotificationPermission;

/** The choices of this browser. They are not on the server. */
interface AlertChoices {
  enabled: boolean;
  level: NotificationLevel;
  /** The person answered the offer on a question toast. */
  offerDone: boolean;
}

export interface BrowserAlertState extends AlertChoices {
  support: AlertSupport;
  /** Asks the browser for permission when it did not ask yet, then turns alerts on. */
  turnOn: () => Promise<void>;
  turnOff: () => void;
  setLevel: (level: NotificationLevel) => void;
  closeOffer: () => void;
  /** Reads the permission again: the person can change it in the browser. */
  refresh: () => void;
}

export const BROWSER_ALERTS_KEY = 'nerve-browser-alerts';

export function alertSupport(): AlertSupport {
  if (!window.isSecureContext) return 'insecure';
  const Alert = window.Notification;
  if (typeof Alert !== 'function') return 'unsupported';
  if (Alert.permission !== 'granted' && !workerSupported() && !constructs(Alert)) return 'unsupported';
  return Alert.permission;
}

/**
 * Some phone browsers have the Notification API but show alerts only through
 * a service worker, so the constructor throws. The panel's worker shows the
 * alerts where the browser runs it, so only a browser without one needs this
 * check. Without permission the constructor shows nothing, so the check is
 * safe then.
 */
function constructs(Alert: typeof Notification): boolean {
  try {
    new Alert('');
    return true;
  } catch {
    return false;
  }
}

function readChoices(): AlertChoices {
  let saved: Partial<Record<keyof AlertChoices, unknown>> = {};
  try {
    saved = JSON.parse(localStorage.getItem(BROWSER_ALERTS_KEY) ?? '{}') ?? {};
  } catch { /* a damaged value reads as the defaults */ }
  return {
    enabled: saved.enabled === true,
    level: saved.level === 'all' ? 'all' : 'important',
    offerDone: saved.offerDone === true,
  };
}

/** Alerts are on and the browser lets the panel show them. */
export function alertsOn(state: Pick<BrowserAlertState, 'enabled' | 'support'>): boolean {
  return state.enabled && state.support === 'granted';
}

/**
 * Browser alerts, kept per browser in localStorage. Every panel tab of the
 * browser reads the same value, and a change in one tab reaches the others
 * through the `storage` event. The permission is per browser too, so a tab
 * reads it again with the change: another tab can have asked for it.
 */
export const useBrowserAlertStore = create<BrowserAlertState>((set, get) => {
  const save = (patch: Partial<AlertChoices>) => {
    const { enabled, level, offerDone } = { ...get(), ...patch };
    localStorage.setItem(BROWSER_ALERTS_KEY, JSON.stringify({ enabled, level, offerDone }));
    set(patch);
  };
  window.addEventListener('storage', (e) => {
    if (e.key === BROWSER_ALERTS_KEY) set({ ...readChoices(), support: alertSupport() });
  });

  return {
    ...readChoices(),
    support: alertSupport(),
    turnOn: async () => {
      let support = alertSupport();
      if (support === 'default') support = await window.Notification.requestPermission();
      set({ support });
      save({ enabled: support === 'granted', offerDone: true });
    },
    turnOff: () => save({ enabled: false }),
    setLevel: (level) => save({ level }),
    closeOffer: () => save({ offerDone: true }),
    refresh: () => set({ support: alertSupport() }),
  };
});
