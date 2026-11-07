import { vi } from 'vitest';
import { useBrowserAlertStore } from '../stores/browserAlertStore';

/** A browser with the Notification API at `permission`; `answer` is what the permission prompt gives. */
export function stubBrowser({ secure = true, permission = 'default', answer = 'granted' }: {
  secure?: boolean;
  permission?: NotificationPermission | null;
  answer?: NotificationPermission;
} = {}) {
  vi.stubGlobal('isSecureContext', secure);
  const requestPermission = vi.fn(async () => answer);
  vi.stubGlobal('Notification', permission === null ? undefined : Object.assign(function Notification() {}, {
    permission, requestPermission,
  }));
  useBrowserAlertStore.getState().refresh();
  return requestPermission;
}
