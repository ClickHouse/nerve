/** Types of the Web Push API (`/api/push/subscriptions`, see `docs/api.md`). */
import type { NotificationLevel } from './settings';

/** A browser of this person that gets push notifications. */
export interface PushDevice {
  id: number;
  endpoint: string;
  level: NotificationLevel;
  user_agent: string;
  created_at: string;
  last_success_at: string | null;
}

export interface PushDevices {
  /** The VAPID public key (base64url) that a browser subscribes with. */
  public_key: string;
  subscriptions: PushDevice[];
}

/** `PushSubscription.toJSON()` of the browser, and the level of its alerts. */
export interface PushSubscriptionBody {
  endpoint: string;
  keys: { p256dh: string; auth: string };
  level: NotificationLevel;
}
