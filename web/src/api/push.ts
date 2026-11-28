/**
 * Types of the Web Push API.
 *
 * In local mode Nerve keeps the browsers (`/api/push/subscriptions`, see
 * `docs/api.md`). In hosted mode the platform delivers Web Push, and the
 * gateway keeps the browsers (`/_nerve/push/subscriptions`). The functions
 * below map the gateway's answers to the shapes of the local API.
 */
import { gatewayRequest } from './hosted';
import type { NotificationLevel } from './settings';

/** A browser of this person that gets push notifications. */
export interface PushDevice {
  /** A number in local mode, a string in hosted mode. */
  id: number | string;
  /** The push endpoint of the browser. Local mode only. */
  endpoint?: string;
  /** SHA-256 of the endpoint, base64url without padding. Hosted mode only. */
  endpoint_digest?: string;
  level: NotificationLevel;
  user_agent: string;
  created_at: string;
  /** Absent in hosted mode: the gateway does not report it. */
  last_success_at?: string | null;
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

/** The path of the gateway's push routes, below `/_nerve/`. */
const GATEWAY_PUSH_PATH = 'push/subscriptions';

/** A browser as the gateway returns it. It has no endpoint and no key. */
interface GatewaySubscription {
  id: string;
  endpointDigest: string;
  level: NotificationLevel;
  userAgent: string;
  createdAt: string;
}

interface GatewaySubscriptions {
  publicKey: string;
  subscriptions: GatewaySubscription[];
}

function fromGateway(subscription: GatewaySubscription): PushDevice {
  return {
    id: subscription.id,
    endpoint_digest: subscription.endpointDigest,
    level: subscription.level,
    user_agent: subscription.userAgent,
    created_at: subscription.createdAt,
  };
}

/** The gateway's public key and the browsers of this person. */
export async function gatewayPushDevices(): Promise<PushDevices> {
  const body = await (await gatewayRequest(GATEWAY_PUSH_PATH)).json() as GatewaySubscriptions;
  return { public_key: body.publicKey, subscriptions: body.subscriptions.map(fromGateway) };
}

/** Turn push on for this browser at the gateway, or change its keys or level. */
export async function gatewaySavePushDevice(body: PushSubscriptionBody): Promise<PushDevice> {
  const res = await gatewayRequest(GATEWAY_PUSH_PATH, { method: 'POST', body: JSON.stringify(body) });
  return fromGateway(await res.json() as GatewaySubscription);
}

/** Turn push off at the gateway for one browser of this person. */
export async function gatewayDeletePushDevice(id: number | string): Promise<void> {
  await gatewayRequest(`${GATEWAY_PUSH_PATH}/${encodeURIComponent(String(id))}`, { method: 'DELETE' });
}
