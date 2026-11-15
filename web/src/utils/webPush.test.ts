import { describe, expect, it } from 'vitest';
import { ENDPOINT, OTHER_KEY, PUBLIC_KEY, fakeSubscription } from '../test/pushFixtures';
import { base64UrlBytes, deviceLabel, madeWithKey } from './webPush';

describe('deviceLabel', () => {
  it.each([
    ['Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Mobile Safari/537.36', 'Chrome on Android'],
    ['Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1', 'Safari on iPhone'],
    ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:140.0) Gecko/20100101 Firefox/140.0', 'Firefox on macOS'],
    ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36 Edg/141.0', 'Edge on Windows'],
    ['Mozilla/5.0 (X11; CrOS x86_64 16002.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36', 'Chrome on ChromeOS'],
    ['Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0', 'Firefox on Linux'],
    ['', 'Browser'],
  ])('names %s', (userAgent, label) => {
    expect(deviceLabel(userAgent)).toBe(label);
  });
});

describe('VAPID keys', () => {
  it('decodes base64url', () => {
    expect([...base64UrlBytes('_-8')]).toEqual([0xff, 0xef]);
    expect(base64UrlBytes(PUBLIC_KEY)).toHaveLength(65);
  });

  it('tells whether a subscription uses a key', () => {
    const subscription = fakeSubscription(ENDPOINT, PUBLIC_KEY) as unknown as PushSubscription;
    expect(madeWithKey(subscription, PUBLIC_KEY)).toBe(true);
    expect(madeWithKey(subscription, OTHER_KEY)).toBe(false);
  });
});
