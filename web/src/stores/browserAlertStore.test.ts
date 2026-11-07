import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { stubBrowser } from '../test/browserAlertFixtures';
import { BROWSER_ALERTS_KEY, alertSupport, alertsOn, useBrowserAlertStore } from './browserAlertStore';

const saved = () => JSON.parse(localStorage.getItem(BROWSER_ALERTS_KEY) ?? 'null');

beforeEach(() => {
  localStorage.clear();
  useBrowserAlertStore.setState({ enabled: false, level: 'important', offerDone: false });
});

afterEach(() => vi.unstubAllGlobals());

describe('alertSupport', () => {
  it('names why the browser cannot show alerts', () => {
    stubBrowser({ secure: false });
    expect(alertSupport()).toBe('insecure');
    stubBrowser({ permission: null });
    expect(alertSupport()).toBe('unsupported');
    stubBrowser({ permission: 'denied' });
    expect(alertSupport()).toBe('denied');
  });

  it('calls a browser without the Notification constructor not supported', () => {
    stubBrowser();
    vi.stubGlobal('Notification', Object.assign(function Notification() { throw new TypeError('Illegal constructor'); }, {
      permission: 'default',
    }));
    expect(alertSupport()).toBe('unsupported');
  });
});

describe('useBrowserAlertStore', () => {
  it('asks the browser once, then keeps alerts on in this browser', async () => {
    const request = stubBrowser();

    await useBrowserAlertStore.getState().turnOn();

    expect(request).toHaveBeenCalledTimes(1);
    expect(alertsOn(useBrowserAlertStore.getState())).toBe(true);
    expect(saved()).toEqual({ enabled: true, level: 'important', offerDone: true });
  });

  it('stays off when the browser refuses', async () => {
    stubBrowser({ answer: 'denied' });

    await useBrowserAlertStore.getState().turnOn();

    expect(useBrowserAlertStore.getState()).toMatchObject({ enabled: false, support: 'denied', offerDone: true });
  });

  it('does not ask again when the browser already allows alerts', async () => {
    const request = stubBrowser({ permission: 'granted' });
    await useBrowserAlertStore.getState().turnOn();
    expect(request).not.toHaveBeenCalled();
    expect(useBrowserAlertStore.getState().enabled).toBe(true);
  });

  it('saves the level and the answer to the offer', () => {
    stubBrowser();
    useBrowserAlertStore.getState().setLevel('all');
    useBrowserAlertStore.getState().closeOffer();
    expect(saved()).toEqual({ enabled: false, level: 'all', offerDone: true });
  });

  it('takes a change from another tab', () => {
    localStorage.setItem(BROWSER_ALERTS_KEY, JSON.stringify({ enabled: true, level: 'all', offerDone: true }));
    window.dispatchEvent(new StorageEvent('storage', { key: BROWSER_ALERTS_KEY }));
    expect(useBrowserAlertStore.getState()).toMatchObject({ enabled: true, level: 'all', offerDone: true });
  });
});
