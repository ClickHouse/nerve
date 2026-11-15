import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { NotificationSettings } from '../api/settings';

vi.mock('../api/client', () => ({
  api: {
    settingsNotifications: vi.fn(),
    saveSettingsNotifications: vi.fn(),
    testSettingsNotifications: vi.fn(),
  },
  getToken: vi.fn(() => 'session-token'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const client = await import('../api/client');
const { useSettingsStore } = await import('./settingsStore');
const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

const BODY = { read_only_reason: null, fields: {}, slack: {}, telegram: {} } as unknown as NotificationSettings;
const store = () => useSettingsStore.getState();

beforeEach(() => {
  vi.clearAllMocks();
  useSettingsStore.setState(useSettingsStore.getInitialState(), true);
});

describe('settingsStore', () => {
  it('keeps the answer of a save and the parts that did not reload', async () => {
    api.saveSettingsNotifications.mockResolvedValue({ ...BODY, reload_errors: { mcp: 'boom' } });
    expect(await store().saveNotifications({ 'notifications.levels.slack': 'important' })).toBe(true);
    expect(store().notifications).toEqual(BODY);
    expect(store().reloadErrors).toEqual({ mcp: 'boom' });
    expect(store().saved).toBe(true);
    expect(store().saving).toEqual([]);
  });

  it('keeps a refusal with the field it names, or for the whole body', async () => {
    api.saveSettingsNotifications.mockRejectedValueOnce(
      new Error('409: {"detail": "Set from the environment.", "field": "notifications.levels.slack"}'),
    );
    expect(await store().saveNotifications({ 'notifications.levels.slack': 'all' })).toBe(false);
    expect(store().saveError).toEqual({ field: 'notifications.levels.slack', detail: 'Set from the environment.' });

    api.saveSettingsNotifications.mockRejectedValueOnce(new Error('409: {"detail": "Locked."}'));
    await store().saveNotifications({ 'notifications.levels.slack': 'all' });
    expect(store().saveError).toEqual({ field: null, detail: 'Locked.' });
  });

  it('runs saves one after another, in order', async () => {
    let answer: (body: NotificationSettings) => void = () => {};
    api.saveSettingsNotifications.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    api.saveSettingsNotifications.mockResolvedValueOnce(BODY);
    const first = store().saveNotifications({ 'notifications.channels': ['web'] });
    const second = store().saveNotifications({ 'notifications.max_redeliveries': 2 });
    await Promise.resolve();
    expect(store().saving).toEqual(['notifications.channels']);
    expect(api.saveSettingsNotifications).toHaveBeenCalledTimes(1);

    answer(BODY);
    expect(await first).toBe(true);
    expect(await second).toBe(true);
    expect(api.saveSettingsNotifications).toHaveBeenNthCalledWith(2, { 'notifications.max_redeliveries': 2 });
    expect(store().saving).toEqual([]);
  });

  it('starts a visit without the notes of the last one', async () => {
    useSettingsStore.setState({ saved: true, saveError: { field: null, detail: 'Locked.' }, reloadErrors: { mcp: 'x' } });
    api.settingsNotifications.mockResolvedValue(BODY);
    await store().loadNotifications();
    expect(store()).toMatchObject({ saved: false, saveError: null, reloadErrors: {}, notifications: BODY });
  });

  it('sends a test after the save in flight, and a change clears the last result', async () => {
    let answer: (body: NotificationSettings) => void = () => {};
    api.saveSettingsNotifications.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = { notification_id: 'notif-1', silenced: false, results: [{ channel: 'web', delivered: true }] };
    api.testSettingsNotifications.mockResolvedValue(result);
    useSettingsStore.setState({ testResult: result });

    const saved = store().saveNotifications({ 'notifications.slack_channel_id': 'C0456DEFG' });
    expect(store().testResult).toBeNull();
    const tested = store().sendTest();
    await Promise.resolve();
    expect(api.testSettingsNotifications).not.toHaveBeenCalled();

    answer(BODY);
    await saved;
    await tested;
    expect(api.testSettingsNotifications).toHaveBeenCalledTimes(1);
    expect(api.saveSettingsNotifications.mock.invocationCallOrder[0])
      .toBeLessThan(api.testSettingsNotifications.mock.invocationCallOrder[0]);
    expect(store().testResult).toEqual(result);
  });

  it('keeps the error of a test that was not sent', async () => {
    api.testSettingsNotifications.mockRejectedValue(new Error('503: {"detail": "Notifications are not running."}'));
    await store().sendTest();
    expect(store().testError).toBe('Notifications are not running.');
    expect(store().testing).toBe(false);
  });
});
