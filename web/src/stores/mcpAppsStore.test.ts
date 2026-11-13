import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({ api: { getMcpAppsConfig: vi.fn() } }));

import { api } from '../api/client';
import { setAuthMode } from '../api/hosted';
import { featureEnabled, resetMcpAppsConfig, useMcpAppsStore } from './mcpAppsStore';

const getConfig = vi.mocked(api.getMcpAppsConfig);

beforeEach(() => {
  vi.clearAllMocks();
  setAuthMode('local');
  resetMcpAppsConfig();
});

describe('featureEnabled', () => {
  it('is true for a config that turns the feature on', () => {
    expect(featureEnabled({ enabled: true }, false)).toBe(true);
  });

  it.each([
    ['the feature off', { enabled: false }],
    ['enabled that is not true', { enabled: 'yes' as never }],
    ['a config without the flag', {} as never],
    ['no config', null],
    ['nothing', undefined],
  ])('is false for %s', (_, config) => {
    expect(featureEnabled(config, false)).toBe(false);
  });

  it('is false behind the sign-in gateway, whatever the config says', () => {
    expect(featureEnabled({ enabled: true }, true)).toBe(false);
  });
});

describe('the feature config', () => {
  it('is off until it is read', () => {
    expect(useMcpAppsStore.getState().enabled).toBe(false);
  });

  it('turns the feature on from the config', async () => {
    getConfig.mockResolvedValue({ enabled: true });

    await useMcpAppsStore.getState().load();

    expect(useMcpAppsStore.getState().enabled).toBe(true);
    // The read has a deadline.
    expect(getConfig.mock.calls[0][0]).toBeInstanceOf(AbortSignal);
  });

  it('reads the config once, however often it is asked for', async () => {
    getConfig.mockResolvedValue({ enabled: false });

    await Promise.all([useMcpAppsStore.getState().load(), useMcpAppsStore.getState().load()]);
    await useMcpAppsStore.getState().load();

    expect(getConfig).toHaveBeenCalledTimes(1);
    expect(useMcpAppsStore.getState().enabled).toBe(false);
  });

  it('stays off on a gateway without the feature, and reads again the next time', async () => {
    getConfig.mockRejectedValueOnce(new Error('404: {"detail":"Not Found"}'));
    await useMcpAppsStore.getState().load();
    expect(useMcpAppsStore.getState().enabled).toBe(false);

    getConfig.mockResolvedValue({ enabled: true });
    await useMcpAppsStore.getState().load();
    expect(getConfig).toHaveBeenCalledTimes(2);
    expect(useMcpAppsStore.getState().enabled).toBe(true);
  });

  it('asks nothing behind the sign-in gateway, and stays off', async () => {
    setAuthMode('external');
    getConfig.mockResolvedValue({ enabled: true });

    await useMcpAppsStore.getState().load();

    expect(getConfig).not.toHaveBeenCalled();
    expect(useMcpAppsStore.getState().enabled).toBe(false);
  });
});
