import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { PARK_MS, claimHost, endParkedHosts, hostKey, releaseHost } from './registry';
import type { McpAppHostHandle } from './types';

/** A view, as far as the registry uses one. */
function view(parks = true) {
  return {
    subscribe: () => () => {},
    getSnapshot: () => { throw new Error('not read here'); },
    attach: vi.fn(() => true),
    park: vi.fn((lot: HTMLElement) => { void lot; return parks; }),
    setToolCall: vi.fn(),
    setTheme: vi.fn(),
    retry: vi.fn(),
    answer: vi.fn(),
    dispose: vi.fn(async () => {}),
  } satisfies McpAppHostHandle;
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  // Whatever a test left parked ends here.
  vi.advanceTimersByTime(PARK_MS);
  vi.useRealTimers();
});

describe('hostKey', () => {
  it('is one key per tool call of a session', () => {
    expect(hostKey('s1', 'tu-1')).toBe('s1:tu-1');
    expect(hostKey('s1', 'tu-1')).not.toBe(hostKey('s2', 'tu-1'));
    expect(hostKey('s1', 'tu-1')).not.toBe(hostKey('s1', 'tu-2'));
  });
});

describe('a view whose block went away', () => {
  it('is parked in the page, out of sight and out of reach', () => {
    const host = view();

    releaseHost('s1:tu-1', host);

    expect(host.park).toHaveBeenCalledTimes(1);
    const lot = host.park.mock.calls[0][0];
    expect(lot.isConnected).toBe(true);
    expect(lot.parentElement).toBe(document.body);
    expect(lot.getAttribute('aria-hidden')).toBe('true');
    expect(lot.inert).toBe(true);
    expect(lot.style.visibility).toBe('hidden');
    expect(host.dispose).not.toHaveBeenCalled();
  });

  it('goes to the block that claims it in time, and is not ended', () => {
    const host = view();
    releaseHost('s1:tu-1', host);

    vi.advanceTimersByTime(PARK_MS - 1);
    expect(claimHost('s1:tu-1')).toBe(host);
    vi.advanceTimersByTime(PARK_MS * 4);

    expect(host.dispose).not.toHaveBeenCalled();
    // Claimed once.
    expect(claimHost('s1:tu-1')).toBeNull();
  });

  it('is ended when nobody claims it', () => {
    const host = view();
    releaseHost('s1:tu-1', host);

    vi.advanceTimersByTime(PARK_MS - 1);
    expect(host.dispose).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);

    expect(host.dispose).toHaveBeenCalledTimes(1);
    expect(claimHost('s1:tu-1')).toBeNull();
  });

  it('is ended at once when it cannot be parked', () => {
    const host = view(false);

    releaseHost('s1:tu-1', host);

    expect(host.dispose).toHaveBeenCalledTimes(1);
    expect(claimHost('s1:tu-1')).toBeNull();
  });

  it('is claimed only by a block for the same call', () => {
    const host = view();
    releaseHost('s1:tu-1', host);

    expect(claimHost('s1:tu-2')).toBeNull();
    expect(claimHost('s2:tu-1')).toBeNull();
    expect(claimHost('s1:tu-1')).toBe(host);
  });

  it('can be parked again after it was claimed', () => {
    const host = view();
    releaseHost('s1:tu-1', host);
    expect(claimHost('s1:tu-1')).toBe(host);

    releaseHost('s1:tu-1', host);
    vi.advanceTimersByTime(PARK_MS);

    expect(host.dispose).toHaveBeenCalledTimes(1);
  });

  it('ends an earlier view parked for the same call', () => {
    const first = view();
    const second = view();
    releaseHost('s1:tu-1', first);

    releaseHost('s1:tu-1', second);

    expect(first.dispose).toHaveBeenCalledTimes(1);
    expect(claimHost('s1:tu-1')).toBe(second);
    vi.advanceTimersByTime(PARK_MS * 2);
    expect(first.dispose).toHaveBeenCalledTimes(1);
    expect(second.dispose).not.toHaveBeenCalled();
  });

  it('ends every parked view at once when asked, for the tests', () => {
    const first = view();
    const second = view();
    releaseHost('s1:tu-1', first);
    releaseHost('s1:tu-2', second);

    endParkedHosts();

    expect(first.dispose).toHaveBeenCalledTimes(1);
    expect(second.dispose).toHaveBeenCalledTimes(1);
    expect(claimHost('s1:tu-1')).toBeNull();
    vi.advanceTimersByTime(PARK_MS * 2);
    expect(first.dispose).toHaveBeenCalledTimes(1);
  });

  it('uses one parking place for every view', () => {
    const first = view();
    const second = view();

    releaseHost('s1:tu-1', first);
    releaseHost('s1:tu-2', second);

    expect(first.park.mock.calls[0][0]).toBe(second.park.mock.calls[0][0]);
    expect(document.querySelectorAll('[data-mcp-app-parking]')).toHaveLength(1);
  });
});
