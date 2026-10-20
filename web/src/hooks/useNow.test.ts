import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { nowTickerRunning, useNow } from './useNow';

describe('useNow', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-27T02:10:00Z'));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns the current time on the first render', () => {
    const { result } = renderHook(() => useNow(1000));
    expect(result.current).toBe(Date.parse('2026-02-27T02:10:00Z'));
  });

  it('ticks while mounted and stops the shared ticker when the last caller leaves', () => {
    const a = renderHook(() => useNow(1000));
    const b = renderHook(() => useNow(1000));
    expect(nowTickerRunning()).toBe(true);
    const first = a.result.current;

    act(() => { vi.advanceTimersByTime(3000); });
    expect(a.result.current).toBe(first + 3000);
    expect(b.result.current).toBe(a.result.current);

    a.unmount();
    expect(nowTickerRunning()).toBe(true);
    b.unmount();
    expect(nowTickerRunning()).toBe(false);
  });

  it('does not tick when disabled', () => {
    const { result } = renderHook(() => useNow(1000, false));
    expect(nowTickerRunning()).toBe(false);
    const first = result.current;
    act(() => { vi.advanceTimersByTime(5000); });
    expect(result.current).toBe(first);
  });

  it('rounds to each caller\'s own interval', () => {
    const fast = renderHook(() => useNow(1000));
    const slow = renderHook(() => useNow(5000));
    act(() => { vi.advanceTimersByTime(2000); });
    expect(fast.result.current % 1000).toBe(0);
    expect(slow.result.current % 5000).toBe(0);
    expect(slow.result.current).toBeLessThanOrEqual(fast.result.current);
    fast.unmount();
    slow.unmount();
  });
});
