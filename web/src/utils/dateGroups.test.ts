// @vitest-environment jsdom
import { describe, it, expect, beforeEach } from 'vitest';
import {
  getDateGroup, groupByDate, DATE_GROUPS, DEFAULT_COLLAPSED_GROUPS,
  loadCollapsedGroups, saveCollapsedGroups,
} from './dateGroups';

const KEY = 'nerve_sidebar_collapsed_groups';

// Node 25 injects its own inert `localStorage` global (it warns
// "--localstorage-file was provided without a valid path"), and it shadows
// jsdom's Storage — the ambient object has no getItem/setItem at all. Install a
// real in-memory Storage so these assertions test our logic, not the host's.
function installStorage(): void {
  const data = new Map<string, string>();
  const storage = {
    getItem: (k: string) => (data.has(k) ? data.get(k)! : null),
    setItem: (k: string, v: string) => void data.set(k, String(v)),
    removeItem: (k: string) => void data.delete(k),
    clear: () => data.clear(),
    key: (i: number) => [...data.keys()][i] ?? null,
    get length() { return data.size; },
  };
  for (const target of [globalThis, globalThis.window]) {
    if (target) Object.defineProperty(target, 'localStorage', {
      value: storage, configurable: true, writable: true,
    });
  }
}

/** ISO string N hours before now. */
const hoursAgo = (h: number) => new Date(Date.now() - h * 3600000).toISOString();

/** ISO string at local noon N calendar days back (stable inside a day). */
function daysBackAtNoon(days: number): string {
  const d = new Date();
  d.setHours(12, 0, 0, 0);
  d.setDate(d.getDate() - days);
  return d.toISOString();
}

describe('getDateGroup', () => {
  it('buckets the last hour by elapsed time, not by calendar day', () => {
    expect(getDateGroup(hoursAgo(0.1))).toBe('Last hour');
    expect(getDateGroup(hoursAgo(0.9))).toBe('Last hour');
  });

  it('buckets earlier-today separately from yesterday', () => {
    const now = new Date();
    // Only meaningful once the local day is old enough to hold a 2h-old stamp.
    if (now.getHours() >= 3) expect(getDateGroup(hoursAgo(2))).toBe('Today');
    expect(getDateGroup(daysBackAtNoon(1))).toBe('Yesterday');
  });

  it('buckets the rest of the week, then Older', () => {
    expect(getDateGroup(daysBackAtNoon(3))).toBe('This week');
    expect(getDateGroup(daysBackAtNoon(6))).toBe('This week');
    expect(getDateGroup(daysBackAtNoon(30))).toBe('Older');
  });

  it('falls back to Older for a missing timestamp', () => {
    expect(getDateGroup('')).toBe('Older');
  });

  it('only emits labels from the declared taxonomy', () => {
    const stamps = [hoursAgo(0.5), hoursAgo(2), daysBackAtNoon(1), daysBackAtNoon(4), daysBackAtNoon(90)];
    for (const s of stamps) expect(DATE_GROUPS).toContain(getDateGroup(s) as never);
  });

  it('collapses This week and Older by default', () => {
    expect(DEFAULT_COLLAPSED_GROUPS).toEqual(['This week', 'Older']);
  });
});

describe('collapsed-group persistence', () => {
  beforeEach(() => installStorage());

  it('starts with the default buckets collapsed', () => {
    expect([...loadCollapsedGroups()].sort()).toEqual([...DEFAULT_COLLAPSED_GROUPS].sort());
  });

  it('folds the defaults into a legacy (pre-version) payload, once', () => {
    localStorage.setItem(KEY, JSON.stringify(['Starred']));   // old bare-array format
    expect([...loadCollapsedGroups()].sort()).toEqual(['Older', 'Starred', 'This week']);
    // Migration is written back, so the next read is a plain versioned load.
    expect(JSON.parse(localStorage.getItem(KEY)!).v).toBe(2);
  });

  it('respects an expanded default once the user has toggled it', () => {
    saveCollapsedGroups(new Set(['Older']));                  // user expanded "This week"
    expect([...loadCollapsedGroups()]).toEqual(['Older']);
  });

  it('falls back to the defaults on unreadable storage', () => {
    localStorage.setItem(KEY, '{not json');
    expect([...loadCollapsedGroups()].sort()).toEqual([...DEFAULT_COLLAPSED_GROUPS].sort());
  });
});

describe('groupByDate', () => {
  it('never creates an empty group and keeps input order', () => {
    const items = [
      { id: 'a', updated_at: hoursAgo(0.2) },
      { id: 'b', updated_at: daysBackAtNoon(1) },
      { id: 'c', updated_at: daysBackAtNoon(1) },
      { id: 'd', updated_at: daysBackAtNoon(40) },
    ];
    const groups = groupByDate(items);
    expect(groups.every(g => g.items.length > 0)).toBe(true);
    expect(groups.map(g => g.group)).toEqual(['Last hour', 'Yesterday', 'Older']);
    expect(groups[1].items.map(i => i.id)).toEqual(['b', 'c']);
  });
});
