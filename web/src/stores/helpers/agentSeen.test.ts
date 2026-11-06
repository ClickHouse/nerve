// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

// Node 25+ shadows jsdom's Storage with an inert global (see chatStore.test.ts).
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
  if (target) Object.defineProperty(target, 'localStorage', { value: storage, configurable: true, writable: true });
}

const { clearAllSeen, loadSeen, MAX_SEEN_IDS, persistSeen, removeSeen } = await import('./agentSeen');
const { selectViewerStorage } = await import('./viewerStorage');

beforeEach(() => data.clear());
afterEach(() => selectViewerStorage(null));

describe('agentSeen', () => {
  it('round-trips one session\'s set under nerve_agents_seen:<sid>', () => {
    persistSeen('s-1', ['ag_a', 'ag_b']);

    expect(data.get('nerve_agents_seen:s-1')).toBe('["ag_a","ag_b"]');
    expect(loadSeen('s-1')).toEqual({ ag_a: true, ag_b: true });
    expect(loadSeen('s-2')).toEqual({});
  });

  it('keeps the newest ids and ignores junk', () => {
    persistSeen('s-1', Array.from({ length: MAX_SEEN_IDS + 3 }, (_, i) => `ag_${i}`));
    const stored = JSON.parse(data.get('nerve_agents_seen:s-1')!);
    expect(stored).toHaveLength(MAX_SEEN_IDS);
    expect(stored[0]).toBe('ag_3');

    data.set('nerve_agents_seen:s-2', '{"not": "a list"}');
    data.set('nerve_agents_seen:s-3', '["ok", 7, ""]');
    expect(loadSeen('s-2')).toEqual({});
    expect(loadSeen('s-3')).toEqual({ ok: true });
  });

  it('removes one session, or every session of the viewer', () => {
    persistSeen('s-1', ['ag_a']);
    persistSeen('s-2', ['ag_b']);
    data.set('nerve_draft_s-1', 'keep me');

    removeSeen('s-1');
    expect(loadSeen('s-1')).toEqual({});

    clearAllSeen();
    expect(loadSeen('s-2')).toEqual({});
    expect(data.get('nerve_draft_s-1')).toBe('keep me');
  });

  it('scopes the keys to the verified viewer', () => {
    selectViewerStorage('principal-a');
    persistSeen('s-1', ['ag_a']);
    expect([...data.keys()]).toEqual(['nerve_principal_principal-a:nerve_agents_seen:s-1']);

    selectViewerStorage('principal-b');
    expect(loadSeen('s-1')).toEqual({});
  });
});
