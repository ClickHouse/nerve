// Per-session "seen" agents (client-only: the server keeps no read state).
//
// Opening an agent's transcript, or "Mark all seen" in the Agents panel, moves
// a failed / interrupted / lost agent out of "Needs attention" into "Ended".
// The set is a convenience, never a blocker, so it lives in localStorage like
// the read stamps (readStorage.ts): one key per session, scoped to the viewer,
// and every access is quota- and disabled-safe.

import { viewerStorageKey } from './viewerStorage';

const PREFIX = 'nerve_agents_seen:';

/** At most this many ids are kept per session (the newest win). */
export const MAX_SEEN_IDS = 500;

const keyFor = (sessionId: string) => viewerStorageKey(`${PREFIX}${sessionId}`);

/** The persisted seen set of one session (empty when absent or unreadable). */
export function loadSeen(sessionId: string): Record<string, true> {
  const out: Record<string, true> = {};
  if (!sessionId) return out;
  try {
    const raw = localStorage.getItem(keyFor(sessionId));
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    if (Array.isArray(parsed)) {
      for (const id of parsed.slice(-MAX_SEEN_IDS)) {
        if (typeof id === 'string' && id) out[id] = true;
      }
    }
  } catch { /* unreadable or storage off: nothing seen yet */ }
  return out;
}

/**
 * Write one session's seen set. `ids` is the whole set in insertion order; only
 * the last MAX_SEEN_IDS are kept, so the oldest marks fall off first.
 */
export function persistSeen(sessionId: string, ids: string[]): void {
  if (!sessionId) return;
  try {
    if (ids.length === 0) localStorage.removeItem(keyFor(sessionId));
    else localStorage.setItem(keyFor(sessionId), JSON.stringify(ids.slice(-MAX_SEEN_IDS)));
  } catch { /* quota / disabled: the in-memory set still applies this session */ }
}

/** Drop one session's seen set (the session was deleted). */
export function removeSeen(sessionId: string): void {
  if (!sessionId) return;
  try { localStorage.removeItem(keyFor(sessionId)); } catch { /* ignore */ }
}

/** Wipe every seen set of this viewer: the shared-browser control, run on logout. */
export function clearAllSeen(): void {
  const keys: string[] = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (k && k.startsWith(viewerStorageKey(PREFIX))) keys.push(k);
    }
  } catch { /* storage unavailable */ }
  for (const k of keys) {
    try { localStorage.removeItem(k); } catch { /* ignore */ }
  }
}
