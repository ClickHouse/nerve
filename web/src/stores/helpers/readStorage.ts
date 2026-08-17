// Per-session read/unread tracking (client-only).
//
// "Unread" is derived, not stored: a session is unread when its server
// `updated_at` is newer than the last time you opened/viewed it. We persist
// only that per-session "last seen" moment (ms since epoch) plus a one-time
// baseline — both in localStorage, no server state. Read-state is inherently
// per-viewer, and the one server datum it needs (updated_at) already ships in
// the session list payload.
//
// One key per session (`nerve_read_<id>`) mirrors draftStorage: two tabs can't
// clobber each other and per-session cleanup is a single removeItem. Every
// access is quota-/disabled-safe — an unread marker is a convenience, never a
// blocker.

const PREFIX = 'nerve_read_';
const BASELINE_KEY = 'nerve_reads_baseline';

const keyFor = (sessionId: string) => `${PREFIX}${sessionId}`;

/** Collect the keys of all persisted read stamps (safe if storage is off). */
function readKeys(): string[] {
  const keys: string[] = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (k && k.startsWith(PREFIX)) keys.push(k);
    }
  } catch { /* storage unavailable */ }
  return keys;
}

/** Hydrate every persisted "last seen" stamp into a { sessionId: ms } map. */
export function loadReads(): Record<string, number> {
  const out: Record<string, number> = {};
  for (const k of readKeys()) {
    try {
      const raw = localStorage.getItem(k);
      const ts = raw ? parseInt(raw, 10) : NaN;
      if (Number.isFinite(ts)) out[k.slice(PREFIX.length)] = ts;
    } catch { /* ignore a single unreadable key */ }
  }
  return out;
}

/** Write-through one session's "last seen" moment (ms since epoch). */
export function persistRead(sessionId: string, ts: number): void {
  if (!sessionId) return;
  try { localStorage.setItem(keyFor(sessionId), String(ts)); }
  catch { /* quota / disabled — the in-memory stamp still applies this session */ }
}

/** Drop one session's persisted stamp (session deleted). */
export function removeRead(sessionId: string): void {
  if (!sessionId) return;
  try { localStorage.removeItem(keyFor(sessionId)); } catch { /* ignore */ }
}

/**
 * The "everything at or before this is already read" cutoff, set once on the
 * first run so a fresh browser doesn't light up every pre-existing session as
 * unread. Persisted so it survives reloads; re-created if storage was cleared.
 */
export function loadBaseline(): number {
  try {
    const raw = localStorage.getItem(BASELINE_KEY);
    const ts = raw ? parseInt(raw, 10) : NaN;
    if (Number.isFinite(ts)) return ts;
  } catch { /* fall through to (re)initialise */ }
  const now = Date.now();
  try { localStorage.setItem(BASELINE_KEY, String(now)); } catch { /* best-effort */ }
  return now;
}

/** Wipe all read stamps + baseline — the shared-browser control, run on logout. */
export function clearAllReads(): void {
  for (const k of readKeys()) {
    try { localStorage.removeItem(k); } catch { /* ignore */ }
  }
  try { localStorage.removeItem(BASELINE_KEY); } catch { /* ignore */ }
}
