// Per-session composer draft persistence.
//
// Unsent composer text is kept per session in localStorage so a page reload,
// tab close, or browser restart never loses what you were typing — a draft
// lives until you send it, delete its session, or log out.
//
// One key per session (`nerve_draft_<id>`) rather than a single JSON blob so
// two tabs editing *different* sessions can't clobber each other's drafts, and
// per-session cleanup is a single removeItem. Every write is quota-safe: if
// localStorage is full or disabled the draft simply stays in memory — typing
// is never blocked.

const PREFIX = 'nerve_draft_';

const keyFor = (sessionId: string) => `${PREFIX}${sessionId}`;

/** Collect the session ids of all persisted draft keys (safe if storage is off). */
function draftKeys(): string[] {
  const keys: string[] = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (k && k.startsWith(PREFIX)) keys.push(k);
    }
  } catch { /* storage unavailable */ }
  return keys;
}

/** Read every persisted draft into a { sessionId: text } map (store hydration). */
export function loadDrafts(): Record<string, string> {
  const out: Record<string, string> = {};
  for (const k of draftKeys()) {
    try {
      const text = localStorage.getItem(k);
      if (text) out[k.slice(PREFIX.length)] = text;
    } catch { /* ignore a single unreadable key */ }
  }
  return out;
}

/** Write-through one session's draft. Empty/blank text removes the key. */
export function persistDraft(sessionId: string, text: string): void {
  if (!sessionId) return;
  try {
    if (text) localStorage.setItem(keyFor(sessionId), text);
    else localStorage.removeItem(keyFor(sessionId));
  } catch { /* quota exceeded / disabled — keep the in-memory draft only */ }
}

/** Drop one session's persisted draft (session deleted or virtual chat discarded). */
export function removeDraft(sessionId: string): void {
  if (!sessionId) return;
  try { localStorage.removeItem(keyFor(sessionId)); } catch { /* ignore */ }
}

// Orphan bookkeeping: { [sessionId]: firstSeenUnrecognizedMs }.
const ORPHAN_KEY = 'nerve_draft_orphans';
// How long an unrecognized draft is kept before it is reclaimed.
const ORPHAN_GRACE_MS = 7 * 24 * 60 * 60 * 1000;  // 7 days

function readOrphans(): Record<string, number> {
  try {
    const raw = localStorage.getItem(ORPHAN_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return parsed && typeof parsed === 'object' ? parsed as Record<string, number> : {};
  } catch { return {}; }
}

function writeOrphans(map: Record<string, number>): void {
  try {
    if (Object.keys(map).length) localStorage.setItem(ORPHAN_KEY, JSON.stringify(map));
    else localStorage.removeItem(ORPHAN_KEY);
  } catch { /* quota / disabled — bookkeeping is best-effort */ }
}

/**
 * Reclaim persisted drafts whose session no longer exists (deleted or archived
 * elsewhere — server-side, another tab, Telegram). Callers must include the
 * active session and any unsent virtual chat in `keep`.
 *
 * A draft is NEVER deleted the first time it looks unrecognized; it is only
 * marked, and reclaimed on a later pass once ORPHAN_GRACE_MS has passed. This
 * used to delete on sight, which turned a transient blind spot into permanent
 * data loss: a reload dropped the in-memory id of an unsent *new* chat, and
 * the very next `loadSessions()` swept that chat's draft — a long unsent
 * prompt — out of localStorage before anything could restore it.
 */
export function pruneDrafts(keep: Set<string>): void {
  const orphans = readOrphans();
  const now = Date.now();
  let changed = false;

  for (const k of draftKeys()) {
    const id = k.slice(PREFIX.length);

    if (keep.has(id)) {
      // Recognized again — clear any pending reclaim.
      if (orphans[id] !== undefined) { delete orphans[id]; changed = true; }
      continue;
    }

    const firstSeen = orphans[id];
    if (firstSeen === undefined) {
      // First sighting: start the clock, keep the draft.
      orphans[id] = now;
      changed = true;
      continue;
    }
    // Clock skew (or a cleared system clock) must not trigger an early sweep.
    if (firstSeen > now) { orphans[id] = now; changed = true; continue; }
    if (now - firstSeen < ORPHAN_GRACE_MS) continue;

    try { localStorage.removeItem(k); } catch { /* ignore */ }
    delete orphans[id];
    changed = true;
  }

  if (changed) writeOrphans(orphans);
}

/** Persisted drafts for sessions the app no longer knows about. */
export function orphanedDrafts(keep: Set<string>): Array<{ id: string; text: string }> {
  const out: Array<{ id: string; text: string }> = [];
  for (const k of draftKeys()) {
    const id = k.slice(PREFIX.length);
    if (keep.has(id)) continue;
    try {
      const text = localStorage.getItem(k);
      if (text && text.trim()) out.push({ id, text });
    } catch { /* ignore a single unreadable key */ }
  }
  return out;
}

/** Wipe every persisted draft — the shared-browser safety control, run on logout. */
export function clearAllDrafts(): void {
  for (const k of draftKeys()) {
    try { localStorage.removeItem(k); } catch { /* ignore */ }
  }
  writeOrphans({});
}
