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

/**
 * Drop persisted drafts whose session id is not in `keep` — reclaims drafts for
 * sessions deleted or archived elsewhere (server-side, another tab, Telegram).
 * Callers must include the active session and any unsent virtual chat in `keep`.
 */
export function pruneDrafts(keep: Set<string>): void {
  for (const k of draftKeys()) {
    if (!keep.has(k.slice(PREFIX.length))) {
      try { localStorage.removeItem(k); } catch { /* ignore */ }
    }
  }
}

/** Wipe every persisted draft — the shared-browser safety control, run on logout. */
export function clearAllDrafts(): void {
  for (const k of draftKeys()) {
    try { localStorage.removeItem(k); } catch { /* ignore */ }
  }
}
