// Persistence for the *virtual* (unsent) chat.
//
// A new chat is client-side only until its first message: `createSession()`
// mints a random UUID and the server knows nothing about it. That id keyed the
// composer draft — and lived only in React state, so any reload (an expired
// token used to force one) dropped it, orphaning the draft and losing whatever
// long prompt was being written. Persisting the id closes that hole: a reload
// rehydrates the same virtual chat and its draft comes back with it.
//
// Only the id and creation time are stored. The rest of the row is cosmetic
// and rebuilt on load.

const KEY = 'nerve_virtual_session';

export interface StoredVirtualSession {
  id: string;
  created: string;
}

/** Remember the current unsent chat so a reload can restore it. */
export function persistVirtualSession(id: string, created: string): void {
  try {
    localStorage.setItem(KEY, JSON.stringify({ id, created }));
  } catch { /* quota / disabled — the chat simply won't survive a reload */ }
}

/** Read back the unsent chat, if any. */
export function loadVirtualSession(): StoredVirtualSession | null {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed.id === 'string' && parsed.id) {
      return { id: parsed.id, created: typeof parsed.created === 'string' ? parsed.created : new Date().toISOString() };
    }
  } catch { /* unreadable / malformed — treat as absent */ }
  return null;
}

/** Forget it — the chat was sent (and adopted a real id) or discarded. */
export function clearVirtualSession(): void {
  try { localStorage.removeItem(KEY); } catch { /* ignore */ }
}
