// Hosted browser cookies can change people across tabs or a login redirect.
// Keep this tab's storage scope pinned to the actor that /api/auth/me verified.
// Local mode retains its existing keys; unowned legacy drafts are never
// adopted by a hosted actor.
let principalId: string | null = null;
const listeners = new Set<() => void>();

export function viewerStorageKey(key: string): string {
  return principalId === null ? key : `nerve_principal_${encodeURIComponent(principalId)}:${key}`;
}

export function selectViewerStorage(principal: string | null): void {
  if (principalId === principal) return;
  principalId = principal;
  for (const listener of listeners) listener();
}

/** Rehydrate memory before the newly verified viewer can see the app. */
export function onViewerStorageChange(listener: () => void): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}
