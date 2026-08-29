import type { Session } from '../types/chat';

// Resolve a session id across the feed and the lazy archived/system groups, preferring the feed.
export function findSessionById(
  id: string | null | undefined,
  sessions: Session[],
  archivedSessions: Session[] | null | undefined,
  systemSessions: Session[] | null | undefined,
): Session | undefined {
  if (!id) return undefined;
  return sessions.find(s => s.id === id)
    ?? (archivedSessions ?? []).find(s => s.id === id)
    ?? (systemSessions ?? []).find(s => s.id === id);
}
