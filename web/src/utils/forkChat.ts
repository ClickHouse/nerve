import { api } from '../api/client';
import { useChatStore } from '../stores/chatStore';

/**
 * Fork a chat (optionally from a specific message) and return the new
 * session id, or null on failure (after surfacing the reason).
 *
 * The fork is a real server session immediately — it branches the source's
 * native conversation on its first turn and starts pre-populated with the
 * source's messages up to the fork point. Callers navigate to the returned
 * id themselves (navigation is router-owned, see ChatPage's URL contract).
 */
export async function forkChat(
  sourceSessionId: string,
  atMessageId?: number,
): Promise<string | null> {
  try {
    const fork = await api.forkSession(
      sourceSessionId,
      atMessageId != null ? String(atMessageId) : undefined,
    );
    await useChatStore.getState().loadSessions();
    return (fork?.id as string) ?? null;
  } catch (e) {
    // request() throws Error("<status>: <json body>") — surface the
    // server's human-readable detail when present.
    let reason = e instanceof Error ? e.message : String(e);
    const jsonStart = reason.indexOf('{');
    if (jsonStart >= 0) {
      try {
        const parsed = JSON.parse(reason.slice(jsonStart));
        if (typeof parsed?.detail === 'string') reason = parsed.detail;
      } catch { /* keep raw message */ }
    }
    window.alert(`Could not fork this chat: ${reason}`);
    return null;
  }
}
