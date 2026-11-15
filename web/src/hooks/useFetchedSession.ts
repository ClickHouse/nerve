import { useEffect, useState } from 'react';
import { api } from '../api/client';
import type { Session } from '../types/chat';

/**
 * The row of session `id` from `GET /api/sessions/{id}`, for a session that
 * no loaded list has: a link to a cron session while the System group is
 * closed, or to an older or archived chat. It is read once for each id.
 * `null` while it loads, after an error, and for a `null` id.
 */
export function useFetchedSession(id: string | null): Session | null {
  const [row, setRow] = useState<Session | null>(null);
  useEffect(() => {
    if (!id) return;
    let current = true;
    api.getSession(id).then(
      (fetched: Session) => { if (current) setRow(fetched); },
      () => { /* no row: the page keeps its plain fallbacks */ },
    );
    return () => { current = false; };
  }, [id]);
  return row?.id === id ? row : null;
}
