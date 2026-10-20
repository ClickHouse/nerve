// A spawn card whose agent record the store does not hold. Two cases are
// worth one request each (AgentLine):
// - a fork inherits the cards of its source session, whose records stay there:
//   the detail route answers 404 `agent_in_other_session` with the owner, and
//   the card links to it;
// - a session with more records than the snapshot carries (200 by default):
//   the detail route returns the record, which joins the store so the card
//   renders as a live line.
// Everything else (old history without an agent id) renders the legacy card
// without asking anything.

import { useEffect, useState } from 'react';
import { api, apiErrorCode, apiErrorDetail } from '../../../api/client';
import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import type { ToolCallBlockData } from '../../../types/chat';

export type AgentLookup =
  | { kind: 'other'; ownerSessionId: string | null }
  | { kind: 'none' };

const results = new Map<string, AgentLookup>();
const waiters = new Map<string, Set<(r: AgentLookup) => void>>();
/** Lookups run one at a time: a fork of a big wave would otherwise fire them all at once. */
let queue: Promise<void> = Promise.resolve();

function ownerSessionOf(e: unknown): string | null {
  const owner = apiErrorDetail(e)?.owner_session_id;
  return typeof owner === 'string' && owner ? owner : null;
}

async function resolve(sessionId: string, agentId: string): Promise<AgentLookup> {
  try {
    const detail = await api.getAgentDetail(sessionId, agentId);
    const store = useAgentStore.getState();
    if (detail?.agent && store.sessionId === sessionId) {
      // The record exists here, outside the snapshot window: add it under the
      // rev rule, leaving the session summary as it is.
      store.upsertAgents([detail.agent]);
    }
    return { kind: 'none' };
  } catch (e) {
    return apiErrorCode(e) === 'agent_in_other_session'
      ? { kind: 'other', ownerSessionId: ownerSessionOf(e) }
      : { kind: 'none' };
  }
}

/** Look an agent id up once per session; `onResult` is always called asynchronously. */
function lookUp(sessionId: string, agentId: string, onResult: (r: AgentLookup) => void): () => void {
  const key = `${sessionId}:${agentId}`;
  const known = results.get(key);
  if (known) {
    let live = true;
    queueMicrotask(() => { if (live) onResult(known); });
    return () => { live = false; };
  }
  let pending = waiters.get(key);
  if (!pending) {
    pending = new Set();
    waiters.set(key, pending);
    queue = queue.then(async () => {
      const result = await resolve(sessionId, agentId);
      results.set(key, result);
      for (const notify of waiters.get(key) ?? []) notify(result);
      waiters.delete(key);
    });
  }
  pending.add(onResult);
  return () => { waiters.get(key)?.delete(onResult); };
}

function nonAmbientCount(agents: Record<string, { ambient: boolean }>): number {
  let n = 0;
  for (const id in agents) if (!agents[id].ambient) n += 1;
  return n;
}

/**
 * Where the record of a settled spawn card without a local record lives.
 * Asks only in a fork, or when the session has more records than the store
 * holds; null while unknown or when nothing was asked.
 */
export function useAgentLookup(block: ToolCallBlockData, hasRecord: boolean): AgentLookup | null {
  const agentId = block.agentId ?? null;
  // Only a settled card that names a record the store lacks can need a lookup;
  // every other line skips the (linear) count below.
  const candidate = !!agentId && !hasRecord && block.status === 'complete';
  const sessionId = useAgentStore(s => s.sessionId);
  const beyondWindow = useAgentStore(s => candidate && s.summary.total > nonAmbientCount(s.agents));
  const isFork = useChatStore(s => candidate && s.activeSession.startsWith('fork-'));
  const ask = candidate && !!sessionId && (isFork || beyondWindow);
  const key = ask ? `${sessionId}:${agentId}` : null;
  const [result, setResult] = useState<{ key: string; lookup: AgentLookup } | null>(null);

  useEffect(() => {
    if (!key || !sessionId || !agentId) return;
    return lookUp(sessionId, agentId, (lookup) => setResult({ key, lookup }));
  }, [key, sessionId, agentId]);

  return result && result.key === key ? result.lookup : null;
}
