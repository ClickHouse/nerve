import { useEffect, useMemo } from 'react';
import { create } from 'zustand';
import { api, type ActorRef } from '../api/client';

/** Current actor names used to render the stable ids stored on sessions/messages. */
export interface ActorState {
  actors: Record<string, ActorRef>;
  resolve: (ids: Iterable<string | null | undefined>) => void;
  /** Force a name refresh after an account mutation. Never rejects. */
  refresh: () => Promise<void>;
  reset: () => void;
}

export const UNNAMED_ACTOR = 'Unnamed account';
export const SYSTEM_ACTOR_NAME = 'Nerve';

let inFlight: Promise<void> | null = null;
let pending = new Set<string>();
let generation = 0;

export const useActorStore = create<ActorState>((set, get) => {
  async function fetchOnce(): Promise<boolean> {
    const mine = ++generation;
    const claimed = pending;
    pending = new Set();
    try {
      const { actors } = await api.listActors();
      if (mine !== generation) return false;
      const map = Object.fromEntries(actors.map((actor) => [actor.id, actor]));
      set({ actors: map });
      for (const id of pending) if (id in map) pending.delete(id);
      return true;
    } catch {
      if (mine === generation) for (const id of claimed) pending.add(id);
      return false;
    }
  }

  async function drain(): Promise<void> {
    if (!(await fetchOnce())) return;
    // A live id can arrive after the coalesced request was sent. The endpoint
    // returns the whole directory, so one follow-up resolves that late wave.
    if (pending.size > 0) await fetchOnce();
  }

  function start(): Promise<void> {
    const run = drain();
    const tracked = run.finally(() => {
      if (inFlight === tracked) inFlight = null;
    });
    inFlight = tracked;
    return tracked;
  }

  return {
    actors: {},

    resolve: (ids) => {
      let wanted = false;
      for (const id of ids) {
        if (!id || id in get().actors) continue;
        pending.add(id);
        wanted = true;
      }
      if (wanted && !inFlight) void start();
    },

    refresh: async () => {
      // Do not coalesce with a read sent before the mutation. The generation
      // guard prevents that older snapshot from committing afterwards.
      await start();
    },

    reset: () => {
      generation++;
      inFlight = null;
      pending = new Set();
      set({ actors: {} });
    },
  };
});

/** The display name, else the login name, else a neutral placeholder. */
export function actorName(actor: ActorRef | undefined): string {
  const name = actor?.display_name?.trim() || actor?.username?.trim();
  if (name) return name;
  return actor?.kind === 'system' ? SYSTEM_ACTOR_NAME : UNNAMED_ACTOR;
}

export function isSystemActor(actor: ActorRef | undefined): boolean {
  return actor?.kind === 'system';
}

const MIN_DISCRIMINATOR = 6;
let discriminatorCache:
  { actors: Record<string, ActorRef>; byId: Map<string, string> } | null = null;

/** Collision-safe visible suffixes for actors whose rendered names are equal. */
export function actorDiscriminators(actors: Record<string, ActorRef>): Map<string, string> {
  if (discriminatorCache?.actors === actors) return discriminatorCache.byId;

  const byLabel = new Map<string, string[]>();
  for (const actor of Object.values(actors)) {
    const sharing = byLabel.get(actorName(actor));
    if (sharing) sharing.push(actor.id);
    else byLabel.set(actorName(actor), [actor.id]);
  }

  const byId = new Map<string, string>();
  for (const sharing of byLabel.values()) {
    if (sharing.length < 2) continue;
    const longest = Math.max(...sharing.map((id) => id.length));
    let length = Math.min(MIN_DISCRIMINATOR, longest);
    for (; length < longest; length++) {
      if (new Set(sharing.map((id) => id.slice(-length))).size === sharing.length) break;
    }
    for (const id of sharing) byId.set(id, id.slice(-length));
  }

  discriminatorCache = { actors, byId };
  return byId;
}

/**
 * Labels are relative to the signed-in viewer: Nerve is always named, and a
 * human is named exactly when they are somebody else. Null legacy rows stay
 * quiet. An unknown non-null id is treated as human and compared by stable id.
 */
export function visibleActorIds(
  ids: Iterable<string | null | undefined>,
  actors: Record<string, ActorRef>,
  viewerActorId: string | null,
): Set<string> {
  const visible = new Set<string>();
  for (const id of ids) {
    if (!id) continue;
    if (actors[id]?.kind === 'system' || id !== viewerActorId) visible.add(id);
  }
  return visible;
}

export function useActorRef(id: string | null | undefined): ActorRef | undefined {
  const actor = useActorStore((s) => (id ? s.actors[id] : undefined));
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => {
    if (id) resolve([id]);
  }, [id, resolve]);
  return actor;
}

export function useVisibleActorIds(
  ids: (string | null | undefined)[],
  viewerActorId: string | null,
): Set<string> {
  const key = ids.filter((id): id is string => !!id).join(',');
  const distinct = useMemo(() => (key ? [...new Set(key.split(','))] : []), [key]);
  const actors = useActorStore((s) => s.actors);
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => { resolve(distinct); }, [distinct, resolve]);
  return useMemo(
    () => visibleActorIds(distinct, actors, viewerActorId),
    [distinct, actors, viewerActorId],
  );
}
