import { useEffect, useMemo } from 'react';
import { create } from 'zustand';
import { api, type ActorRef } from '../api/client';

/**
 * Who the ids stored on sessions and messages belong to, for display only.
 *
 * Sessions carry `created_by_actor_id` and messages carry `actor_id`; neither
 * ever carries a name, so that renaming somebody changes every label without
 * rewriting a single stored row. This store is the other half of that: it holds
 * the *current* names, re-read from the server rather than remembered.
 *
 * Three properties it is built to keep, in order of how badly they break things
 * if lost:
 *
 * - **Nothing is persisted.** No `persist` middleware, no `localStorage`, and
 *   no name ever written onto a message or session object. A display name is a
 *   presentation snapshot; a cached one is a lie with a long half-life.
 * - **Requests are bounded.** One fetch per app session, plus at most one more
 *   per id the server turns out not to know, plus one per account mutation.
 *   There is no polling and no retry loop.
 * - **It cannot break what it decorates.** Every failure path leaves the last
 *   known map in place and renders the neutral fallback. A name lookup must
 *   never turn a working screen into an error.
 */

/**
 * The label for an actor with no display name, and for an id the server does
 * not know. Deliberately the same string for both: the accounts screen renders
 * `No display name` rather than a blank or an id, and the distinction between
 * "has no name" and "not in the map" is not something a reader of a chat
 * transcript can act on. The id itself is in the tooltip for anyone who needs
 * to tell the two apart.
 */
export const UNNAMED_ACTOR = 'Unnamed account';

/** What the agent's own principal is called when bootstrap gave it no name. */
export const SYSTEM_ACTOR_NAME = 'Nerve';

export interface ActorState {
  /** id → ActorRef. In memory only, replaced wholesale on every re-read. */
  actors: Record<string, ActorRef>;
  /** A fetch has completed at least once (successfully). */
  loaded: boolean;
  loading: boolean;
  /**
   * Ids a *completed* fetch did not know. Never requested again, which is what
   * makes an unknown id cost one extra request rather than one per render.
   * An id from a second actor namespace (a later move to external identity
   * leaves history pointing at ids this instance never had) lands here and
   * renders as `Unnamed account` forever, which is correct.
   */
  unresolved: string[];
  /**
   * The one entry point components use. Loads the map the first time it is
   * handed a non-null id, and re-reads it when handed an id that is neither
   * known nor already known-missing — which is how a person created in another
   * tab gets a name here without polling.
   */
  resolve: (ids: Iterable<string | null | undefined>) => void;
  /**
   * Force a re-read. The rename path: `accountStore` calls this after every
   * mutation, on the same line it re-reads `/api/auth/status`. Never rejects —
   * a failed name lookup must not turn a successful rename into an error.
   */
  refresh: () => Promise<void>;
  /** Drop everything, including the module-level request state. Tests only. */
  reset: () => void;
}

/**
 * Request state lives outside the store because it is not rendered: putting an
 * in-flight promise in zustand state would re-render every subscriber twice per
 * fetch for nothing.
 */
let inFlight: Promise<void> | null = null;
let pending = new Set<string>();

export const useActorStore = create<ActorState>((set, get) => {
  async function load(force = false): Promise<void> {
    if (inFlight) {
      // Coalesce: forty bubbles mounting together make one request.
      if (!force) return inFlight;
      // A forced re-read that arrives while a fetch is in flight has to wait
      // for it and then start its own — the one in flight may have been sent
      // before the rename this call exists to pick up.
      await inFlight.catch(() => {});
    }
    const claimed = pending;
    pending = new Set();
    set({ loading: true });
    const run = (async () => {
      try {
        const { actors } = await api.listActors();
        const map: Record<string, ActorRef> = {};
        for (const actor of actors) map[actor.id] = actor;
        // Anything asked about that a completed fetch still does not know is
        // recorded so it is never asked about again; anything previously
        // missing that now resolves drops back out.
        const unresolved = [...new Set([...get().unresolved, ...claimed])]
          .filter((id) => !(id in map));
        set({ actors: map, loaded: true, loading: false, unresolved });
      } catch {
        // Keep the last known map and stay un-`loaded`, so the next render that
        // needs a name tries again. That retry is driven by an id changing, not
        // by a timer, so a server that is down costs one request per navigation
        // rather than a loop.
        set({ loading: false });
      }
    })();
    inFlight = run.finally(() => { inFlight = null; });
    return inFlight;
  }

  return {
    actors: {},
    loaded: false,
    loading: false,
    unresolved: [],

    resolve: (ids) => {
      const { actors, unresolved } = get();
      let wanted = false;
      for (const id of ids) {
        if (!id) continue;                    // null is the common case, not a miss
        if (id in actors) continue;           // already have a name for it
        if (unresolved.includes(id)) continue; // asked once; the server does not know it
        pending.add(id);
        wanted = true;
      }
      if (wanted) void load();
    },

    refresh: async () => {
      // Give the known-missing ids one more chance: "somebody was just created"
      // is exactly what an account mutation means.
      for (const id of get().unresolved) pending.add(id);
      set({ unresolved: [] });
      await load(true);
    },

    reset: () => {
      inFlight = null;
      pending = new Set();
      set({ actors: {}, loaded: false, loading: false, unresolved: [] });
    },
  };
});

/** The name to show for an actor, or the neutral fallback when there is none. */
export function actorName(actor: ActorRef | undefined): string {
  const name = actor?.display_name?.trim();
  if (name) return name;
  if (actor?.kind === 'system') return SYSTEM_ACTOR_NAME;
  return UNNAMED_ACTOR;
}

/** Whether this is the agent's own principal rather than a person. */
export function isSystemActor(actor: ActorRef | undefined): boolean {
  return actor?.kind === 'system';
}

/**
 * Which of these ids are worth labelling.
 *
 * A label earns its place when it tells two things apart. In a list that means
 * either of:
 *
 * - the actor is the agent's own principal — "Nerve did this on a schedule" is
 *   always different from "a person asked for this", even on a one-person
 *   install; or
 * - the list holds two or more distinct human actors, so a name answers "which
 *   of you".
 *
 * An id the map does not know counts as human here, which is the safe reading:
 * two unknown ids are two people until something says otherwise.
 *
 * The consequence, which is intended: on a single-account install nothing is
 * labelled, and the transcript looks exactly as it does today. The first time a
 * second person speaks in a session, every bubble in it gains a name — the
 * earlier ones included, because that is the moment the earlier ones became
 * ambiguous.
 */
export function visibleActorIds(
  ids: Iterable<string | null | undefined>,
  actors: Record<string, ActorRef>,
): Set<string> {
  const distinct = new Set<string>();
  for (const id of ids) if (id) distinct.add(id);

  const visible = new Set<string>();
  const humans: string[] = [];
  for (const id of distinct) {
    if (actors[id]?.kind === 'system') visible.add(id);
    else humans.push(id);
  }
  if (humans.length > 1) for (const id of humans) visible.add(id);
  return visible;
}

/**
 * The `ActorRef` for one id, asking the store to fetch it if it has not
 * already. Returns `undefined` both while the first fetch is in flight and for
 * an id the server does not know — callers render the neutral fallback for
 * both, so there is no loading state to show for a name.
 */
export function useActorRef(id: string | null | undefined): ActorRef | undefined {
  const actor = useActorStore((s) => (id ? s.actors[id] : undefined));
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => {
    if (id) resolve([id]);
  }, [id, resolve]);
  return actor;
}

/**
 * The set of ids a list should label, loading the map as a side effect.
 *
 * The list has to ask for the map itself rather than leaving it to the labels:
 * the map is what decides whether there are any labels, so waiting for a label
 * to mount would be circular.
 */
export function useVisibleActorIds(ids: (string | null | undefined)[]): Set<string> {
  // Ids are opaque server-side identifiers with no commas in them, so a joined
  // string is a sound dependency and keeps the array out of the dep lists.
  const key = ids.filter((id): id is string => !!id).join(',');
  const distinct = useMemo(() => (key ? key.split(',') : []), [key]);
  const actors = useActorStore((s) => s.actors);
  const resolve = useActorStore((s) => s.resolve);
  useEffect(() => { resolve(distinct); }, [distinct, resolve]);
  return useMemo(() => visibleActorIds(distinct, actors), [distinct, actors]);
}
