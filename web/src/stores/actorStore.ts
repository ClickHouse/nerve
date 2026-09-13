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
 *
 * `generation` is what makes a response commit only if it is still the answer
 * anybody is waiting for. Every request takes a number on the way out and
 * checks it on the way back; `reset()` bumps it. Without it, two failures are
 * reachable and neither is loud: a slow response landing after a logout
 * repopulates a map that was deliberately emptied, and two overlapping forced
 * re-reads commit in whatever order the network returns them, so the older
 * snapshot can land last and undo the rename that prompted the newer one.
 */
let inFlight: Promise<void> | null = null;
let pending = new Set<string>();
let generation = 0;

/**
 * A hard stop on the follow-up chain in `drain`.
 *
 * Production cannot reach it: every completed request either resolves a queued
 * id or records it in `unresolved`, and `resolve` skips both, so each round has
 * strictly fewer ids to ask about than the last. It is here because this is a
 * loop that makes network requests, and a loop that makes network requests
 * should not be able to run forever on the strength of an argument.
 */
const MAX_FOLLOW_UPS = 4;

export const useActorStore = create<ActorState>((set, get) => {
  /** One request. Returns false when it failed or was superseded. */
  async function fetchOnce(): Promise<boolean> {
    const mine = ++generation;
    const claimed = pending;
    pending = new Set();
    set({ loading: true });
    try {
      const { actors } = await api.listActors();
      // Superseded — a newer request, or a logout, happened while this was in
      // flight. Committing now would undo whatever replaced it.
      if (mine !== generation) return false;
      const map: Record<string, ActorRef> = {};
      for (const actor of actors) map[actor.id] = actor;
      // Anything asked about that a completed fetch still does not know is
      // recorded so it is never asked about again; anything previously missing
      // that now resolves drops back out.
      const unresolved = [...new Set([...get().unresolved, ...claimed])]
        .filter((id) => !(id in map));
      set({ actors: map, loaded: true, loading: false, unresolved });
      // Ids that arrived while this request was open are still queued. Drop the
      // ones this answer settled; whatever is left is what a follow-up is for.
      for (const id of [...pending]) {
        if (id in map || unresolved.includes(id)) pending.delete(id);
      }
      return true;
    } catch {
      if (mine !== generation) return false;
      // Keep the last known map and stay un-`loaded`, so the next render that
      // needs a name tries again. That retry is driven by an id changing, not
      // by a timer, so a server that is down costs one request per navigation
      // rather than a loop. The ids this attempt claimed go back in the queue
      // so they are not silently dropped on the floor.
      for (const id of claimed) pending.add(id);
      set({ loading: false });
      return false;
    }
  }

  /**
   * Request, then keep following up until nothing is queued.
   *
   * The follow-up is the point. `resolve` cannot start a second request while
   * one is in flight — that is the coalescing forty mounting bubbles depend on
   * — so without this, an id first seen during a lookup (a second person's live
   * message, arriving between the map being fetched and it landing) would sit
   * in the queue with nothing to drain it, and read `Unnamed account` until the
   * next navigation.
   *
   * **The exit condition is an empty queue, not a round count.** An earlier
   * version stopped after a fixed number of follow-ups, which recreated the
   * very bug it was written for one wave further out: a fifth arrival, queued
   * during the fifth response, was left with `inFlight` cleared and no hook
   * dependency changed to restart it.
   *
   * It terminates because every round settles everything it claims — into the
   * map or into `unresolved`, and `resolve` skips both — so the queue can only
   * refill from *new* arrivals, and each of those costs exactly one request.
   * `pending` never holds an id that is already known, because `fetchOnce`
   * prunes the ones its own answer settled. The inner bound is a tripwire, not
   * a limit: it breaks the work into finite chains so this reads as bounded
   * segments rather than an open `while (true)`, and re-entering is what keeps
   * arrivals from being stranded.
   */
  async function drain(): Promise<void> {
    // `do`, not `while`: the first request has to happen even with an empty
    // queue, because that is what a forced re-read after a rename is.
    do {
      for (let round = 0; round <= MAX_FOLLOW_UPS; round++) {
        if (!(await fetchOnce())) return;
        if (pending.size === 0) return;
      }
    } while (pending.size > 0);
  }

  /** Start a request chain and publish it as the one to coalesce onto. */
  function start(): Promise<void> {
    const run = drain();
    const tracked: Promise<void> = run.finally(() => {
      if (inFlight === tracked) inFlight = null;
    });
    inFlight = tracked;
    return tracked;
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
      // Queued either way: a request already in flight will follow up on it.
      if (wanted && !inFlight) void start();
    },

    refresh: async () => {
      // Give the known-missing ids one more chance: "somebody was just created"
      // is exactly what an account mutation means.
      for (const id of get().unresolved) pending.add(id);
      set({ unresolved: [] });
      // Deliberately not coalesced onto `inFlight`: a request already in flight
      // may have been sent before the rename this call exists to pick up. The
      // generation guard is what keeps the older one from committing after.
      await start();
    },

    reset: () => {
      // Bump first: a response already on the wire must not repopulate the map
      // this just emptied.
      generation++;
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
 * How short a discriminator is allowed to be.
 *
 * A floor rather than the length: the suffix grows until it is unique within
 * its group (see `actorDiscriminators`), and this only stops it being *shorter*
 * than six characters when two characters would already do. A one-character
 * discriminator is technically the shortest unique one and reads like a typo;
 * six is short enough to sit in a label and long enough to look deliberate, and
 * holding it constant means adding a third person with the same name does not
 * silently restyle the other two.
 */
const MIN_DISCRIMINATOR = 6;

let discriminatorCache:
  { actors: Record<string, ActorRef>; byId: Map<string, string> } | null = null;

/**
 * The suffix each actor needs in order to be told apart from another actor that
 * renders the same label — two people called Alex, or two accounts with no
 * display name at all. Ids that need nothing are absent.
 *
 * Display names are not identity and nothing stops two of them being equal
 * (0.7: names are never identity keys), so a label that is only a name can name
 * two different people.
 *
 * **The suffix is computed per colliding group and is unique by construction,
 * not by luck.** A fixed-length tail is not good enough: two ids can share
 * their last six characters — more easily than it sounds, since these are
 * UUIDs and a group is usually two rows from the same generator — and then the
 * disambiguator does not disambiguate. The length grows a character at a time
 * until every id in the group has a distinct tail, extending to the whole id if
 * that is what it takes. That terminates because ids are the map's own keys and
 * are therefore already distinct.
 *
 * Taken from the *end* of the id rather than the front: two UUIDs are far
 * likelier to share a prefix — some generators put a timestamp there — than a
 * tail, so a front slice would need to grow much further much more often.
 *
 * Collisions are computed over **the whole map, not the list being rendered**.
 * Per-list would be narrower and would show the suffix less often, but the same
 * person would then gain and lose it depending on which surface you were
 * looking at, and a label that changes shape when nothing about the actor
 * changed is worse than one that is occasionally more precise than it needs to
 * be. The map is a handful of rows, and the result is memoised on its identity,
 * so every label on a page shares one computation.
 *
 * Ids the map does not know are absent and keep the plain fallback: there is
 * nothing to compare them against, and their tooltip already carries the id.
 */
export function actorDiscriminators(actors: Record<string, ActorRef>): Map<string, string> {
  if (discriminatorCache?.actors === actors) return discriminatorCache.byId;

  const byLabel = new Map<string, string[]>();
  for (const actor of Object.values(actors)) {
    const label = actorName(actor);
    const sharing = byLabel.get(label);
    if (sharing) sharing.push(actor.id);
    else byLabel.set(label, [actor.id]);
  }

  const byId = new Map<string, string>();
  for (const sharing of byLabel.values()) {
    if (sharing.length < 2) continue;
    const longest = Math.max(...sharing.map((id) => id.length));
    let length = Math.min(MIN_DISCRIMINATOR, longest);
    // Grow until the tails are distinct. The last candidate is the whole id,
    // which always is — these are the map's keys.
    for (; length < longest; length++) {
      if (new Set(sharing.map((id) => id.slice(-length))).size === sharing.length) break;
    }
    for (const id of sharing) byId.set(id, id.slice(-length));
  }

  discriminatorCache = { actors, byId };
  return byId;
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
