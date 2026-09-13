// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';

/**
 * The actor map: what turns the ids stored on sessions and messages into the
 * names a reader sees.
 *
 * What these specs are defending, in the order the mistakes would hurt:
 *
 * - **A name is never remembered.** Not in `localStorage`, not on a message
 *   object. A cached display name survives the rename it was supposed to
 *   reflect, and nothing would ever tell you.
 * - **Requests are bounded.** One per app session, one more per id that turns
 *   out not to exist, one per account mutation. An id from a namespace this
 *   instance has never had — which is what a later move to external identity
 *   leaves behind — must cost one request, not one per render forever.
 * - **It cannot break what it decorates.** Every failure path leaves a working
 *   screen with a neutral label on it.
 */

vi.mock('../api/client', () => ({
  api: {
    listActors: vi.fn(),
    getActor: vi.fn(),
    listAccounts: vi.fn(),
    createAccount: vi.fn(),
    updateAccount: vi.fn(),
    setAccountEnabled: vi.fn(),
    changeOwnPassword: vi.fn(),
    authStatus: vi.fn(),
    login: vi.fn(),
    checkAuth: vi.fn(),
  },
  setToken: vi.fn(),
  clearToken: vi.fn(),
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('./helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('./helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../api/client');
const {
  useActorStore, actorName, isSystemActor, visibleActorIds,
  actorDiscriminators,
  UNNAMED_ACTOR, SYSTEM_ACTOR_NAME,
} = await import('./actorStore');
const { useAccountStore } = await import('./accountStore');
const { useAuthStore } = await import('./authStore');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const listAccounts = api.listAccounts as unknown as ReturnType<typeof vi.fn>;
const authStatus = api.authStatus as unknown as ReturnType<typeof vi.fn>;

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actor(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, profile_version: 1, ...overrides };
}

const alice = (name: string | null = 'Alice') => actor(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actor(BOB, { display_name: name });
const system = (name: string | null = 'Nerve') =>
  actor(SYSTEM, { kind: 'system', display_name: name });

/** Wait for whatever fetch the last `resolve()` kicked off to settle. */
async function settled(calls = 1): Promise<void> {
  await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(calls));
  await vi.waitFor(() => expect(useActorStore.getState().loading).toBe(false));
}

/**
 * A response this test decides when to deliver, so it can make something else
 * happen while the request is still open.
 */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let release!: (value: T) => void;
  const promise = new Promise<T>((r) => { release = r; });
  return { promise, resolve: release };
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  listAccounts.mockResolvedValue({ accounts: [] });
  authStatus.mockResolvedValue({
    auth_required: true, mode: 'local', login: 'password',
    setup_pending: false, multiple_accounts: true,
  });
  localStorage.clear();
});

describe('loading the map', () => {
  it('reads /api/actors once however many ids ask for it', async () => {
    const { resolve } = useActorStore.getState();
    resolve([ALICE]);
    resolve([BOB]);
    resolve([ALICE, BOB, SYSTEM]);
    await settled();

    expect(listActors).toHaveBeenCalledTimes(1);
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice');
    expect(useActorStore.getState().actors[SYSTEM].kind).toBe('system');
  });

  it('does not ask again once an id is known', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();
    useActorStore.getState().resolve([ALICE, BOB, SYSTEM]);
    await new Promise((r) => setTimeout(r, 0));

    expect(listActors).toHaveBeenCalledTimes(1);
  });

  it('asks for nothing when every id is null', async () => {
    useActorStore.getState().resolve([null, undefined, '']);
    await new Promise((r) => setTimeout(r, 0));

    expect(listActors).not.toHaveBeenCalled();
  });

  it('stores no name anywhere outside the store', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();

    expect(localStorage.length).toBe(0);
    const dump = JSON.stringify(Object.entries(localStorage));
    expect(dump).not.toContain('Alice');
  });

  it('does not outlive the session that fetched it', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();
    expect(useActorStore.getState().actors[ALICE]).toBeDefined();

    useAuthStore.getState().logout();

    // A map kept across a logout would be a cache, and this one is not one.
    expect(useActorStore.getState().actors).toEqual({});
    expect(useActorStore.getState().loaded).toBe(false);
  });
});

describe('an id the server does not know', () => {
  it('is re-read once and then never asked about again', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();

    // A person created in another tab would show up on a re-read. This one
    // does not exist, so it must not become a request per render.
    useActorStore.getState().resolve(['actor-ghost']);
    await settled(2);
    expect(useActorStore.getState().unresolved).toContain('actor-ghost');

    useActorStore.getState().resolve(['actor-ghost']);
    useActorStore.getState().resolve(['actor-ghost', ALICE]);
    await new Promise((r) => setTimeout(r, 0));
    expect(listActors).toHaveBeenCalledTimes(2);
  });

  it('resolves when the re-read does know it', async () => {
    listActors.mockResolvedValueOnce({ actors: [alice()] });
    useActorStore.getState().resolve([ALICE]);
    await settled();
    expect(useActorStore.getState().actors[BOB]).toBeUndefined();

    listActors.mockResolvedValueOnce({ actors: [alice(), bob()] });
    useActorStore.getState().resolve([BOB]);
    await settled(2);

    expect(useActorStore.getState().actors[BOB].display_name).toBe('Bob');
    expect(useActorStore.getState().unresolved).not.toContain(BOB);
  });

  it('renders as the neutral fallback rather than crashing', async () => {
    useActorStore.getState().resolve(['actor-ghost']);
    await settled();

    expect(actorName(useActorStore.getState().actors['actor-ghost'])).toBe(UNNAMED_ACTOR);
  });
});

/**
 * Everything here is about a response that is *slow*. The store coalesces, so
 * anything that happens between a request going out and coming back has no
 * request of its own to ride on — and that window is exactly when a second
 * person's first live message arrives, when somebody logs out, and when a
 * rename is saved.
 */
describe('while a lookup is still open', () => {
  it('follows up on an id that arrived after the request went out', async () => {
    // Bob does not exist yet when the map is fetched; his first live message
    // lands while it is in flight. Without a follow-up his id sits in the
    // queue with nothing to drain it and reads `Unnamed account` until the
    // next navigation.
    const first = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(first.promise);
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(1));

    useActorStore.getState().resolve([BOB]);
    expect(listActors).toHaveBeenCalledTimes(1);   // coalesced, as intended

    listActors.mockResolvedValue({ actors: [alice(), bob()] });
    first.resolve({ actors: [alice()] });

    await settled(2);
    expect(useActorStore.getState().actors[BOB].display_name).toBe('Bob');
    expect(useActorStore.getState().unresolved).not.toContain(BOB);
  });

  it('keeps following up however many waves arrive', async () => {
    // A new person per response window, for longer than any fixed number of
    // follow-ups. The earlier version stopped after four and left the fifth
    // arrival queued with `inFlight` cleared and no hook dependency changed to
    // restart it — the original bug, one wave further out.
    const WAVES = 8;
    const ids = Array.from({ length: WAVES }, (_, i) => `actor-wave-${i}`);
    const known: ActorRef[] = [];

    // Every request is deferred, so the test controls exactly when each answer
    // lands and can queue the next id while the previous one is open.
    const gates: Array<{ promise: Promise<{ actors: ActorRef[] }>; resolve: (v: { actors: ActorRef[] }) => void }> = [];
    listActors.mockImplementation(() => {
      const gate = deferred<{ actors: ActorRef[] }>();
      gates.push(gate);
      return gate.promise;
    });

    useActorStore.getState().resolve([ids[0]]);
    known.push(actor(ids[0], { display_name: 'Wave 0' }));

    for (let wave = 1; wave < WAVES; wave++) {
      await vi.waitFor(() => expect(gates.length).toBe(wave));
      // Queued while request `wave` is still open — the stranding window.
      useActorStore.getState().resolve([ids[wave]]);
      // The answer was composed before that person existed, so it cannot
      // resolve them. Only the follow-up can, which is the whole point.
      gates[wave - 1].resolve({ actors: [...known] });
      known.push(actor(ids[wave], { display_name: `Wave ${wave}` }));
    }
    await vi.waitFor(() => expect(gates.length).toBe(WAVES));
    gates[WAVES - 1].resolve({ actors: [...known] });

    await vi.waitFor(() => expect(useActorStore.getState().loading).toBe(false));
    await vi.waitFor(() =>
      expect(Object.keys(useActorStore.getState().actors).length).toBe(WAVES));

    // Every wave has a name, and nothing was written off as unknown.
    for (const id of ids) expect(useActorStore.getState().actors[id]).toBeDefined();
    expect(useActorStore.getState().unresolved).toEqual([]);
  });

  it('stops following up once the id is known to be nobody', async () => {
    const first = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(first.promise);
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(1));

    useActorStore.getState().resolve(['actor-ghost']);
    listActors.mockResolvedValue({ actors: [alice()] });
    first.resolve({ actors: [alice()] });

    // One follow-up, which settles the question, and then it stops: two
    // requests total and the id recorded so nothing asks a third time.
    await settled(2);
    await new Promise((r) => setTimeout(r, 20));
    expect(listActors).toHaveBeenCalledTimes(2);
    expect(useActorStore.getState().unresolved).toContain('actor-ghost');
  });

  it('does not let a logout be undone by the response that was already sent', async () => {
    const slow = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(slow.promise);
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(1));

    useAuthStore.getState().logout();
    expect(useActorStore.getState().actors).toEqual({});

    slow.resolve({ actors: [alice(), bob(), system()] });
    await new Promise((r) => setTimeout(r, 20));

    // The map stays empty. Repopulating it here would put the previous
    // session's names back on screen after somebody deliberately ended it.
    expect(useActorStore.getState().actors).toEqual({});
    expect(useActorStore.getState().loaded).toBe(false);
  });

  it('keeps the newest snapshot when two forced re-reads land out of order', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();

    // Two renames in quick succession. The first request is answered last.
    const stale = deferred<{ actors: ActorRef[] }>();
    const fresh = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(stale.promise).mockReturnValueOnce(fresh.promise);

    const firstRefresh = useActorStore.getState().refresh();
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(2));
    const secondRefresh = useActorStore.getState().refresh();
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(3));

    fresh.resolve({ actors: [alice('Alice Two'), bob(), system()] });
    stale.resolve({ actors: [alice('Alice One'), bob(), system()] });
    await Promise.all([firstRefresh, secondRefresh]);
    await new Promise((r) => setTimeout(r, 20));

    // The older answer arriving last must not roll the name backwards.
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice Two');
  });
});

describe('refresh', () => {
  it('picks up a rename without touching anything that stored the id', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();
    // What a message row looks like: an id and no name, which is the whole
    // point — this object must come out of the rename byte for byte identical.
    const message = Object.freeze({ id: 7, role: 'user', actor_id: ALICE });

    listActors.mockResolvedValue({
      actors: [alice('Alice Doe'), bob(), system()],
    });
    await useActorStore.getState().refresh();

    expect(actorName(useActorStore.getState().actors[ALICE])).toBe('Alice Doe');
    expect(useActorStore.getState().actors[ALICE].profile_version).toBe(1);
    expect(message).toEqual({ id: 7, role: 'user', actor_id: ALICE });
  });

  it('gives a previously unknown id another chance', async () => {
    listActors.mockResolvedValueOnce({ actors: [alice()] });
    useActorStore.getState().resolve([BOB]);
    await settled();
    expect(useActorStore.getState().unresolved).toContain(BOB);

    listActors.mockResolvedValue({ actors: [alice(), bob()] });
    await useActorStore.getState().refresh();

    expect(useActorStore.getState().unresolved).toEqual([]);
    expect(useActorStore.getState().actors[BOB].display_name).toBe('Bob');
  });

  it('never rejects, so a name lookup cannot fail the thing it decorates', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();

    listActors.mockRejectedValue(new Error('500: nope'));
    await expect(useActorStore.getState().refresh()).resolves.toBeUndefined();

    // The last known map survives: a broken lookup must not blank out names
    // that were already on screen.
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice');
    expect(useActorStore.getState().loading).toBe(false);
  });
});

describe('a failing first load', () => {
  it('leaves an empty map and no unhandled rejection', async () => {
    listActors.mockRejectedValue(new Error('500: nope'));
    useActorStore.getState().resolve([ALICE]);
    await settled();

    expect(useActorStore.getState().actors).toEqual({});
    expect(useActorStore.getState().loaded).toBe(false);
    expect(actorName(useActorStore.getState().actors[ALICE])).toBe(UNNAMED_ACTOR);
  });

  it('survives an instance with no actors at all', async () => {
    listActors.mockResolvedValue({ actors: [] });
    useActorStore.getState().resolve([ALICE]);
    await settled();

    expect(useActorStore.getState().actors).toEqual({});
    expect(visibleActorIds([ALICE], useActorStore.getState().actors)).toEqual(new Set());
  });
});

describe('the account screen re-reads the map', () => {
  it('after a mutation, so a rename shows up in the chat without a reload', async () => {
    useActorStore.getState().resolve([ALICE]);
    await settled();
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice');

    listActors.mockResolvedValue({ actors: [alice('Alice Doe'), bob(), system()] });
    (api.updateAccount as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({});
    await useAccountStore.getState().update('acc-1', { display_name: 'Alice Doe' });

    expect(listActors).toHaveBeenCalledTimes(2);
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice Doe');
  });
});

describe('actorName', () => {
  it('prefers the display name', () => {
    expect(actorName(alice())).toBe('Alice');
  });

  it('treats a blank display name as no display name', () => {
    expect(actorName(alice('   '))).toBe(UNNAMED_ACTOR);
  });

  it('names the agent even when bootstrap gave it none', () => {
    expect(actorName(system(null))).toBe(SYSTEM_ACTOR_NAME);
  });

  it('never falls back to the raw id', () => {
    expect(actorName(alice(null))).toBe(UNNAMED_ACTOR);
    expect(actorName(undefined)).toBe(UNNAMED_ACTOR);
    expect(actorName(alice(null))).not.toContain(ALICE);
  });

  it('tells the agent apart from a person', () => {
    expect(isSystemActor(system())).toBe(true);
    expect(isSystemActor(alice())).toBe(false);
    expect(isSystemActor(undefined)).toBe(false);
  });
});

/**
 * Display names are not identity (0.7) and nothing stops two of them being
 * equal, so a label that is only a name can name two different people.
 */
describe('two actors with the same name', () => {
  const twoAlexes = {
    'actor-alex-1': actor('actor-alex-1', { display_name: 'Alex' }),
    'actor-alex-2': actor('actor-alex-2', { display_name: 'Alex' }),
    [BOB]: bob(),
  };

  it('both get a discriminator', () => {
    const byId = actorDiscriminators(twoAlexes);
    expect([...byId.keys()].sort()).toEqual(['actor-alex-1', 'actor-alex-2']);
  });

  it('leaves a unique name alone', () => {
    expect(actorDiscriminators(twoAlexes).has(BOB)).toBe(false);
    expect(actorDiscriminators({ [ALICE]: alice(), [BOB]: bob() }).size).toBe(0);
  });

  it('counts the label, so two accounts with no name collide too', () => {
    const nameless = {
      'actor-x': actor('actor-x'),
      'actor-y': actor('actor-y'),
    };
    expect([...actorDiscriminators(nameless).keys()].sort()).toEqual(['actor-x', 'actor-y']);
  });

  it('trims before comparing, so "Alex" and "Alex " are the same name', () => {
    const padded = {
      'actor-alex-1': actor('actor-alex-1', { display_name: 'Alex' }),
      'actor-alex-2': actor('actor-alex-2', { display_name: 'Alex ' }),
    };
    expect(actorDiscriminators(padded).size).toBe(2);
  });

  it('memoises on the map it was given', () => {
    const first = actorDiscriminators(twoAlexes);
    expect(actorDiscriminators(twoAlexes)).toBe(first);
    expect(actorDiscriminators({ ...twoAlexes })).not.toBe(first);
  });

  it('takes the suffix from the end, where two UUIDs are likeliest to differ', () => {
    // A timestamp-prefixed generator makes a shared prefix the normal case,
    // not the pathological one.
    const a = '0199aaaa-1111-7000-8000-0000000000ab';
    const b = '0199aaaa-1111-7000-8000-0000000000cd';
    const byId = actorDiscriminators({
      [a]: actor(a, { display_name: 'Alex' }),
      [b]: actor(b, { display_name: 'Alex' }),
    });
    expect(byId.get(a)).toBe('0000ab');
    expect(byId.get(b)).toBe('0000cd');
    expect(a.endsWith(byId.get(a)!)).toBe(true);
  });

  /**
   * The reason a fixed-length tail is not good enough. Two ids from the same
   * generator can agree on their last six characters, and then the thing whose
   * entire job is to tell them apart renders identically for both.
   */
  it('grows until the suffixes are actually distinct', () => {
    const a = '0199aaaa-1111-7000-8000-00000a0000ab';
    const b = '0199aaaa-1111-7000-8000-00000b0000ab';   // same last 10 characters
    const byId = actorDiscriminators({
      [a]: actor(a, { display_name: 'Alex' }),
      [b]: actor(b, { display_name: 'Alex' }),
    });

    expect(byId.get(a)).not.toBe(byId.get(b));
    expect(a.endsWith(byId.get(a)!)).toBe(true);
    expect(b.endsWith(byId.get(b)!)).toBe(true);
    // Grown past six to reach the character that differs, and no further.
    expect(byId.get(a)).toBe('a0000ab');
    expect(byId.get(b)).toBe('b0000ab');
  });

  it('extends to the whole id when only the first character differs', () => {
    // Same length, identical everywhere but the first character, so nothing
    // shorter than the entire id separates them.
    const a = 'aa0000ab';
    const b = 'ba0000ab';
    const byId = actorDiscriminators({
      [a]: actor(a, { display_name: 'Alex' }),
      [b]: actor(b, { display_name: 'Alex' }),
    });

    expect(byId.get(a)).toBe(a);
    expect(byId.get(b)).toBe(b);
  });

  it('handles a group whose ids are shorter than the floor', () => {
    const short = '0000ab';
    const long = 'cafe0000ab';
    const byId = actorDiscriminators({
      [short]: actor(short, { display_name: 'Alex' }),
      [long]: actor(long, { display_name: 'Alex' }),
    });

    // The short one cannot yield six *distinct* characters of its own, so the
    // group grows one further; slicing past an id's length is not an error, it
    // just yields the whole id.
    expect(byId.get(short)).not.toBe(byId.get(long));
    expect(byId.get(short)).toBe(short);
    expect(long.endsWith(byId.get(long)!)).toBe(true);
  });

  it('keeps a group of three apart from each other', () => {
    const ids = ['aaa111', 'bbb111', 'ccc111'];
    const map = Object.fromEntries(ids.map((id) => [id, actor(id, { display_name: 'Alex' })]));
    const byId = actorDiscriminators(map);

    expect(new Set(ids.map((id) => byId.get(id))).size).toBe(3);
  });
});

describe('visibleActorIds', () => {
  const map = {
    [ALICE]: alice(), [BOB]: bob(), [SYSTEM]: system(),
  };

  it('labels nothing when one person is talking to themselves', () => {
    expect(visibleActorIds([ALICE, ALICE, null, ALICE], map)).toEqual(new Set());
  });

  it('labels both once a second person speaks', () => {
    expect(visibleActorIds([ALICE, null, BOB], map)).toEqual(new Set([ALICE, BOB]));
  });

  it('always labels the agent, even alone', () => {
    expect(visibleActorIds([SYSTEM, SYSTEM], map)).toEqual(new Set([SYSTEM]));
  });

  it('does not let the agent make one person ambiguous', () => {
    expect(visibleActorIds([ALICE, SYSTEM], map)).toEqual(new Set([SYSTEM]));
  });

  it('reads an unknown id as a person, so two of them are two people', () => {
    expect(visibleActorIds([ALICE, 'actor-elsewhere'], map))
      .toEqual(new Set([ALICE, 'actor-elsewhere']));
    expect(visibleActorIds(['actor-elsewhere'], map)).toEqual(new Set());
  });

  it('ignores nulls entirely', () => {
    expect(visibleActorIds([null, undefined, null], map)).toEqual(new Set());
  });
});
