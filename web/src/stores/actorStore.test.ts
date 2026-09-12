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
  UNNAMED_ACTOR, SYSTEM_ACTOR_NAME,
} = await import('./actorStore');
const { useAccountStore } = await import('./accountStore');

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
