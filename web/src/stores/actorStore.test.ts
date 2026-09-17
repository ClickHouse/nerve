// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    listActors: vi.fn(), listAccounts: vi.fn(), updateAccount: vi.fn(),
    authStatus: vi.fn(), getOwnAccount: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('./helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('./helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../api/client');
const {
  useActorStore, actorName, actorDiscriminators, visibleActorIds,
  UNNAMED_ACTOR, SYSTEM_ACTOR_NAME,
} = await import('./actorStore');
const { useAccountStore } = await import('./accountStore');

const listActors = api.listActors as unknown as ReturnType<typeof vi.fn>;
const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actor(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, ...overrides } as ActorRef;
}

const alice = (name: string | null = 'Alice') => actor(ALICE, { display_name: name });
const bob = (name: string | null = 'Bob') => actor(BOB, { display_name: name });
const system = () => actor(SYSTEM, { kind: 'system', display_name: null });

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let release!: (value: T) => void;
  const promise = new Promise<T>((resolve) => { release = resolve; });
  return { promise, resolve: release };
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({ actors: [alice(), bob(), system()] });
  (api.listAccounts as unknown as ReturnType<typeof vi.fn>)
    .mockResolvedValue({ accounts: [] });
  (api.authStatus as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
    auth_required: true, login: 'password',
  });
  (api.getOwnAccount as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
    id: 'acc-1', actor_id: ALICE, username: 'alice', display_name: 'Alice',
    enabled: true, has_password: true, created_at: 't',
  });
});

describe('actor directory reads', () => {
  it('coalesces concurrent ids into one bulk read and ignores null history', async () => {
    const { resolve } = useActorStore.getState();
    resolve([ALICE]);
    resolve([null, BOB, SYSTEM]);

    await vi.waitFor(() => expect(useActorStore.getState().actors[BOB]).toBeDefined());
    expect(listActors).toHaveBeenCalledTimes(1);
    expect(useActorStore.getState().actors[SYSTEM].kind).toBe('system');
    expect(localStorage.length).toBe(0);

    resolve([ALICE, BOB]);
    await new Promise((done) => setTimeout(done, 0));
    expect(listActors).toHaveBeenCalledTimes(1);
  });

  it('makes one follow-up for an id that arrives during the bulk read', async () => {
    const first = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(first.promise)
      .mockResolvedValueOnce({ actors: [alice(), bob()] });
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(1));

    useActorStore.getState().resolve([BOB]);
    expect(listActors).toHaveBeenCalledTimes(1);
    first.resolve({ actors: [alice()] });

    await vi.waitFor(() => expect(useActorStore.getState().actors[BOB]).toBeDefined());
    expect(listActors).toHaveBeenCalledTimes(2);
  });

  it('does not let a response already in flight repopulate a reset store', async () => {
    const slow = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(slow.promise);
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalled());

    useActorStore.getState().reset();
    slow.resolve({ actors: [alice()] });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(useActorStore.getState().actors).toEqual({});
  });

  it('keeps the newest forced refresh and swallows lookup failures', async () => {
    const stale = deferred<{ actors: ActorRef[] }>();
    const fresh = deferred<{ actors: ActorRef[] }>();
    listActors.mockReturnValueOnce(stale.promise).mockReturnValueOnce(fresh.promise);
    const first = useActorStore.getState().refresh();
    const second = useActorStore.getState().refresh();

    fresh.resolve({ actors: [alice('Alice Two')] });
    stale.resolve({ actors: [alice('Alice One')] });
    await Promise.all([first, second]);
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice Two');

    listActors.mockRejectedValueOnce(new Error('offline'));
    await expect(useActorStore.getState().refresh()).resolves.toBeUndefined();
    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice Two');

    useActorStore.getState().reset();
    listActors.mockRejectedValueOnce(new Error('still offline'));
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(listActors).toHaveBeenCalledTimes(4));
    expect(useActorStore.getState().actors).toEqual({});
  });

  it('refreshes after an account mutation so a same-tab rename is current', async () => {
    useActorStore.getState().resolve([ALICE]);
    await vi.waitFor(() => expect(useActorStore.getState().actors[ALICE]).toBeDefined());
    listActors.mockResolvedValue({ actors: [alice('Alice Doe'), bob(), system()] });
    (api.updateAccount as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({});

    await useAccountStore.getState().update('acc-1', { display_name: 'Alice Doe' });

    expect(useActorStore.getState().actors[ALICE].display_name).toBe('Alice Doe');
  });
});

describe('actor presentation helpers', () => {
  it('uses current names, neutral human fallback, and the Nerve fallback', () => {
    expect(actorName(alice())).toBe('Alice');
    expect(actorName(alice('  '))).toBe(UNNAMED_ACTOR);
    expect(actorName(undefined)).toBe(UNNAMED_ACTOR);
    expect(actorName(system())).toBe(SYSTEM_ACTOR_NAME);
  });

  it('grows equal-name suffixes until every visible label is distinct', () => {
    const a = '0199aaaa-1111-7000-8000-00000a0000ab';
    const b = '0199aaaa-1111-7000-8000-00000b0000ab';
    const byId = actorDiscriminators({
      [a]: actor(a, { display_name: 'Alex' }),
      [b]: actor(b, { display_name: 'Alex' }),
      [BOB]: bob(),
    });

    expect(byId.get(a)).toBe('a0000ab');
    expect(byId.get(b)).toBe('b0000ab');
    expect(byId.has(BOB)).toBe(false);
  });

  it('also discriminates equal fallback names and short ids', () => {
    const byId = actorDiscriminators({
      short: actor('short'),
      'long-short': actor('long-short'),
    });

    expect(byId.get('short')).not.toBe(byId.get('long-short'));
    expect('short'.endsWith(byId.get('short')!)).toBe(true);
    expect('long-short'.endsWith(byId.get('long-short')!)).toBe(true);
  });

  it('labels Nerve and other humans, but not the viewer or null history', () => {
    const map = { [ALICE]: alice(), [BOB]: bob(), [SYSTEM]: system() };

    expect(visibleActorIds([ALICE, BOB, SYSTEM, null], map, ALICE))
      .toEqual(new Set([BOB, SYSTEM]));
    expect(visibleActorIds([ALICE, null], map, ALICE)).toEqual(new Set());
    expect(visibleActorIds(['unknown'], map, ALICE)).toEqual(new Set(['unknown']));
  });
});
