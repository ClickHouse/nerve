import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const reply = (status: number, body: unknown) => new Response(JSON.stringify(body), {
  status, headers: { 'Content-Type': 'application/json' },
});
const externalStatus = { mode: 'external', auth_required: true, login: 'username_password' };
const viewer = (id: string) => ({ actor: { id, kind: 'human', display_name: id }, account: null });

beforeEach(() => {
  vi.resetModules();
  localStorage.clear();
  vi.stubGlobal('location', { pathname: '/chat/shared', search: '', assign: vi.fn() });
});
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

it('opens hosted mode on consecutive page loads despite a stale local JWT', async () => {
  localStorage.setItem('nerve_token', 'stale-local-jwt');
  const requests: Array<{ path: string; bearer: string | null }> = [];
  vi.stubGlobal('fetch', vi.fn(async (path: string, init?: RequestInit) => {
    const bearer = new Headers(init?.headers).get('Authorization');
    requests.push({ path, bearer });
    // The gateway has a valid browser cookie, but any bearer selects its
    // API-token authentication. A local JWT is not a gateway API token.
    if (bearer) return reply(401, { reason: 'login_required', requestId: 'r' });
    if (path === '/api/auth/status') return reply(200, externalStatus);
    if (path === '/api/auth/me') return reply(200, viewer('alice'));
    throw new Error(`Unexpected request ${path}`);
  }));
  for (let pageLoad = 0; pageLoad < 2; pageLoad++) {
    vi.resetModules();
    const { useAuthStore } = await import('../stores/authStore');
    await useAuthStore.getState().checkAuth();
    expect(useAuthStore.getState().authenticated).toBe(true);
    expect(useAuthStore.getState().viewer?.id).toBe('alice');
    expect(localStorage.getItem('nerve_token')).toBe('stale-local-jwt');
  }
  expect(window.location.assign).not.toHaveBeenCalled();
  expect(requests).toEqual([
    { path: '/api/auth/status', bearer: null },
    { path: '/api/auth/me', bearer: null },
    { path: '/api/auth/status', bearer: null },
    { path: '/api/auth/me', bearer: null },
  ]);
});

it('still opens a local session with its stored token after credential-free mode discovery', async () => {
  localStorage.setItem('nerve_token', 'local-jwt');
  const requests: Array<{ path: string; bearer: string | null }> = [];
  vi.stubGlobal('fetch', vi.fn(async (path: string, init?: RequestInit) => {
    const bearer = new Headers(init?.headers).get('Authorization');
    requests.push({ path, bearer });
    if (path === '/api/auth/status') return reply(200, {
      mode: 'local', auth_required: true, login: 'password',
    });
    if (path === '/api/auth/me' && bearer === 'Bearer local-jwt') {
      return reply(200, { ...viewer('alice'), account: { id: 'account-alice', username: 'alice' } });
    }
    return reply(401, { detail: 'Not authenticated' });
  }));
  const { useAuthStore } = await import('../stores/authStore');
  await useAuthStore.getState().checkAuth();

  expect(useAuthStore.getState().authenticated).toBe(true);
  expect(useAuthStore.getState().account?.id).toBe('account-alice');
  expect(requests).toEqual([
    { path: '/api/auth/status', bearer: null },
    { path: '/api/auth/me', bearer: 'Bearer local-jwt' },
  ]);
});

it('restores drafts, new chats and read state only for their verified principal', async () => {
  // Unowned keys from a previous version must not be assigned to whoever
  // happens to authenticate first after this version is installed.
  localStorage.setItem('nerve_draft_shared', 'unowned legacy text');
  let principal = 'alice';
  vi.stubGlobal('fetch', vi.fn(async (path: string) => {
    if (path === '/api/auth/status') return reply(200, externalStatus);
    if (path === '/api/auth/me') return reply(200, viewer(principal));
    throw new Error(`Unexpected request ${path}`);
  }));
  async function openPage(as: string) {
    principal = as;
    vi.resetModules();
    const auth = (await import('../stores/authStore')).useAuthStore;
    const chat = (await import('../stores/chatStore')).useChatStore;
    await auth.getState().checkAuth();
    expect(auth.getState().viewer?.id).toBe(as);
    expect(auth.getState().authenticated).toBe(true);
    return chat;
  }

  const alice = await openPage('alice');
  expect(alice.getState().drafts).toEqual({});
  alice.getState().setDraft('shared', 'Alice private unsent text');
  alice.getState().setDraft('virtual-alice', 'Alice new chat');
  alice.getState().markSeen('shared');
  (await import('../stores/helpers/virtualSessionStorage')).persistVirtualSession('virtual-alice', '2026-10-05');
  (await import('./hosted')).reenter();

  const bob = await openPage('bob');
  expect(bob.getState().drafts).toEqual({});
  expect(bob.getState().reads).toEqual({});
  expect(bob.getState().virtualSession).toBeNull();
  bob.getState().setDraft('shared', 'Bob private unsent text');
  // The still-open Alice tab must continue writing to Alice's namespace.
  alice.getState().setDraft('another', 'Alice in another tab');

  const bobReloaded = await openPage('bob');
  expect(bobReloaded.getState().drafts).toEqual({ shared: 'Bob private unsent text' });
  const aliceReloaded = await openPage('alice');
  expect(aliceReloaded.getState().drafts).toEqual({
    shared: 'Alice private unsent text', 'virtual-alice': 'Alice new chat', another: 'Alice in another tab',
  });
  expect(aliceReloaded.getState().reads.shared).toBeGreaterThan(0);
  expect(aliceReloaded.getState().virtualSession?.id).toBe('virtual-alice');
});

it('saves queued text for Alice and never opens a Bob socket in Alices mounted app', async () => {
  vi.useFakeTimers();
  let cookiePrincipal = 'alice';
  class Socket {
    static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
    static instances: Socket[] = [];
    readyState = Socket.CONNECTING;
    principal = cookiePrincipal;
    onopen: (() => void) | null = null;
    onclose: (() => void) | null = null;
    send = vi.fn();
    close = vi.fn();
    constructor() { Socket.instances.push(this); }
    open() { this.readyState = Socket.OPEN; this.onopen?.(); }
    drop() { this.readyState = Socket.CLOSED; this.onclose?.(); }
  }
  vi.stubGlobal('WebSocket', Socket);
  vi.stubGlobal('fetch', vi.fn(async (path: string) => {
    if (path === '/api/auth/status') return reply(200, externalStatus);
    if (path === '/api/auth/me') return reply(200, viewer(cookiePrincipal));
    if (path === '/_nerve/session') return reply(200, {
      authenticated: true, principalId: cookiePrincipal, expiresAt: '2099-01-01T00:00:00Z',
    });
    throw new Error(`Unexpected request ${path}`);
  }));
  const { useAuthStore } = await import('../stores/authStore');
  const { useChatStore } = await import('../stores/chatStore');
  await useAuthStore.getState().checkAuth();
  const { ws } = await import('./websocket');
  ws.connect();
  await vi.advanceTimersByTimeAsync(0);
  Socket.instances[0].open();
  cookiePrincipal = 'bob';
  Socket.instances[0].drop();
  expect(ws.sendMessage('Alice queued text', 'shared')).toBe('queued');
  await vi.advanceTimersByTimeAsync(6000);

  expect(Socket.instances).toHaveLength(1);
  expect(Socket.instances[0].send).not.toHaveBeenCalled();
  expect(window.location.assign).toHaveBeenCalledOnce();
  expect(useAuthStore.getState().viewer?.id).toBe('alice');
  expect(useChatStore.getState().drafts.shared).toBe('Alice queued text');
  ws.disconnect();

  vi.resetModules();
  const bobAuth = (await import('../stores/authStore')).useAuthStore;
  const bobChat = (await import('../stores/chatStore')).useChatStore;
  await bobAuth.getState().checkAuth();
  expect(bobAuth.getState().viewer?.id).toBe('bob');
  expect(bobChat.getState().drafts).toEqual({});
});
