import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('./client', () => ({ getToken: () => null }));

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: MockWebSocket[] = [];

  readonly url: string;
  readyState = MockWebSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  readonly send = vi.fn();
  readonly close = vi.fn(() => {
    this.readyState = MockWebSocket.CLOSING;
  });

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  open() {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }

  closeUnexpectedly() {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.();
  }
}

const { NerveWebSocket } = await import('./websocket');

// A client listens to the page after connect. Disconnect each one after its
// test, so that a page event in a later test reaches only that test's client.
const clients = new Set<InstanceType<typeof NerveWebSocket>>();
const realConnect = NerveWebSocket.prototype.connect;
NerveWebSocket.prototype.connect = function connect(this: InstanceType<typeof NerveWebSocket>) {
  clients.add(this);
  realConnect.call(this);
};
afterEach(() => {
  clients.forEach((client) => client.disconnect());
  clients.clear();
});

describe('NerveWebSocket', () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal('WebSocket', MockWebSocket);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('does not reconnect after an explicit disconnect', () => {
    const client = new NerveWebSocket();
    client.connect();
    const socket = MockWebSocket.instances[0];
    socket.open();

    client.disconnect();
    socket.closeUnexpectedly();
    vi.advanceTimersByTime(3000);

    expect(socket.close).toHaveBeenCalledOnce();
    expect(MockWebSocket.instances).toHaveLength(1);
    expect(client.connected).toBe(false);
  });

  it('drops messages after cancelling a scheduled reconnect', () => {
    const client = new NerveWebSocket();
    client.connect();
    const socket = MockWebSocket.instances[0];
    socket.open();
    socket.closeUnexpectedly();
    expect(vi.getTimerCount()).toBe(1);

    client.disconnect();

    expect(client.send({ type: 'message' })).toBe('dropped');
    vi.advanceTimersByTime(3000);
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('allows reconnecting after a later explicit connect', () => {
    const client = new NerveWebSocket();
    client.connect();
    client.disconnect();

    client.connect();
    const socket = MockWebSocket.instances[1];
    socket.closeUnexpectedly();
    vi.advanceTimersByTime(3000);

    expect(MockWebSocket.instances).toHaveLength(3);
  });

  it('sends the client id with a message only when given', () => {
    const client = new NerveWebSocket();
    client.connect();
    const socket = MockWebSocket.instances[0];
    socket.open();

    client.sendMessage('first', 's-1234');
    client.sendMessage('second', 's-1234', ['f1'], 'c-1');

    expect(socket.send.mock.calls.map(([frame]) => JSON.parse(frame))).toEqual([
      { type: 'message', content: 'first', session_id: 's-1234' },
      { type: 'message', content: 'second', session_id: 's-1234', file_ids: ['f1'], client_msg_id: 'c-1' },
    ]);
  });

  it('subscribes to an agent on an open socket and never queues the frame', () => {
    const client = new NerveWebSocket();
    client.connect();
    const socket = MockWebSocket.instances[0];

    expect(client.subscribeAgent('s-1234', 'ag_3f9a1c2b7d4e')).toBe('dropped');
    socket.open();
    expect(socket.send).not.toHaveBeenCalled();

    expect(client.subscribeAgent('s-1234', 'ag_3f9a1c2b7d4e')).toBe('sent');
    expect(client.unsubscribeAgent('s-1234', 'ag_3f9a1c2b7d4e')).toBe('sent');
    expect(socket.send.mock.calls.map(([frame]) => JSON.parse(frame))).toEqual([
      { type: 'agent_subscribe', session_id: 's-1234', agent_id: 'ag_3f9a1c2b7d4e' },
      { type: 'agent_unsubscribe', session_id: 's-1234', agent_id: 'ag_3f9a1c2b7d4e' },
    ]);
  });

  it('runs open handlers once connected and before the queued frames flush', () => {
    const client = new NerveWebSocket();
    const seen: boolean[] = [];
    client.onOpen(() => {
      seen.push(client.connected);
      client.switchSession('s-1234');
    });
    client.connect();
    expect(client.sendMessage('queued while connecting', 's-1234')).toBe('queued');

    MockWebSocket.instances[0].open();

    expect(seen).toEqual([true]);
    expect(MockWebSocket.instances[0].send.mock.calls.map(([frame]) => JSON.parse(frame).type))
      .toEqual(['switch_session', 'message']);
  });

  it('runs open handlers on every reconnect until removed, and survives one that throws', () => {
    const client = new NerveWebSocket();
    const opened = vi.fn();
    client.onOpen(() => { throw new Error('boom'); });
    const remove = client.onOpen(opened);
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    client.connect();
    MockWebSocket.instances[0].open();
    MockWebSocket.instances[0].closeUnexpectedly();
    vi.advanceTimersByTime(3000);
    MockWebSocket.instances[1].open();
    remove();
    MockWebSocket.instances[1].closeUnexpectedly();
    vi.advanceTimersByTime(3000);
    MockWebSocket.instances[2].open();

    expect(opened).toHaveBeenCalledTimes(2);
    quiet.mockRestore();
  });

  it('ignores close events from a detached socket', () => {
    const client = new NerveWebSocket();
    client.connect();
    const firstSocket = MockWebSocket.instances[0];
    client.disconnect();

    client.connect();
    firstSocket.closeUnexpectedly();
    vi.advanceTimersByTime(3000);

    expect(MockWebSocket.instances).toHaveLength(2);
  });
});

const { setAuthMode, setBeforeReenter, setTokenReader, useHostedStore } = await import('./hosted');

describe('NerveWebSocket credentials and session checks', () => {
  let fetchMock: ReturnType<typeof vi.fn>;
  let assign: ReturnType<typeof vi.fn>;

  function session(status: number, principalId = 'alice'): Response {
    return new Response(JSON.stringify({ authenticated: status === 200, principalId }), {
      status, headers: { 'Content-Type': 'application/json' },
    });
  }

  /** Let the session check answer. */
  async function settle() {
    await vi.advanceTimersByTimeAsync(0);
  }

  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal('WebSocket', MockWebSocket);
    vi.useFakeTimers();
    useHostedStore.setState({ mode: 'local', principalId: 'alice', problem: null, reentering: false });
    setBeforeReenter(null);
    setTokenReader(() => 'tok');
    fetchMock = vi.fn(async () => session(200));
    vi.stubGlobal('fetch', fetchMock);
    assign = vi.fn();
    vi.stubGlobal('location', { ...window.location, pathname: '/chat', search: '', assign });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    setTokenReader(() => null);
    useHostedStore.setState({ mode: 'local', principalId: null, problem: null, reentering: false });
    setBeforeReenter(null);
  });

  it('puts the local token in the URL', () => {
    new NerveWebSocket().connect();

    expect(MockWebSocket.instances[0].url).toMatch(/\/ws\?token=tok$/);
  });

  it('checks the principal before opening a hosted socket without a token', async () => {
    setAuthMode('external');

    new NerveWebSocket().connect();
    expect(MockWebSocket.instances).toHaveLength(0);
    await settle();

    expect(MockWebSocket.instances[0].url).toMatch(/\/ws$/);
    expect(MockWebSocket.instances[0].url).not.toContain('token=');
  });

  it('retries without a session check in local mode', () => {
    new NerveWebSocket().connect();
    MockWebSocket.instances[0].closeUnexpectedly();
    vi.advanceTimersByTime(3000);

    expect(fetchMock).not.toHaveBeenCalled();
    expect(MockWebSocket.instances).toHaveLength(2);
  });

  it('checks the gateway session after a failed connect, then retries', async () => {
    setAuthMode('external');
    new NerveWebSocket().connect();
    await settle();

    MockWebSocket.instances[0].closeUnexpectedly();
    expect(fetchMock).toHaveBeenCalledWith('/_nerve/session', expect.anything());
    await settle();
    expect(MockWebSocket.instances).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(3000);

    expect(MockWebSocket.instances).toHaveLength(2);
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('checks the gateway session after an open socket closes', async () => {
    setAuthMode('external');
    new NerveWebSocket().connect();
    await settle();
    MockWebSocket.instances[0].open();

    // A close with 4001 from Nerve, or a close by the gateway.
    MockWebSocket.instances[0].closeUnexpectedly();

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('signs in again and stops retrying when the session is gone', async () => {
    setAuthMode('external');
    const client = new NerveWebSocket();
    client.connect();
    await settle();
    fetchMock.mockImplementation(async () => session(401));

    MockWebSocket.instances[0].closeUnexpectedly();
    await settle();
    vi.advanceTimersByTime(3000);

    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat');
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it.each([[403, 'access_denied'], [410, 'agent_archived']] as const)(
    'shows the screen and stops retrying on %i', async (status, problem) => {
      setAuthMode('external');
      const client = new NerveWebSocket();
      client.connect();
      await settle();
      fetchMock.mockImplementation(async () => session(status));

      MockWebSocket.instances[0].closeUnexpectedly();
      await settle();
      vi.advanceTimersByTime(3000);

      expect(useHostedStore.getState().problem).toBe(problem);
      expect(assign).not.toHaveBeenCalled();
      expect(MockWebSocket.instances).toHaveLength(1);
      expect(client.send({ type: 'message' })).toBe('dropped');
    });

  it('queues messages while the session check runs', () => {
    setAuthMode('external');
    fetchMock.mockReturnValue(new Promise(() => {}));
    const client = new NerveWebSocket();
    client.connect();

    expect(client.sendMessage('hello', 's1')).toBe('queued');
  });

  it('ignores a session check that a disconnect made obsolete', async () => {
    setAuthMode('external');
    let answer: (res: Response) => void = () => {};
    fetchMock.mockReturnValue(new Promise<Response>((done) => { answer = done; }));
    const client = new NerveWebSocket();
    client.connect();

    client.disconnect();
    fetchMock.mockImplementation(async () => session(200));
    client.connect();
    answer(session(401));
    await settle();
    vi.advanceTimersByTime(3000);

    // The old refusal neither redirects nor schedules another connection.
    expect(assign).not.toHaveBeenCalled();
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('saves queued work instead of sending it through another principals cookie', async () => {
    setAuthMode('external');
    const client = new NerveWebSocket();
    client.connect();
    await settle();
    const first = MockWebSocket.instances[0];
    first.open();
    let saved: unknown;
    setBeforeReenter(() => { saved = client.takePendingMessages(); });
    fetchMock.mockImplementation(async () => session(200, 'bob'));

    first.closeUnexpectedly();
    expect(client.sendMessage('Alice private text', 'shared')).toBe('queued');
    await settle();
    await vi.advanceTimersByTimeAsync(6000);

    expect(saved).toEqual([{ session_id: 'shared', content: 'Alice private text' }]);
    expect(assign).toHaveBeenCalledOnce();
    expect(MockWebSocket.instances).toHaveLength(1);
    expect(first.send).not.toHaveBeenCalled();
    expect(client.sendMessage('late text', 'shared')).toBe('dropped');
  });

  it('waits for positive confirmation after an unavailable probe, then flushes for the same actor', async () => {
    setAuthMode('external');
    fetchMock.mockResolvedValueOnce(session(503));
    const client = new NerveWebSocket();
    client.connect();
    expect(client.sendMessage('keep this', 's1')).toBe('queued');
    await settle();
    expect(MockWebSocket.instances).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(3000);
    expect(MockWebSocket.instances).toHaveLength(1);
    MockWebSocket.instances[0].open();
    expect(MockWebSocket.instances[0].send).toHaveBeenCalledWith(JSON.stringify({
      type: 'message', content: 'keep this', session_id: 's1',
    }));
    client.disconnect();
  });

  it('checks for an identity change during the reconnect delay', async () => {
    setAuthMode('external');
    const client = new NerveWebSocket();
    client.connect();
    await settle();
    MockWebSocket.instances[0].closeUnexpectedly();
    await settle();
    fetchMock.mockImplementation(async () => session(200, 'bob'));

    await vi.advanceTimersByTimeAsync(3000);

    expect(assign).toHaveBeenCalledOnce();
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('hands back queued chat messages and keeps other frames', () => {
    const client = new NerveWebSocket();
    client.connect();
    client.sendMessage('first', 's1');
    client.switchSession('s2');
    client.sendMessage('second', 's2', ['f1']);

    expect(client.takePendingMessages()).toEqual([
      { session_id: 's1', content: 'first' },
      { session_id: 's2', content: 'second' },
    ]);
    expect(client.takePendingMessages()).toEqual([]);

    MockWebSocket.instances[0].open();
    expect(MockWebSocket.instances[0].send).toHaveBeenCalledOnce();
    expect(JSON.parse(MockWebSocket.instances[0].send.mock.calls[0][0]))
      .toEqual({ type: 'switch_session', session_id: 's2' });
  });
});

describe('NerveWebSocket after the page comes back', () => {
  let visibility: DocumentVisibilityState;

  function show() {
    visibility = 'visible';
    document.dispatchEvent(new Event('visibilitychange'));
  }

  function hide() {
    visibility = 'hidden';
    document.dispatchEvent(new Event('visibilitychange'));
  }

  const frames = (socket: MockWebSocket) => socket.send.mock.calls.map(([frame]) => JSON.parse(frame));

  /** A client with an open socket. */
  function connected() {
    const client = new NerveWebSocket();
    client.connect();
    MockWebSocket.instances[0].open();
    return { client, socket: MockWebSocket.instances[0] };
  }

  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal('WebSocket', MockWebSocket);
    vi.useFakeTimers();
    setAuthMode('local');
    visibility = 'visible';
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => visibility });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    Reflect.deleteProperty(document, 'visibilityState');
  });

  it('connects at once instead of waiting for the retry', () => {
    const { socket } = connected();
    hide();
    socket.closeUnexpectedly();

    show();

    expect(MockWebSocket.instances).toHaveLength(2);
    // The retry that waited does not open a third socket.
    vi.advanceTimersByTime(3000);
    expect(MockWebSocket.instances).toHaveLength(2);
  });

  it('does nothing while the page is hidden', () => {
    const { socket } = connected();
    socket.closeUnexpectedly();

    hide();

    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('replaces an open socket that does not answer, and sends the held frames on the new one', () => {
    const { client, socket } = connected();
    const opened = vi.fn(() => client.switchSession('s-1234'));
    client.onOpen(opened);

    show();
    expect(frames(socket)).toEqual([{ type: 'ping' }]);
    expect(client.sendMessage('hello', 's-1234')).toBe('queued');

    vi.advanceTimersByTime(4000);
    expect(socket.close).toHaveBeenCalledOnce();
    expect(MockWebSocket.instances).toHaveLength(2);
    // The late close event of the dead socket changes nothing.
    socket.closeUnexpectedly();
    vi.advanceTimersByTime(3000);
    expect(MockWebSocket.instances).toHaveLength(2);

    MockWebSocket.instances[1].open();
    expect(opened).toHaveBeenCalledOnce();
    expect(frames(MockWebSocket.instances[1]).map((f) => f.type)).toEqual(['switch_session', 'message']);
    expect(frames(socket)).toEqual([{ type: 'ping' }]);
  });

  it('keeps an open socket that answers, then sends the held frames', () => {
    const { client, socket } = connected();

    show();
    expect(client.sendMessage('hello', 's-1234')).toBe('queued');
    socket.onmessage?.({ data: JSON.stringify({ type: 'pong' }) });

    expect(frames(socket)).toEqual([
      { type: 'ping' },
      { type: 'message', content: 'hello', session_id: 's-1234' },
    ]);
    expect(client.sendMessage('more', 's-1234')).toBe('sent');
    vi.advanceTimersByTime(10_000);
    expect(socket.close).not.toHaveBeenCalled();
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('checks an open socket when the network comes back or the page is restored', () => {
    const { socket } = connected();

    window.dispatchEvent(new Event('online'));
    expect(frames(socket)).toEqual([{ type: 'ping' }]);
    socket.onmessage?.({ data: JSON.stringify({ type: 'pong' }) });

    window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: false }));
    expect(frames(socket)).toHaveLength(1);
    window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: true }));
    expect(frames(socket)).toHaveLength(2);
  });

  it('closes a socket that does not answer the regular ping', () => {
    const { socket } = connected();

    vi.advanceTimersByTime(30_000);
    expect(frames(socket)).toEqual([{ type: 'ping' }]);
    vi.advanceTimersByTime(9_999);
    expect(MockWebSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(1);

    expect(socket.close).toHaveBeenCalledOnce();
    expect(MockWebSocket.instances).toHaveLength(2);
  });

  it('counts any frame as an answer', () => {
    const { socket } = connected();

    vi.advanceTimersByTime(30_000);
    socket.onmessage?.({ data: JSON.stringify({ type: 'token', session_id: 's-1234', content: 'x' }) });
    vi.advanceTimersByTime(10_000);

    expect(socket.close).not.toHaveBeenCalled();
  });

  it('stops listening to the page after a disconnect', () => {
    const { client, socket } = connected();
    client.disconnect();

    show();
    window.dispatchEvent(new Event('online'));

    expect(socket.send).not.toHaveBeenCalled();
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it('checks the gateway session before it connects at once', async () => {
    setAuthMode('external');
    useHostedStore.setState({ mode: 'external', principalId: 'alice', problem: null, reentering: false });
    const valid = () => new Response(JSON.stringify({ authenticated: true, principalId: 'alice' }), {
      status: 200, headers: { 'Content-Type': 'application/json' },
    });
    const fetchMock = vi.fn(async () => valid());
    vi.stubGlobal('fetch', fetchMock);
    const client = new NerveWebSocket();
    client.connect();
    await vi.advanceTimersByTimeAsync(0);
    MockWebSocket.instances[0].open();
    let answer: (res: Response) => void = () => {};
    fetchMock.mockReturnValueOnce(new Promise<Response>((done) => { answer = done; }));

    show();
    await vi.advanceTimersByTimeAsync(4000);
    // The socket did not answer: the session check runs, no socket yet.
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(MockWebSocket.instances).toHaveLength(1);
    answer(valid());
    await vi.advanceTimersByTimeAsync(0);

    expect(MockWebSocket.instances).toHaveLength(2);
    useHostedStore.setState({ mode: 'local', principalId: null, problem: null, reentering: false });
  });
});
