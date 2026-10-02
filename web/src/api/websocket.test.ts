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

const { setAuthMode, setTokenReader, useHostedStore } = await import('./hosted');

describe('NerveWebSocket credentials and session checks', () => {
  let fetchMock: ReturnType<typeof vi.fn>;
  let assign: ReturnType<typeof vi.fn>;

  function session(status: number): Response {
    return new Response('{}', { status, headers: { 'Content-Type': 'application/json' } });
  }

  /** Let the session check answer. */
  async function settle() {
    await vi.advanceTimersByTimeAsync(0);
  }

  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal('WebSocket', MockWebSocket);
    vi.useFakeTimers();
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
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
    useHostedStore.setState({ mode: 'local', problem: null, reentering: false });
  });

  it('puts the local token in the URL', () => {
    new NerveWebSocket().connect();

    expect(MockWebSocket.instances[0].url).toMatch(/\/ws\?token=tok$/);
  });

  it('adds no token in hosted mode', () => {
    setAuthMode('external');

    new NerveWebSocket().connect();

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

    MockWebSocket.instances[0].closeUnexpectedly();
    expect(fetchMock).toHaveBeenCalledWith('/_nerve/session', expect.anything());
    await settle();
    expect(MockWebSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(3000);

    expect(MockWebSocket.instances).toHaveLength(2);
  });

  it('checks the gateway session after an open socket closes', async () => {
    setAuthMode('external');
    new NerveWebSocket().connect();
    MockWebSocket.instances[0].open();

    // A close with 4001 from Nerve, or a close by the gateway.
    MockWebSocket.instances[0].closeUnexpectedly();

    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('signs in again and stops retrying when the session is gone', async () => {
    setAuthMode('external');
    fetchMock.mockImplementation(async () => session(401));
    const client = new NerveWebSocket();
    client.connect();

    MockWebSocket.instances[0].closeUnexpectedly();
    await settle();
    vi.advanceTimersByTime(3000);

    expect(assign).toHaveBeenCalledWith('/_nerve/login?return_to=%2Fchat');
    expect(MockWebSocket.instances).toHaveLength(1);
  });

  it.each([[403, 'access_denied'], [410, 'agent_archived']] as const)(
    'shows the screen and stops retrying on %i', async (status, problem) => {
      setAuthMode('external');
      fetchMock.mockImplementation(async () => session(status));
      const client = new NerveWebSocket();
      client.connect();

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
    MockWebSocket.instances[0].closeUnexpectedly();

    expect(client.sendMessage('hello', 's1')).toBe('queued');
  });

  it('ignores a session check that a disconnect made obsolete', async () => {
    setAuthMode('external');
    let answer: (res: Response) => void = () => {};
    fetchMock.mockReturnValue(new Promise<Response>((done) => { answer = done; }));
    const client = new NerveWebSocket();
    client.connect();
    MockWebSocket.instances[0].closeUnexpectedly();

    client.disconnect();
    client.connect();
    answer(session(200));
    await settle();
    vi.advanceTimersByTime(3000);

    // The old answer schedules no retry beside the new socket.
    expect(MockWebSocket.instances).toHaveLength(2);
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
