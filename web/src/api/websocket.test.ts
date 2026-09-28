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
