import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SandboxTransport } from './transport';

/** What a message of a document without an origin says its origin is. */
const NONE = 'null';
const READY = { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-ready', params: {} };
const REFUSED = {
  jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-refused',
  params: { reason: 'this page was served without its sandbox policy.' },
};

/** A message as the browser delivers it to the chat page. */
function deliver(data: unknown, from: { origin: string; source: MessageEventSource | null }) {
  window.dispatchEvent(new MessageEvent('message', { data, origin: from.origin, source: from.source }));
}

describe('SandboxTransport', () => {
  let frame: HTMLIFrameElement;
  let other: HTMLIFrameElement;
  let transport: SandboxTransport;
  const received = vi.fn();
  const failed = vi.fn();

  beforeEach(async () => {
    frame = document.createElement('iframe');
    other = document.createElement('iframe');
    document.body.append(frame, other);
    transport = new SandboxTransport(frame);
    transport.onmessage = received;
    transport.onerror = failed;
    await transport.start();
  });

  afterEach(async () => {
    await transport.close();
    frame.remove();
    other.remove();
    vi.clearAllMocks();
  });

  it('takes a JSON-RPC message of its frame, from a document without an origin', () => {
    deliver(READY, { origin: NONE, source: frame.contentWindow });

    expect(received).toHaveBeenCalledTimes(1);
    expect(received).toHaveBeenCalledWith(READY);
  });

  it.each([
    ['another origin', 'https://evil.example.com'],
    ['the origin of the chat page', window.location.origin],
    ['another port of the chat\'s host', 'https://nerve.example:8901'],
    ['no origin', ''],
  ])('ignores a message of its frame from a document of %s', (_, origin) => {
    deliver(READY, { origin, source: frame.contentWindow });
    deliver({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'x' } }, { origin, source: frame.contentWindow });

    expect(received).not.toHaveBeenCalled();
    expect(failed).not.toHaveBeenCalled();
  });

  it('ignores a message without an origin that another window sent', () => {
    // Every sandboxed frame says "null": a view that posts to the chat page
    // itself, a frame that a view made, the frame of another view.
    deliver(READY, { origin: NONE, source: other.contentWindow });
    deliver(READY, { origin: NONE, source: window });
    deliver(READY, { origin: NONE, source: null });

    expect(received).not.toHaveBeenCalled();
    expect(failed).not.toHaveBeenCalled();
  });

  it('ignores every message once its frame has left the page', () => {
    const gone = frame.contentWindow;
    frame.remove();

    deliver(READY, { origin: NONE, source: gone });
    deliver(READY, { origin: NONE, source: null });

    expect(received).not.toHaveBeenCalled();
  });

  it('leaves what is not JSON-RPC alone', () => {
    deliver('hello', { origin: NONE, source: frame.contentWindow });
    deliver({ type: 'resize', height: 20 }, { origin: NONE, source: frame.contentWindow });
    deliver(null, { origin: NONE, source: frame.contentWindow });

    expect(received).not.toHaveBeenCalled();
    expect(failed).not.toHaveBeenCalled();
  });

  it('reports a message that says JSON-RPC and is not valid', () => {
    deliver({ jsonrpc: '2.0', id: 1 }, { origin: NONE, source: frame.contentWindow });

    expect(received).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledTimes(1);
    expect(failed.mock.calls[0][0]).toBeInstanceOf(Error);
  });

  it('passes on the refusal of a proxy page that has the chat\'s origin, and nothing else it says', () => {
    // A page served without its policy has the origin of the chat. It runs nothing and says why.
    deliver(REFUSED, { origin: window.location.origin, source: frame.contentWindow });

    expect(received).toHaveBeenCalledTimes(1);
    expect(received).toHaveBeenCalledWith(REFUSED);

    deliver(READY, { origin: window.location.origin, source: frame.contentWindow });
    deliver({ ...REFUSED, id: 7 }, { origin: window.location.origin, source: frame.contentWindow });
    expect(received).toHaveBeenCalledTimes(1);
  });

  it('takes no refusal from another window, nor from a page of another origin in its frame', () => {
    deliver(REFUSED, { origin: window.location.origin, source: other.contentWindow });
    deliver(REFUSED, { origin: 'https://evil.example.com', source: frame.contentWindow });

    expect(received).not.toHaveBeenCalled();
  });

  it('asks the proxy page whether the person has just pressed in the view, and hears the answer apart from the view\'s messages', () => {
    const post = vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(() => {});
    const pressed = vi.fn();
    transport.onpressed = pressed;

    transport.askPressed();
    expect(post).toHaveBeenCalledWith({ jsonrpc: '2.0', method: 'ui/notifications/sandbox-pressed' }, '*');

    deliver({ jsonrpc: '2.0', method: 'ui/notifications/sandbox-pressed', params: { pressed: true } }, { origin: NONE, source: frame.contentWindow });
    deliver({ jsonrpc: '2.0', method: 'ui/notifications/sandbox-pressed', params: { pressed: false } }, { origin: NONE, source: frame.contentWindow });
    deliver({ jsonrpc: '2.0', method: 'ui/notifications/sandbox-pressed', params: { pressed: null } }, { origin: NONE, source: frame.contentWindow });

    expect(pressed.mock.calls).toEqual([[true], [false], ['unknown']]);
    // The view's protocol hears nothing of it.
    expect(received).not.toHaveBeenCalled();
    expect(failed).not.toHaveBeenCalled();
  });

  it('takes the word of a press from its own frame only, while it holds a document without an origin', () => {
    const pressed = vi.fn();
    transport.onpressed = pressed;
    const yes = { jsonrpc: '2.0', method: 'ui/notifications/sandbox-pressed', params: { pressed: true } };

    // A view that posts to the page itself, the frame of another view, a document with an origin in the frame.
    deliver(yes, { origin: NONE, source: other.contentWindow });
    deliver(yes, { origin: NONE, source: window });
    deliver(yes, { origin: window.location.origin, source: frame.contentWindow });

    expect(pressed).not.toHaveBeenCalled();
    expect(received).not.toHaveBeenCalled();
  });

  it('asks nothing of a frame that has left the page', () => {
    frame.remove();

    expect(() => transport.askPressed()).not.toThrow();
  });

  it('sends to the window of its frame, whatever document it holds: one without an origin has no name', async () => {
    const post = vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(() => {});
    const message = { jsonrpc: '2.0' as const, method: 'ui/notifications/tool-input', params: { arguments: {} } };

    await transport.send(message);

    expect(post).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith(message, '*');
  });

  it('refuses to send once its frame has left the page', async () => {
    frame.remove();

    await expect(transport.send({ jsonrpc: '2.0', method: 'ping' })).rejects.toThrow(/gone/);
  });

  it('stops listening when it closes, and says so once', async () => {
    const closed = vi.fn();
    transport.onclose = closed;

    await transport.close();
    await transport.close();
    deliver(READY, { origin: NONE, source: frame.contentWindow });

    expect(closed).toHaveBeenCalledTimes(1);
    expect(received).not.toHaveBeenCalled();
  });

  it('listens once, however often it is started', async () => {
    await transport.start();

    deliver(READY, { origin: NONE, source: frame.contentWindow });

    expect(received).toHaveBeenCalledTimes(1);
  });
});
