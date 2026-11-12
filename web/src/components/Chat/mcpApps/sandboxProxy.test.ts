import { describe, expect, it, vi } from 'vitest';
import page from '../../../../public/mcp-app-sandbox.html?raw';

/**
 * The sandbox proxy document (`web/public/mcp-app-sandbox.html`), as it is
 * committed: its one inline script, run here against a window and a document
 * that the test makes. The file is static and built by nothing, so no other
 * test would notice a change to what it checks before it takes the HTML of a
 * view, or to how it frames that HTML.
 *
 * The page is served from the chat's own address with a policy that takes
 * its origin away. So the address it reads (`location.origin`) is the
 * chat's, and its own origin (`window.origin`) is "null".
 */

const SCRIPT = /<script>([\s\S]*?)<\/script>/.exec(page)![1];

/** The address of the chat, which is also where the page was loaded from. */
const CHAT = 'https://nerve.example:8900';
const READY = 'ui/notifications/sandbox-proxy-ready';
const REFUSED = 'ui/notifications/sandbox-proxy-refused';
const RESOURCE_READY = 'ui/notifications/sandbox-resource-ready';
const PRESSED = 'ui/notifications/sandbox-pressed';

interface FakeFrame {
  attributes: Record<string, string>;
  srcdoc?: string;
  contentWindow: { postMessage: ReturnType<typeof vi.fn> };
  remove: ReturnType<typeof vi.fn>;
}

interface Options {
  /** Opened as a page of its own, not in a frame. */
  top?: boolean;
  /** What `window.origin` says: 'null' when the policy arrived, the chat's origin when it did not. */
  origin?: string;
  /** A script of the page can reach into the page that embeds it. */
  parentReachable?: boolean;
  /** `location.ancestorOrigins`; null where the browser has none. */
  ancestors?: string[] | null;
  referrer?: string;
  /**
   * What the browser counts for this document: a press of the person's in
   * it or in the content just done (true), none (false), or no such word at
   * all (null: a browser without `navigator.userActivation`).
   */
  pressed?: boolean | null;
  /** A browser whose `postMessage` takes a target origin and no options. */
  noOptions?: boolean;
}

/** Load the proxy document's script into a world of its own. */
function load(options: Options = {}) {
  const notice = { textContent: '', hidden: true };
  const frames: FakeFrame[] = [];
  const listeners: Array<(event: unknown) => void> = [];

  const parent = {
    postMessage: vi.fn((_message: unknown, target: unknown) => {
      if (options.noOptions && typeof target !== 'string') throw new SyntaxError('Invalid target origin');
    }),
    get document(): unknown {
      if (options.parentReachable) return {};
      throw new DOMException('Blocked a frame from accessing a cross-origin frame.', 'SecurityError');
    },
  };
  const document = {
    referrer: options.referrer ?? '',
    getElementById: (id: string) => (id === 'notice' ? notice : null),
    createElement: (): FakeFrame => {
      const frame: FakeFrame = {
        attributes: {},
        contentWindow: { postMessage: vi.fn() },
        remove: vi.fn(),
      };
      Object.assign(frame, {
        setAttribute: (name: string, value: string) => {
          frame.attributes[name] = value;
        },
      });
      return frame;
    },
    body: { appendChild: (frame: FakeFrame) => frames.push(frame) },
  };
  const window: Record<string, unknown> = {
    location: {
      origin: CHAT, href: `${CHAT}/mcp-app-sandbox.html?csp=%7B%7D`, protocol: 'https:', hostname: 'nerve.example',
      ancestorOrigins: options.ancestors === null ? undefined : (options.ancestors ?? [CHAT]),
    },
    origin: options.origin ?? 'null',
    parent,
    navigator: options.pressed === null ? {} : { userActivation: { isActive: options.pressed ?? false } },
    addEventListener: (type: string, listener: (event: unknown) => void) => {
      if (type === 'message') listeners.push(listener);
    },
  };
  window.self = window;
  window.top = options.top ? window : parent;

  // The page asks the gateway for nothing: a request would fail the script here.
  const fetch = vi.fn(() => {
    throw new Error('the sandbox page made a request');
  });
  new Function('window', 'document', 'fetch', SCRIPT)(window, document, fetch);

  return {
    notice, frames, parent, fetch, window,
    /** What the page posted to the page that embeds it: [message, target origin]. */
    posted: () => parent.postMessage.mock.calls as unknown as Array<[{ method?: string; params?: { reason?: string } }, string]>,
    /** Deliver a message event to the page. */
    message(data: unknown, from: { source?: unknown; origin?: string } = {}) {
      const event = { data, source: 'source' in from ? from.source : parent, origin: from.origin ?? CHAT };
      for (const listener of listeners) listener(event);
    },
    listening: () => listeners.length > 0,
  };
}

const refusal = (world: ReturnType<typeof load>) => world.posted().find(([message]) => message.method === REFUSED);

describe('where the sandbox proxy document works', () => {
  it('does nothing as a page of its own, and says what it is', () => {
    const world = load({ top: true });

    expect(world.notice).toEqual({
      hidden: false,
      textContent: 'Nerve app sandbox. This page works only inside a Nerve chat. You can close this tab.',
    });
    expect(world.posted()).toEqual([]);
    expect(world.listening()).toBe(false);
  });

  it('says it is ready to the chat of its own address, and to nobody else', () => {
    const world = load();

    expect(world.posted()).toEqual([[{ jsonrpc: '2.0', method: READY, params: {} }, CHAT]]);
    expect(world.notice.hidden).toBe(true);
    expect(world.listening()).toBe(true);
  });

  it('asks the gateway for nothing: one load of the page is all a view costs', () => {
    const world = load();
    world.message({ jsonrpc: '2.0', method: RESOURCE_READY, params: { html: '<p>view</p>' } });

    expect(world.fetch).not.toHaveBeenCalled();
    expect(SCRIPT).not.toMatch(/\bfetch\s*\(|XMLHttpRequest|sendBeacon|new\s+Image|\.src\s*=/);
  });

  it.each<[string, Options, string]>([
    // The policy that takes the origin away is the policy that limits what a view may load.
    ['with the origin of the chat: the gateway\'s policy did not arrive', { origin: CHAT }, 'this page was served without its sandbox policy.'],
    ['with any origin at all', { origin: 'https://other.example' }, 'this page was served without its sandbox policy.'],
    ['with an origin that only looks like none', { origin: 'null ' }, 'this page was served without its sandbox policy.'],
    ['where it can reach the page that embeds it', { parentReachable: true }, 'the embedding page is within reach of this one.'],
    ['embedded by a page of another host', { ancestors: ['https://other.example:8900'] }, 'the embedding page is not the chat of this address.'],
    ['embedded by a page of another scheme', { ancestors: ['http://nerve.example:8900'] }, 'the embedding page is not the chat of this address.'],
    ['embedded by a page of another port of the host', { ancestors: ['https://nerve.example:8901'] }, 'the embedding page is not the chat of this address.'],
    ['embedded by a page without an origin, as a view or this page itself is', { ancestors: ['null', CHAT] }, 'the embedding page is not the chat of this address.'],
    ['embedded by a page it knows only by a referrer of another host', { ancestors: null, referrer: 'https://other.example/chat' }, 'the embedding page is not the chat of this address.'],
  ])('refuses %s', (_where, options, reason) => {
    const world = load(options);

    expect(refusal(world)).toEqual([{ jsonrpc: '2.0', method: REFUSED, params: { reason } }, '*']);
    expect(world.notice.textContent).toBe(`Nerve app sandbox: refused, because ${reason}`);
    expect(world.posted().some(([message]) => message.method === READY)).toBe(false);
    // It takes no HTML, from anybody.
    expect(world.listening()).toBe(false);
    world.message({ jsonrpc: '2.0', method: RESOURCE_READY, params: { html: '<p>no</p>' } });
    expect(world.frames).toHaveLength(0);
  });

  it('checks that it has no origin before anything else it could be asked', () => {
    // Everything else is as it should be: in a frame of the chat, out of reach.
    const world = load({ origin: CHAT });
    expect(refusal(world)![0].params!.reason).toBe('this page was served without its sandbox policy.');
    // And with nothing else right, the same answer comes first.
    const worse = load({ origin: CHAT, parentReachable: true, ancestors: ['https://other.example'] });
    expect(refusal(worse)![0].params!.reason).toBe('this page was served without its sandbox policy.');
  });

  it.each<[string, Options]>([
    ['the browser names the chat as the page that embeds it', { ancestors: [CHAT] }],
    ['the browser names nobody, and the referrer is the chat\'s origin', { ancestors: null, referrer: `${CHAT}/` }],
    ['the browser names nobody at all', { ancestors: null, referrer: '' }],
  ])('works where %s', (_where, options) => {
    const world = load(options);

    expect(refusal(world)).toBeUndefined();
    // Ready goes to the chat's origin whether or not the browser said who embeds the page.
    expect(world.posted()).toEqual([[{ jsonrpc: '2.0', method: READY, params: {} }, CHAT]]);
  });
});

describe('the frame the sandbox proxy makes for a view', () => {
  const HTML = '<!doctype html><title>view</title>';
  const ready = (params: Record<string, unknown>) => ({ jsonrpc: '2.0', method: RESOURCE_READY, params });

  it('holds the HTML as srcdoc, in a frame without an origin', () => {
    const world = load();

    world.message(ready({ html: HTML }));

    expect(world.frames).toHaveLength(1);
    expect(world.frames[0].srcdoc).toBe(HTML);
    expect(world.frames[0].attributes).toEqual({ sandbox: 'allow-scripts allow-forms' });
  });

  it.each([
    ['allow-scripts allow-same-origin allow-forms', 'allow-scripts allow-forms'],
    ['allow-scripts ALLOW-SAME-ORIGIN', 'allow-scripts'],
    ['allow-same-origin', ''],
    ['allow-scripts allow-forms', 'allow-scripts allow-forms'],
    ['allow-scripts', 'allow-scripts'],
    // More than this page may do itself is not passed on either.
    ['allow-scripts allow-top-navigation allow-popups allow-popups-to-escape-sandbox', 'allow-scripts'],
    ['allow-forms allow-modals allow-downloads allow-top-navigation-by-user-activation', 'allow-forms'],
    ['ALLOW-SCRIPTS Allow-Forms', 'ALLOW-SCRIPTS Allow-Forms'],
    ['', ''],
  ])('gives it scripts and forms at most, whatever it is asked for (%s)', (asked, given) => {
    const world = load();

    world.message(ready({ html: HTML, sandbox: asked }));

    expect(world.frames[0].attributes.sandbox).toBe(given);
  });

  it('passes on the browser features it is told to', () => {
    const world = load();

    world.message(ready({ html: HTML, permissions: { clipboardWrite: {}, camera: {} } }));

    expect(world.frames[0].attributes.allow).toBe('camera; clipboard-write');
  });

  it('takes HTML from the chat only: the window that embeds it, at the address it came from', () => {
    const world = load();

    // Another window, though it says the chat's origin.
    world.message(ready({ html: HTML }), { source: {} });
    // The window that embeds it, with another origin.
    world.message(ready({ html: HTML }), { origin: 'https://other.example:8900' });
    world.message(ready({ html: HTML }), { origin: 'https://nerve.example:8901' });
    world.message(ready({ html: HTML }), { origin: 'http://nerve.example:8900' });
    // A page without an origin: a view, or a frame that a view made.
    world.message(ready({ html: HTML }), { origin: 'null' });
    world.message(ready({ html: HTML }), { source: {}, origin: 'null' });
    world.message(ready({ html: 42 }));

    expect(world.frames).toHaveLength(0);
  });

  it('puts a new document into a new frame, and removes the old one', () => {
    const world = load();

    world.message(ready({ html: HTML }));
    world.message(ready({ html: '<p>second</p>' }));

    expect(world.frames).toHaveLength(2);
    expect(world.frames[0].remove).toHaveBeenCalledTimes(1);
    expect(world.frames[1].srcdoc).toBe('<p>second</p>');
  });
});

describe('what the sandbox proxy passes on', () => {
  const HTML = '<!doctype html><title>view</title>';

  function withView() {
    const world = load();
    world.message({ jsonrpc: '2.0', method: RESOURCE_READY, params: { html: HTML } });
    world.parent.postMessage.mockClear();
    return { world, view: world.frames[0].contentWindow };
  }

  it('passes what the chat says to the view, which has no origin to address', () => {
    const { world, view } = withView();
    const message = { jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: { arguments: {} } };

    world.message(message);

    expect(view.postMessage.mock.calls).toEqual([[message, '*']]);
  });

  it('passes what the view says to the chat, addressed to the chat', () => {
    const { world, view } = withView();
    const message = { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'get-time' } };

    world.message(message, { source: view, origin: 'null' });

    expect(world.posted()).toEqual([[message, CHAT]]);
  });

  it('drops what claims to come from the view with an origin: the frame it made has none', () => {
    const { world, view } = withView();

    world.message({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: {} }, { source: view, origin: CHAT });
    world.message({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: {} }, { source: view, origin: 'https://other.example' });

    expect(world.posted()).toEqual([]);
  });

  it('drops a message from any other window, though every sandboxed frame says the same origin', () => {
    const { world, view } = withView();

    // A frame the view made, or a second view of the page: no origin either, another window.
    world.message({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: {} }, { source: {}, origin: 'null' });
    world.message({ jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: {} }, { source: {}, origin: 'null' });
    world.message({ jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: {} }, { source: {}, origin: CHAT });
    // The view itself speaking as the chat: it is not the window that embeds this page.
    world.message({ jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: {} }, { source: view, origin: CHAT });

    expect(world.posted()).toEqual([]);
    expect(view.postMessage).not.toHaveBeenCalled();
  });

  it('keeps its own messages to itself: a view cannot speak as the proxy, and the chat cannot pass one on', () => {
    const { world, view } = withView();

    world.message({ jsonrpc: '2.0', method: READY, params: {} }, { source: view, origin: 'null' });
    world.message({ jsonrpc: '2.0', method: REFUSED, params: { reason: 'x' } }, { source: view, origin: 'null' });
    world.message({ jsonrpc: '2.0', method: 'ui/notifications/sandbox-anything', params: {} });

    expect(world.posted()).toEqual([]);
    expect(view.postMessage).not.toHaveBeenCalled();
  });
});

describe('what the sandbox proxy says of a press', () => {
  const HTML = '<!doctype html><title>view</title>';
  const QUESTION = { jsonrpc: '2.0', method: PRESSED };
  const WITH_THE_BROWSERS_WORD = { targetOrigin: CHAT, includeUserActivation: true };

  function withView(options: Options = {}) {
    const world = load(options);
    world.message({ jsonrpc: '2.0', method: RESOURCE_READY, params: { html: HTML } });
    world.parent.postMessage.mockClear();
    return { world, view: world.frames[0].contentWindow };
  }

  it.each([[true], [false]])('answers the chat with what the browser counts for this document (%s), and asks the browser to say so itself', (pressed) => {
    const { world } = withView({ pressed });

    world.message(QUESTION);

    expect(world.parent.postMessage.mock.calls).toEqual([
      [{ jsonrpc: '2.0', method: PRESSED, params: { pressed } }, WITH_THE_BROWSERS_WORD],
    ]);
  });

  it('reads the browser anew for every question: a press is counted for a few seconds', () => {
    const { world } = withView({ pressed: false });

    world.message(QUESTION);
    (world.window.navigator as { userActivation: { isActive: boolean } }).userActivation.isActive = true;
    world.message(QUESTION);

    expect(world.parent.postMessage.mock.calls.map(([message]) => (message as { params: unknown }).params))
      .toEqual([{ pressed: false }, { pressed: true }]);
  });

  it('says that it cannot tell where the browser has no such word', () => {
    const { world } = withView({ pressed: null });

    world.message(QUESTION);

    expect(world.parent.postMessage.mock.calls).toEqual([
      [{ jsonrpc: '2.0', method: PRESSED, params: { pressed: null } }, WITH_THE_BROWSERS_WORD],
    ]);
  });

  it('answers to the chat alone where the browser takes no options for a message', () => {
    const { world } = withView({ pressed: true, noOptions: true });

    world.message(QUESTION);

    expect(world.parent.postMessage.mock.calls.at(-1)).toEqual([
      { jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, CHAT,
    ]);
  });

  it('answers before it has a view too: the frame can take the keyboard from its first moment', () => {
    const world = load({ pressed: false });
    world.parent.postMessage.mockClear();

    world.message(QUESTION);

    expect(world.parent.postMessage.mock.calls).toEqual([
      [{ jsonrpc: '2.0', method: PRESSED, params: { pressed: false } }, WITH_THE_BROWSERS_WORD],
    ]);
  });

  it('keeps the question from the view: the view is not asked, and not told that the chat asks', () => {
    const { world, view } = withView({ pressed: true });

    world.message(QUESTION);

    expect(view.postMessage).not.toHaveBeenCalled();
  });

  it('answers the chat only: the view, or any other window, gets no answer and no say', () => {
    const { world, view } = withView({ pressed: true });

    // The view asks, and the view answers for the page: neither is passed on.
    world.message(QUESTION, { source: view, origin: 'null' });
    world.message({ jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, { source: view, origin: 'null' });
    // Another window, and the page that embeds this one under another origin.
    world.message(QUESTION, { source: {}, origin: 'null' });
    world.message(QUESTION, { source: {}, origin: CHAT });
    world.message(QUESTION, { origin: 'https://other.example:8900' });

    expect(world.parent.postMessage).not.toHaveBeenCalled();
    expect(view.postMessage).not.toHaveBeenCalled();
  });
});
