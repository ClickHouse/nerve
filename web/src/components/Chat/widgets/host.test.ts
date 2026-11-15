import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/client', () => ({
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));

/** Called each time the host reads the chat's colours for a widget, when a test sets it. */
const styleReads = vi.hoisted(() => ({ watch: null as (() => void) | null }));
vi.mock('../mcpApps/hostStyles', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../mcpApps/hostStyles')>();
  return {
    ...actual,
    hostStyleVariables: (...args: Parameters<typeof actual.hostStyleVariables>) => {
      styleReads.watch?.();
      return actual.hostStyleVariables(...args);
    },
  };
});

import { api } from '../../../api/client';
import { PROOF_WAIT_MS } from '../mcpApps/keyboard';
import { MAX_FRAME_HEIGHT, MIN_FRAME_HEIGHT } from '../mcpApps/policy';
import {
  DEFAULT_WIDGET_HEIGHT, MAX_MESSAGES_PER_SECOND, REVEAL_MS, SANDBOX_WAIT_MS, SIZE_INTERVAL_MS, WidgetHost,
  readWidgetMessage, type WidgetSnapshot,
} from './host';
import { MAX_WIDGET_DATA_CHARS, type WidgetItems } from './storage';
import { HOST_CONTEXT_CHANGED, SIZE_CHANGED, STORAGE_CHANGED, WIDGET_POLICY, widgetHead } from './wrap';

/**
 * The host of a widget, run against a fake frame: a real `<iframe>` of jsdom
 * that loads nothing. The test plays the two parties behind it — the sandbox
 * proxy and the widget — by delivering their messages to the page, and reads
 * what the host posts to the frame.
 */

/**
 * What a message from the frame says its origin is: the sandbox proxy is a
 * page of the chat's own address, served without an origin. Every sandboxed
 * frame says "null", so the frame is known by its window.
 */
const NONE = 'null';
const SOURCE = '<button id="b">Hi</button><script>window.ran = true</script>';
const PROXY_READY = 'ui/notifications/sandbox-proxy-ready';
const PROXY_REFUSED = 'ui/notifications/sandbox-proxy-refused';
const RESOURCE_READY = 'ui/notifications/sandbox-resource-ready';

type Rpc = Record<string, unknown> & { method?: string; params?: Record<string, unknown> };

function setup(overrides: Partial<ConstructorParameters<typeof WidgetHost>[0]> = {}) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const snapshots: WidgetSnapshot[] = [];
  const heights: number[] = [];
  const stored: WidgetItems[] = [];
  const host = new WidgetHost({
    source: SOURCE, theme: 'dark',
    onChange: snapshot => snapshots.push(snapshot),
    onHeight: height => heights.push(height),
    onStorage: items => stored.push(items),
    ...overrides,
  });
  host.start(container);
  const frame = container.querySelector('iframe')!;
  const posted: Array<{ message: Rpc; targetOrigin: string }> = [];
  vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(((message: Rpc, targetOrigin: string) => {
    posted.push({ message, targetOrigin });
  }) as never);
  const say = (data: unknown, from: { origin?: string; source?: MessageEventSource | null } = {}) => {
    window.dispatchEvent(new MessageEvent('message', {
      data, origin: from.origin ?? NONE, source: 'source' in from ? from.source : frame.contentWindow,
    }));
  };
  const notify = (method: string, params?: Record<string, unknown>) => say({ jsonrpc: '2.0', method, ...(params ? { params } : {}) });
  return {
    host, container, frame, snapshots, heights, stored, posted, say, notify,
    proxyReady: () => notify(PROXY_READY, {}),
    /** The widget says what its `localStorage` holds. */
    saves: (items: unknown) => notify(STORAGE_CHANGED, { items }),
    size: (height: unknown) => notify(SIZE_CHANGED, { width: 600, height }),
    sent: (method: string) => posted.filter(p => p.message.method === method),
    status: () => host.getSnapshot().status,
  };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  styleReads.watch = null;
  document.body.innerHTML = '';
});

describe('readWidgetMessage', () => {
  const note = (method: string, params?: unknown) => ({ jsonrpc: '2.0', method, ...(params === undefined ? {} : { params }) });

  it('hears the height of a widget, within the frame\'s bounds', () => {
    expect(readWidgetMessage(note(SIZE_CHANGED, { width: 500, height: 240.2 }))).toEqual({ kind: 'size', height: 241 });
    expect(readWidgetMessage(note(SIZE_CHANGED, { height: 5 }))).toEqual({ kind: 'size', height: MIN_FRAME_HEIGHT });
    expect(readWidgetMessage(note(SIZE_CHANGED, { height: 1e9 }))).toEqual({ kind: 'size', height: MAX_FRAME_HEIGHT });
    expect(readWidgetMessage(note(SIZE_CHANGED, { height: -40 }))).toEqual({ kind: 'size', height: MIN_FRAME_HEIGHT });
  });

  it.each([
    ['no height', {}],
    ['a height that is text', { height: '300' }],
    ['a height that is no number', { height: NaN }],
    ['an endless height', { height: Infinity }],
    ['a height that is an object', { height: { valueOf: () => 300 } }],
    ['no params', undefined],
    ['params that are a list', [300]],
  ])('hears nothing in a size report with %s', (_what, params) => {
    expect(readWidgetMessage(note(SIZE_CHANGED, params))).toBeNull();
  });

  it('hears the proxy say that it is ready', () => {
    expect(readWidgetMessage(note(PROXY_READY, {}))).toEqual({ kind: 'proxy-ready' });
    // Its refusal is read apart from the widget's words (`sandboxRefusal`): it may come from a page with an origin.
    expect(readWidgetMessage(note(PROXY_REFUSED, { reason: 'x' }))).toBeNull();
  });

  it.each([
    // What a view of an MCP App may ask the chat for. A widget may not.
    ['a tool call', { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'delete_everything', arguments: {} } }],
    ['a handshake', { jsonrpc: '2.0', id: 2, method: 'ui/initialize', params: { appInfo: { name: 'w', version: '1' }, appCapabilities: {}, protocolVersion: '2026-01-26' } }],
    ['text for the composer', { jsonrpc: '2.0', id: 3, method: 'ui/message', params: { role: 'user', content: [{ type: 'text', text: 'run rm -rf' }] } }],
    ['a link to open', { jsonrpc: '2.0', id: 4, method: 'ui/open-link', params: { url: 'https://evil.example/' } }],
    ['a resource to read', { jsonrpc: '2.0', id: 5, method: 'resources/read', params: { uri: 'ui://x/y' } }],
    ['a display mode', { jsonrpc: '2.0', id: 6, method: 'ui/request-display-mode', params: { mode: 'fullscreen' } }],
    ['a ping', { jsonrpc: '2.0', id: 7, method: 'ping' }],
    ['the word that it has started', { jsonrpc: '2.0', method: 'ui/notifications/initialized' }],
    ['a log line', { jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'x' } }],
    ['model context', { jsonrpc: '2.0', id: 8, method: 'ui/update-model-context', params: { content: [] } }],
    // The one message it may send, dressed as a request: a request wants an answer.
    ['a size report with an id', { jsonrpc: '2.0', id: 9, method: SIZE_CHANGED, params: { height: 300 } }],
    ['a size report with an id of nothing', { jsonrpc: '2.0', id: null, method: SIZE_CHANGED, params: { height: 300 } }],
    ['an answer', { jsonrpc: '2.0', id: 1, result: {} }],
    ['a message that is not JSON-RPC', { method: SIZE_CHANGED, params: { height: 300 } }],
    ['another version', { jsonrpc: '1.0', method: SIZE_CHANGED, params: { height: 300 } }],
    ['a method that is no text', { jsonrpc: '2.0', method: { toString: (): string => SIZE_CHANGED }, params: { height: 300 } }],
    ['text', SIZE_CHANGED],
    ['a list', [{ jsonrpc: '2.0', method: SIZE_CHANGED, params: { height: 300 } }]],
    ['nothing', null],
    ['a number', 300],
  ])('hears nothing in %s', (_what, data) => {
    expect(readWidgetMessage(data)).toBeNull();
  });
});

describe('the frame', () => {
  it('is the sandbox proxy at the chat\'s own address, with nothing else in its address', () => {
    const w = setup();
    // The path alone, of the chat's own address.
    expect(w.frame.getAttribute('src')).toBe('/mcp-app-sandbox.html');
    // No `sandbox` of the chat's: the gateway's header takes the page's
    // origin away, and the page runs only when it has none.
    expect(w.frame.hasAttribute('sandbox')).toBe(false);
    expect(w.frame.getAttribute('referrerpolicy')).toBe('origin');
    // No feature of the browser is passed on: no clipboard, no camera.
    expect(w.frame.hasAttribute('allow')).toBe(false);
    expect(w.frame.style.colorScheme).toBe('dark');
    expect(w.frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
    expect(w.status()).toBe('loading');
  });

  it('starts at the height the widget had before, within bounds', () => {
    expect(setup({ height: 412 }).frame.style.height).toBe('412px');
    expect(setup({ height: 99999 }).frame.style.height).toBe(`${MAX_FRAME_HEIGHT}px`);
    expect(setup({ height: Number.NaN }).frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
  });

  it('asks the gateway for nothing', () => {
    const w = setup();
    w.proxyReady();
    w.size(300);
    vi.advanceTimersByTime(60_000);
    expect(Object.values(api as unknown as Record<string, ReturnType<typeof vi.fn>>).flatMap(fn => fn.mock.calls)).toEqual([]);
  });

  it('opens once', () => {
    const w = setup();
    w.host.start(w.container);
    expect(w.container.querySelectorAll('iframe')).toHaveLength(1);
  });
});

describe('the document', () => {
  it('goes to the proxy when it says it is ready, for a frame that only runs scripts', () => {
    const w = setup();
    expect(w.posted).toEqual([]);
    w.proxyReady();
    expect(w.posted).toHaveLength(1);
    const [{ message, targetOrigin }] = w.posted;
    // A document without an origin cannot be named: the frame is addressed as it is.
    expect(targetOrigin).toBe('*');
    expect(message.jsonrpc).toBe('2.0');
    expect(message.method).toBe(RESOURCE_READY);
    expect(Object.keys(message.params!).sort()).toEqual(['html', 'sandbox']);
    expect(message.params!.sandbox).toBe('allow-scripts');
    const html = message.params!.html as string;
    // Nerve's head, then the source as it is.
    expect(html.startsWith('<!doctype html><meta charset="utf-8">')).toBe(true);
    expect(html).toContain(`content="${WIDGET_POLICY}"`);
    expect(html).toContain(':root{color-scheme:dark;');
    expect(html).toContain('--color-text-primary:light-dark(');
    expect(html.endsWith(SOURCE)).toBe(true);
  });

  it('carries the theme the chat has when the proxy is ready, not the one it started with', () => {
    const w = setup();
    w.host.setTheme('light');
    vi.advanceTimersByTime(10);
    // Nothing to tell yet: there is no document.
    expect(w.posted).toEqual([]);
    w.proxyReady();
    expect(w.posted[0].message.params!.html).toContain(':root{color-scheme:light;');
    expect(w.frame.style.colorScheme).toBe('light');
  });

  it('is handed over once: a frame that says it is ready again has failed', () => {
    const w = setup();
    w.proxyReady();
    w.size(300);
    expect(w.status()).toBe('shown');
    w.proxyReady();
    expect(w.sent(RESOURCE_READY)).toHaveLength(1);
    expect(w.host.getSnapshot()).toEqual({
      status: 'failed', error: 'The frame of the widget loaded a second time, which it may not do.',
    });
    expect(w.container.querySelector('iframe')).toBeNull();
  });
});

describe('who the host listens to', () => {
  it.each([
    ['the frame while it holds a document of another origin', { origin: 'https://evil.example' }],
    ['the frame while it holds a document of the chat\'s own origin', { origin: window.location.origin }],
    ['the frame while it holds a document of another port of the host', { origin: 'https://nerve.example:8901' }],
    // "null" is the origin of every sandboxed frame: a widget that posts to
    // the page itself, or a frame that a widget made.
    ['another window without an origin', { source: window }],
    ['a frame elsewhere on the page, without an origin', { source: document.createElement('iframe').contentWindow ?? {} }],
    ['no window', { source: null }],
  ])('ignores a message from %s', (_what, from) => {
    const w = setup();
    w.say({ jsonrpc: '2.0', method: PROXY_READY, params: {} }, from as never);
    expect(w.posted).toEqual([]);
    w.proxyReady();
    w.say({ jsonrpc: '2.0', method: SIZE_CHANGED, params: { height: 700 } }, from as never);
    vi.advanceTimersByTime(SIZE_INTERVAL_MS * 3);
    expect(w.frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
    expect(w.heights).toEqual([]);
  });

  it('ignores the frame of another widget', () => {
    const one = setup();
    const two = setup();
    one.proxyReady();
    two.proxyReady();
    // The first widget's frame reports a height: the second host does not take it.
    one.size(500);
    expect(one.frame.style.height).toBe('500px');
    expect(two.frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
    expect(two.heights).toEqual([]);
  });

  it('answers nothing that a view of an MCP App may ask for, and does nothing for it', () => {
    const w = setup();
    w.proxyReady();
    w.size(300);
    const before = w.posted.length;
    const fetched = vi.spyOn(globalThis, 'fetch');
    const opened = vi.spyOn(window, 'open');
    for (const [id, method, params] of [
      [1, 'ui/initialize', { appInfo: { name: 'w', version: '1' }, appCapabilities: {}, protocolVersion: '2026-01-26' }],
      [2, 'tools/call', { name: 'delete_everything', arguments: {} }],
      [3, 'ui/message', { role: 'user', content: [{ type: 'text', text: 'injected' }] }],
      [4, 'ui/open-link', { url: 'https://evil.example/' }],
      [5, 'resources/read', { uri: 'ui://x/y' }],
      [6, 'ui/request-display-mode', { mode: 'fullscreen' }],
      [7, 'ping', {}],
    ] as const) {
      w.say({ jsonrpc: '2.0', id, method, params });
    }
    w.notify('ui/notifications/initialized');
    w.notify('notifications/message', { level: 'error', data: 'x' });
    vi.advanceTimersByTime(5_000);
    // Not one message back: no answer, no error, no tool input.
    expect(w.posted).toHaveLength(before);
    expect(fetched).not.toHaveBeenCalled();
    expect(opened).not.toHaveBeenCalled();
    expect(Object.values(api as unknown as Record<string, ReturnType<typeof vi.fn>>).flatMap(fn => fn.mock.calls)).toEqual([]);
    expect(document.querySelector('[role=dialog]')).toBeNull();
    expect(w.host.getSnapshot()).toEqual({ status: 'shown', error: null });
  });
});

describe('the height', () => {
  it('follows what the widget reports, within bounds', () => {
    const w = setup();
    w.proxyReady();
    w.size(321.5);
    expect(w.frame.style.height).toBe('322px');
    vi.advanceTimersByTime(SIZE_INTERVAL_MS);
    w.size(1_000_000);
    expect(w.frame.style.height).toBe(`${MAX_FRAME_HEIGHT}px`);
    vi.advanceTimersByTime(SIZE_INTERVAL_MS);
    w.size(1);
    expect(w.frame.style.height).toBe(`${MIN_FRAME_HEIGHT}px`);
    expect(w.heights).toEqual([322, MAX_FRAME_HEIGHT, MIN_FRAME_HEIGHT]);
  });

  it('counts for nothing before the widget has its document', () => {
    const w = setup();
    w.size(700);
    vi.advanceTimersByTime(SIZE_INTERVAL_MS * 3);
    expect(w.frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
    expect(w.status()).toBe('loading');
  });

  it('changes at most every SIZE_INTERVAL_MS, and ends at the newest report', () => {
    const w = setup();
    w.proxyReady();
    w.size(200);
    expect(w.frame.style.height).toBe('200px');
    // A widget that reports with every frame of an animation, and more.
    for (let height = 201; height <= 400; height++) w.size(height);
    expect(w.frame.style.height).toBe('200px');
    expect(w.heights).toEqual([200]);
    vi.advanceTimersByTime(SIZE_INTERVAL_MS - 1);
    expect(w.frame.style.height).toBe('200px');
    vi.advanceTimersByTime(1);
    expect(w.frame.style.height).toBe('400px');
    expect(w.heights).toEqual([200, 400]);
  });

  it('moves the frame ten times a second at most, however often the widget reports', () => {
    const w = setup();
    w.proxyReady();
    let height = 100;
    for (let ms = 0; ms < 1000; ms += 5) {
      w.size(height++ % 2 ? 300 : 400);
      vi.advanceTimersByTime(5);
    }
    expect(w.heights.length).toBeLessThanOrEqual(11);
    expect(w.status()).toBe('shown');
  });

  it('tells nobody about a report that changes nothing', () => {
    const w = setup({ height: 300 });
    w.proxyReady();
    w.size(300);
    expect(w.heights).toEqual([]);
    expect(w.status()).toBe('shown');
  });
});

describe('what the widget saves', () => {
  const TICKS: WidgetItems = [['ticks', '["tent"]'], ['tab', 'gear']];

  it('starts with what it saved before: the data is in the document the proxy gets', () => {
    const w = setup({ saved: TICKS });
    w.proxyReady();
    const html = w.sent(RESOURCE_READY)[0].message.params!.html as string;
    expect(html).toContain('var saved = [["ticks","[\\"tent\\"]"],["tab","gear"]];');
    // In the head, before the source, and nowhere else.
    expect(html.indexOf('var saved = ')).toBeLessThan(html.length - SOURCE.length);
    expect(html.match(/var saved = /g)).toHaveLength(1);
    expect(html.endsWith(SOURCE)).toBe(true);
  });

  it('starts with nothing when it saved nothing', () => {
    const w = setup();
    w.proxyReady();
    expect(w.sent(RESOURCE_READY)[0].message.params!.html).toContain('var saved = [];');
  });

  it('may keep as much as it is told: the room is in the document too', () => {
    const w = setup({ limit: 4321 });
    w.proxyReady();
    expect(w.sent(RESOURCE_READY)[0].message.params!.html).toContain('var room = 4321;');
    const whole = setup();
    whole.proxyReady();
    expect(whole.sent(RESOURCE_READY)[0].message.params!.html).toContain(`var room = ${MAX_WIDGET_DATA_CHARS};`);
  });

  it('is passed on as the widget reports it, checked: a list of its own', () => {
    const w = setup();
    w.proxyReady();
    const sent = [['ticks', '["tent"]'], ['tab', 'gear']];
    w.saves(sent);
    expect(w.stored).toEqual([TICKS]);
    expect(w.stored[0]).not.toBe(sent);
    w.saves([]);
    expect(w.stored).toEqual([TICKS, []]);
  });

  it('counts for nothing before the widget has its document', () => {
    const w = setup();
    w.saves(TICKS);
    expect(w.stored).toEqual([]);
  });

  it.each([
    ['no list', { ticks: '["tent"]' }],
    ['a text', '[["ticks","[]"]]'],
    ['a value that is no text', [['ticks', ['tent']]]],
    ['a key that is no text', [[1, 'x']]],
    ['a key twice', [['a', '1'], ['a', '2']]],
    ['a pair of three', [['a', 'b', 'c']]],
    ['more than a widget may keep', [['a', 'x'.repeat(MAX_WIDGET_DATA_CHARS)]]],
    ['more keys than a widget may have', Array.from({ length: 1025 }, (_, i) => [String(i), ''])],
    ['nothing', undefined],
  ])('is not passed on when the report is %s', (_what, items) => {
    const w = setup();
    w.proxyReady();
    w.saves(items);
    expect(w.stored).toEqual([]);
    expect(w.status()).toBe('loading');
    expect(w.container.querySelector('iframe')).not.toBeNull();
  });

  it('is not passed on from a report dressed as a request, or with another name', () => {
    const w = setup();
    w.proxyReady();
    w.say({ jsonrpc: '2.0', id: 1, method: STORAGE_CHANGED, params: { items: TICKS } });
    w.say({ jsonrpc: '2.0', method: 'ui/notifications/storage', params: { items: TICKS } });
    w.say({ method: STORAGE_CHANGED, params: { items: TICKS } });
    expect(w.stored).toEqual([]);
  });

  it.each([
    ['the frame while it holds a document of another origin', { origin: 'https://evil.example' }],
    ['the frame while it holds a document of the chat\'s own origin', { origin: window.location.origin }],
    ['another window without an origin', { source: window }],
    ['no window', { source: null }],
  ])('is not taken from %s', (_what, from) => {
    const w = setup();
    w.proxyReady();
    w.say({ jsonrpc: '2.0', method: STORAGE_CHANGED, params: { items: TICKS } }, from);
    expect(w.stored).toEqual([]);
  });

  it('is the widget\'s own: the report of one widget does not reach another', () => {
    const one = setup();
    const two = setup();
    one.proxyReady();
    two.proxyReady();
    one.saves(TICKS);
    expect(one.stored).toEqual([TICKS]);
    expect(two.stored).toEqual([]);
  });

  it('gets no answer, and the chat does nothing else for it', () => {
    const w = setup();
    w.proxyReady();
    w.size(200);
    const posted = w.posted.length;
    const changes = w.snapshots.length;
    w.saves(TICKS);
    w.saves([['nerve_token', 'x'], ['__proto__', '{}'], ['</script>', '<!--']]);
    vi.advanceTimersByTime(5_000);
    expect(w.posted).toHaveLength(posted);
    expect(w.snapshots).toHaveLength(changes);
    expect(w.heights).toEqual([200]);
    expect(Object.values(api as unknown as Record<string, ReturnType<typeof vi.fn>>).flatMap(fn => fn.mock.calls)).toEqual([]);
  });

  it('counts like every message of the frame: a widget that reports without end is stopped', () => {
    const w = setup();
    w.proxyReady();
    for (let i = 1; i < MAX_MESSAGES_PER_SECOND; i++) w.saves([['n', String(i)]]);
    expect(w.status()).toBe('loading');
    w.saves([['n', 'one more']]);
    expect(w.host.getSnapshot().error).toBe('The widget was stopped: it sent messages without end.');
    // And what it still sends is not passed on.
    const taken = w.stored.length;
    w.saves(TICKS);
    expect(w.stored).toHaveLength(taken);
  });
});

describe('a frame that floods', () => {
  it('is stopped once it sends more messages in a second than a widget has to say', () => {
    const w = setup();
    w.proxyReady();
    w.size(200);
    // The proxy's word and the first height were two.
    for (let i = 2; i < MAX_MESSAGES_PER_SECOND; i++) w.size(200 + (i % 300));
    expect(w.status()).toBe('shown');
    w.size(555);
    expect(w.host.getSnapshot()).toEqual({
      status: 'failed', error: 'The widget was stopped: it sent messages without end.',
    });
    expect(w.container.querySelector('iframe')).toBeNull();
    // What is still on its way changes nothing: nobody listens any more.
    const changes = w.snapshots.length;
    for (let i = 0; i < 1000; i++) w.size(300);
    vi.advanceTimersByTime(5_000);
    expect(w.snapshots).toHaveLength(changes);
  });

  it('counts every message of the frame, whatever it says', () => {
    const w = setup();
    w.proxyReady();
    for (let i = 0; i < MAX_MESSAGES_PER_SECOND; i++) {
      w.say(i % 2 ? 'noise' : { jsonrpc: '2.0', id: i, method: 'tools/call', params: { name: 'x' } });
    }
    expect(w.status()).toBe('failed');
  });

  it('counts by the second: a widget that reports sixty times a second for a long time runs on', () => {
    const w = setup();
    w.proxyReady();
    for (let frame = 0; frame < 60 * 30; frame++) {
      w.size(100 + (frame % 500));
      vi.advanceTimersByTime(1000 / 60);
    }
    expect(w.status()).toBe('shown');
    expect(w.container.querySelector('iframe')).not.toBeNull();
  });

  it('does not count what other windows send', () => {
    const w = setup();
    w.proxyReady();
    for (let i = 0; i < MAX_MESSAGES_PER_SECOND * 3; i++) {
      w.say({ jsonrpc: '2.0', method: SIZE_CHANGED, params: { height: 300 } }, { origin: 'null', source: window });
    }
    expect(w.status()).toBe('loading');
    expect(w.container.querySelector('iframe')).not.toBeNull();
  });

  /** A window inside the frame: the widget's own (`depth` 1), or a frame the widget made. */
  function windowInside(frame: HTMLIFrameElement, depth = 1): Window {
    let inside = frame;
    for (let level = 0; level < depth; level++) {
      const within = inside.contentDocument!;
      const next = within.createElement('iframe');
      (within.body ?? within.documentElement ?? within).appendChild(next);
      inside = next;
    }
    return inside.contentWindow!;
  }

  it.each([
    ['the widget itself', 1],
    ['a frame the widget made', 2],
    ['a frame deep inside the widget', 5],
  ])('counts what %s says to the page past the proxy, and reads none of it', (_, depth) => {
    const w = setup();
    w.proxyReady();
    const inside = windowInside(w.frame, depth);
    const direct = { jsonrpc: '2.0', method: SIZE_CHANGED, params: { height: 444 } };

    // The proxy's word was one.
    for (let i = 1; i < MAX_MESSAGES_PER_SECOND; i++) w.say(direct, { source: inside });
    vi.advanceTimersByTime(SIZE_INTERVAL_MS);
    expect(w.heights).toEqual([]);
    expect(w.status()).not.toBe('failed');

    w.say(direct, { source: inside });
    expect(w.host.getSnapshot()).toEqual({
      status: 'failed', error: 'The widget was stopped: it sent messages without end.',
    });
    expect(w.container.querySelector('iframe')).toBeNull();
  });

  it('does not count what the frame of another widget says to the page', () => {
    const w = setup();
    w.proxyReady();
    const other = document.createElement('iframe');
    document.body.appendChild(other);
    const inside = windowInside(other);

    for (let i = 0; i < MAX_MESSAGES_PER_SECOND * 3; i++) w.say('noise', { source: inside });

    expect(w.status()).not.toBe('failed');
  });
});

describe('what the block shows', () => {
  it('is loading until the widget reports its first height', () => {
    const w = setup();
    w.proxyReady();
    expect(w.status()).toBe('loading');
    w.size(240);
    expect(w.status()).toBe('shown');
    expect(w.snapshots.map(s => s.status)).toEqual(['shown']);
  });

  it('shows a widget that never reports a height after a moment, as it is', () => {
    const w = setup();
    w.proxyReady();
    vi.advanceTimersByTime(REVEAL_MS - 1);
    expect(w.status()).toBe('loading');
    vi.advanceTimersByTime(1);
    expect(w.status()).toBe('shown');
    expect(w.frame.style.height).toBe(`${DEFAULT_WIDGET_HEIGHT}px`);
  });

  it('fails when the sandbox does not answer', () => {
    const w = setup();
    vi.advanceTimersByTime(SANDBOX_WAIT_MS - 1);
    expect(w.status()).toBe('loading');
    vi.advanceTimersByTime(1);
    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: 'The widget sandbox does not answer.' });
    expect(w.container.querySelector('iframe')).toBeNull();
    // A late word of a frame that is gone changes nothing.
    w.proxyReady();
    expect(w.posted).toEqual([]);
  });

  it('does not fail a widget that got its document in time', () => {
    const w = setup();
    w.proxyReady();
    vi.advanceTimersByTime(SANDBOX_WAIT_MS * 2);
    expect(w.status()).toBe('shown');
  });

  it('fails with the proxy\'s reason when the proxy refuses to start', () => {
    const w = setup();
    w.notify(PROXY_REFUSED, { reason: 'the embedding page is not the chat of this address.' });
    expect(w.host.getSnapshot()).toEqual({
      status: 'failed',
      error: 'The widget sandbox refused to start: the embedding page is not the chat of this address.',
    });
    expect(w.container.querySelector('iframe')).toBeNull();
  });

  it('fails with the reason of a proxy page that was served without its policy, and hands it nothing', () => {
    const w = setup();
    // Without the header the page has the chat's own origin. It runs nothing and says why.
    w.say(
      { jsonrpc: '2.0', method: PROXY_REFUSED, params: { reason: 'this page was served without its sandbox policy.' } },
      { origin: window.location.origin },
    );
    expect(w.host.getSnapshot()).toEqual({
      status: 'failed',
      error: 'The widget sandbox refused to start: this page was served without its sandbox policy.',
    });
    expect(w.container.querySelector('iframe')).toBeNull();
    // Not the widget's document, not what it saved.
    expect(w.posted).toEqual([]);
  });

  it('hands nothing to a frame that says it is ready with the chat\'s origin', () => {
    const w = setup();
    w.say({ jsonrpc: '2.0', method: PROXY_READY, params: {} }, { origin: window.location.origin });
    expect(w.posted).toEqual([]);
    expect(w.status()).toBe('loading');
  });

  it('takes no refusal from another window or another origin', () => {
    const w = setup();
    const refusal = { jsonrpc: '2.0', method: PROXY_REFUSED, params: { reason: 'made up' } };
    w.say(refusal, { source: window });
    w.say(refusal, { source: window, origin: window.location.origin });
    w.say(refusal, { origin: 'https://evil.example' });
    expect(w.status()).toBe('loading');
    expect(w.container.querySelector('iframe')).not.toBeNull();
  });

  it('takes no refusal once the widget has its document', () => {
    const w = setup();
    w.proxyReady();
    w.notify(PROXY_REFUSED, { reason: 'made up' });
    w.size(200);
    expect(w.status()).toBe('shown');
  });
});

describe('the theme', () => {
  it('reaches a widget that runs: on the frame at once, in the document a moment later', () => {
    const w = setup();
    w.proxyReady();
    w.size(200);
    const before = w.posted.length;
    w.host.setTheme('light');
    expect(w.frame.style.colorScheme).toBe('light');
    // Not while the page is between two themes.
    expect(w.posted).toHaveLength(before);
    vi.advanceTimersByTime(0);
    expect(w.posted).toHaveLength(before + 1);
    const { message, targetOrigin } = w.posted.at(-1)!;
    expect(targetOrigin).toBe('*');
    expect(message.method).toBe(HOST_CONTEXT_CHANGED);
    expect('id' in message).toBe(false);
    expect(message.params!.theme).toBe('light');
    const variables = (message.params!.styles as { variables: Record<string, string> }).variables;
    expect(variables['--color-text-primary']).toMatch(/^light-dark\(/);
  });

  it('reads the chat\'s colours after the switch, not during it', () => {
    const w = setup();
    w.proxyReady();
    let reads = 0;
    styleReads.watch = () => { reads += 1; };
    w.host.setTheme('light');
    expect(reads).toBe(0);
    vi.advanceTimersByTime(0);
    expect(reads).toBe(1);
  });

  it('says nothing for the theme the widget has, and tells the last of several', () => {
    const w = setup();
    w.proxyReady();
    const before = w.posted.length;
    w.host.setTheme('dark');
    vi.advanceTimersByTime(10);
    expect(w.posted).toHaveLength(before);
    w.host.setTheme('light');
    w.host.setTheme('dark');
    w.host.setTheme('light');
    vi.advanceTimersByTime(10);
    expect(w.sent(HOST_CONTEXT_CHANGED).map(p => p.message.params!.theme)).toEqual(['light']);
  });
});

describe('the keyboard', () => {
  const PRESSED = 'ui/notifications/sandbox-pressed';
  const STOPPED = 'The widget was stopped: it took the keyboard without a click in it.';
  let active: { mockRestore: () => void } | null = null;
  const veil = (container: HTMLElement) => container.querySelector<HTMLElement>('[data-frame-veil]');
  /** The pointer of the person comes onto the widget. */
  const pointerOn = (container: HTMLElement) => veil(container)!.dispatchEvent(new Event('pointermove', { bubbles: true }));
  /** The frame has the keyboard, as the page sees it, and the page hears that it lost it. */
  function frameTakesKeyboard(frame: HTMLIFrameElement) {
    active = vi.spyOn(document, 'activeElement', 'get').mockReturnValue(frame);
    window.dispatchEvent(new Event('blur'));
  }

  afterEach(() => {
    active?.mockRestore();
    active = null;
  });

  it('lays a veil over the frame from the start, and takes it away with the frame', () => {
    const w = setup();
    expect(veil(w.container)).not.toBeNull();
    expect(w.container.lastElementChild).toBe(veil(w.container));

    w.host.dispose();
    expect(veil(w.container)).toBeNull();
  });

  it('stops a widget that takes the keyboard while the person is elsewhere, and says why', () => {
    const w = setup();
    w.proxyReady();

    frameTakesKeyboard(w.frame);

    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: STOPPED });
    expect(w.container.querySelector('iframe')).toBeNull();
    expect(veil(w.container)).toBeNull();
    // Nobody had to be asked.
    expect(w.sent(PRESSED)).toEqual([]);
  });

  it('stops it before the proxy is ready too: the frame is there from the first moment', () => {
    const w = setup();

    frameTakesKeyboard(w.frame);

    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: STOPPED });
  });

  it('asks the proxy page when the widget takes the keyboard under the pointer, and leaves it the keyboard for a press', () => {
    const w = setup();
    w.proxyReady();
    pointerOn(w.container);

    frameTakesKeyboard(w.frame);
    expect(w.sent(PRESSED)).toEqual([{ message: { jsonrpc: '2.0', method: PRESSED }, targetOrigin: '*' }]);

    w.notify(PRESSED, { pressed: true });
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(w.status()).not.toBe('failed');
    expect(w.container.querySelector('iframe')).toBe(w.frame);
  });

  it('stops it when the proxy page knows of no press: the pointer only rests there', () => {
    const w = setup();
    w.proxyReady();
    pointerOn(w.container);
    frameTakesKeyboard(w.frame);

    w.notify(PRESSED, { pressed: false });

    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: STOPPED });
  });

  it('stops it when the proxy page does not answer in time', () => {
    const w = setup();
    w.proxyReady();
    pointerOn(w.container);
    frameTakesKeyboard(w.frame);

    vi.advanceTimersByTime(PROOF_WAIT_MS - 1);
    expect(w.status()).not.toBe('failed');
    vi.advanceTimersByTime(1);
    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: STOPPED });
  });

  it('believes the proxy page of its own frame only', () => {
    const w = setup();
    w.proxyReady();
    const other = document.createElement('iframe');
    document.body.appendChild(other);
    pointerOn(w.container);
    frameTakesKeyboard(w.frame);

    // A widget that says it to the page itself, another frame, a document with an origin in the frame.
    w.say({ jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, { source: other.contentWindow });
    w.say({ jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, { source: window });
    w.say({ jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, { origin: window.location.origin });
    vi.advanceTimersByTime(PROOF_WAIT_MS);

    expect(w.host.getSnapshot()).toEqual({ status: 'failed', error: STOPPED });
  });

  it('does not take the word of a press for anything else: no height, nothing to keep', () => {
    const w = setup();
    w.proxyReady();

    w.notify(PRESSED, { pressed: true, height: 500, items: [['k', 'v']] });
    vi.advanceTimersByTime(SIZE_INTERVAL_MS * 2);

    expect(w.heights).toEqual([]);
    expect(w.stored).toEqual([]);
  });
});

describe('the end', () => {
  it('removes the frame and stops listening', () => {
    const w = setup();
    w.proxyReady();
    const post = vi.mocked(w.frame.contentWindow!.postMessage);
    post.mockClear();
    w.host.dispose();
    expect(w.container.querySelector('iframe')).toBeNull();
    w.size(600);
    w.host.setTheme('light');
    vi.advanceTimersByTime(SANDBOX_WAIT_MS * 2);
    expect(post).not.toHaveBeenCalled();
    expect(w.heights).toEqual([]);
    // No word of a widget that is gone.
    expect(w.snapshots).toEqual([]);
  });

  it('does not open a frame again', () => {
    const w = setup();
    w.host.dispose();
    w.host.start(w.container);
    expect(w.container.querySelector('iframe')).toBeNull();
  });
});

describe('the head of the document', () => {
  it('is the same for every widget of a theme: nothing of the source is in it', () => {
    const one = setup({ source: '<p>one</p>' });
    const two = setup({ source: '</script><script>alert(1)</script>' });
    one.proxyReady();
    two.proxyReady();
    const head = (w: typeof one) => (w.posted[0].message.params!.html as string).slice(0, -w.host['options'].source.length);
    expect(head(one)).toBe(head(two));
    expect(head(one).startsWith(widgetHead({ theme: 'dark', variables: {} }).slice(0, 60))).toBe(true);
  });
});
