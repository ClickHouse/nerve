import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/client', () => ({
  api: { getMcpAppResource: vi.fn(), getMcpAppTools: vi.fn(), callMcpAppTool: vi.fn() },
}));

/** Called each time the host reads the chat's colours for a view, when a test sets it. */
const styleReads = vi.hoisted(() => ({ watch: null as (() => void) | null }));
vi.mock('./hostStyles', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./hostStyles')>();
  return {
    ...actual,
    hostStyleVariables: (...args: Parameters<typeof actual.hostStyleVariables>) => {
      styleReads.watch?.();
      return actual.hostStyleVariables(...args);
    },
  };
});

import { api } from '../../../api/client';
import { ApiError } from '../../../api/hosted';
import type { McpAppResource, McpAppTool } from '../../../types/mcpApps';
import { APP_START_MS, MAX_REFUSALS, PROMPT_SETTLE_MS, SANDBOX_WAIT_MS, createHost } from './host';
import { PROOF_WAIT_MS } from './keyboard';
import type { McpAppHostHandle, McpAppToolCall } from './types';

/**
 * The host of a view, run against a fake frame: a real `<iframe>` of jsdom
 * that loads nothing. The test plays the two parties behind it — the sandbox
 * proxy and the view — by delivering their messages to the page, and reads
 * what the host posts to the frame.
 */

/**
 * What a message from the frame says its origin is. The sandbox proxy is a
 * page of the chat's own address, served without an origin: "null", like
 * every sandboxed frame. The frame is known by its window.
 */
const NONE = 'null';
const SESSION = 'session-1';
const HTML = '<!doctype html><title>view</title>';

const getResource = vi.mocked(api.getMcpAppResource);
const getTools = vi.mocked(api.getMcpAppTools);
const callTool = vi.mocked(api.callMcpAppTool);

function resource(overrides: Partial<McpAppResource> = {}): McpAppResource {
  return {
    uri: 'ui://example/view.html', mime_type: 'text/html;profile=mcp-app', html: HTML,
    csp: null, permissions: null, prefers_border: null, cached: false, ...overrides,
  };
}

function tool(name: string, overrides: Partial<McpAppTool> = {}): McpAppTool {
  return { name, read_only: false, visibility: ['model', 'app'], resource_uri: null, ...overrides };
}

const TOOLS = [
  tool('get-time', { read_only: true, resource_uri: 'ui://example/view.html' }),
  tool('set-time'),
  tool('refresh', { visibility: ['app'] }),
  tool('summarize', { visibility: ['model'], read_only: true }),
];

type Rpc = Record<string, unknown> & { id?: number; method?: string; params?: Record<string, unknown> };

/** Wait until `check` passes. It polls fast: the host answers within a few turns of the event loop. */
const until = (check: () => void) => vi.waitFor(check, { interval: 2 });

/** The sandbox proxy and the view behind the frame. */
class Remote {
  /** What the host posted to the frame, with the origin it addressed. */
  readonly posted: Array<{ message: Rpc; targetOrigin: string }> = [];
  /** A view answers `ui/resource-teardown`; set false for one that does not. */
  answersTeardown = true;
  private readonly frame: HTMLIFrameElement;
  private nextId = 100;

  constructor(frame: HTMLIFrameElement) {
    this.frame = frame;
    vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(((message: Rpc, targetOrigin: string) => {
      this.posted.push({ message, targetOrigin });
      if (message.method === 'ui/resource-teardown' && this.answersTeardown) {
        queueMicrotask(() => this.say({ jsonrpc: '2.0', id: message.id, result: {} }));
      }
    }) as never);
  }

  say(data: unknown, from: { origin?: string; source?: MessageEventSource | null } = {}) {
    window.dispatchEvent(new MessageEvent('message', {
      data, origin: from.origin ?? NONE, source: 'source' in from ? from.source : this.frame.contentWindow,
    }));
  }

  notify(method: string, params?: Record<string, unknown>) {
    this.say({ jsonrpc: '2.0', method, ...(params ? { params } : {}) });
  }

  /** Send a request of the view and wait for the host's answer. */
  async request(method: string, params?: Record<string, unknown>): Promise<Rpc> {
    const id = this.send(method, params);
    return this.answerTo(id);
  }

  send(method: string, params?: Record<string, unknown>): number {
    const id = this.nextId++;
    this.say({ jsonrpc: '2.0', id, method, ...(params ? { params } : {}) });
    return id;
  }

  answered(id: number): Rpc | undefined {
    return this.messages().find(m => m.id === id && m.method === undefined);
  }

  async answerTo(id: number): Promise<Rpc> {
    await until(() => expect(this.answered(id)).toBeDefined());
    return this.answered(id)!;
  }

  messages(): Rpc[] {
    return this.posted.map(p => p.message);
  }

  sent(method: string): Rpc[] {
    return this.messages().filter(m => m.method === method);
  }

  /** The methods of the host's notifications and requests, in order. */
  methods(): string[] {
    return this.messages().filter(m => m.method).map(m => m.method!);
  }

  proxyReady() {
    this.notify('ui/notifications/sandbox-proxy-ready', {});
  }

  /** The view's side of the handshake; gives the host's answer to `ui/initialize`. */
  async initialize(): Promise<Rpc> {
    const answer = await this.request('ui/initialize', {
      appInfo: { name: 'view', version: '1.0.0' }, appCapabilities: {}, protocolVersion: '2026-01-26',
    });
    this.notify('ui/notifications/initialized');
    return answer;
  }
}

/** The chat's composer, as the host is given it: empty, and it takes what it is offered. */
const insertText = vi.fn<(text: string) => Promise<boolean>>(async () => true);
const composerHasText = vi.fn(() => false);
let place: HTMLDivElement;
let host: McpAppHostHandle | null;

function start(toolUseId = 'tu-1'): McpAppHostHandle {
  host = createHost({
    sessionId: SESSION, toolUseId, theme: 'dark', insertText, composerHasText,
    app: { server: 'example-server', tool: 'get-time', resourceUri: 'ui://example/view.html' },
  });
  expect(host.attach(place)).toBe(true);
  return host;
}

/** The frame, once the host has opened it at the sandbox address. */
async function opened(): Promise<Remote> {
  await until(() => expect(place.querySelector('iframe')?.getAttribute('src')).toBeTruthy());
  return new Remote(place.querySelector('iframe')!);
}

/** A host whose view has started. */
async function ready(call?: McpAppToolCall): Promise<{ host: McpAppHostHandle; remote: Remote }> {
  const started = start();
  if (call) started.setToolCall(call);
  const remote = await opened();
  remote.proxyReady();
  await remote.initialize();
  await until(() => expect(started.getSnapshot().status).toBe('ready'));
  return { host: started, remote };
}

/**
 * The person's answer to the question on screen, given as a person gives it:
 * once the question has been there for a moment (a yes counts from
 * `readyAt` on).
 */
function answer(started: McpAppHostHandle, allow: boolean): void {
  const prompt = started.getSnapshot().prompt!;
  const now = vi.spyOn(Date, 'now').mockReturnValue(Math.max(Date.now(), prompt.readyAt));
  try {
    started.answer(prompt.id, allow);
  } finally {
    now.mockRestore();
  }
}

const RUNNING: McpAppToolCall = { input: { zone: 'UTC' }, settled: false };
const RESULT = { content: [{ type: 'text', text: '10:00' }], structuredContent: { time: '10:00' } };

beforeEach(() => {
  vi.clearAllMocks();
  insertText.mockResolvedValue(true);
  composerHasText.mockReturnValue(false);
  getResource.mockResolvedValue(resource());
  getTools.mockResolvedValue({ tools: TOOLS });
  callTool.mockResolvedValue({ content: [{ type: 'text', text: '11:00' }], structuredContent: { time: '11:00' } });
  vi.spyOn(console, 'debug').mockImplementation(() => {});
  place = document.createElement('div');
  document.body.appendChild(place);
  host = null;
});

afterEach(async () => {
  await host?.dispose();
  place.remove();
  styleReads.watch = null;
  document.documentElement.removeAttribute('data-cui-theme');
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('the start of a view', () => {
  it('reads the document and the tools of the server, then opens the sandbox frame', async () => {
    const started = start();
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'resource' });

    await opened();

    expect(getResource).toHaveBeenCalledTimes(1);
    expect(getResource.mock.calls[0].slice(0, 3)).toEqual([SESSION, 'example-server', 'ui://example/view.html']);
    expect(getTools).toHaveBeenCalledTimes(1);
    expect(getTools.mock.calls[0].slice(0, 2)).toEqual([SESSION, 'example-server']);
    // Every request has a deadline.
    expect(getResource.mock.calls[0][3]).toBeInstanceOf(AbortSignal);
    expect(getTools.mock.calls[0][2]).toBeInstanceOf(AbortSignal);

    const frame = place.querySelector('iframe')!;
    // A path of the chat's own address.
    expect(frame.getAttribute('src')).toBe('/mcp-app-sandbox.html');
    // No `sandbox` of the chat's: the gateway's header takes the page's
    // origin away, and the page runs only when it has none.
    expect(frame.hasAttribute('sandbox')).toBe(false);
    expect(frame.getAttribute('referrerpolicy')).toBe('origin');
    expect(frame.hasAttribute('allow')).toBe(false);
    expect(frame.title).toBe('App of example-server');
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'sandbox' });
  });

  it('puts the origins of the document into the address, and passes on clipboard-write alone', async () => {
    getResource.mockResolvedValue(resource({
      csp: { connectDomains: ['https://api.example.com', "'unsafe-eval'"], frameDomains: [window.location.origin] },
      permissions: { camera: {}, clipboardWrite: {} },
    }));
    start();
    const remote = await opened();

    const frame = place.querySelector('iframe')!;
    expect(frame.getAttribute('src')!.startsWith('/mcp-app-sandbox.html?csp=')).toBe(true);
    const url = new URL(frame.getAttribute('src')!, window.location.origin);
    expect([...url.searchParams.keys()]).toEqual(['csp']);
    expect(JSON.parse(url.searchParams.get('csp')!)).toEqual({ connectDomains: ['https://api.example.com'] });
    expect(frame.getAttribute('allow')).toBe('clipboard-write *');

    remote.proxyReady();
    await until(() => expect(remote.sent('ui/notifications/sandbox-resource-ready')).toHaveLength(1));
    expect(remote.sent('ui/notifications/sandbox-resource-ready')[0].params).toEqual({
      html: HTML, sandbox: 'allow-scripts allow-forms',
      csp: { connectDomains: ['https://api.example.com'] }, permissions: { clipboardWrite: {} },
    });
  });

  it('gives the proxy the HTML when it is ready, for a frame with an origin of its own', async () => {
    const started = start();
    const remote = await opened();
    expect(remote.posted).toEqual([]);

    remote.proxyReady();

    await until(() => expect(remote.posted).toHaveLength(1));
    // No allow-same-origin for the view's frame: it shares an origin with nothing.
    // The frame of the proxy has no origin that could be named: it is addressed as it is.
    expect(remote.posted[0]).toEqual({
      message: {
        jsonrpc: '2.0', method: 'ui/notifications/sandbox-resource-ready',
        params: { html: HTML, sandbox: 'allow-scripts allow-forms' },
      },
      targetOrigin: '*',
    });
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'app' });
  });

  it('answers ui/initialize with what the chat is and offers', async () => {
    start();
    const remote = await opened();
    remote.proxyReady();

    const { result } = await remote.initialize() as unknown as {
      result: {
        protocolVersion: string;
        hostInfo: { name: string };
        hostCapabilities: Record<string, unknown>;
        hostContext: { styles: { variables: Record<string, string> } };
      };
    };

    expect(result.protocolVersion).toBe('2026-01-26');
    expect(result.hostInfo.name).toBe('nerve');
    // Only what the chat does: no model context, no sampling, no downloads.
    expect(Object.keys(result.hostCapabilities).sort()).toEqual(['logging', 'message', 'openLinks', 'serverResources', 'serverTools']);
    expect(result.hostContext).toMatchObject({
      theme: 'dark',
      displayMode: 'inline',
      availableDisplayModes: ['inline'],
      platform: 'web',
      containerDimensions: { maxHeight: 800 },
      locale: navigator.language,
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      toolInfo: {
        id: 'tu-1',
        tool: {
          name: 'get-time', inputSchema: { type: 'object' }, annotations: { readOnlyHint: true },
          _meta: { ui: { resourceUri: 'ui://example/view.html', visibility: ['model', 'app'] } },
        },
      },
    });
    expect(result.hostContext.styles.variables['--color-background-primary']).toMatch(/^light-dark\(/);
  });

  it('is ready once the view says it is initialized', async () => {
    const { host: started } = await ready();

    expect(started.getSnapshot()).toMatchObject({ status: 'ready', step: null, error: null });
  });

  it('does not wait long for the tools of the server: the view starts, and can call them once they are read', async () => {
    vi.useFakeTimers();
    let answerTools!: (value: { tools: McpAppTool[] }) => void;
    getTools.mockReturnValue(new Promise((resolve) => { answerTools = resolve; }));
    const started = start();

    await vi.advanceTimersByTimeAsync(1400);
    expect(place.querySelector('iframe')).toBeNull();
    await vi.advanceTimersByTimeAsync(200);
    const remote = await opened();
    remote.proxyReady();
    const { result } = await remote.initialize() as unknown as { result: { hostContext: { toolInfo: { tool: Record<string, unknown> } } } };
    await vi.advanceTimersByTimeAsync(0);

    expect(started.getSnapshot().status).toBe('ready');
    // What the chat knows of the tool without the read: its name and its view.
    expect(result.hostContext.toolInfo.tool).toEqual({
      name: 'get-time', inputSchema: { type: 'object' }, _meta: { ui: { resourceUri: 'ui://example/view.html' } },
    });
    expect((await remote.request('tools/call', { name: 'get-time' })).error)
      .toMatchObject({ message: 'The tools of example-server could not be read yet.' });

    answerTools({ tools: TOOLS });
    await vi.advanceTimersByTimeAsync(0);

    expect((await remote.request('tools/call', { name: 'get-time' })).result).toBeDefined();
    expect(callTool).toHaveBeenCalledTimes(1);
  });

  it('starts once, however often it is attached', async () => {
    const started = start();
    started.attach(place);
    await opened();
    started.attach(place);

    expect(getResource).toHaveBeenCalledTimes(1);
    expect(place.querySelectorAll('iframe')).toHaveLength(1);
  });

  it('shows no border for a document that asks for none', async () => {
    getResource.mockResolvedValue(resource({ prefers_border: false }));
    const started = start();
    expect(started.getSnapshot().bordered).toBe(true);

    await opened();

    expect(started.getSnapshot().bordered).toBe(false);
  });
});

describe('the tool call a view is for', () => {
  it('sends nothing to the view before it is initialized', async () => {
    const started = start();
    started.setToolCall({ input: { zone: 'UTC' }, result: RESULT, settled: true });
    const remote = await opened();
    remote.proxyReady();
    await remote.request('ui/initialize', {
      appInfo: { name: 'view', version: '1.0.0' }, appCapabilities: {}, protocolVersion: '2026-01-26',
    });
    started.setTheme('light');
    started.setToolCall({ input: { zone: 'UTC' }, result: RESULT, settled: true });

    // The HTML for the proxy, the answer to the handshake, and no more.
    expect(remote.methods()).toEqual(['ui/notifications/sandbox-resource-ready']);
    expect(remote.messages()).toHaveLength(2);
  });

  it('sends the arguments, then the result', async () => {
    const { remote } = await ready({ input: { zone: 'UTC' }, result: RESULT, settled: true });

    await until(() => expect(remote.sent('ui/notifications/tool-result')).toHaveLength(1));
    expect(remote.sent('ui/notifications/tool-input')).toEqual([
      { jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: { arguments: { zone: 'UTC' } } },
    ]);
    expect(remote.sent('ui/notifications/tool-result')[0].params).toEqual(RESULT);
    const methods = remote.methods();
    expect(methods.indexOf('ui/notifications/tool-input')).toBeLessThan(methods.indexOf('ui/notifications/tool-result'));
    expect(remote.posted.every(p => p.targetOrigin === '*')).toBe(true);
  });

  it('sends the result when the call gets one, once', async () => {
    const { host: started, remote } = await ready(RUNNING);
    await until(() => expect(remote.sent('ui/notifications/tool-input')).toHaveLength(1));
    expect(remote.sent('ui/notifications/tool-result')).toEqual([]);

    started.setToolCall({ input: { zone: 'UTC' }, result: RESULT, settled: true });
    started.setToolCall({ input: { zone: 'UTC' }, result: RESULT, settled: true });

    await until(() => expect(remote.sent('ui/notifications/tool-result')).toHaveLength(1));
    expect(remote.sent('ui/notifications/tool-input')).toHaveLength(1);
    expect(remote.sent('ui/notifications/tool-cancelled')).toEqual([]);
  });

  it('says so when the call ends without a result', async () => {
    const { host: started, remote } = await ready(RUNNING);

    started.setToolCall({ input: { zone: 'UTC' }, settled: true, failure: 'Connection closed' });

    await until(() => expect(remote.sent('ui/notifications/tool-cancelled')).toHaveLength(1));
    expect(remote.sent('ui/notifications/tool-cancelled')[0].params).toEqual({ reason: 'Connection closed' });
    expect(remote.sent('ui/notifications/tool-result')).toEqual([]);
  });

  it('gives a reason of its own when the call has none', async () => {
    const { remote } = await ready({ input: {}, settled: true });

    await until(() => expect(remote.sent('ui/notifications/tool-cancelled')).toHaveLength(1));
    expect(remote.sent('ui/notifications/tool-cancelled')[0].params).toEqual({ reason: 'The tool call ended without a result.' });
  });

  it('ends a view whose frame loads a second time, and hands the document to no second frame', async () => {
    const { host: started, remote } = await ready({ input: { zone: 'UTC' }, result: RESULT, settled: true });
    await until(() => expect(remote.sent('ui/notifications/tool-result')).toHaveLength(1));

    // The frame was loaded again: not by the chat, so at an address (and with a policy) of somebody else's choosing.
    remote.proxyReady();

    await until(() => expect(started.getSnapshot().status).toBe('error'));
    expect(started.getSnapshot().error).toBe('The frame of the app loaded a second time, which a view may not do.');
    expect(remote.sent('ui/notifications/sandbox-resource-ready')).toHaveLength(1);
    expect(remote.sent('ui/notifications/tool-input')).toHaveLength(1);
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('ends it too when the second load comes before the view has started', async () => {
    const started = start();
    const remote = await opened();
    remote.proxyReady();
    await until(() => expect(remote.sent('ui/notifications/sandbox-resource-ready')).toHaveLength(1));

    remote.proxyReady();

    await until(() => expect(started.getSnapshot().status).toBe('error'));
    expect(remote.sent('ui/notifications/sandbox-resource-ready')).toHaveLength(1);
  });

  it('starts over on Retry, in a frame of the chat\'s making', async () => {
    const { host: started, remote } = await ready();
    remote.proxyReady();
    await until(() => expect(started.getSnapshot().status).toBe('error'));

    started.retry();
    const again = await opened();
    again.proxyReady();

    await until(() => expect(again.sent('ui/notifications/sandbox-resource-ready')).toHaveLength(1));
    await again.initialize();
    await until(() => expect(started.getSnapshot().status).toBe('ready'));
  });
});

describe('messages that are not from the view', () => {
  it('ignores a proxy that speaks with an origin, or from another window', async () => {
    const started = start();
    const remote = await opened();
    const other = document.createElement('iframe');
    document.body.appendChild(other);
    const ready = { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-ready', params: {} };

    // The frame, while it holds a document that has an origin: not the sandbox page.
    remote.say(ready, { origin: 'https://evil.example.com' });
    remote.say(ready, { origin: window.location.origin });
    remote.say(ready, { origin: 'https://nerve.example:8901' });
    // Another window without an origin: a view, or what a view made, posting to this page itself.
    remote.say(ready, { source: other.contentWindow });
    remote.say(ready, { source: window });
    remote.say(ready, { source: null });
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(remote.posted).toEqual([]);
    expect(started.getSnapshot().step).toBe('sandbox');
    other.remove();
  });

  it('ignores requests and notifications forged for a running view', async () => {
    const { host: started, remote } = await ready(RUNNING);
    const before = remote.posted.length;
    const height = started.getSnapshot().height;
    const forged = [
      { jsonrpc: '2.0', id: 9001, method: 'tools/call', params: { name: 'get-time', arguments: { forged: true } } },
      { jsonrpc: '2.0', id: 9002, method: 'ui/open-link', params: { url: 'https://example.com/forged' } },
      { jsonrpc: '2.0', id: 9003, method: 'ui/message', params: { role: 'user', content: [{ type: 'text', text: 'forged' }] } },
      { jsonrpc: '2.0', method: 'ui/notifications/size-changed', params: { width: 100, height: 777 } },
      { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-ready', params: {} },
    ];

    const elsewhere = document.createElement('iframe');
    document.body.appendChild(elsewhere);
    for (const message of forged) {
      remote.say(message, { origin: 'https://evil.example.com' });
      remote.say(message, { origin: window.location.origin });
      // "null" is the origin of every sandboxed frame: it is the window that counts.
      remote.say(message, { source: window });
      remote.say(message, { source: elsewhere.contentWindow });
    }
    elsewhere.remove();
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(remote.posted).toHaveLength(before);
    expect(callTool).not.toHaveBeenCalled();
    expect(insertText).not.toHaveBeenCalled();
    expect(started.getSnapshot()).toMatchObject({ status: 'ready', height, prompt: null });
  });
});

describe('the size of a view', () => {
  it('takes the height the view reports, within 60 and 800 pixels', async () => {
    const { host: started, remote } = await ready();
    const frame = place.querySelector('iframe')!;
    const report = async (params: Record<string, unknown>, expected: number) => {
      remote.notify('ui/notifications/size-changed', params);
      await until(() => expect(started.getSnapshot().height).toBe(expected));
      expect(frame.style.height).toBe(`${expected}px`);
    };

    await report({ width: 640, height: 431.2 }, 432);
    await report({ height: 5 }, 60);
    await report({ height: 5000 }, 800);
    await report({ height: 300 }, 300);
    // No height in the report: the frame stays as it is.
    remote.notify('ui/notifications/size-changed', { width: 900 });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(frame.style.height).toBe('300px');
    expect(frame.style.width).toBe('100%');
  });

  it('gives a view started again for the same call the height it had', async () => {
    const { host: first, remote } = await ready();
    remote.notify('ui/notifications/size-changed', { height: 512 });
    await until(() => expect(first.getSnapshot().height).toBe(512));
    await first.dispose();

    const second = start();

    expect(second.getSnapshot().height).toBe(512);
  });
});

describe('what a view asks for', () => {
  it('runs a read-only tool of its server at once, through the gateway', async () => {
    const { host: started, remote } = await ready();

    const answer = await remote.request('tools/call', { name: 'get-time', arguments: { zone: 'CET' } });

    expect(answer.result).toEqual({ content: [{ type: 'text', text: '11:00' }], structuredContent: { time: '11:00' } });
    expect(callTool).toHaveBeenCalledTimes(1);
    expect(callTool.mock.calls[0].slice(0, 2)).toEqual([
      SESSION, { server: 'example-server', tool: 'get-time', arguments: { zone: 'CET' } },
    ]);
    expect(callTool.mock.calls[0][2]).toBeInstanceOf(AbortSignal);
    expect(started.getSnapshot().prompt).toBeNull();
  });

  it('runs a tool made for views only at once', async () => {
    const { remote } = await ready();

    await remote.request('tools/call', { name: 'refresh' });

    expect(callTool.mock.calls[0][1]).toEqual({ server: 'example-server', tool: 'refresh', arguments: {} });
  });

  it('asks the person before any other tool, and runs it when allowed, that once', async () => {
    const { host: started, remote } = await ready();

    const first = remote.send('tools/call', { name: 'set-time', arguments: { to: '12:00' } });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;
    expect(prompt).toMatchObject({ kind: 'tool', tool: 'set-time', args: { to: '12:00' } });
    expect(callTool).not.toHaveBeenCalled();
    expect(remote.answered(first)).toBeUndefined();

    answer(started, true);

    expect((await remote.answerTo(first)).result).toMatchObject({ structuredContent: { time: '11:00' } });
    expect(callTool).toHaveBeenCalledTimes(1);
    expect(started.getSnapshot().prompt).toBeNull();

    // Allowed once: the next call asks again.
    const second = remote.send('tools/call', { name: 'set-time', arguments: { to: '13:00' } });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    started.answer(started.getSnapshot().prompt!.id, false);

    expect((await remote.answerTo(second)).error).toMatchObject({ message: 'The tool call was not allowed.' });
    expect(callTool).toHaveBeenCalledTimes(1);
  });

  it('counts a yes only a moment after the question came on screen, and a no at once', async () => {
    const { host: started, remote } = await ready();
    const first = remote.send('tools/call', { name: 'set-time', arguments: { n: 1 } });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;
    expect(prompt.readyAt - Date.now()).toBeGreaterThan(PROMPT_SETTLE_MS - 200);
    expect(prompt.readyAt - Date.now()).toBeLessThanOrEqual(PROMPT_SETTLE_MS);

    // A click that lands before the person can have read the question.
    started.answer(prompt.id, true);
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(callTool).not.toHaveBeenCalled();
    expect(started.getSnapshot().prompt).toBe(prompt);
    expect(remote.answered(first)).toBeUndefined();

    // A no is safe whenever it comes.
    started.answer(prompt.id, false);
    expect((await remote.answerTo(first)).error).toMatchObject({ message: 'The tool call was not allowed.' });
    expect(callTool).not.toHaveBeenCalled();
  });

  it('gives a double click to one question, not to the one that takes its place', async () => {
    const { host: started, remote } = await ready();
    const first = remote.send('tools/call', { name: 'set-time', arguments: { n: 1 } });
    const second = remote.send('tools/call', { name: 'set-time', arguments: { n: 2 } });
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 1 } }));

    // The first click answers the question the person read; the second lands on its successor.
    answer(started, true);
    const next = started.getSnapshot().prompt!;
    expect(next).toMatchObject({ args: { n: 2 } });
    started.answer(next.id, true);

    expect((await remote.answerTo(first)).result).toBeDefined();
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(callTool).toHaveBeenCalledTimes(1);
    expect(callTool.mock.calls[0][1]).toMatchObject({ arguments: { n: 1 } });
    expect(started.getSnapshot().prompt).toBe(next);
    expect(remote.answered(second)).toBeUndefined();
  });

  it('asks no more after three refusals in a row, until the view is loaded again', async () => {
    const { host: started, remote } = await ready();
    const refuse = async (n: number) => {
      const id = remote.send('tools/call', { name: 'set-time', arguments: { n } });
      await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n } }));
      answer(started, false);
      expect((await remote.answerTo(id)).error).toMatchObject({ message: 'The tool call was not allowed.' });
    };
    for (let n = 1; n <= MAX_REFUSALS; n++) await refuse(n);

    // The view asks again, as it did each time it heard no.
    const refused = await remote.request('tools/call', { name: 'set-time', arguments: { n: 99 } });

    expect(refused.error).toMatchObject({
      message: 'The person refused three times in a row. Nerve asks no more for this view until it is loaded again.',
    });
    expect(started.getSnapshot().prompt).toBeNull();
    expect(callTool).not.toHaveBeenCalled();
    // What needs no question still runs, and a link is another matter.
    expect((await remote.request('tools/call', { name: 'get-time' })).result).toBeDefined();
    const link = remote.send('ui/open-link', { url: 'https://example.com/' });
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ kind: 'link' }));
    answer(started, false);
    await remote.answerTo(link);

    // Loaded again, the view may ask again.
    started.retry();
    const again = await opened();
    again.proxyReady();
    await again.initialize();
    again.send('tools/call', { name: 'set-time', arguments: { n: 100 } });
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 100 } }));
  });

  it('refuses with the third no what already waited behind it', async () => {
    const { host: started, remote } = await ready();
    // A view that asks twice at a time, and again each time it hears no.
    const ids = [1, 2].map(n => remote.send('tools/call', { name: 'set-time', arguments: { n } }));
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 1 } }));
    answer(started, false);
    ids.push(remote.send('tools/call', { name: 'set-time', arguments: { n: 3 } }));
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 2 } }));
    answer(started, false);
    ids.push(remote.send('tools/call', { name: 'set-time', arguments: { n: 4 } }));
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 3 } }));

    answer(started, false);

    // The fourth was waiting behind the third: it is not shown.
    expect(started.getSnapshot().prompt).toBeNull();
    for (const id of ids) expect((await remote.answerTo(id)).error).toBeDefined();
    expect(callTool).not.toHaveBeenCalled();
  });

  it('starts the count of refusals again with a yes', async () => {
    const { host: started, remote } = await ready();
    const ask = async (n: number, allow: boolean) => {
      const id = remote.send('tools/call', { name: 'set-time', arguments: { n } });
      await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n } }));
      answer(started, allow);
      await remote.answerTo(id);
    };
    await ask(1, false);
    await ask(2, false);
    await ask(3, true);
    await ask(4, false);
    await ask(5, false);

    remote.send('tools/call', { name: 'set-time', arguments: { n: 6 } });

    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 6 } }));
  });

  it('does not count what the view took back, or what ended with the view, as a refusal', async () => {
    const { host: started, remote } = await ready();
    for (let n = 1; n <= MAX_REFUSALS + 1; n++) {
      const id = remote.send('tools/call', { name: 'set-time', arguments: { n } });
      await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n } }));
      remote.notify('notifications/cancelled', { requestId: id });
      await until(() => expect(started.getSnapshot().prompt).toBeNull());
    }

    remote.send('tools/call', { name: 'set-time', arguments: { n: 50 } });

    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 50 } }));
  });

  it('calls no tool for a view that was replaced while the person decided', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('tools/call', { name: 'set-time' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());

    started.retry();
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(started.getSnapshot().prompt).toBeNull();
    expect(callTool).not.toHaveBeenCalled();
    expect(remote.answered(id)).toBeUndefined();
  });

  it('takes its question back when the view stops waiting for the tool call, and calls nothing after', async () => {
    const { host: started, remote } = await ready();
    const first = remote.send('tools/call', { name: 'set-time', arguments: { n: 1 } });
    const second = remote.send('tools/call', { name: 'set-time', arguments: { n: 2 } });
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 1 } }));
    const prompt = started.getSnapshot().prompt!;

    // What a view's SDK sends when its request has waited too long (a minute, by default).
    remote.notify('notifications/cancelled', { requestId: first, reason: 'Request timed out' });

    // The question is gone, and the next one is on screen.
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 2 } }));
    // A yes that comes late has nothing to act on.
    started.answer(prompt.id, true);
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(callTool).not.toHaveBeenCalled();
    expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 2 } });

    answer(started, true);
    expect((await remote.answerTo(second)).result).toMatchObject({ structuredContent: { time: '11:00' } });
    expect(callTool).toHaveBeenCalledTimes(1);
    expect(callTool.mock.calls[0][1]).toMatchObject({ tool: 'set-time', arguments: { n: 2 } });
  });

  it('calls no tool that the view took back before it could run', async () => {
    const { remote } = await ready();
    // The view names a tool that was not there at the last read, long enough ago to read again.
    vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 6000);
    let landed!: (value: { tools: McpAppTool[] }) => void;
    getTools.mockReturnValueOnce(new Promise((resolve) => { landed = resolve; }));
    const id = remote.send('tools/call', { name: 'new-tool' });
    await until(() => expect(getTools).toHaveBeenCalledTimes(2));

    remote.notify('notifications/cancelled', { requestId: id });
    await new Promise(resolve => setTimeout(resolve, 10));
    landed({ tools: [...TOOLS, tool('new-tool', { read_only: true })] });
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(callTool).not.toHaveBeenCalled();
  });

  it('refuses a tool that the server does not show to views, without asking anybody', async () => {
    const { host: started, remote } = await ready();

    const answer = await remote.request('tools/call', { name: 'summarize' });

    expect(answer.error).toMatchObject({ message: 'The tool summarize is not available to apps.' });
    expect(callTool).not.toHaveBeenCalled();
    expect(started.getSnapshot().prompt).toBeNull();
  });

  it('refuses a tool that the server does not have, and a tool of another server', async () => {
    const { remote } = await ready();

    expect((await remote.request('tools/call', { name: 'drop-table' })).error).toBeDefined();
    expect((await remote.request('tools/call', { name: 'mcp__nerve__notify' })).error).toBeDefined();
    expect(callTool).not.toHaveBeenCalled();
    // The tools were read a moment ago: an unknown name is no reason to read them again.
    expect(getTools).toHaveBeenCalledTimes(1);
  });

  it('refuses every tool while the tools of the server could not be read', async () => {
    getTools.mockRejectedValue(new ApiError(409, '{"detail":"session_not_running"}'));
    const { remote } = await ready();

    const answer = await remote.request('tools/call', { name: 'get-time' });

    expect(answer.error).toMatchObject({ message: 'The tools of example-server could not be read: The session is asleep.' });
    expect(callTool).not.toHaveBeenCalled();
  });

  it('gives the view the reason when the gateway refuses or fails', async () => {
    const { remote } = await ready();
    callTool.mockRejectedValueOnce(new ApiError(504, '{"detail":"the tool did not answer in time"}'));

    const answer = await remote.request('tools/call', { name: 'get-time' });

    expect(answer.error).toMatchObject({ message: 'the tool did not answer in time' });
  });

  it('passes on a result in which the tool reports an error', async () => {
    const { remote } = await ready();
    callTool.mockResolvedValueOnce({ content: [{ type: 'text', text: 'no such zone' }], isError: true });

    const answer = await remote.request('tools/call', { name: 'get-time', arguments: { zone: '?' } });

    expect(answer.result).toEqual({ content: [{ type: 'text', text: 'no such zone' }], isError: true });
  });

  it('asks before it opens a link, shows the address in full, and opens it without a handle', async () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    const { host: started, remote } = await ready();

    const id = remote.send('ui/open-link', { url: 'https://example.com/docs?a=1#top' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;
    expect(prompt).toMatchObject({ kind: 'link', url: 'https://example.com/docs?a=1#top' });
    expect(open).not.toHaveBeenCalled();

    answer(started, true);

    expect(open).toHaveBeenCalledTimes(1);
    expect(open).toHaveBeenCalledWith('https://example.com/docs?a=1#top', '_blank', 'noopener,noreferrer');
    expect((await remote.answerTo(id)).result).toEqual({});
  });

  it('opens nothing when the person says no', async () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    const { host: started, remote } = await ready();

    const id = remote.send('ui/open-link', { url: 'https://example.com/' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    started.answer(started.getSnapshot().prompt!.id, false);

    expect((await remote.answerTo(id)).result).toEqual({ isError: true });
    expect(open).not.toHaveBeenCalled();
  });

  it('takes its question back when the view stops waiting for the link, and opens nothing after', async () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    const { host: started, remote } = await ready();
    const id = remote.send('ui/open-link', { url: 'https://example.com/' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;

    remote.notify('notifications/cancelled', { requestId: id });

    await until(() => expect(started.getSnapshot().prompt).toBeNull());
    started.answer(prompt.id, true);
    expect(open).not.toHaveBeenCalled();
  });

  it.each([
    'javascript:alert(1)', 'data:text/html,<script>alert(1)</script>', 'file:///etc/passwd', 'ftp://example.com/x',
    '/relative', 'not a url',
  ])('refuses the link %s without asking anybody', async (url) => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    const { host: started, remote } = await ready();

    const answer = await remote.request('ui/open-link', { url });

    expect(answer.result).toEqual({ isError: true });
    expect(started.getSnapshot().prompt).toBeNull();
    expect(open).not.toHaveBeenCalled();
  });

  it('ignores an answer to a question that is not the one on screen', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('tools/call', { name: 'set-time' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;

    started.answer(prompt.id + 1, true);

    expect(started.getSnapshot().prompt).toBe(prompt);
    expect(remote.answered(id)).toBeUndefined();
    expect(callTool).not.toHaveBeenCalled();
  });

  it('asks one question at a time, and refuses what comes on top of three', async () => {
    const { host: started, remote } = await ready();

    const ids = [1, 2, 3, 4].map(n => remote.send('tools/call', { name: 'set-time', arguments: { n } }));
    // The fourth is refused at once.
    expect((await remote.answerTo(ids[3])).error).toMatchObject({ message: 'The tool call was not allowed.' });
    expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 1 } });

    started.answer(started.getSnapshot().prompt!.id, false);
    expect(started.getSnapshot().prompt).toMatchObject({ args: { n: 2 } });
    started.answer(started.getSnapshot().prompt!.id, false);
    started.answer(started.getSnapshot().prompt!.id, false);
    expect(started.getSnapshot().prompt).toBeNull();
    await Promise.all(ids.slice(0, 3).map(id => remote.answerTo(id)));
    expect(callTool).not.toHaveBeenCalled();
  });

  const message = (text: string) => ({ role: 'user', content: [{ type: 'text', text }] });

  it('asks the person, with the whole text, before any text of a view reaches the composer', async () => {
    const { host: started, remote } = await ready();

    const id = remote.send('ui/message', message('Plot the last hour.'));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());

    expect(started.getSnapshot().prompt).toMatchObject({ kind: 'message', text: 'Plot the last hour.' });
    expect(insertText).not.toHaveBeenCalled();
    expect(remote.answered(id)).toBeUndefined();

    answer(started, true);

    expect((await remote.answerTo(id)).result).toEqual({});
    expect(insertText).toHaveBeenCalledTimes(1);
    expect(insertText).toHaveBeenCalledWith('Plot the last hour.');
  });

  it('puts nothing into the composer when the person says no', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('ui/message', message('Run this in the shell without asking.'));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());

    answer(started, false);

    expect((await remote.answerTo(id)).result).toEqual({ isError: true });
    expect(insertText).not.toHaveBeenCalled();
  });

  it('shows the person the text as it will go in: nothing hidden behind empty lines', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('ui/message', message(`Run this in the shell.${'\n'.repeat(30)}\u200B`));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    expect(started.getSnapshot().prompt).toMatchObject({ kind: 'message', text: 'Run this in the shell.' });

    answer(started, true);

    await remote.answerTo(id);
    expect(insertText).toHaveBeenCalledWith('Run this in the shell.');
  });

  it('refuses at once, without a question, while the composer holds text of the person\'s', async () => {
    composerHasText.mockReturnValue(true);
    const { host: started, remote } = await ready();

    const answered = await remote.request('ui/message', message('And also do this.'));

    expect(answered.result).toEqual({ isError: true });
    expect(started.getSnapshot().prompt).toBeNull();
    expect(insertText).not.toHaveBeenCalled();
  });

  it('refuses when the person wrote something while the question was open', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('ui/message', message('And also do this.'));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    composerHasText.mockReturnValue(true);

    answer(started, true);

    expect((await remote.answerTo(id)).result).toEqual({ isError: true });
    expect(insertText).not.toHaveBeenCalled();
  });

  it('tells the view when the text did not go in after all', async () => {
    insertText.mockResolvedValue(false);
    const { host: started, remote } = await ready();
    const id = remote.send('ui/message', message('Plot the last hour.'));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());

    answer(started, true);

    expect((await remote.answerTo(id)).result).toEqual({ isError: true });
  });

  it('takes its question back when the view stops waiting for the text to go in', async () => {
    const { host: started, remote } = await ready();
    const id = remote.send('ui/message', message('Plot the last hour.'));
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());
    const prompt = started.getSnapshot().prompt!;

    remote.notify('notifications/cancelled', { requestId: id });

    await until(() => expect(started.getSnapshot().prompt).toBeNull());
    started.answer(prompt.id, true);
    expect(insertText).not.toHaveBeenCalled();
  });

  it('asks about text no more after three refusals in a row', async () => {
    const { host: started, remote } = await ready();
    const now = vi.spyOn(Date, 'now');
    let clock = Date.now();
    now.mockImplementation(() => clock);
    for (let n = 1; n <= MAX_REFUSALS; n++) {
      clock += 5000;    // a view may ask once a second
      const id = remote.send('ui/message', message(`Do this, attempt ${n}.`));
      await until(() => expect(started.getSnapshot().prompt).toMatchObject({ text: `Do this, attempt ${n}.` }));
      clock += 2000;
      started.answer(started.getSnapshot().prompt!.id, false);
      await remote.answerTo(id);
    }
    clock += 5000;

    const answered = await remote.request('ui/message', message('Do this, once more.'));

    expect(answered.result).toEqual({ isError: true });
    expect(started.getSnapshot().prompt).toBeNull();
    expect(insertText).not.toHaveBeenCalled();
  });

  it('takes a request for the composer at most once a second', async () => {
    const { host: started, remote } = await ready();
    const first = remote.send('ui/message', message('one'));
    await until(() => expect(started.getSnapshot().prompt).toMatchObject({ text: 'one' }));

    // What comes sooner is refused without a question.
    expect((await remote.request('ui/message', message('two'))).result).toEqual({ isError: true });
    expect((await remote.request('ui/message', message('three'))).result).toEqual({ isError: true });
    expect(started.getSnapshot().prompt).toMatchObject({ text: 'one' });

    answer(started, true);
    await remote.answerTo(first);
    expect(insertText).toHaveBeenCalledTimes(1);
    expect(insertText).toHaveBeenLastCalledWith('one');
  });

  it('refuses a ui/message with no text', async () => {
    const { host: started, remote } = await ready();

    const answered = await remote.request('ui/message', {
      role: 'user', content: [{ type: 'image', data: 'AAAA', mimeType: 'image/png' }],
    });

    expect(answered.result).toEqual({ isError: true });
    expect(started.getSnapshot().prompt).toBeNull();
    expect(insertText).not.toHaveBeenCalled();
  });

  it('reads a ui:// resource of its server for the view, and no other resource', async () => {
    const { remote } = await ready();
    getResource.mockClear();
    getResource.mockResolvedValueOnce(resource({ uri: 'ui://example/other.html', html: '<p>other</p>', prefers_border: true }));

    const answer = await remote.request('resources/read', { uri: 'ui://example/other.html' });

    expect(getResource.mock.calls[0].slice(0, 3)).toEqual([SESSION, 'example-server', 'ui://example/other.html']);
    expect(answer.result).toEqual({
      contents: [{
        uri: 'ui://example/other.html', mimeType: 'text/html;profile=mcp-app', text: '<p>other</p>',
        _meta: { ui: { prefersBorder: true } },
      }],
    });

    for (const uri of ['https://example.com/secret', 'file:///etc/passwd', 'config://settings']) {
      expect((await remote.request('resources/read', { uri })).error).toMatchObject({ message: 'An app can read ui:// resources only.' });
    }
    expect(getResource).toHaveBeenCalledTimes(1);
  });

  it('stays inline when the view asks for another display mode', async () => {
    const { remote } = await ready();

    expect((await remote.request('ui/request-display-mode', { mode: 'fullscreen' })).result).toEqual({ mode: 'inline' });
  });

  it('answers ping, and logs a log message', async () => {
    const { remote } = await ready();

    expect((await remote.request('ping')).result).toEqual({});
    remote.notify('notifications/message', { level: 'info', data: 'a log line' });
    await until(() => expect(console.debug).toHaveBeenCalledWith('[mcp-app example-server]', 'info', 'a log line'));
  });

  it.each([
    'ui/update-model-context', 'ui/download-file', 'sampling/createMessage', 'resources/list', 'prompts/list',
  ])('has no %s', async (method) => {
    const { remote } = await ready();

    const answer = await remote.request(method, {});

    expect(answer.error).toMatchObject({ code: -32601 });
  });
});

describe('what a view is told later', () => {
  it('follows the theme of the chat', async () => {
    const { host: started, remote } = await ready();

    started.setTheme('light');

    await until(() => expect(remote.sent('ui/notifications/host-context-changed')).toHaveLength(1));
    expect(remote.sent('ui/notifications/host-context-changed')[0].params).toEqual({ theme: 'light' });

    started.setTheme('light');
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(remote.sent('ui/notifications/host-context-changed')).toHaveLength(1);
  });

  it('reads the colours of the chat once a theme change is on the page, not while it is being applied', async () => {
    const { host: started, remote } = await ready();
    const root = document.documentElement;
    const seen: Array<string | null> = [];
    styleReads.watch = () => {
      seen.push(root.getAttribute('data-cui-theme'));
    };

    // What happens in the page when the theme changes: the design system's
    // provider takes its theme attribute off the page, the block of the view
    // tells its host, and the provider puts the attribute back with the new theme.
    root.removeAttribute('data-cui-theme');
    started.setTheme('light');
    root.setAttribute('data-cui-theme', 'light');

    await until(() => expect(remote.sent('ui/notifications/host-context-changed')).toHaveLength(1));
    expect(remote.sent('ui/notifications/host-context-changed')[0].params).toEqual({ theme: 'light' });
    expect(seen.length).toBeGreaterThan(0);
    expect(seen.every(theme => theme === 'light')).toBe(true);
  });

  it('tells the theme it has last, when it changes twice in a moment', async () => {
    const { host: started, remote } = await ready();

    started.setTheme('light');
    started.setTheme('dark');
    await new Promise(resolve => setTimeout(resolve, 20));

    // Dark is what the view started with: nothing to tell.
    expect(remote.sent('ui/notifications/host-context-changed')).toHaveLength(0);
  });

  it('tells a view the theme that changed while it was starting', async () => {
    const started = start();
    const remote = await opened();
    remote.proxyReady();
    started.setTheme('light');

    await remote.initialize();

    await until(() => expect(remote.sent('ui/notifications/host-context-changed')).toHaveLength(1));
    expect(remote.sent('ui/notifications/host-context-changed')[0].params).toEqual({ theme: 'light' });
  });
});

describe('a view that does not load', () => {
  it('says the session is asleep on 409 session_not_running, and opens no frame', async () => {
    getResource.mockRejectedValue(new ApiError(409, '{"detail":"session_not_running"}'));
    const started = start();

    await until(() => expect(started.getSnapshot().status).toBe('asleep'));

    expect(started.getSnapshot()).toMatchObject({ step: null, error: null });
    expect(place.querySelector('iframe')).toBeNull();
  });

  it.each([
    ['a gateway error', new ApiError(502, '{"detail":"the server closed the connection"}'), 'the server closed the connection'],
    ['an unknown resource', new ApiError(404, '{"detail":"resource not found"}'), 'resource not found'],
    ['no answer', new TypeError('Failed to fetch'), 'Nerve could not be reached.'],
  ])('gives the reason of %s', async (_, error, reason) => {
    getResource.mockRejectedValue(error);
    const started = start();

    await until(() => expect(started.getSnapshot().status).toBe('error'));

    expect(started.getSnapshot().error).toBe(reason);
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('refuses a document that is not an MCP App, or is empty', async () => {
    getResource.mockResolvedValueOnce(resource({ mime_type: 'text/html' }));
    const started = start();
    await until(() => expect(started.getSnapshot().status).toBe('error'));
    expect(started.getSnapshot().error).toMatch(/of a kind that Nerve does not show \(text\/html\)/);

    getResource.mockResolvedValueOnce(resource({ html: '' }));
    started.retry();
    await until(() => expect(started.getSnapshot().error).toBe('The app has no document.'));
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('refuses a document larger than the chat hands to a frame', async () => {
    getResource.mockResolvedValue(resource({ html: 'x'.repeat(5 * 1024 * 1024 + 1) }));
    const started = start();

    await until(() => expect(started.getSnapshot().status).toBe('error'));

    expect(started.getSnapshot().error).toBe("The app's document is too large.");
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('starts again on retry', async () => {
    getResource.mockRejectedValueOnce(new ApiError(502, '{"detail":"boom"}'));
    const started = start();
    await until(() => expect(started.getSnapshot().status).toBe('error'));

    started.retry();

    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'resource', error: null });
    const remote = await opened();
    remote.proxyReady();
    await remote.initialize();
    await until(() => expect(started.getSnapshot().status).toBe('ready'));
    expect(getResource).toHaveBeenCalledTimes(2);
  });

  it('replaces a running view on retry: one frame, and the old one hears nothing more', async () => {
    const { host: started, remote: old } = await ready({ input: {}, result: RESULT, settled: true });
    const oldFrame = place.querySelector('iframe')!;

    started.retry();
    await until(() => expect(place.querySelector('iframe')).not.toBe(oldFrame));
    const remote = await opened();
    const before = old.posted.length;
    remote.proxyReady();
    await remote.initialize();

    await until(() => expect(remote.sent('ui/notifications/tool-result')).toHaveLength(1));
    expect(place.querySelectorAll('iframe')).toHaveLength(1);
    expect(oldFrame.isConnected).toBe(false);
    expect(old.posted).toHaveLength(before);
  });

  it('fails after 8 seconds without a word from the proxy, and can be tried again', async () => {
    vi.useFakeTimers();
    const started = start();
    await opened();

    await vi.advanceTimersByTimeAsync(SANDBOX_WAIT_MS - 100);
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'sandbox' });
    await vi.advanceTimersByTimeAsync(100);

    expect(started.getSnapshot()).toMatchObject({ status: 'error', error: 'The app sandbox does not answer.' });
    expect(place.querySelector('iframe')).toBeNull();

    started.retry();
    await vi.advanceTimersByTimeAsync(0);
    expect(started.getSnapshot().status).toBe('loading');
  });

  it('does not fail a view whose proxy answered in time', async () => {
    vi.useFakeTimers();
    const started = start();
    const remote = await opened();

    remote.proxyReady();
    await vi.advanceTimersByTimeAsync(SANDBOX_WAIT_MS + 1000);

    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'app', error: null });
  });

  it('gives the reason of a proxy that refuses to work where it was served', async () => {
    const started = start();
    const remote = await opened();

    remote.notify('ui/notifications/sandbox-proxy-refused', {
      reason: 'the embedding page is not the chat of this address.',
    });

    await until(() => expect(started.getSnapshot().status).toBe('error'));
    expect(started.getSnapshot().error).toBe(
      'The app sandbox refused to start: the embedding page is not the chat of this address.',
    );
    expect(place.querySelector('iframe')).toBeNull();
    expect(remote.posted).toEqual([]);
  });

  it('gives the reason of a proxy page that was served without its policy, and hands it nothing', async () => {
    const started = start();
    const remote = await opened();

    // Without the header the page has the chat's own origin. It runs nothing and says why.
    remote.say({
      jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-refused',
      params: { reason: 'this page was served without its sandbox policy.' },
    }, { origin: window.location.origin });

    await until(() => expect(started.getSnapshot().status).toBe('error'));
    expect(started.getSnapshot().error).toBe(
      'The app sandbox refused to start: this page was served without its sandbox policy.',
    );
    expect(place.querySelector('iframe')).toBeNull();
    // Not the HTML of the view, not a word.
    expect(remote.posted).toEqual([]);
  });

  it('hands nothing to a frame that says it is ready with the chat\'s origin', async () => {
    const started = start();
    const remote = await opened();

    remote.say(
      { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-ready', params: {} },
      { origin: window.location.origin },
    );
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(remote.posted).toEqual([]);
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'sandbox' });
  });

  it('ignores a refusal from another origin or window, and one that comes after the proxy was ready', async () => {
    const started = start();
    const remote = await opened();
    const refusal = { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-refused', params: { reason: 'forged' } };

    remote.say(refusal, { origin: 'https://evil.example.com' });
    remote.say(refusal, { source: window });
    remote.say(refusal, { source: window, origin: window.location.origin });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(started.getSnapshot()).toMatchObject({ status: 'loading', step: 'sandbox' });

    // From then on the view speaks through the proxy: it does not end itself this way.
    remote.proxyReady();
    await remote.initialize();
    await until(() => expect(started.getSnapshot().status).toBe('ready'));
    remote.say(refusal);
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(started.getSnapshot().status).toBe('ready');
    expect(place.querySelector('iframe')).not.toBeNull();
  });

  it('fails a view that has its HTML and does not start', async () => {
    vi.useFakeTimers();
    const started = start();
    const remote = await opened();
    remote.proxyReady();

    await vi.advanceTimersByTimeAsync(APP_START_MS - 100);
    expect(started.getSnapshot().status).toBe('loading');
    await vi.advanceTimersByTimeAsync(100);

    expect(started.getSnapshot()).toMatchObject({ status: 'error', error: 'The app did not start in time.' });
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('drops what an earlier start still had under way', async () => {
    let answerFirst!: (value: McpAppResource) => void;
    getResource.mockReturnValueOnce(new Promise((resolve) => { answerFirst = resolve; }));
    const started = start();

    started.retry();
    await opened();
    answerFirst(resource({ html: '<p>stale</p>' }));
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(place.querySelectorAll('iframe')).toHaveLength(1);
    expect(started.getSnapshot().step).toBe('sandbox');
  });
});

describe('the keyboard', () => {
  const PRESSED = 'ui/notifications/sandbox-pressed';
  const TAKEN = 'it took the keyboard without a click in it.';
  let active: { mockRestore: () => void } | null = null;
  const veil = () => place.querySelector<HTMLElement>('[data-mcp-app-frame] [data-frame-veil]');
  /** The pointer of the person comes onto the view. */
  const pointerOn = () => veil()!.dispatchEvent(new Event('pointermove', { bubbles: true }));
  /** The frame has the keyboard, as the page sees it, and the page hears that it lost it. */
  function frameTakesKeyboard() {
    active = vi.spyOn(document, 'activeElement', 'get').mockReturnValue(place.querySelector('iframe')!);
    window.dispatchEvent(new Event('blur'));
  }
  const wait = (ms: number) => new Promise((resolve) => { setTimeout(resolve, ms); });

  afterEach(() => {
    active?.mockRestore();
    active = null;
  });

  it('lays a veil over the frame of a view, and takes it away with the frame', async () => {
    const { host: started } = await ready();
    expect(veil()).not.toBeNull();

    await started.dispose();
    expect(place.querySelector('[data-frame-veil]')).toBeNull();
  });

  it('takes the veil away when the view ends for another reason, and watches the keyboard no longer', async () => {
    const { host: started, remote } = await ready();
    expect(veil()).not.toBeNull();

    // A frame that says it is ready a second time ends the view.
    remote.proxyReady();
    await until(() => expect(started.getSnapshot().status).toBe('error'));

    expect(place.querySelector('[data-frame-veil]')).toBeNull();
    expect(started.getSnapshot().stopped).toBe(false);
  });

  it('stops a view that takes the keyboard while the person is elsewhere, and says that it was stopped', async () => {
    const { host: started, remote } = await ready();

    frameTakesKeyboard();

    expect(started.getSnapshot()).toMatchObject({ status: 'error', stopped: true, error: TAKEN, prompt: null });
    expect(place.querySelector('iframe')).toBeNull();
    expect(veil()).toBeNull();
    // Nobody had to be asked.
    expect(remote.sent(PRESSED)).toEqual([]);
  });

  it('stops it while it still loads too: the frame is there before the view', async () => {
    const started = start();
    await opened();

    frameTakesKeyboard();

    expect(started.getSnapshot()).toMatchObject({ status: 'error', stopped: true, error: TAKEN });
  });

  it('asks the proxy page when the view takes the keyboard under the pointer, and leaves it the keyboard for a press', async () => {
    const { host: started, remote } = await ready();
    pointerOn();

    frameTakesKeyboard();
    expect(remote.posted.filter(p => p.message.method === PRESSED))
      .toEqual([{ message: { jsonrpc: '2.0', method: PRESSED }, targetOrigin: '*' }]);

    remote.notify(PRESSED, { pressed: true });
    await wait(PROOF_WAIT_MS * 2);
    expect(started.getSnapshot()).toMatchObject({ status: 'ready' });
    expect(place.querySelector('iframe')).not.toBeNull();
  });

  it('stops it when the proxy page knows of no press: the pointer only rests there', async () => {
    const { host: started, remote } = await ready();
    pointerOn();
    frameTakesKeyboard();

    remote.notify(PRESSED, { pressed: false });

    expect(started.getSnapshot()).toMatchObject({ status: 'error', stopped: true, error: TAKEN });
  });

  it('stops it when the proxy page does not answer in time, or only another window does', async () => {
    const { host: started, remote } = await ready();
    pointerOn();
    frameTakesKeyboard();

    // The view says it to the page itself: that is not the proxy page of its frame.
    remote.say({ jsonrpc: '2.0', method: PRESSED, params: { pressed: true } }, { source: window });
    await wait(PROOF_WAIT_MS + 50);

    expect(started.getSnapshot()).toMatchObject({ status: 'error', stopped: true, error: TAKEN });
  });

  it('keeps the word of a press from the view\'s own protocol', async () => {
    const { host: started, remote } = await ready();

    remote.notify(PRESSED, { pressed: true });
    remote.notify(PRESSED, { pressed: false });
    await wait(20);

    expect(started.getSnapshot()).toMatchObject({ status: 'ready', error: null });
  });

  it('is a view like any other again once it is loaded anew', async () => {
    const { host: started } = await ready();
    frameTakesKeyboard();
    expect(started.getSnapshot()).toMatchObject({ stopped: true });
    active?.mockRestore();
    active = null;

    started.retry();
    const remote = await opened();
    remote.proxyReady();
    await remote.initialize();
    await until(() => expect(started.getSnapshot().status).toBe('ready'));

    expect(started.getSnapshot()).toMatchObject({ stopped: false, error: null });
    expect(veil()).not.toBeNull();
  });
});

describe('the end of a view', () => {
  it('tells the view to end, waits for its answer, then removes the frame', async () => {
    const { host: started, remote } = await ready();
    remote.answersTeardown = false;
    const frame = place.querySelector('iframe')!;

    const disposed = started.dispose();
    await until(() => expect(remote.sent('ui/resource-teardown')).toHaveLength(1));
    expect(frame.isConnected).toBe(true);

    remote.say({ jsonrpc: '2.0', id: remote.sent('ui/resource-teardown')[0].id, result: {} });
    await disposed;

    expect(frame.isConnected).toBe(false);
    expect(place.children).toHaveLength(0);
  });

  it('removes a view that does not answer, after a moment', async () => {
    vi.useFakeTimers();
    const started = start();
    const remote = await opened();
    remote.answersTeardown = false;
    remote.proxyReady();
    await remote.initialize();
    await vi.advanceTimersByTimeAsync(0);

    const disposed = started.dispose();
    await vi.advanceTimersByTimeAsync(1500);
    await disposed;

    expect(remote.sent('ui/resource-teardown')).toHaveLength(1);
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('sends no teardown to a view that never started', async () => {
    const started = start();
    const remote = await opened();
    remote.proxyReady();

    await started.dispose();

    expect(remote.sent('ui/resource-teardown')).toEqual([]);
    expect(place.querySelector('iframe')).toBeNull();
  });

  it('hears nothing after it ended, and answers no to what waited for the person', async () => {
    const { host: started, remote } = await ready();
    const waiting = remote.send('tools/call', { name: 'set-time' });
    await until(() => expect(started.getSnapshot().prompt).not.toBeNull());

    await started.dispose();
    const before = remote.posted.length;

    remote.send('tools/call', { name: 'get-time' });
    remote.proxyReady();
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(remote.posted).toHaveLength(before);
    expect(callTool).not.toHaveBeenCalled();
    expect(started.getSnapshot().prompt).toBeNull();
    // The view heard the no while it was still there.
    expect(remote.answered(waiting)?.error).toMatchObject({ message: 'The tool call was not allowed.' });
    // Ended is ended.
    expect(started.attach(place)).toBe(false);
    started.retry();
    expect(place.querySelector('iframe')).toBeNull();
  });
});

describe('a view that moves between blocks', () => {
  it('moves its frame in one step where the browser can, and keeps running', async () => {
    const { host: started, remote } = await ready(RUNNING);
    const frame = place.querySelector('iframe')!;
    const lot = document.createElement('div');
    const next = document.createElement('div');
    document.body.append(lot, next);
    const moves: Array<[Element, Node]> = [];
    // jsdom has no moveBefore, and an iframe it takes out of the page loses
    // its window. So the move is recorded and not made: what counts here is
    // that the host moves a loaded frame with moveBefore and with nothing else.
    const moveBefore = function (this: Element, node: Node) {
      moves.push([this, node]);
    };
    Object.assign(lot, { moveBefore });
    Object.assign(next, { moveBefore });

    expect(started.park(lot)).toBe(true);
    expect(started.attach(next)).toBe(true);

    expect(moves.map(([to]) => to)).toEqual([lot, next]);
    expect(moves.every(([, node]) => node === frame.parentElement)).toBe(true);
    expect(frame.isConnected).toBe(true);
    expect(document.querySelectorAll('iframe')).toHaveLength(1);
    expect(getResource).toHaveBeenCalledTimes(1);
    started.setToolCall({ input: { zone: 'UTC' }, result: RESULT, settled: true });
    await until(() => expect(remote.sent('ui/notifications/tool-result')).toHaveLength(1));
    lot.remove();
    await started.dispose();
    next.remove();
  });

  it('cannot park a loaded frame where the browser cannot move it', async () => {
    const { host: started } = await ready();
    const lot = document.createElement('div');
    document.body.appendChild(lot);

    expect(started.park(lot)).toBe(false);

    expect(place.querySelector('iframe')).not.toBeNull();
    lot.remove();
  });

  it('parks freely while it has no frame yet', async () => {
    getResource.mockReturnValue(new Promise(() => {}));
    const started = start();
    const lot = document.createElement('div');
    document.body.appendChild(lot);

    expect(started.park(lot)).toBe(true);
    expect(started.attach(place)).toBe(true);

    expect(getResource).toHaveBeenCalledTimes(1);
    lot.remove();
  });
});
