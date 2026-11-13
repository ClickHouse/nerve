import { describe, expect, it } from 'vitest';
import {
  NO_ORIGIN, PROXY_PRESSED, PROXY_REFUSED, SANDBOX_PATH, SANDBOX_TARGET, VIEW_SANDBOX, allowAttribute,
  grantedPermissions, isFromSandbox, isWithinFrame, namesHost, sandboxPressed, sandboxRefusal, sandboxUrl, sanitizeCsp,
} from './sandbox';

const CHAT = 'https://nerve.example:8900';

describe('sandboxUrl', () => {
  it('is a path of the chat\'s own address', () => {
    expect(sandboxUrl(null)).toBe('/mcp-app-sandbox.html');
    expect(sandboxUrl(null)).toBe(SANDBOX_PATH);
    for (const url of [sandboxUrl(null), sandboxUrl({ connectDomains: ['https://api.example.com'] })]) {
      expect(url.startsWith('/')).toBe(true);
      expect(url.startsWith('//')).toBe(false);
      // Wherever the chat is served, the frame asks the same address.
      expect(new URL(url, 'https://agent.example.net/chat/s-1').origin).toBe('https://agent.example.net');
      expect(new URL(url, 'http://localhost:8900/chat').origin).toBe('http://localhost:8900');
    }
  });

  it('carries the origins of the view as its only query', () => {
    const url = new URL(sandboxUrl({ connectDomains: ['https://api.example.com/a b?c=d&e'] }), CHAT);
    expect(url.pathname).toBe('/mcp-app-sandbox.html');
    expect([...url.searchParams.keys()]).toEqual(['csp']);
    expect(JSON.parse(url.searchParams.get('csp')!)).toEqual({ connectDomains: ['https://api.example.com/a b?c=d&e'] });
  });
});

describe('sanitizeCsp', () => {
  it('keeps the four lists of origins and nothing else', () => {
    const csp = {
      connectDomains: ['https://api.example.com', 'wss://live.example.com'],
      resourceDomains: ['https://*.cdn.example.com'],
      frameDomains: ['https://player.example.com'],
      baseUriDomains: ['https://cdn.example.com'],
      scriptDomains: ['https://evil.example.com'],
    };
    expect(sanitizeCsp(csp, CHAT)).toEqual({
      connectDomains: ['https://api.example.com', 'wss://live.example.com'],
      resourceDomains: ['https://*.cdn.example.com'],
      frameDomains: ['https://player.example.com'],
      baseUriDomains: ['https://cdn.example.com'],
    });
  });

  it.each([
    ["a quote, which could add 'unsafe-eval'", "https://a.example.com 'unsafe-eval'"],
    ['a quoted keyword', "'unsafe-inline'"],
    ['a semicolon, which would start a directive', 'https://a.example.com; script-src *'],
    ['a space, which would add a source', 'https://a.example.com https://b.example.com'],
    ['a line break', 'https://a.example.com\nscript-src *'],
    ['a carriage return', 'https://a.example.com\rX'],
    ['a tab', 'https://a.example.com\t*'],
    ['a comma, which would start a policy', 'https://a.example.com, default-src *'],
    ['a double quote', '"https://a.example.com"'],
    ['an empty string', ''],
  ])('drops an origin with %s', (_, hostile) => {
    expect(sanitizeCsp({ connectDomains: [hostile, 'https://ok.example.com'] }, CHAT))
      .toEqual({ connectDomains: ['https://ok.example.com'] });
  });

  it('drops what is not a string, and a list that is not a list', () => {
    const csp = { connectDomains: [7, null, { a: 1 }, 'https://ok.example.com'], resourceDomains: 'https://cdn.example.com' };
    expect(sanitizeCsp(csp as never, CHAT)).toEqual({ connectDomains: ['https://ok.example.com'] });
  });

  it('gives nothing when nothing is left, so the view gets the restrictive default', () => {
    expect(sanitizeCsp(null)).toBeNull();
    expect(sanitizeCsp(undefined)).toBeNull();
    expect(sanitizeCsp({})).toBeNull();
    expect(sanitizeCsp({ connectDomains: [] })).toBeNull();
    expect(sanitizeCsp({ connectDomains: ["'self' *"] })).toBeNull();
    expect(sanitizeCsp('https://a.example.com' as never)).toBeNull();
  });

  it('gives nothing for a policy too long for an address', () => {
    const many = Array.from({ length: 400 }, (_, i) => `https://host-${i}.example.com`);
    expect(sanitizeCsp({ connectDomains: many }, CHAT)).toBeNull();
  });

  it.each([
    'https://nerve.example:8900',       // the chat, its API and the sandbox page
    'https://nerve.example:8901',       // another port of the host
    'https://nerve.example:3000',       // anything else on the host
    'https://nerve.example',
    'https://nerve.example:*',
    'http://nerve.example:8900',
    'ws://nerve.example:8900',
    'wss://nerve.example:8900',
    'nerve.example:8900',
    'NERVE.example:8900',
    'https://nerve.example:8900/api/',
    'https://*.example:8900',
    '*:8900',
    'https://*:*',
    '*',
    'https:',
  ])('keeps the chat\'s own host out of every list: %s', (source) => {
    const lists = ['connectDomains', 'resourceDomains', 'frameDomains', 'baseUriDomains'] as const;
    for (const list of lists) {
      expect(sanitizeCsp({ [list]: [source, 'https://ok.example.com'] }, CHAT))
        .toEqual({ [list]: ['https://ok.example.com'] });
    }
  });

  it('on a loopback host, keeps out every loopback name', () => {
    const csp = {
      connectDomains: ['https://127.0.0.1:8900', 'http://localhost:3000', 'https://api.example.com'],
      frameDomains: ['https://app.localhost:8900', 'http://[::1]:8900'],
    };
    expect(sanitizeCsp(csp, 'https://localhost:8900')).toEqual({ connectDomains: ['https://api.example.com'] });
    // For a chat on a public name, localhost is the person's own machine: another host.
    expect(sanitizeCsp({ connectDomains: ['http://localhost:3000'] }, CHAT)).toEqual({ connectDomains: ['http://localhost:3000'] });
  });
});

describe('namesHost', () => {
  it.each([
    ['https://nerve.example:8900', true],
    ['https://nerve.example:8900/some/path', true],
    ['https://nerve.example', true],
    ['https://nerve.example:8901', true],
    ['wss://nerve.example:8900', true],
    ['NERVE.example:8900', true],
    ['https://*.example:8900', true],
    ['https://other.example:8900', false],
    ['https://*.other:8900', false],
    ['https://nerve.example.com', false],
    ['https://xnerve.example', false],
    ['https://*.nerve.example', false], // its subdomains, not the host itself
    ['*', true],
    ['https://*', true],
    ['*:*', true],
    ['data:', true], // not a host source: on the safe side
    ['not a source at all', true],
  ])('%s is on the host nerve.example: %s', (source, names) => {
    expect(namesHost(source, 'nerve.example')).toBe(names);
  });
});

describe('what a view is granted', () => {
  it('passes on writing to the clipboard, and nothing else', () => {
    expect(grantedPermissions({ clipboardWrite: {}, camera: {}, microphone: {}, geolocation: {} })).toEqual({ clipboardWrite: {} });
    expect(grantedPermissions({ camera: {}, microphone: {} })).toBeNull();
    expect(grantedPermissions(null)).toBeNull();
    expect(grantedPermissions(undefined)).toBeNull();
  });

  it('names the granted features in the allow attribute, for whatever document the frame holds', () => {
    // Without `*` the feature goes to the origin of the frame's address, and the proxy page there has none.
    expect(allowAttribute({ clipboardWrite: {} })).toBe('clipboard-write *');
    expect(allowAttribute(null)).toBe('');
    expect(allowAttribute({ camera: {}, microphone: {}, geolocation: {} })).toBe('');
  });

  it('gives the frame of a view scripts and forms, and no origin', () => {
    expect(VIEW_SANDBOX.split(' ').sort()).toEqual(['allow-forms', 'allow-scripts']);
  });
});

describe('isFromSandbox', () => {
  const frame = {} as MessageEventSource;
  const other = {} as MessageEventSource;

  it('takes a message of the frame, from a document without an origin', () => {
    expect(NO_ORIGIN).toBe('null');
    expect(isFromSandbox({ origin: 'null', source: frame }, frame)).toBe(true);
  });

  it.each([
    // "null" is the origin of every sandboxed frame: it names nobody.
    ['another window without an origin: a view that posts to the page itself', { origin: 'null', source: other }],
    ['a frame that a view made, without an origin', { origin: 'null', source: {} as MessageEventSource }],
    ['the frame while it holds a document of the chat\'s origin', { origin: CHAT, source: frame }],
    ['the frame while it holds a document of another origin', { origin: 'https://evil.example.com', source: frame }],
    ['the frame while it holds a document of another port of the host', { origin: 'https://nerve.example:8901', source: frame }],
    ['an origin that only looks like none', { origin: 'null ', source: frame }],
    ['no origin at all', { origin: '', source: frame }],
    ['no window', { origin: 'null', source: null }],
  ])('ignores a message from %s', (_, event) => {
    expect(isFromSandbox(event, frame)).toBe(false);
  });

  it('ignores every message while the frame has no window', () => {
    expect(isFromSandbox({ origin: 'null', source: null }, null)).toBe(false);
    expect(isFromSandbox({ origin: 'null', source: frame }, undefined)).toBe(false);
  });

  it('addresses the frame as whatever it holds: a document without an origin cannot be named', () => {
    expect(SANDBOX_TARGET).toBe('*');
  });
});

describe('sandboxRefusal', () => {
  const frame = {} as MessageEventSource;
  const other = {} as MessageEventSource;
  const refused = (reason: unknown = 'this page was served without its sandbox policy.') => (
    { jsonrpc: '2.0', method: PROXY_REFUSED, params: { reason } }
  );

  it('is the reason of a proxy page that has the chat\'s origin and runs nothing', () => {
    expect(sandboxRefusal({ origin: CHAT, source: frame, data: refused() }, frame, CHAT))
      .toBe('this page was served without its sandbox policy.');
  });

  it('is taken from a page without an origin too', () => {
    expect(sandboxRefusal({ origin: 'null', source: frame, data: refused('the embedding page is not the chat of this address.') }, frame, CHAT))
      .toBe('the embedding page is not the chat of this address.');
  });

  it('is cut to a line, and empty when the page gives no reason', () => {
    expect(sandboxRefusal({ origin: CHAT, source: frame, data: refused('x'.repeat(500)) }, frame, CHAT)).toHaveLength(200);
    expect(sandboxRefusal({ origin: CHAT, source: frame, data: refused(7) }, frame, CHAT)).toBe('');
    expect(sandboxRefusal({ origin: CHAT, source: frame, data: { jsonrpc: '2.0', method: PROXY_REFUSED } }, frame, CHAT)).toBe('');
  });

  it.each([
    ['another window', { origin: CHAT, source: other, data: refused() }],
    ['a window without an origin that is not the frame', { origin: 'null', source: other, data: refused() }],
    ['the frame while it holds a page of another origin', { origin: 'https://evil.example.com', source: frame, data: refused() }],
    ['the frame, as a request', { origin: CHAT, source: frame, data: { ...refused(), id: 1 } }],
    ['the frame, with another method', { origin: CHAT, source: frame, data: { jsonrpc: '2.0', method: 'ui/notifications/sandbox-proxy-ready', params: {} } }],
    ['the frame, with a tool call of the chat\'s origin', { origin: CHAT, source: frame, data: { jsonrpc: '2.0', id: 1, method: 'tools/call', params: {} } }],
    ['the frame, with no JSON-RPC', { origin: CHAT, source: frame, data: { method: PROXY_REFUSED, params: { reason: 'x' } } }],
    ['the frame, with text', { origin: CHAT, source: frame, data: 'refused' }],
    ['the frame, with nothing', { origin: CHAT, source: frame, data: null }],
  ])('is nothing for a message from %s', (_, event) => {
    expect(sandboxRefusal(event, frame, CHAT)).toBeNull();
  });

  it('is nothing while the frame has no window', () => {
    expect(sandboxRefusal({ origin: CHAT, source: null, data: refused() }, null, CHAT)).toBeNull();
  });
});

describe('sandboxPressed', () => {
  const frame = {} as MessageEventSource;
  const other = {} as MessageEventSource;
  const says = (pressed: unknown) => ({ jsonrpc: '2.0', method: PROXY_PRESSED, params: { pressed } });
  /** A message as a browser delivers it that adds its own word of a press: the word, or none on this message. */
  const vouched = (data: unknown, isActive: unknown) => (
    { origin: 'null', source: frame, data, userActivation: isActive === null ? null : { isActive } }
  );

  it('is a reserved word of the proxy page, which the page passes on to no view and from none', () => {
    expect(PROXY_PRESSED.startsWith('ui/notifications/sandbox-')).toBe(true);
  });

  it('is the proxy page\'s yes or no, in a browser that adds nothing of its own to a message', () => {
    expect(sandboxPressed({ origin: 'null', source: frame, data: says(true) }, frame)).toBe(true);
    expect(sandboxPressed({ origin: 'null', source: frame, data: says(false) }, frame)).toBe(false);
  });

  it('is a yes only with the browser\'s own word, where the browser gives one', () => {
    expect(sandboxPressed(vouched(says(true), true), frame)).toBe(true);
    // The page says yes and the browser says no, or says nothing on this message.
    expect(sandboxPressed(vouched(says(true), false), frame)).toBe(false);
    expect(sandboxPressed(vouched(says(true), null), frame)).toBe(false);
    expect(sandboxPressed(vouched(says(true), 'true'), frame)).toBe(false);
    // The browser's word alone is no yes: it is the page that was asked.
    expect(sandboxPressed(vouched(says(false), true), frame)).toBe(false);
  });

  it('is "unknown" where the page\'s browser cannot tell of a press', () => {
    expect(sandboxPressed({ origin: 'null', source: frame, data: says(null) }, frame)).toBe('unknown');
  });

  it('is a no for anything else the page says there', () => {
    for (const pressed of [undefined, 'true', 1, {}, []]) {
      expect(sandboxPressed({ origin: 'null', source: frame, data: says(pressed) }, frame)).toBe(false);
    }
    expect(sandboxPressed({ origin: 'null', source: frame, data: { jsonrpc: '2.0', method: PROXY_PRESSED } }, frame)).toBe(false);
  });

  it.each([
    ['another window without an origin: a view that posts to the page itself', { origin: 'null', source: other, data: says(true) }],
    ['the frame while it holds a document with an origin', { origin: CHAT, source: frame, data: says(true) }],
    ['the frame, as a request', { origin: 'null', source: frame, data: { ...says(true), id: 1 } }],
    ['the frame, with another method', { origin: 'null', source: frame, data: { jsonrpc: '2.0', method: 'ui/notifications/size-changed', params: { pressed: true } } }],
    ['the frame, not as JSON-RPC', { origin: 'null', source: frame, data: { method: PROXY_PRESSED, params: { pressed: true } } }],
    ['the frame, with no object at all', { origin: 'null', source: frame, data: 'pressed' }],
    ['the frame, with nothing', { origin: 'null', source: frame, data: null }],
  ])('is nothing from %s', (_, event) => {
    expect(sandboxPressed(event, frame)).toBeNull();
  });

  it('is nothing while the frame has no window', () => {
    expect(sandboxPressed({ origin: 'null', source: frame, data: says(true) }, null)).toBeNull();
  });
});

describe('isWithinFrame', () => {
  /** A window as another origin sees it: its parent can be asked, and the top of a page is its own parent. */
  type Fake = { parent: Fake };
  const page = {} as Fake;
  page.parent = page;
  const inside = (parent: Fake): Fake => ({ parent });
  const frame = inside(page);
  const as = (window: Fake) => window as unknown as MessageEventSource;

  it('is true for the frame\'s own window, the content\'s, and any frame the content made', () => {
    const content = inside(frame);
    const deep = inside(inside(inside(content)));

    expect(isWithinFrame(as(frame), as(frame))).toBe(true);
    expect(isWithinFrame(as(content), as(frame))).toBe(true);
    expect(isWithinFrame(as(deep), as(frame))).toBe(true);
  });

  it('is false for the page itself, another frame of the page and what is inside that one', () => {
    const other = inside(page);

    expect(isWithinFrame(as(page), as(frame))).toBe(false);
    expect(isWithinFrame(as(other), as(frame))).toBe(false);
    expect(isWithinFrame(as(inside(other)), as(frame))).toBe(false);
  });

  it('is false for what is no window, for no window at all, and while the frame has none', () => {
    expect(isWithinFrame({} as MessageEventSource, as(frame))).toBe(false);
    expect(isWithinFrame(null, as(frame))).toBe(false);
    expect(isWithinFrame(as(inside(frame)), null)).toBe(false);
    expect(isWithinFrame(as(inside(frame)), undefined)).toBe(false);
  });

  it('gives up on a chain of windows without an end, and on a window that cannot be asked', () => {
    const a = {} as Fake;
    const b = { parent: a } as Fake;
    a.parent = b;
    const gone = { get parent(): Fake { throw new DOMException('gone', 'SecurityError'); } } as Fake;

    expect(isWithinFrame(as(a), as(frame))).toBe(false);
    expect(isWithinFrame(as(gone), as(frame))).toBe(false);
  });
});
