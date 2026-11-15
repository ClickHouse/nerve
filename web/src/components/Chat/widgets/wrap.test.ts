import { describe, expect, it, vi } from 'vitest';
import { MAX_WIDGET_DATA_CHARS, MAX_WIDGET_DATA_KEYS, type WidgetItems } from './storage';
import {
  HOST_CONTEXT_CHANGED, SIZE_CHANGED, STORAGE_CHANGED, STORAGE_REPORT_MS, WIDGET_POLICY, WIDGET_SANDBOX,
  widgetHead, wrapWidget, type WidgetLook,
} from './wrap';

/**
 * The document of a widget, as the chat hands it to the sandbox frame. The
 * tests read it the way a browser does (the parser of jsdom follows the same
 * rules), and run the head's script in a world the test makes.
 */

const LOOK: WidgetLook = {
  theme: 'dark',
  variables: {
    '--color-text-primary': 'light-dark(#1c1f26, #e8eaed)',
    '--color-background-primary': 'light-dark(#f6f7f9, #22252b)',
    '--font-sans': 'Inter, ui-sans-serif, "Segoe UI", sans-serif',
  },
};

const HEAD = widgetHead(LOOK);
const HEAD_SCRIPT = /<script>([\s\S]*?)<\/script>/.exec(HEAD)![1];
const BASE_STYLE = /<style>([\s\S]*?)<\/style>/.exec(HEAD)![1];

const parse = (html: string) => new DOMParser().parseFromString(html, 'text/html');

/** What a browser makes of the document: that Nerve's head leads it, whole. */
function expectHeadIntact(html: string) {
  const doc = parse(html);
  // Standards mode: the one document type, Nerve's, is first.
  expect(html.startsWith('<!doctype html>')).toBe(true);
  expect(doc.compatMode).toBe('CSS1Compat');
  const [charset, policy, style, script] = [...doc.head.children];
  expect(charset.getAttribute('charset')).toBe('utf-8');
  expect(policy.getAttribute('http-equiv')).toBe('Content-Security-Policy');
  expect(policy.getAttribute('content')).toBe(WIDGET_POLICY);
  expect(style.localName).toBe('style');
  expect(style.textContent).toBe(BASE_STYLE);
  expect(script.localName).toBe('script');
  expect(script.textContent).toBe(HEAD_SCRIPT);
  return doc;
}

/** The same for a widget that saved something: the head's script is the one with that data. */
function expectHeadWith(html: string, saved: WidgetItems) {
  const doc = parse(html);
  const [, , style, script] = [...doc.head.children];
  expect(style.textContent).toBe(BASE_STYLE);
  expect(script.localName).toBe('script');
  expect(script.textContent).toBe(parse(widgetHead(LOOK, saved)).head.querySelector('script')!.textContent);
  // All of the script is there: what stands before the data, and what stands after it.
  const [before, after] = HEAD_SCRIPT.split('var saved = [];');
  expect(script.textContent!.startsWith(`${before}var saved = [`)).toBe(true);
  expect(script.textContent!.endsWith(`];${after}`)).toBe(true);
}

describe('wrapWidget', () => {
  it('puts the head in front of a fragment, which becomes the body', () => {
    const doc = expectHeadIntact(wrapWidget('<div id="w">hello</div><script>window.ran = 1</script>', LOOK));
    expect(doc.head.children).toHaveLength(4);
    expect(doc.body.querySelector('#w')?.textContent).toBe('hello');
    expect(doc.body.querySelector('script')?.textContent).toBe('window.ran = 1');
  });

  it('puts the head first in the head of a whole document, and keeps what is on its <html>', () => {
    const source = '<!DOCTYPE html>\n<html lang="de" class="app"><head><meta charset="utf-8"><title>T</title>'
      + '<style>p { color: red }</style></head><body class="b"><p>text</p></body></html>';
    const html = wrapWidget(source, LOOK);
    // One document type: the source's own is dropped.
    expect(html.match(/<!doctype/gi)).toHaveLength(1);
    const doc = expectHeadIntact(html);
    expect(doc.documentElement.getAttribute('lang')).toBe('de');
    expect(doc.documentElement.getAttribute('class')).toBe('app');
    // The source's own head follows in the same head.
    expect(doc.head.querySelector('title')?.textContent).toBe('T');
    expect([...doc.head.querySelectorAll('style')].map(s => s.textContent)).toEqual([BASE_STYLE, 'p { color: red }']);
    expect(doc.body.className).toBe('b');
    expect(doc.body.querySelector('p')?.textContent).toBe('text');
  });

  it('drops a leading document type whatever its case and the blank space before it', () => {
    for (const doctype of ['<!doctype html>', '<!DOCTYPE HTML>', '\n  <!DocType html>', '﻿<!doctype html>']) {
      const html = wrapWidget(`${doctype}<p>x</p>`, LOOK);
      expect(html.match(/<!doctype/gi)).toHaveLength(1);
      expect(html.endsWith('</script><p>x</p>')).toBe(true);
    }
  });

  it('puts nothing after the source', () => {
    const source = '<div>last</div>';
    expect(wrapWidget(source, LOOK).endsWith(source)).toBe(true);
    expect(wrapWidget('', LOOK)).toBe(HEAD);
  });

  it.each([
    ['closes a script that it never opened', '</script><script>window.evil = 1</script>'],
    ['closes a style that it never opened', '</style><style>* { display: none }</style>'],
    ['closes the head and the document', '</head></html><p>after</p>'],
    ['opens a comment and never closes it', '<p>before</p><!-- <p>swallowed'],
    ['opens a script and never closes it', '<script>var swallowed = "'],
    ['is plain text from here on', '<plaintext><p>raw'],
    ['opens a title and never closes it', '<title>the rest is a title'],
    ['opens a textarea and never closes it', '<textarea>the rest is a value'],
    ['starts with a script before its own <html>', '<script>window.first = 1</script><html><head><base href="https://evil.example/"></head>'],
    ['starts with a comment before its document type', '<!-- hi --><!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"><p>x</p>'],
    ['brings a policy of its own', '<meta http-equiv="Content-Security-Policy" content="default-src *; script-src * \'unsafe-inline\'"><p>x</p>'],
    ['is a frameset document', '<html><frameset><frame src="https://evil.example/"></frameset></html>'],
    ['closes tags Nerve never opened', '</body></div></span></noscript></template></svg></math><p>x</p>'],
    ['is the head again', widgetHead({ theme: 'light', variables: {} })],
  ])('keeps its head whole and first when the source %s', (_what, source) => {
    expectHeadIntact(wrapWidget(source, LOOK));
  });

  it('hands the source on as it is, character for character', () => {
    const source = '<p title="a&b">1 < 2 && 3 > 2 </script> ${x} `tick` \\n \u2028 &amp;</p>';
    expect(wrapWidget(source, LOOK).slice(HEAD.length)).toBe(source);
  });
});

describe('the base styles', () => {
  it('give the theme to the root, and the variables with it', () => {
    expect(BASE_STYLE.startsWith(':root{color-scheme:dark;')).toBe(true);
    expect(BASE_STYLE).toContain('--color-text-primary:light-dark(#1c1f26, #e8eaed);');
    expect(BASE_STYLE).toContain('--font-sans:Inter, ui-sans-serif, "Segoe UI", sans-serif;');
    expect(widgetHead({ ...LOOK, theme: 'light' })).toContain(':root{color-scheme:light;');
  });

  it('take any theme that is not light for dark, never a word of their own', () => {
    const odd = widgetHead({ theme: 'light;}</style><script>' as never, variables: {} });
    expect(odd).toContain(':root{color-scheme:dark;');
    expect(parse(odd).head.querySelectorAll('script')).toHaveLength(1);
  });

  it('make an unstyled widget look like the chat: no background, the chat\'s text colour and font', () => {
    expect(BASE_STYLE).toContain(':where(html){background:transparent;}');
    expect(BASE_STYLE).toMatch(/:where\(body\)\{[^}]*background:transparent;[^}]*color:var\(--color-text-primary\);[^}]*font-family:var\(--font-sans\);/);
  });

  it('weigh nothing, so that any rule of the widget wins', () => {
    // Every selector but the root's own (and the pseudo-elements, which
    // `:where()` cannot hold) is inside `:where()`. The rule for `hidden`
    // is the one that is meant to win.
    const HIDDEN = '[hidden]:where(:not([hidden="until-found"]))';
    const selectors = BASE_STYLE.replace(/\{[^}]*\}/g, '\n').split('\n').filter(Boolean);
    expect(selectors.length).toBeGreaterThan(10);
    expect(selectors.filter(selector => selector === HIDDEN)).toHaveLength(1);
    for (const selector of selectors) {
      expect(
        selector === ':root' || selector === '::before,::after' || selector === HIDDEN || /^(:where\([^{]*\))+$/.test(selector),
        selector,
      ).toBe(true);
    }
  });

  it('keep what a widget hides hidden, whatever display its own rules give it', () => {
    expect(BASE_STYLE).toContain('[hidden]:where(:not([hidden="until-found"])){display:none !important;}');
  });

  it.each([
    ['a value that ends the style element', 'red;}</style><script>alert(1)</script>'],
    ['a value that ends the declaration', 'red; background: url(https://evil.example/)'],
    ['a value that ends the rule', 'red } body { display: none'],
    ['a value with a tag in it', '<b>'],
    ['an empty value', ''],
  ])('leave out a variable with %s', (_what, value) => {
    const head = widgetHead({ theme: 'dark', variables: { '--color-text-primary': value, '--ok': 'blue' } });
    expect(head).not.toContain('--color-text-primary:');
    expect(head).toContain('--ok:blue;');
    expect(parse(head).head.querySelectorAll('script')).toHaveLength(1);
  });

  it('leave out what is no custom property', () => {
    const head = widgetHead({
      theme: 'dark',
      variables: { color: 'red', '--a b': 'red', '--x:y': 'red', '-single': 'red', '--fine_1': 'red', '--absent': undefined },
    });
    const root = /^:root\{([^}]*)\}/.exec(/<style>([\s\S]*?)<\/style>/.exec(head)![1])![1];
    expect(root).toBe('color-scheme:dark;--fine_1:red;accent-color:var(--color-text-info);');
  });
});

describe('the policy of a widget', () => {
  it('names no address at all', () => {
    expect(WIDGET_POLICY).not.toMatch(/https?:|wss?:|\*|'self'/);
    const directives = Object.fromEntries(WIDGET_POLICY.split('; ').map(d => [d.split(' ')[0], d.split(' ').slice(1).join(' ')]));
    expect(directives).toEqual({
      'default-src': "'none'",
      'script-src': "'unsafe-inline' 'unsafe-eval' blob: data:",
      'style-src': "'unsafe-inline'",
      'img-src': 'data: blob:',
      'media-src': 'data: blob:',
      'font-src': 'data:',
      'connect-src': "'none'",
      'worker-src': 'blob:',
      'frame-src': "'none'",
      'object-src': "'none'",
      'base-uri': "'none'",
      'form-action': "'none'",
    });
  });

  it('fits into the attribute it is written into', () => {
    expect(WIDGET_POLICY).not.toMatch(/["<>]/);
  });

  it('lets the frame of a widget run scripts and nothing else', () => {
    expect(WIDGET_SANDBOX).toBe('allow-scripts');
  });
});

/**
 * The head's script in a world of its own: a document of jsdom that belongs
 * to no window, and a window and a parent that the test makes.
 */
function world({
  height = 123.4, bodyScroll = 0, innerHeight = 500, clientHeight = 500, saved = [] as WidgetItems,
  limit = MAX_WIDGET_DATA_CHARS,
} = {}) {
  const doc = document.implementation.createHTMLDocument('widget');
  const size = { height, bodyScroll, innerHeight, clientHeight };
  const root = doc.documentElement;
  root.getBoundingClientRect = () => ({ width: 640.2, height: size.height }) as DOMRect;
  Object.defineProperty(root, 'clientHeight', { get: () => size.clientHeight });
  Object.defineProperty(doc.body, 'scrollHeight', { get: () => size.bodyScroll });
  const parent = { postMessage: vi.fn() };
  // What the script listens for on the document, kept as well: a test cannot
  // make an event that the browser calls trusted, but it can call a listener.
  const onDocument: Record<string, Array<(event: unknown) => void>> = {};
  const listen = doc.addEventListener.bind(doc);
  doc.addEventListener = ((type: string, listener: EventListener, options?: boolean) => {
    (onDocument[type] ??= []).push(listener as (event: unknown) => void);
    listen(type, listener, options);
  }) as typeof doc.addEventListener;
  const listeners: Record<string, Array<(event: unknown) => void>> = {};
  const observers: Array<() => void> = [];
  const timers: Array<{ run: () => void; delay: number }> = [];
  const win = {
    get innerHeight() { return size.innerHeight; },
    addEventListener: (type: string, listener: (event: unknown) => void) => {
      (listeners[type] ??= []).push(listener);
    },
  } as { innerHeight: number; addEventListener: unknown; localStorage?: Storage; sessionStorage?: Storage };
  // The script as a browser reads it out of the document, with what the widget saved in it.
  const script = saved.length === 0 && limit === MAX_WIDGET_DATA_CHARS
    ? HEAD_SCRIPT
    : parse(widgetHead(LOOK, saved, limit)).head.querySelector('script')!.textContent!;
  class FakeResizeObserver {
    private readonly callback: () => void;
    constructor(callback: () => void) { this.callback = callback; }
    observe(target: Element) {
      expect(target).toBe(root);
      observers.push(this.callback);
    }
  }
  new Function('window', 'document', 'parent', 'ResizeObserver', 'setTimeout', 'SubmitEvent', 'Event', script)(
    win, doc, parent, FakeResizeObserver, (run: () => void, delay = 0) => { timers.push({ run, delay }); },
    doc.defaultView?.SubmitEvent ?? SubmitEvent, Event,
  );
  return {
    doc, parent, size, timers,
    /** The widget's `localStorage` and `sessionStorage`, as its own script finds them. */
    local: win.localStorage!,
    brief: win.sessionStorage!,
    /** What the script told the chat that the `localStorage` holds, oldest first. */
    told: () => parent.postMessage.mock.calls
      .map(([message, target]) => ({ message: message as { method: string; params: { items: WidgetItems } }, target }))
      .filter(({ message }) => message.method === STORAGE_CHANGED),
    /** The content changed size. */
    resize: () => observers.forEach(run => run()),
    /** A message event reaches the widget's window. */
    message: (data: unknown, source: unknown = parent) => (listeners.message ?? []).forEach(run => run({ data, source })),
    /** An event of the window itself: `error`, `unhandledrejection`, `load`. */
    fire: (type: string, event: unknown = {}) => (listeners[type] ?? []).forEach(run => run(event)),
    /** Run what the script scheduled. */
    tick: () => timers.splice(0).forEach(timer => timer.run()),
    /** The browser's own event of `type` reaches the document: one it calls trusted. */
    trusted: (type: string) => (onDocument[type] ?? []).forEach(run => run({ type, isTrusted: true })),
    reports: () => parent.postMessage.mock.calls.map(([message, target]) => ({ ...(message as { params: object }).params, method: (message as { method: string }).method, target })),
  };
}

describe('the head\'s script: the height', () => {
  it('reports the height of the content, rounded up, to its parent', () => {
    const w = world({ height: 123.4 });
    w.resize();
    expect(w.parent.postMessage).toHaveBeenCalledWith(
      { jsonrpc: '2.0', method: SIZE_CHANGED, params: { width: 641, height: 124 } }, '*',
    );
  });

  it('reports again only when the height changed', () => {
    const w = world({ height: 100 });
    w.resize();
    w.resize();
    w.fire('load');
    expect(w.reports()).toHaveLength(1);
    w.size.height = 80;
    w.resize();
    expect(w.reports().map(r => (r as unknown as { height: number }).height)).toEqual([100, 80]);
  });

  it('adds what overflows a body of a fixed height, and the room of a horizontal scrollbar', () => {
    const w = world({ height: 200, bodyScroll: 450, innerHeight: 200, clientHeight: 185 });
    w.resize();
    expect(w.reports()[0]).toMatchObject({ height: 465 });
  });
});

describe('the head\'s script: the theme', () => {
  const change = (params: unknown) => ({ jsonrpc: '2.0', method: HOST_CONTEXT_CHANGED, params });

  it('takes the theme and the variables the chat sends', () => {
    const w = world();
    w.message(change({ theme: 'light', styles: { variables: { '--color-text-primary': 'black', '--font-sans': 'Inter' } } }));
    const root = w.doc.documentElement;
    expect(root.style.colorScheme).toBe('light');
    expect(root.style.getPropertyValue('--color-text-primary')).toBe('black');
    expect(root.style.getPropertyValue('--font-sans')).toBe('Inter');
    w.message(change({ theme: 'dark' }));
    expect(root.style.colorScheme).toBe('dark');
  });

  it('takes nothing that is no theme and no custom property', () => {
    const w = world();
    w.message(change({ theme: 'purple', styles: { variables: { color: 'red', '--n': 7 } } }));
    const root = w.doc.documentElement;
    expect(root.style.colorScheme).toBe('');
    expect(root.style.color).toBe('');
    expect(root.style.getPropertyValue('--n')).toBe('');
  });

  it('takes it from its parent only, and only this one message', () => {
    const w = world();
    w.message(change({ theme: 'light' }), { postMessage: vi.fn() });
    w.message({ jsonrpc: '2.0', method: 'ui/notifications/tool-input', params: { theme: 'light' } });
    w.message(null);
    w.message('host-context-changed');
    expect(w.doc.documentElement.style.colorScheme).toBe('');
  });
});

describe('the head\'s script: links', () => {
  function click(w: ReturnType<typeof world>, html: string, selector: string) {
    w.doc.body.innerHTML = html;
    const event = new Event('click', { bubbles: true, cancelable: true });
    w.doc.body.querySelector(selector)!.dispatchEvent(event);
    return event;
  }

  it.each([
    ['an outside address', '<a href="https://example.com/">x</a>'],
    ['an address of the document itself', '<a href="">x</a>'],
    ['a script', '<a href="javascript:void 0">x</a>'],
    ['a place in the document', '<a href="#there">x</a><p id="there">y</p>'],
    ['a bare #', '<a href="#">x</a>'],
  ])('lets no link to %s navigate', (_what, html) => {
    expect(click(world(), html, 'a').defaultPrevented).toBe(true);
  });

  it('counts a click on what is inside a link', () => {
    expect(click(world(), '<a href="https://example.com/"><b>x</b></a>', 'b').defaultPrevented).toBe(true);
  });

  it('scrolls to the place a link names in the document', () => {
    const w = world();
    w.doc.body.innerHTML = '<a href="#there">x</a><p id="there">y</p>';
    const there = w.doc.getElementById('there')!;
    there.scrollIntoView = vi.fn();
    w.doc.querySelector('a')!.dispatchEvent(new Event('click', { bubbles: true, cancelable: true }));
    expect(there.scrollIntoView).toHaveBeenCalledWith({ block: 'nearest' });
  });

  it('leaves every other click alone', () => {
    expect(click(world(), '<button>x</button><a>no href</a>', 'button').defaultPrevented).toBe(false);
    expect(click(world(), '<a>no href</a>', 'a').defaultPrevented).toBe(false);
  });
});

describe('the head\'s script: forms', () => {
  /** A form in the widget; `submits` counts the submit events its handler gets. */
  function form(w: ReturnType<typeof world>, html: string) {
    w.doc.body.innerHTML = html;
    const element = w.doc.querySelector('form')!;
    const submits: Event[] = [];
    element.addEventListener('submit', (event) => {
      submits.push(event);
      event.preventDefault();
    });
    return { element, submits };
  }
  const click = (target: Element) => target.dispatchEvent(new Event('click', { bubbles: true, cancelable: true }));
  const enter = (target: Element) => target.dispatchEvent(Object.assign(new Event('keydown', { bubbles: true, cancelable: true }), { key: 'Enter' }));

  it('fires submit for a click on the form\'s button, which the frame\'s sandbox would swallow', () => {
    const w = world();
    const { element, submits } = form(w, '<form><input name="a"><button id="go">Go</button></form>');
    click(element.querySelector('#go')!);
    expect(submits).toHaveLength(0);
    w.tick();
    expect(submits).toHaveLength(1);
    expect(submits[0].cancelable).toBe(true);
    expect(submits[0].bubbles).toBe(true);
  });

  it.each([
    ['<form><button type="submit" id="b">Go</button></form>', 1],
    ['<form><input type="submit" id="b" value="Go"></form>', 1],
    ['<form><button id="b"><span>Go</span></button></form>', 1],
    ['<form><button type="button" id="b">Go</button></form>', 0],
    ['<form><button type="reset" id="b">Go</button></form>', 0],
    ['<form><button id="b" disabled>Go</button></form>', 0],
    ['<form><input type="button" id="b" value="Go"></form>', 0],
  ])('for %s fires %i submit', (html, count) => {
    const w = world();
    const { element, submits } = form(w, html);
    click(element.querySelector('#b span') ?? element.querySelector('#b')!);
    w.tick();
    expect(submits).toHaveLength(count);
  });

  it('fires submit for Enter in a field', () => {
    const w = world();
    const { element, submits } = form(w, '<form><input id="a" type="text"><input id="c" type="checkbox"></form>');
    enter(element.querySelector('#a')!);
    w.tick();
    expect(submits).toHaveLength(1);
    // Enter on a checkbox ticks it; it submits nothing.
    enter(element.querySelector('#c')!);
    w.tick();
    expect(submits).toHaveLength(1);
  });

  it('fires one submit for Enter and the click on the button that the browser makes of it', () => {
    const w = world();
    const { element, submits } = form(w, '<form><input id="a"><button id="go">Go</button></form>');
    enter(element.querySelector('#a')!);
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(1);
    // The next press submits again.
    enter(element.querySelector('#a')!);
    w.tick();
    expect(submits).toHaveLength(2);
  });

  it('fires nothing when the click was cancelled by the widget', () => {
    const w = world();
    const { element, submits } = form(w, '<form><button id="go">Go</button></form>');
    element.querySelector('#go')!.addEventListener('click', event => event.preventDefault());
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(0);
  });

  it('fires nothing when the browser fired the submit itself', () => {
    const w = world();
    const { element, submits } = form(w, '<form><button id="go">Go</button></form>');
    click(element.querySelector('#go')!);
    // In a frame that may submit forms, the browser fires the event itself,
    // right after the click. The widget's handler has run then.
    w.trusted('submit');
    w.tick();
    expect(submits).toHaveLength(0);
    // Without it, the same click does fire one.
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(1);
  });

  it('does not submit a form with an empty required field', () => {
    const w = world();
    const { element, submits } = form(w, '<form><input id="a" required><button id="go">Go</button></form>');
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(0);
    (element.querySelector('#a') as HTMLInputElement).value = 'x';
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(1);
  });

  it('submits a form that asks for no validation as it is', () => {
    const w = world();
    const { element, submits } = form(w, '<form novalidate><input required><button id="go">Go</button></form>');
    click(element.querySelector('#go')!);
    w.tick();
    expect(submits).toHaveLength(1);
  });
});

describe('the head\'s script: storage', () => {
  const TICKS: WidgetItems = [['ticks', '["tent","stove"]'], ['tab', 'gear']];
  /** What the storage holds, read the way a script reads it. */
  const entries = (storage: Storage) => Array.from({ length: storage.length }, (_, i) => [storage.key(i)!, storage.getItem(storage.key(i)!)!]);

  it('gives the widget a localStorage and a sessionStorage, which the browser gives this frame not', () => {
    const w = world();
    expect(w.local.length).toBe(0);
    expect(w.local.getItem('ticks')).toBeNull();
    expect(w.local.key(0)).toBeNull();
    expect(w.brief.length).toBe(0);
    expect(w.local).not.toBe(w.brief);
  });

  it('starts with what the widget saved before, from the first line of the widget\'s script', () => {
    const w = world({ saved: TICKS });
    expect(w.local.length).toBe(2);
    expect(w.local.getItem('ticks')).toBe('["tent","stove"]');
    expect(w.local.getItem('tab')).toBe('gear');
    expect([w.local.key(0), w.local.key(1), w.local.key(2)]).toEqual(['ticks', 'tab', null]);
    // The session's storage has no past.
    expect(w.brief.length).toBe(0);
    // Reading tells the chat nothing.
    expect(w.timers).toEqual([]);
  });

  it('sets, reads, removes and clears as the browser\'s own does', () => {
    const w = world();
    w.local.setItem('a', '1');
    w.local.setItem('b', '2');
    w.local.setItem('a', '3');
    expect(entries(w.local)).toEqual([['a', '3'], ['b', '2']]);
    expect(w.local.getItem('missing')).toBeNull();
    w.local.removeItem('a');
    w.local.removeItem('missing');
    expect(entries(w.local)).toEqual([['b', '2']]);
    w.local.clear();
    expect(w.local.length).toBe(0);
    expect(w.local.key(0)).toBeNull();
    expect(w.local.key(-1)).toBeNull();
  });

  it('keeps texts: whatever is set is read back as its text', () => {
    const w = world();
    const set = w.local.setItem.bind(w.local) as (key: unknown, value: unknown) => void;
    set('n', 5);
    set('none', null);
    set('list', [1, 2]);
    set(7, true);
    set('object', { a: 1 });
    expect(entries(w.local)).toEqual([
      ['n', '5'], ['none', 'null'], ['list', '1,2'], ['7', 'true'], ['object', '[object Object]'],
    ]);
  });

  it('has its items as named properties too', () => {
    const w = world({ saved: TICKS });
    const named = w.local as unknown as Record<string, string | undefined>;
    expect(named.tab).toBe('gear');
    expect(named.missing).toBeUndefined();
    named.theme = 'dark';
    expect(w.local.getItem('theme')).toBe('dark');
    expect('theme' in w.local).toBe(true);
    expect('missing' in w.local).toBe(false);
    expect(Object.keys(w.local)).toEqual(['ticks', 'tab', 'theme']);
    expect(JSON.parse(JSON.stringify(w.local))).toEqual({ ticks: '["tent","stove"]', tab: 'gear', theme: 'dark' });
    delete named.tab;
    expect(w.local.getItem('tab')).toBeNull();
    expect(w.local.length).toBe(2);
    // An item cannot take the place of a method.
    w.local.setItem('getItem', 'x');
    w.local.setItem('length', 'y');
    expect(typeof w.local.getItem).toBe('function');
    expect(w.local.getItem('getItem')).toBe('x');
    expect(w.local.length).toBe(4);
  });

  it('is a Storage, as far as a script can tell', () => {
    const w = world();
    expect(w.local instanceof Storage).toBe(true);
    expect(Object.prototype.toString.call(w.local)).toBe('[object Storage]');
    expect(String(w.local)).toBe('[object Storage]');
  });

  it('tells the chat what it holds after a change: once for all that one script does at a time', () => {
    const w = world({ saved: TICKS });
    w.local.setItem('ticks', '["tent"]');
    w.local.setItem('count', '1');
    w.local.removeItem('tab');
    expect(w.told()).toEqual([]);
    w.tick();
    expect(w.told()).toEqual([{
      message: { jsonrpc: '2.0', method: STORAGE_CHANGED, params: { items: [['ticks', '["tent"]'], ['count', '1']] } },
      target: '*',
    }]);
  });

  it('tells the chat when the widget has cleared everything', () => {
    const w = world({ saved: TICKS });
    w.local.clear();
    w.tick();
    expect(w.told().map(t => t.message.params.items)).toEqual([[]]);
  });

  it('tells the chat nothing for what changes nothing', () => {
    const w = world({ saved: TICKS });
    w.local.setItem('tab', 'gear');
    w.local.removeItem('missing');
    w.local.getItem('tab');
    w.brief.clear();
    expect(w.timers).toEqual([]);
  });

  it('tells the chat at most every STORAGE_REPORT_MS: at once after a quiet time, later when it has just told', () => {
    const w = world();
    w.local.setItem('n', '1');
    expect(w.timers.map(timer => timer.delay)).toEqual([0]);
    w.tick();
    w.local.setItem('n', '2');
    w.local.setItem('n', '3');
    expect(w.timers).toHaveLength(1);
    expect(w.timers[0].delay).toBeGreaterThan(STORAGE_REPORT_MS - 50);
    expect(w.timers[0].delay).toBeLessThanOrEqual(STORAGE_REPORT_MS);
    w.tick();
    expect(w.told().map(t => t.message.params.items)).toEqual([[['n', '1']], [['n', '3']]]);
  });

  it('throws the browser\'s own error for more than one widget may keep, and keeps what it had', () => {
    const w = world();
    w.local.setItem('a', 'x'.repeat(MAX_WIDGET_DATA_CHARS - 1));
    let thrown: unknown;
    try {
      w.local.setItem('b', '');
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(DOMException);
    expect((thrown as DOMException).name).toBe('QuotaExceededError');
    expect((thrown as DOMException).code).toBe(22);
    expect(w.local.getItem('b')).toBeNull();
    expect(w.local.length).toBe(1);
    // A shorter text in the place of the long one has room, and then the other key has too.
    w.local.setItem('a', 'short');
    w.local.setItem('b', 'y'.repeat(MAX_WIDGET_DATA_CHARS - 7));
    expect(() => w.local.setItem('b', 'y'.repeat(MAX_WIDGET_DATA_CHARS - 6))).toThrow(/exceeded the quota/);
    expect(w.local.getItem('b')).toHaveLength(MAX_WIDGET_DATA_CHARS - 7);
    // What it tells the chat is never more than the chat takes.
    w.tick();
    const told = w.told().at(-1)!.message.params.items;
    expect(told.reduce((sum, [key, text]) => sum + key.length + text.length, 0)).toBe(MAX_WIDGET_DATA_CHARS);
  });

  it('may keep as many characters as the chat gives it, when that is less', () => {
    const w = world({ limit: 100 });
    w.local.setItem('a', 'x'.repeat(99));
    expect(() => w.local.setItem('b', '')).toThrow(/exceeded the quota/);
    w.local.setItem('a', 'x'.repeat(98));
    w.local.setItem('b', '');
    expect(w.local.length).toBe(2);
    // The session's storage is the frame's own: it has the whole of a widget's room.
    w.brief.setItem('a', 'x'.repeat(1000));
  });

  it('may keep nothing in a chat that has no room left', () => {
    const w = world({ limit: 0 });
    let thrown: unknown;
    try {
      w.local.setItem('', '');
      w.local.setItem('a', '');
    } catch (error) {
      thrown = error;
    }
    expect((thrown as DOMException).name).toBe('QuotaExceededError');
    expect(w.local.getItem('a')).toBeNull();
    w.tick();
    expect(w.told().at(-1)!.message.params.items).toEqual([['', '']]);
  });

  it('has as many keys as a widget may have, and the next one throws the same error', () => {
    const w = world();
    for (let n = 0; n < MAX_WIDGET_DATA_KEYS; n++) w.local.setItem(`k${n}`, '');
    expect(w.local.length).toBe(1024);
    expect(() => w.local.setItem('one more', '')).toThrow(/exceeded the quota/);
    // A key that is there can still change, and one that goes makes room.
    w.local.setItem('k7', 'changed');
    w.local.removeItem('k8');
    w.local.setItem('one more', '');
    expect(w.local.length).toBe(1024);
  });

  it('counts what was saved before toward what the widget may keep', () => {
    const w = world({ saved: [['a', 'x'.repeat(MAX_WIDGET_DATA_CHARS - 1)]] });
    expect(() => w.local.setItem('b', '')).toThrow(/exceeded the quota/);
    w.local.removeItem('a');
    w.local.setItem('b', '');
    expect(w.local.length).toBe(1);
  });

  it('gives a sessionStorage that works the same, and of which the chat hears nothing', () => {
    const w = world({ saved: TICKS });
    w.brief.setItem('step', '3');
    (w.brief as unknown as Record<string, string>).tab = 'food';
    expect(entries(w.brief)).toEqual([['step', '3'], ['tab', 'food']]);
    expect(w.local.getItem('step')).toBeNull();
    expect(w.local.getItem('tab')).toBe('gear');
    expect(() => w.brief.setItem('big', 'x'.repeat(MAX_WIDGET_DATA_CHARS))).toThrow(/exceeded the quota/);
    expect(w.timers).toEqual([]);
    expect(w.parent.postMessage).not.toHaveBeenCalled();
  });
});

describe('what a widget saved, in the head', () => {
  const HOSTILE: WidgetItems = [
    ['close', '</script><script>window.evil = 1</script>'],
    ['</script>', 'a key that closes the script'],
    ['comment', '<!-- <script> never closed'],
    ['end of comment', '--> ]]> </style></head></html>'],
    ['lines', 'one\u2028two\u2029three\nfour\r\nfive'],
    ['nothing', '\u0000\u0001\u001f'],
    ['half a pair', '\ud83d and \udc00'],
    ['quotes', '"\'`\\ \\" \\u0041 ${evil} \\x3c/script\\x3e'],
    ['patterns of replace', '$& $1 $\' $` $$ /*saved*/[]'],
    ['__proto__', '{"polluted":true}'],
    ['entity', '&lt;/script&gt; &amp; &#60;/script&#62;'],
    ['long', '</script>'.repeat(6000)],
  ];

  it('stands in the head as data: the head is whole, and the widget reads back exactly what it saved', () => {
    const doc = parse(wrapWidget('<p id="w">widget</p>', LOOK, HOSTILE));
    // Nothing of the data became an element: the head has Nerve's four, the body the widget's one.
    expect([...doc.head.children].map(element => element.localName)).toEqual(['meta', 'meta', 'style', 'script']);
    expect([...doc.body.children].map(element => element.localName)).toEqual(['p']);
    expect(doc.querySelectorAll('script')).toHaveLength(1);
    const w = world({ saved: HOSTILE });
    expect(Array.from({ length: w.local.length }, (_, i) => [w.local.key(i), w.local.getItem(w.local.key(i)!)])).toEqual(HOSTILE);
    expect((globalThis as { evil?: unknown }).evil).toBeUndefined();
    expect(({} as { polluted?: unknown }).polluted).toBeUndefined();
  });

  it('holds no character there that could end the script or a line of it', () => {
    const script = /<script>([\s\S]*)$/.exec(widgetHead(LOOK, HOSTILE))![1];
    const data = script.slice(script.indexOf('var saved = '), script.indexOf('var room'));
    for (const char of ['<', '>', '&', '\u2028', '\u2029', '\r', '\u0000']) {
      expect(data.includes(char), JSON.stringify(char)).toBe(false);
    }
    // One line: the statement, and the line break after it.
    expect(data.split('\n')).toHaveLength(2);
  });

  it('is the head of a widget that saved nothing, but for the data and the room', () => {
    expect(widgetHead(LOOK, [])).toBe(HEAD);
    expect(HEAD).toContain('var saved = [];');
    expect(HEAD).toContain(`var room = ${MAX_WIDGET_DATA_CHARS};`);
    const withData = widgetHead(LOOK, [['a', 'b']], 1234);
    expect(withData).toBe(
      HEAD.replace('var saved = [];', 'var saved = [["a","b"]];').replace('var room = 65536;', 'var room = 1234;'),
    );
  });

  it.each([
    ['more than a widget may keep', 1e9, MAX_WIDGET_DATA_CHARS],
    ['less than nothing', -5, 0],
    ['no number', NaN, 0],
    ['a fraction', 99.9, 99],
  ])('takes %s for a room it can be', (_what, limit, room) => {
    expect(widgetHead(LOOK, [], limit)).toContain(`var room = ${room};`);
  });

  it('keeps the head whole whatever the source does with it', () => {
    expectHeadWith(wrapWidget('</script><script>window.evil = 1</script>', LOOK, HOSTILE), HOSTILE);
  });
});

describe('the head\'s script: a script error', () => {
  const notes = (w: ReturnType<typeof world>) => [...w.doc.body.querySelectorAll('[role=status]')].map(n => n.textContent);

  it('is said in the widget, once, as text', () => {
    const w = world();
    w.fire('error', { message: 'Uncaught ReferenceError: <b>Chart</b> is not defined' });
    w.fire('error', { message: 'Uncaught TypeError: second' });
    expect(notes(w)).toEqual(['This widget has a script error: Uncaught ReferenceError: <b>Chart</b> is not defined']);
    expect(w.doc.body.querySelector('[role=status] b')).toBeNull();
  });

  it('is cut to a line', () => {
    const w = world();
    w.fire('error', { message: 'x'.repeat(1000) });
    expect(notes(w)[0]).toHaveLength('This widget has a script error: '.length + 200);
  });

  it('covers a promise that nobody caught', () => {
    const w = world();
    w.fire('unhandledrejection', { reason: new TypeError('Failed to fetch') });
    expect(notes(w)).toEqual(['This widget has a script error: Failed to fetch']);
  });

  it('says nothing for what is no error of the widget', () => {
    const w = world();
    // A failed load of an image has no message; the browser's notice about a resize loop is none.
    w.fire('error', {});
    w.fire('error', { message: 'ResizeObserver loop completed with undelivered notifications.' });
    w.fire('unhandledrejection', { reason: undefined });
    expect(notes(w)).toEqual([]);
  });
});
