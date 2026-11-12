import { act, render as renderAlone, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactElement, ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WidgetStates } from '../../../api/client';
import type { McpAppsConfig } from '../../../types/mcpApps';
import type { WidgetHostOptions, WidgetSnapshot } from './host';

/** What `GET /api/mcp-apps/config` answers in a test. */
const config = vi.hoisted(() => ({ answer: { enabled: true } as McpAppsConfig | Error }));
/** The server's side of what widgets save: what it keeps, and whether it answers. */
const server = vi.hoisted(() => ({
  /** session -> widget -> items. */
  kept: new Map<string, Map<string, Array<[string, string]>>>(),
  /** What a read of a chat's widget states fails with. */
  read: null as Error | null,
  /** What a write of a widget's state fails with. */
  write: null as Error | null,
}));
vi.mock('../../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../../api/client')>()),
  api: new Proxy({} as Record<string, ReturnType<typeof vi.fn>>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async (...args: unknown[]) => {
      if (key === 'getMcpAppsConfig') {
        if (config.answer instanceof Error) throw config.answer;
        return config.answer;
      }
      const [session, id, items] = args as [string, string, Array<[string, string]> | undefined];
      const widgets = server.kept.get(session) ?? new Map<string, Array<[string, string]>>();
      if (key === 'getWidgetStates') {
        if (server.read) throw server.read;
        return {
          widgets: [...widgets].map(([widget, kept]) => ({ id: widget, items: kept, size: 0, updated_at: '' })),
          limits: { widget_chars: 65536, widget_keys: 1024, chat_chars: 1048576, chat_widgets: 200 },
        };
      }
      if (key === 'saveWidgetState' || key === 'deleteWidgetState') {
        if (server.write) throw server.write;
        server.kept.set(session, widgets);
        if (key === 'saveWidgetState') widgets.set(id, [...items!]);
        else widgets.delete(id);
      }
      return {};
    })),
  }),
}));
// The block asks the chat's store which chat is on screen; the store's socket stays shut.
vi.mock('../../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

/** A frame the test steers; `host.test.ts` covers the real one. */
class FakeHost {
  static made: FakeHost[] = [];
  options: WidgetHostOptions;
  readonly frame = document.createElement('iframe');
  themes: string[] = [];
  disposed = false;

  constructor(options: WidgetHostOptions) {
    this.options = options;
    FakeHost.made.push(this);
  }

  start(container: HTMLElement) { container.appendChild(this.frame); }
  setTheme(theme: string) { this.themes.push(theme); }
  dispose() {
    this.disposed = true;
    this.frame.remove();
  }
  say(change: Partial<WidgetSnapshot>) {
    act(() => this.options.onChange({ status: 'loading', error: null, ...change }));
  }
}
vi.mock('./host', () => ({ WidgetHost: FakeHost }));

const { api } = await import('../../../api/client');
const { resetMcpAppsConfig, useMcpAppsStore } = await import('../../../stores/mcpAppsStore');
const { useThemeStore } = await import('../../../stores/themeStore');
const { MarkdownContent } = await import('../MarkdownContent');
const { MAX_RICH_SOURCE_CHARS } = await import('../rich/kinds');
const { RichCodeBlock } = await import('../rich/RichCodeBlock');
const { MarkdownStreamingContext } = await import('../streamingContext');
const { MAX_LIVE_WIDGETS, liveWidgets, resetWidgetSlots } = await import('./budget');
const { rememberWidgetHeight, resetWidgetHeights, widgetKey } = await import('./heights');
const { MessageChatContext } = await import('../messageChat');
const { resetWidgetChats, widgetId } = await import('./storage');
const { WidgetBlock } = await import('./WidgetBlock');

const SOURCE = '<button id="b">Hi</button>\n<script>document.getElementById("b").onclick = () => {}</script>';
const everyCall = () => Object.entries(api as unknown as Record<string, ReturnType<typeof vi.fn>>)
  .flatMap(([name, fn]) => fn.mock.calls.map(() => name));
/** The calls about what widgets save, in the order of the three routes. */
const stateCalls = () => everyCall().filter(name => /WidgetState/.test(name)).sort();

const block = (index = 0) => document.querySelectorAll<HTMLElement>('[data-widget]')[index];
const state = (index = 0) => block(index)?.getAttribute('data-widget');
const lastHost = () => FakeHost.made.at(-1)!;
const liveHosts = () => FakeHost.made.filter(h => !h.disposed);
const button = (name: string | RegExp) => screen.getByRole('button', { name });
const noButton = (name: string | RegExp) => screen.queryByRole('button', { name });

/** The chat page around what a test shows: its session, or null where there is no chat. */
let chatAround: string | null = null;
function ChatAround({ children }: { children: ReactNode }) {
  return <MessageChatContext.Provider value={chatAround}>{children}</MessageChatContext.Provider>;
}
/** Everything a test shows is shown as the chat page shows it: told which chat it is in. */
const render = (ui: ReactElement) => renderAlone(ui, { wrapper: ChatAround });

function renderBlock(source: string, streaming = false) {
  const ui = (text: string, live: boolean) => (
    <MarkdownStreamingContext.Provider value={live}>
      <WidgetBlock source={text} />
    </MarkdownStreamingContext.Provider>
  );
  const result = render(ui(source, streaming));
  return {
    update: (text: string, live: boolean) => result.rerender(ui(text, live)),
    unmount: () => result.unmount(),
  };
}

/** What is on its way has arrived: the answers of the server, and what the page does with them. */
const arrived = () => act(async () => {
  await new Promise(resolve => setTimeout(resolve, 0));
});

/**
 * The feature's config has answered, the server has said what the chat's
 * widgets saved (a widget waits for that), and the page has drawn what
 * follows from both.
 */
const settled = async () => {
  await act(async () => {
    await useMcpAppsStore.getState().load();
  });
  await arrived();
};

/** The chat on screen: its page, at its address. What is shown from now on is in it. */
function openChat(session: string) {
  window.history.pushState({}, '', `/chat/${session}`);
  chatAround = session;
}

/** What the server keeps for a widget of a chat; null when it keeps nothing. */
const keptFor = (session: string, source = SOURCE) => server.kept.get(session)?.get(widgetId(source)) ?? null;

beforeEach(() => {
  config.answer = { enabled: true };
  server.kept.clear();
  server.read = null;
  server.write = null;
  FakeHost.made = [];
  resetMcpAppsConfig();
  resetWidgetSlots();
  resetWidgetHeights();
  resetWidgetChats();
  localStorage.clear();
  openChat('s1');
  for (const fn of Object.values(api as unknown as Record<string, ReturnType<typeof vi.fn>>)) fn.mockClear();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('a ```widget block', () => {
  it('is drawn in a frame of the sandbox, with its source as the frame\'s business only', async () => {
    renderBlock(SOURCE);
    await settled();
    expect(state()).toBe('loading');
    expect(lastHost().options).toMatchObject({
      source: SOURCE,
      theme: useThemeStore.getState().resolved,
    });
    const figure = screen.getByRole('figure', { name: 'Widget' });
    expect(figure).toContainElement(lastHost().frame);
    expect(figure).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByRole('status')).toHaveTextContent('Loading the widget…');

    lastHost().say({ status: 'shown' });
    expect(state()).toBe('shown');
    expect(figure).not.toHaveAttribute('aria-busy');
    expect(screen.queryByRole('status')).toBeNull();
    // The source is nowhere in the page: not as text, not as elements.
    expect(document.getElementById('b')).toBeNull();
    expect(document.querySelector('pre')).toBeNull();
    expect(block().textContent).toBe('widget');
  });

  it('has its tools on a row of its own: reload, source, copy', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    const tools = screen.getByRole('group', { name: 'Widget tools' });
    expect([...tools.querySelectorAll('button')].map(b => b.getAttribute('aria-label'))).toEqual(['Reload', 'Source', 'Copy source']);
    // Not over the widget: a sibling of the frame's place, before it.
    expect(tools.contains(lastHost().frame)).toBe(false);
    expect(tools.compareDocumentPosition(lastHost().frame) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    // No export: there is no picture to save.
    expect(noButton(/Download|Expand/)).toBeNull();
  });

  it('follows the theme without a new frame', async () => {
    renderBlock(SOURCE);
    await settled();
    const before = useThemeStore.getState().resolved;
    const other = before === 'dark' ? 'light' : 'dark';
    await act(async () => { useThemeStore.setState({ resolved: other }); });
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().themes).toEqual([other]);
    await act(async () => { useThemeStore.setState({ resolved: before }); });
  });

  it('removes its frame when the block goes', async () => {
    const { unmount } = renderBlock(SOURCE);
    await settled();
    unmount();
    expect(lastHost().disposed).toBe(true);
    expect(liveWidgets()).toBe(0);
  });
});

describe('the source of a widget', () => {
  it('is one click away, as text, and the frame stays alive behind it', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    await userEvent.click(button('Source'));
    expect(document.querySelector('pre code')?.textContent).toBe(SOURCE);
    expect(screen.getByText('widget', { selector: '.rich-source-bar span' })).toBeInTheDocument();
    // Hidden, not removed: a frame that leaves the page loads again.
    expect(screen.queryByRole('figure')).toBeNull();
    expect(document.querySelector('figure')).toContainElement(lastHost().frame);
    expect(lastHost().disposed).toBe(false);
    expect(button('Rendered')).toHaveFocus();

    await userEvent.keyboard('{Enter}');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.getByRole('figure', { name: 'Widget' })).toBeInTheDocument();
    expect(button('Source')).toHaveFocus();
    expect(FakeHost.made).toHaveLength(1);
  });

  it('is highlighted as HTML, with nothing of it becoming an element of the page', async () => {
    const hostile = '<img src=x onerror="window.pwned = 1">\n<script>window.pwned = 2</script>\n<b onclick="x">bold</b>';
    renderBlock(hostile);
    await settled();
    await userEvent.click(button('Source'));
    const code = document.querySelector('pre code')!;
    await waitFor(() => expect(code.querySelectorAll('span').length).toBeGreaterThan(3));
    expect(code.textContent).toBe(hostile);
    // Spans of the highlighter and text: no image, no script, no handler.
    expect([...code.querySelectorAll('*')].every(el => el.localName === 'span')).toBe(true);
    expect([...code.querySelectorAll('*')].flatMap(el => el.getAttributeNames())).toEqual(expect.arrayContaining(['class']));
    expect([...code.querySelectorAll('*')].flatMap(el => el.getAttributeNames()).every(name => name === 'class')).toBe(true);
    expect([...code.querySelectorAll('span')].every(el => /^(hljs|language)-[\w-]+( (hljs|language)-[\w-]+)*$|^$/.test(el.className))).toBe(true);
    expect((window as unknown as { pwned?: number }).pwned).toBeUndefined();
  });

  it('is copied as it is', async () => {
    const written: string[] = [];
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true, value: { writeText: async (text: string) => { written.push(text); } },
    });
    renderBlock(SOURCE);
    await settled();
    await userEvent.click(button('Copy source'));
    await waitFor(() => expect(written).toEqual([SOURCE]));
    expect(button('Copied')).toBeInTheDocument();
  });
});

describe('Reload', () => {
  it('draws the widget again from its source', async () => {
    renderBlock(SOURCE);
    await settled();
    const first = lastHost();
    // Nothing to reload before there is a frame... and there is one now.
    first.say({ status: 'shown' });
    await userEvent.click(button('Reload'));
    expect(first.disposed).toBe(true);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.source).toBe(SOURCE);
    expect(state()).toBe('loading');
  });
});

describe('what a widget saves', () => {
  const TICKS: Array<[string, string]> = [['ticks', '["tent"]']];
  /** The widget on screen says what its `localStorage` holds. */
  const saves = async (items: ReadonlyArray<readonly [string, string]>) => {
    act(() => lastHost().options.onStorage!(items));
    await arrived();
  };
  /** The page is loaded again: it knows nothing of what it read before. */
  const loadedAgain = (shown: { unmount: () => void }) => {
    shown.unmount();
    resetWidgetChats();
  };
  /** What the block says about the widget's saved state, beside its name; null when it says nothing. */
  const notice = () => screen.queryAllByRole('status').map(line => line.textContent ?? '')
    .find(text => /saved/.test(text)) ?? null;

  it('is kept by the server for the widget in this chat, and is what the widget starts with when it is drawn again', async () => {
    const first = renderBlock(SOURCE);
    await settled();
    expect(lastHost().options).toMatchObject({ saved: [], limit: 65536 });
    await saves(TICKS);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(api.saveWidgetState).toHaveBeenCalledWith('s1', widgetId(SOURCE), TICKS, false);
    // Nothing of it stays in the browser.
    expect(localStorage.length).toBe(0);
    loadedAgain(first);
    renderBlock(SOURCE);
    await settled();
    expect(lastHost().options.saved).toEqual(TICKS);
    expect(notice()).toBeNull();
  });

  it('is read before the widget is drawn, once for the chat however many widgets it has', async () => {
    let answer: (states: WidgetStates) => void = () => {};
    vi.mocked(api.getWidgetStates).mockImplementationOnce(() => new Promise(resolve => { answer = resolve; }));
    render(
      <>
        <WidgetBlock source={SOURCE} />
        <WidgetBlock source="<p>two</p>" />
        <WidgetBlock source="<p>three</p>" />
      </>,
    );
    await settled();
    // The widgets have their places, and no frame runs before the server has answered.
    expect(document.querySelectorAll('[data-widget="loading"]')).toHaveLength(3);
    expect(FakeHost.made).toEqual([]);
    expect(stateCalls()).toEqual(['getWidgetStates']);
    await act(async () => answer({
      widgets: [{ id: widgetId(SOURCE), items: TICKS, size: 13, updated_at: '2026-01-01T00:00:00Z' }],
      limits: { widget_chars: 65536, widget_keys: 1024, chat_chars: 1048576, chat_widgets: 200 },
    }));
    await arrived();
    expect(FakeHost.made.map(host => host.options.saved)).toEqual([TICKS, [], []]);
    expect(stateCalls()).toEqual(['getWidgetStates']);
  });

  it('is not what the same widget starts with in another chat', async () => {
    const first = renderBlock(SOURCE);
    await settled();
    await saves(TICKS);
    first.unmount();
    openChat('s2');
    renderBlock(SOURCE);
    await settled();
    expect(lastHost().options.saved).toEqual([]);
    await saves([['ticks', '["stove"]']]);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(keptFor('s2')).toEqual([['ticks', '["stove"]']]);
  });

  it('belongs to the chat that was on screen when the block appeared', async () => {
    const shown = renderBlock(SOURCE);
    await settled();
    // The page names another chat a moment before the block goes.
    chatAround = 's2';
    shown.update(SOURCE, false);
    await saves(TICKS);
    expect(FakeHost.made).toHaveLength(1);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(keptFor('s2')).toBeNull();
  });

  it('is kept nowhere for a widget outside a chat: it lasts as long as the frame', async () => {
    window.history.pushState({}, '', '/tasks/t-17');
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    expect(lastHost().options).toMatchObject({ saved: [], limit: 65536 });
    await saves(TICKS);
    expect(stateCalls()).toEqual([]);
    expect(server.kept.size).toBe(0);
    expect(localStorage.length).toBe(0);
    expect(noButton('Reset')).toBeNull();
    expect(notice()).toBeNull();
  });

  it('is kept nowhere where no chat page is around it, whatever the address says', async () => {
    // A text with a widget shown by some other part of the panel, at a chat's address.
    chatAround = null;
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    await saves(TICKS);
    expect(stateCalls()).toEqual([]);
    expect(server.kept.size).toBe(0);
    expect(noButton('Reset')).toBeNull();
  });

  it('stays through Reload', async () => {
    const user = userEvent.setup();
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    await saves(TICKS);
    await user.click(button('Reload'));
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.saved).toEqual(TICKS);
    expect(button('Reset')).toBeInTheDocument();
    // What the page knows of the chat is enough: the server is not asked again.
    expect(stateCalls()).toEqual(['getWidgetStates', 'saveWidgetState']);
  });

  it('can be forgotten: Reset is there once the widget has saved something, and the server deletes it', async () => {
    const user = userEvent.setup();
    const first = renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    expect(noButton('Reset')).toBeNull();
    await saves(TICKS);
    const reset = button('Reset');
    expect(reset).toHaveAttribute('title', 'Forget what this widget saved and draw it again');
    expect(screen.getByRole('group', { name: 'Widget tools' })).toContainElement(reset);
    await user.click(reset);
    await arrived();
    expect(FakeHost.made).toHaveLength(2);
    expect(FakeHost.made[0].disposed).toBe(true);
    expect(lastHost().options).toMatchObject({ source: SOURCE, saved: [] });
    expect(api.deleteWidgetState).toHaveBeenCalledWith('s1', widgetId(SOURCE), false);
    expect(keptFor('s1')).toBeNull();
    expect(noButton('Reset')).toBeNull();
    // Another device, or this one after a reload: nothing is left for the widget.
    loadedAgain(first);
    renderBlock(SOURCE);
    await settled();
    expect(lastHost().options.saved).toEqual([]);
  });

  it('offers Reset from the start for a widget that saved before, on this device or another', async () => {
    server.kept.set('s1', new Map([[widgetId(SOURCE), TICKS]]));
    renderBlock(SOURCE);
    await settled();
    expect(lastHost().options.saved).toEqual(TICKS);
    expect(button('Reset')).toBeInTheDocument();
  });

  it('runs the widget, empty, when the server does not say what was saved, and says so quietly', async () => {
    server.kept.set('s1', new Map([[widgetId(SOURCE), TICKS]]));
    server.read = new Error('Failed to fetch');
    renderBlock(SOURCE);
    await settled();
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().options.saved).toEqual([]);
    lastHost().say({ status: 'shown' });
    expect(state()).toBe('shown');
    expect(notice()).toBe('saved state is unavailable: changes are not kept');
    expect(screen.getByRole('figure')).toContainElement(screen.getByRole('status'));
    // What the widget does now is not written over what the server has, unseen.
    await saves([['ticks', '[]']]);
    expect(stateCalls()).toEqual(['getWidgetStates']);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(noButton('Reset')).toBeNull();
  });

  it('keeps the widget running when the server does not take a change, and writes it with the next one', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'shown' });
    server.write = new Error('Failed to fetch');
    await saves(TICKS);
    expect(state()).toBe('shown');
    expect(liveHosts()).toHaveLength(1);
    expect(notice()).toBe('the last change is not saved yet');
    expect(keptFor('s1')).toBeNull();
    server.write = null;
    const more: Array<[string, string]> = [['ticks', '["tent","stove"]']];
    await saves(more);
    await waitFor(() => expect(keptFor('s1')).toEqual(more));
    await waitFor(() => expect(notice()).toBeNull());
    expect(FakeHost.made).toHaveLength(1);
  });

  it('is not asked for while the feature is off', async () => {
    server.kept.set('s1', new Map([[widgetId(SOURCE), TICKS]]));
    config.answer = { enabled: false };
    renderBlock(SOURCE);
    await settled();
    expect(state()).toBe('source');
    expect(stateCalls()).toEqual([]);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(noButton('Reset')).toBeNull();
  });
});

describe('a widget that is still being written', () => {
  it('holds a quiet placeholder, and is drawn once the text has stopped changing', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('<div', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(state()).toBe('writing');
    expect(screen.getByRole('status')).toHaveTextContent('Writing the widget… 4 characters');
    expect(FakeHost.made).toEqual([]);
    // No source streaming by, no half a widget.
    expect(document.querySelector('pre')).toBeNull();

    update('<div>Hello', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(700); });
    update('<div>Hello</div>', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(700); });
    expect(FakeHost.made).toEqual([]);
    expect(state()).toBe('writing');
    // The fence has closed: the text of the message goes on, this block's does not.
    await act(async () => { await vi.advanceTimersByTimeAsync(100); });
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().options.source).toBe('<div>Hello</div>');
    expect(state()).toBe('loading');
  });

  it('is drawn at once when the message ends', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('<div>Hel', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(100); });
    expect(FakeHost.made).toEqual([]);
    update('<div>Hello</div>', false);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(lastHost().options.source).toBe('<div>Hello</div>');
  });

  it('keeps the frame of a text that held still when the same text is the message\'s last', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('<div>Hello</div>', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(800); });
    expect(FakeHost.made).toHaveLength(1);
    update('<div>Hello</div>', false);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().disposed).toBe(false);
  });

  it('draws the text again when it went on after a pause', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('<div>Hel', true);
    await act(async () => { await vi.advanceTimersByTimeAsync(800); });
    const first = lastHost();
    expect(first.options.source).toBe('<div>Hel');
    update('<div>Hello</div>', true);
    // The frame of the pause stays until the text holds still again.
    await act(async () => { await vi.advanceTimersByTimeAsync(799); });
    expect(first.disposed).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(first.disposed).toBe(true);
    expect(lastHost().options.source).toBe('<div>Hello</div>');
  });
});

describe('where a widget is not drawn', () => {
  it('with the feature off, is a code block with its source and one line that says why', async () => {
    config.answer = { enabled: false };
    renderBlock(SOURCE);
    await settled();
    expect(state()).toBe('source');
    expect(document.querySelector('pre code')?.textContent).toBe(SOURCE);
    expect(screen.getByRole('status')).toHaveTextContent('widgets are off on this Nerve');
    expect(screen.queryByRole('figure')).toBeNull();
    expect(button('Copy source')).toBeInTheDocument();
    expect(noButton(/Reload|Retry|Rendered|Source/)).toBeNull();
    expect(FakeHost.made).toEqual([]);
    expect(document.querySelector('iframe')).toBeNull();
  });

  it('with the feature off, asks the gateway for nothing but the one read of the config the app makes anyway', async () => {
    config.answer = { enabled: false };
    // The app's own read, at its start.
    await act(async () => { await useMcpAppsStore.getState().load(); });
    render(
      <>
        <WidgetBlock source={SOURCE} />
        <WidgetBlock source="<p>two</p>" />
        <WidgetBlock source="<p>three</p>" />
      </>,
    );
    await settled();
    expect(document.querySelectorAll('[data-widget="source"]')).toHaveLength(3);
    expect(everyCall()).toEqual(['getMcpAppsConfig']);
  });

  it('highlights the source of a widget that is not drawn', async () => {
    config.answer = { enabled: false };
    renderBlock('<p class="a">text</p>');
    await settled();
    const code = document.querySelector('pre code')!;
    expect(code.className).toBe('hljs language-html');
    await waitFor(() => expect(code.querySelector('.hljs-tag .hljs-name')?.textContent).toBe('p'));
    expect(code.textContent).toBe('<p class="a">text</p>');
  });

  it('holds its place, without a flash of source, until the feature\'s config has answered', async () => {
    let answer!: (value: McpAppsConfig) => void;
    vi.mocked(api.getMcpAppsConfig).mockImplementationOnce(() => new Promise(resolve => { answer = resolve; }));
    renderBlock(SOURCE);
    await act(async () => {});
    expect(state()).toBe('idle');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.getByRole('status')).toHaveTextContent('Widget');
    await act(async () => { answer({ enabled: true }); });
    expect(state()).toBe('loading');
    expect(FakeHost.made).toHaveLength(1);
  });

  it('on a gateway without the feature, is a code block', async () => {
    config.answer = new Error('404');
    renderBlock(SOURCE);
    await settled();
    expect(state()).toBe('source');
    expect(screen.getByRole('status')).toHaveTextContent('widgets are off on this Nerve');
  });

  it('keeps streaming as a code block while the feature is off', async () => {
    config.answer = { enabled: false };
    const { update } = renderBlock('<div', true);
    await settled();
    expect(document.querySelector('pre code')?.textContent).toBe('<div');
    update('<div>more', true);
    expect(document.querySelector('pre code')?.textContent).toBe('<div>more');
  });

  it('above the size limit of the rendered blocks, stays source', async () => {
    const large = `<p>${'x'.repeat(MAX_RICH_SOURCE_CHARS)}</p>`;
    renderBlock(large);
    await settled();
    expect(state()).toBe('source');
    expect(screen.getByRole('status')).toHaveTextContent(`too large to draw (${large.length.toLocaleString()} characters)`);
    expect(FakeHost.made).toEqual([]);
    // At the limit it is drawn.
    renderBlock('y'.repeat(MAX_RICH_SOURCE_CHARS));
    await settled();
    expect(FakeHost.made).toHaveLength(1);
  });

  it('with nothing in it, is an empty code block', async () => {
    renderBlock('  \n ');
    await settled();
    expect(state()).toBe('source');
    expect(screen.getByRole('status')).toHaveTextContent('empty');
    expect(FakeHost.made).toEqual([]);
  });
});

describe('a widget that failed', () => {
  it('shows its source with the reason, and can be tried again', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'failed', error: 'The frame of the widget loaded a second time, which it may not do.' });
    expect(state()).toBe('source');
    expect(screen.getByRole('status')).toHaveTextContent('The frame of the widget loaded a second time, which it may not do.');
    expect(document.querySelector('pre code')?.textContent).toBe(SOURCE);
    expect(screen.queryByRole('link')).toBeNull();

    await userEvent.click(button('Retry'));
    expect(FakeHost.made).toHaveLength(2);
    expect(state()).toBe('loading');
    expect(document.querySelector('pre')).toBeNull();
    lastHost().say({ status: 'shown' });
    expect(state()).toBe('shown');
  });

  it('says so when the sandbox does not answer, with Retry and no link out', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({ status: 'failed', error: 'The widget sandbox does not answer.' });
    expect(screen.getByRole('status')).toHaveTextContent('The widget sandbox does not answer.');
    // The block shows no link out.
    expect(screen.queryByRole('link')).toBeNull();
    expect(button('Retry')).toBeInTheDocument();
  });

  it('shows why the sandbox page refused to run', async () => {
    renderBlock(SOURCE);
    await settled();
    lastHost().say({
      status: 'failed',
      error: 'The widget sandbox refused to start: this page was served without its sandbox policy.',
    });
    expect(state()).toBe('source');
    expect(screen.getByRole('status')).toHaveTextContent(
      'The widget sandbox refused to start: this page was served without its sandbox policy.',
    );
    expect(document.querySelector('iframe')).toBeNull();
  });
});

describe('many widgets', () => {
  it('run a few at a time: one too many waits for a click', async () => {
    render(
      <>
        {Array.from({ length: MAX_LIVE_WIDGETS + 1 }, (_, index) => <WidgetBlock key={index} source={`<p>${index}</p>`} />)}
      </>,
    );
    await settled();
    expect(liveHosts()).toHaveLength(MAX_LIVE_WIDGETS);
    expect(state(MAX_LIVE_WIDGETS)).toBe('waiting');
    const waiting = block(MAX_LIVE_WIDGETS);
    expect(waiting.querySelector('iframe')).toBeNull();
    expect(waiting).toHaveTextContent('Other widgets are running.');

    // The person asks for it: the widget used longest ago gives way.
    await userEvent.click(button('Load widget'));
    expect(state(MAX_LIVE_WIDGETS)).toBe('loading');
    expect(liveHosts()).toHaveLength(MAX_LIVE_WIDGETS);
    expect(FakeHost.made[0].disposed).toBe(true);
    expect(state(0)).toBe('waiting');
    expect(document.querySelectorAll('[data-widget] iframe')).toHaveLength(MAX_LIVE_WIDGETS);
  });

  it('holds the room a widget took before while it has no frame', async () => {
    rememberWidgetHeight(widgetKey('<p>6</p>'), 420);
    render(
      <>
        {Array.from({ length: MAX_LIVE_WIDGETS + 1 }, (_, index) => <WidgetBlock key={index} source={`<p>${index}</p>`} />)}
      </>,
    );
    await settled();
    const place = block(MAX_LIVE_WIDGETS).querySelector<HTMLElement>('[data-widget-frame]')!.parentElement!;
    expect(place.style.minHeight).toBe('432px');
    // A widget with a frame takes the frame's height.
    expect(block(0).querySelector<HTMLElement>('[data-widget-frame]')!.parentElement!.style.minHeight).toBe('');
  });
});

describe('a ```widget fence in the chat', () => {
  it('is a widget, through the block that renders fences', async () => {
    render(<RichCodeBlock kind="widget" source={SOURCE} />);
    await settled();
    expect(state()).toBe('loading');
    expect(lastHost().options.source).toBe(SOURCE);
    expect(document.querySelector('[data-rich-kind]')).toBeNull();
  });

  it('is a widget in markdown, and a ```html fence beside it stays a code block', async () => {
    const markdown = [
      'Before.', '', '```widget', '<div id="w">hi</div>', '<script>window.x = 1</script>', '```', '',
      'Between.', '', '```html', '<b>shown as code</b>', '```', '', 'After.',
    ].join('\n');
    render(<MarkdownContent content={markdown} />);
    await settled();
    expect(document.querySelectorAll('[data-widget]')).toHaveLength(1);
    expect(lastHost().options.source).toBe('<div id="w">hi</div>\n<script>window.x = 1</script>');
    // The ```html fence: an ordinary code block, highlighted by the markdown pipeline.
    const code = [...document.querySelectorAll('pre code')].find(c => c.textContent?.includes('shown as code'))!;
    expect(code.textContent).toBe('<b>shown as code</b>\n');
    expect(code.closest('[data-widget]')).toBeNull();
    expect(screen.getByText('Before.')).toBeInTheDocument();
    expect(screen.getByText('After.')).toBeInTheDocument();
    // Neither fence put an element of its own into the page.
    expect(document.getElementById('w')).toBeNull();
    expect(document.querySelector('.markdown-content b')).toBeNull();
  });

  it('with the feature off, is a code block in markdown too', async () => {
    config.answer = { enabled: false };
    render(<MarkdownContent content={'```widget\n<div id="w">hi</div>\n```'} />);
    await settled();
    expect(state()).toBe('source');
    expect(document.querySelector('pre code')?.textContent).toBe('<div id="w">hi</div>');
    expect(document.getElementById('w')).toBeNull();
  });
});
