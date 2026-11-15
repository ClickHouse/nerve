import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WidgetStates } from '../../../api/client';

/**
 * The page's side of a widget's `localStorage`, against a server the test
 * plays: whose data is whose, what a widget starts with, when and how its
 * changes are written, and what happens when the server does not answer.
 */

type Items = Array<[string, string]>;

/** The server of the test: what it keeps, what it was asked, and how it answers. */
const server = vi.hoisted(() => ({
  /** session -> widget -> items. */
  kept: new Map<string, Map<string, Array<[string, string]>>>(),
  limits: { widget_chars: 65536, widget_keys: 1024, chat_chars: 1048576, chat_widgets: 200 },
  reads: [] as string[],
  writes: [] as Array<{ session: string; id: string; items: Array<[string, string]> | null; keepalive: boolean }>,
  /** How a read answers: with what is kept, with an error, or never. */
  read: 'answers' as 'answers' | 'never' | Error,
  /** How writes answer: at once, with an error, or when the test says. */
  write: 'answers' as 'answers' | 'waits' | Error,
  waiting: [] as Array<{ resolve: () => void; reject: (error: unknown) => void }>,
}));

vi.mock('../../../api/client', () => {
  const write = (session: string, id: string, items: Array<[string, string]> | null, keepalive: boolean) => {
    server.writes.push({ session, id, items, keepalive });
    const apply = () => {
      const widgets = server.kept.get(session) ?? new Map<string, Array<[string, string]>>();
      server.kept.set(session, widgets);
      if (items && items.length > 0) widgets.set(id, items);
      else widgets.delete(id);
    };
    if (server.write instanceof Error) return Promise.reject(server.write);
    if (server.write === 'waits') {
      return new Promise<void>((resolve, reject) => {
        server.waiting.push({ resolve: () => { apply(); resolve(); }, reject });
      });
    }
    apply();
    return Promise.resolve();
  };
  return {
    api: {
      getWidgetStates: vi.fn((session: string, signal?: AbortSignal) => {
        server.reads.push(session);
        if (server.read instanceof Error) return Promise.reject(server.read);
        if (server.read === 'never') {
          return new Promise((_resolve, reject) => {
            signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
          });
        }
        const widgets = [...(server.kept.get(session) ?? new Map()).entries()].map(([id, items]) => ({
          id, items, size: 0, updated_at: '2026-01-01T00:00:00+00:00',
        }));
        return Promise.resolve({ widgets, limits: { ...server.limits } });
      }),
      saveWidgetState: vi.fn((session: string, id: string, items: Array<[string, string]>, keepalive = false) =>
        write(session, id, [...items], keepalive)),
      deleteWidgetState: vi.fn((session: string, id: string, keepalive = false) => write(session, id, null, keepalive)),
    },
  };
});

const { ApiError } = await import('../../../api/hosted');
const {
  FORGET_MS, LEAVING_BYTES, MAX_WIDGET_DATA_CHARS, MAX_WIDGET_DATA_KEYS, READ_WAIT_MS,
  RETRY_MS, STORE_INTERVAL_MS, WidgetData, forgetWidgetData, readWidgetItems,
  resetWidgetChats, widgetDataSize, widgetId, widgetScope,
} = await import('./storage');

const SOURCE = '<input type="checkbox" id="tent"> Tent';
const OTHER_SOURCE = '<button>Count</button>';
const CHAT = widgetScope('s1', '/chat/s1');
const OTHER_CHAT = widgetScope('s2', '/chat/s2');
const ID = widgetId(SOURCE);
const TICKS: Items = [['ticks', '["tent"]'], ['tab', 'gear']];

/** The server keeps `items` for a widget before the page asks. */
function keeps(session: string, source: string, items: Items) {
  const widgets = server.kept.get(session) ?? new Map<string, Items>();
  server.kept.set(session, widgets);
  widgets.set(widgetId(source), items);
}

const keptFor = (session: string, source = SOURCE) => server.kept.get(session)?.get(widgetId(source)) ?? null;
/** Let what is on its way arrive: the answers of the server, and what follows from them. */
const settle = () => vi.advanceTimersByTimeAsync(0);

/** A widget that is drawn: its data, opened and started once what its chat's widgets saved is known. */
async function drawn(scope = CHAT, source = SOURCE, onChange: () => void = () => {}) {
  const data = new WidgetData(scope, source, onChange);
  await data.ready;
  return { data, ...data.start() };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
  server.kept.clear();
  server.reads.length = 0;
  server.writes.length = 0;
  server.waiting.length = 0;
  server.read = 'answers';
  server.write = 'answers';
  server.limits = { widget_chars: 65536, widget_keys: 1024, chat_chars: 1048576, chat_widgets: 200 };
  resetWidgetChats();
  localStorage.clear();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('where a widget lives', () => {
  it('is its chat, on the chat page', () => {
    expect(widgetScope('1a2b3c4d', '/chat/1a2b3c4d')).toBe('chat/1a2b3c4d');
    expect(widgetScope('1a2b3c4d', '/chat')).toBe('chat/1a2b3c4d');
  });

  it('is the chat on screen, not the one the address still names', () => {
    // A new chat keeps its first, made-up id in the address after the server gave it its own.
    expect(widgetScope('1a2b3c4d', '/chat/0b7e7c0e-5d0a-4a43-9d0a-0d6d3b8f6c11')).toBe('chat/1a2b3c4d');
  });

  it('is the page, outside a chat and on the chat page without a chat', () => {
    expect(widgetScope('1a2b3c4d', '/tasks/t-17')).toBe('page/%2Ftasks%2Ft-17');
    expect(widgetScope('1a2b3c4d', '/chatter')).toBe('page/%2Fchatter');
    expect(widgetScope('', '/chat')).toBe('page/%2Fchat');
  });

  it('names a widget in its chat by a hash of its source', () => {
    expect(ID).toMatch(/^[0-9a-f]{32}$/);
    expect(widgetId(SOURCE)).toBe(ID);
    expect(widgetId(`${SOURCE} `)).not.toBe(ID);
    // The id the server computes for the same text (tests/test_widget_state.py).
    expect(widgetId('abc')).toBe('13e228567e8249fce53337f25d7970de');
  });
});

describe('readWidgetItems', () => {
  it('takes a list of pairs of texts, as a list of its own', () => {
    const sent = [['ticks', '["tent"]'], ['', ''], ['empty', '']];
    const items = readWidgetItems(sent);
    expect(items).toEqual(sent);
    expect(items).not.toBe(sent);
    expect(items![0]).not.toBe(sent[0]);
    expect(readWidgetItems([])).toEqual([]);
  });

  it('takes any text as a key or a value, and reads nothing in it', () => {
    const odd: Items = [
      ['__proto__', '{"polluted":true}'], ['constructor', 'x'], ['</script><script>alert(1)</script>', '<!--'],
      ['line\u2028separators\u2029', '\u0000\ud83d'], ['nerve_token', 'not the page\'s'],
    ];
    expect(readWidgetItems(odd)).toEqual(odd);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  it.each([
    ['nothing', undefined],
    ['null', null],
    ['a text', '[["a","b"]]'],
    ['an object', { a: 'b' }],
    ['a pair that is no list', ['a', 'b']],
    ['a pair of one', [['a']]],
    ['a pair of three', [['a', 'b', 'c']]],
    ['a key that is a number', [[1, 'b']]],
    ['a value that is a number', [['a', 1]]],
    ['a value that is null', [['a', null]]],
    ['a value that is an object', [['a', { toString: () => 'b' }]]],
    ['a pair that is an object', [{ 0: 'a', 1: 'b', length: 2 }]],
    ['a key twice', [['a', '1'], ['b', '2'], ['a', '3']]],
    // eslint-disable-next-line no-sparse-arrays -- a list with a hole is the case.
    ['a hole in the list', [['a', '1'], , ['b', '2']]],
  ])('takes nothing from %s', (_what, value) => {
    expect(readWidgetItems(value)).toBeNull();
  });

  it('takes what one widget may keep, and not a character or a key more', () => {
    const full: Items = [['k', 'v'.repeat(MAX_WIDGET_DATA_CHARS - 1)]];
    expect(widgetDataSize(full)).toBe(MAX_WIDGET_DATA_CHARS);
    expect(readWidgetItems(full)).toEqual(full);
    expect(readWidgetItems([['k', 'v'.repeat(MAX_WIDGET_DATA_CHARS)]])).toBeNull();
    expect(readWidgetItems([['k'.repeat(MAX_WIDGET_DATA_CHARS), 'v']])).toBeNull();
    expect(readWidgetItems(Array.from({ length: MAX_WIDGET_DATA_KEYS }, (_, i) => [String(i), '']))).toHaveLength(1024);
    expect(readWidgetItems(Array.from({ length: MAX_WIDGET_DATA_KEYS + 1 }, (_, i) => [String(i), '']))).toBeNull();
  });

  it('gives up a list without end at once', () => {
    const started = performance.now();
    expect(readWidgetItems(Array.from({ length: 2_000_000 }, () => ['', '']))).toBeNull();
    expect(readWidgetItems(Array.from({ length: 2_000_000 }, (_, i) => [String(i), '']))).toBeNull();
    expect(performance.now() - started).toBeLessThan(1000);
  });
});

describe('what a widget starts with', () => {
  it('waits for what its chat\'s widgets saved, and starts with its own', async () => {
    keeps('s1', SOURCE, TICKS);
    const data = new WidgetData(CHAT, SOURCE);
    expect(data.isReady).toBe(false);
    await data.ready;
    expect(data.isReady).toBe(true);
    expect(data.start()).toEqual({ saved: TICKS, limit: MAX_WIDGET_DATA_CHARS });
    expect(data.has()).toBe(true);
    expect(data.notice()).toBeNull();
  });

  it('starts with nothing in a chat where it saved nothing', async () => {
    keeps('s1', OTHER_SOURCE, [['n', '3']]);
    const { data, saved } = await drawn();
    expect(saved).toEqual([]);
    expect(data.has()).toBe(false);
  });

  it('asks the server once for a chat, however many of its widgets are drawn', async () => {
    keeps('s1', SOURCE, TICKS);
    keeps('s1', OTHER_SOURCE, [['n', '3']]);
    const first = new WidgetData(CHAT, SOURCE);
    const second = new WidgetData(CHAT, OTHER_SOURCE);
    await Promise.all([first.ready, second.ready]);
    expect(server.reads).toEqual(['s1']);
    expect(first.start().saved).toEqual(TICKS);
    expect(second.start().saved).toEqual([['n', '3']]);
    // And a widget that is drawn later starts at once, without a request.
    const third = new WidgetData(CHAT, SOURCE);
    expect(third.isReady).toBe(true);
    expect(third.start().saved).toEqual(TICKS);
    expect(server.reads).toEqual(['s1']);
  });

  it('asks for each chat, and a widget of one chat gets nothing of another', async () => {
    keeps('s1', SOURCE, [['who', 'first']]);
    keeps('s2', SOURCE, [['who', 'second']]);
    expect((await drawn(CHAT)).saved).toEqual([['who', 'first']]);
    expect((await drawn(OTHER_CHAT)).saved).toEqual([['who', 'second']]);
    expect(server.reads).toEqual(['s1', 's2']);
  });

  it('takes from the server\'s answer only what a widget could have saved', async () => {
    const widgets = new Map<string, unknown>([
      [ID, [['ticks', 1]]],
      [widgetId(OTHER_SOURCE), [['n', '3']]],
      [widgetId('too large'), [['k', 'v'.repeat(MAX_WIDGET_DATA_CHARS)]]],
    ]);
    server.kept.set('s1', widgets as never);
    expect((await drawn()).saved).toEqual([]);
    expect((await drawn(CHAT, OTHER_SOURCE)).saved).toEqual([['n', '3']]);
    expect((await drawn(CHAT, 'too large')).saved).toEqual([]);
  });

  it('may keep what one widget may keep, within the room its chat has left', async () => {
    server.limits = { widget_chars: 65536, widget_keys: 1024, chat_chars: 100_000, chat_widgets: 3 };
    keeps('s1', OTHER_SOURCE, [['fill', 'x'.repeat(59_996)]]);
    keeps('s1', SOURCE, [['fill', 'x'.repeat(996)]]);
    // The other widget keeps 60,000 characters of the chat's 100,000.
    expect((await drawn()).limit).toBe(40_000);
    expect((await drawn(CHAT, OTHER_SOURCE)).limit).toBe(MAX_WIDGET_DATA_CHARS);
    // A third widget has what is left; a fourth would be one too many for the chat.
    expect((await drawn(CHAT, 'a third')).limit).toBe(39_000);
    (await drawn(CHAT, 'a third')).data.take([['k', 'v']]);
    expect((await drawn(CHAT, 'a fourth')).limit).toBe(0);
  });

  it('keeps nothing outside a chat: no request, nothing to start with, nothing written', async () => {
    const page = widgetScope('s1', '/tasks/t-17');
    const data = new WidgetData(page, SOURCE);
    expect(data.isReady).toBe(true);
    expect(data.start()).toEqual({ saved: [], limit: MAX_WIDGET_DATA_CHARS });
    data.take(TICKS);
    data.close();
    await settle();
    expect(data.has()).toBe(false);
    expect(server.reads).toEqual([]);
    expect(server.writes).toEqual([]);
    // The same widget drawn again on that page starts empty.
    expect(new WidgetData(page, SOURCE).start().saved).toEqual([]);
  });
});

describe('when the read fails', () => {
  it('the widget runs all the same, with nothing, and says that saved state is unavailable', async () => {
    keeps('s1', SOURCE, TICKS);
    server.read = new ApiError(500, 'Internal Server Error');
    const { data, saved, limit } = await drawn();
    expect(saved).toEqual([]);
    expect(limit).toBe(MAX_WIDGET_DATA_CHARS);
    expect(data.notice()).toBe('unavailable');
    expect(data.has()).toBe(false);
  });

  it('a server that does not answer is waited for READ_WAIT_MS and no longer', async () => {
    server.read = 'never';
    const data = new WidgetData(CHAT, SOURCE);
    let ready = false;
    void data.ready.then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(READ_WAIT_MS - 1);
    expect(ready).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(ready).toBe(true);
    expect(data.start().saved).toEqual([]);
    expect(data.notice()).toBe('unavailable');
  });

  it('nothing of that widget is written: what the server has was not seen', async () => {
    keeps('s1', SOURCE, TICKS);
    server.read = new Error('Failed to fetch');
    const { data } = await drawn();
    data.take([['ticks', '[]']]);
    data.close();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.writes).toEqual([]);
    expect(keptFor('s1')).toEqual(TICKS);
  });

  it('nothing is forgotten either, should a Reset come all the same', async () => {
    keeps('s1', SOURCE, TICKS);
    server.read = new Error('Failed to fetch');
    const { data } = await drawn();
    // The block offers no Reset for a widget whose saved state it could not read.
    expect(data.has()).toBe(false);
    forgetWidgetData(CHAT, SOURCE);
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    expect(server.writes).toEqual([]);
    expect(keptFor('s1')).toEqual(TICKS);
  });

  it('the next widget asks again, and the one that started without stays apart', async () => {
    keeps('s1', SOURCE, TICKS);
    server.read = new Error('Failed to fetch');
    const heard: string[] = [];
    const blind = await drawn(CHAT, SOURCE, () => heard.push('changed'));
    server.read = 'answers';
    // Reload, or the block drawn again: a new read, which answers now.
    const again = await drawn();
    expect(server.reads).toEqual(['s1', 's1']);
    expect(again.saved).toEqual(TICKS);
    expect(again.data.notice()).toBeNull();
    // The first one still runs with nothing, and says so; it saves nothing.
    expect(blind.data.notice()).toBe('unavailable');
    expect(blind.data.has()).toBe(false);
    blind.data.take([]);
    await settle();
    expect(server.writes).toEqual([]);
    expect(keptFor('s1')).toEqual(TICKS);
  });
});

describe('what a widget saves', () => {
  it('goes to the server at once, under the widget\'s name in its chat', async () => {
    const { data } = await drawn();
    data.take(TICKS);
    expect(server.writes).toEqual([{ session: 's1', id: ID, items: TICKS, keepalive: false }]);
    await settle();
    expect(keptFor('s1')).toEqual(TICKS);
    expect(data.has()).toBe(true);
    expect(data.notice()).toBeNull();
  });

  it('is what the same widget starts with when it is drawn again here, without a new request', async () => {
    const first = await drawn();
    first.data.take(TICKS);
    first.data.close();
    const again = await drawn();
    expect(again.saved).toEqual(TICKS);
    expect(server.reads).toEqual(['s1']);
  });

  it('goes one request at a time, the newest report winning', async () => {
    server.write = 'waits';
    const { data } = await drawn();
    data.take([['n', '1']]);
    for (let n = 2; n <= 50; n++) data.take([['n', String(n)]]);
    expect(server.writes).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(10_000);
    // The first is still on its way: nothing overtakes it.
    expect(server.writes).toHaveLength(1);
    server.waiting.shift()!.resolve();
    await settle();
    expect(server.writes.map(w => w.items)).toEqual([[['n', '1']], [['n', '50']]]);
    server.waiting.shift()!.resolve();
    await settle();
    expect(keptFor('s1')).toEqual([['n', '50']]);
  });

  it('is written at most every STORE_INTERVAL_MS', async () => {
    const { data } = await drawn();
    for (let ms = 0; ms < 1000; ms += 2) {
      data.take([['n', String(ms)]]);
      await vi.advanceTimersByTimeAsync(2);
    }
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    expect(server.writes.length).toBeLessThanOrEqual(1000 / STORE_INTERVAL_MS + 1);
    expect(keptFor('s1')).toEqual([['n', '998']]);
  });

  it('is deleted on the server when the widget holds nothing any more', async () => {
    keeps('s1', SOURCE, TICKS);
    const { data } = await drawn();
    data.take([]);
    await settle();
    expect(server.writes).toEqual([{ session: 's1', id: ID, items: null, keepalive: false }]);
    expect(keptFor('s1')).toBeNull();
    expect(data.has()).toBe(false);
  });

  it('is told about: when the widget has data that it had not, and when it has none any more', async () => {
    const heard: boolean[] = [];
    const { data } = await drawn();
    const watcher = new WidgetData(CHAT, SOURCE, () => heard.push(data.has()));
    data.take(TICKS);
    expect(heard).toEqual([true]);
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    data.take([['ticks', '[]']]);
    expect(heard).toEqual([true]);
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    data.take([]);
    expect(heard).toEqual([true, false]);
    watcher.close();
  });

  it('stays the widget\'s own: two widgets of a chat, and one widget in two chats', async () => {
    const one = await drawn(CHAT, SOURCE);
    const two = await drawn(CHAT, OTHER_SOURCE);
    const elsewhere = await drawn(OTHER_CHAT, SOURCE);
    one.data.take([['who', 'first']]);
    two.data.take([['who', 'second']]);
    elsewhere.data.take([['who', 'third']]);
    await settle();
    expect(keptFor('s1', SOURCE)).toEqual([['who', 'first']]);
    expect(keptFor('s1', OTHER_SOURCE)).toEqual([['who', 'second']]);
    expect(keptFor('s2', SOURCE)).toEqual([['who', 'third']]);
    expect((await drawn(OTHER_CHAT, OTHER_SOURCE)).saved).toEqual([]);
  });

  it('is nothing the browser keeps', async () => {
    const write = vi.spyOn(localStorage, 'setItem');
    const { data } = await drawn();
    data.take(TICKS);
    data.close();
    await settle();
    expect(write).not.toHaveBeenCalled();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});

describe('when a write fails', () => {
  it('the widget goes on, the block hears of it, and the write is tried again by itself', async () => {
    const heard: Array<string | null> = [];
    const { data } = await drawn();
    const watcher = new WidgetData(CHAT, SOURCE, () => heard.push(data.notice()));
    server.write = new Error('Failed to fetch');
    data.take(TICKS);
    await settle();
    expect(data.notice()).toBe('unsaved');
    expect(data.has()).toBe(true);
    expect(server.writes).toHaveLength(1);
    // Again after the first wait, and after the second; then no more by itself.
    await vi.advanceTimersByTimeAsync(RETRY_MS[0]);
    expect(server.writes).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(RETRY_MS[1]);
    expect(server.writes).toHaveLength(3);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(server.writes).toHaveLength(3);
    expect(keptFor('s1')).toBeNull();
    // The server is back, and the widget's next change takes everything along.
    server.write = 'answers';
    data.take([...TICKS, ['count', '1']]);
    await settle();
    expect(keptFor('s1')).toEqual([...TICKS, ['count', '1']]);
    expect(data.notice()).toBeNull();
    expect(heard).toContain('unsaved');
    expect(heard.at(-1)).toBeNull();
    watcher.close();
  });

  it('a write tried again that gets through clears the notice', async () => {
    const { data } = await drawn();
    server.write = new ApiError(503, 'Service Unavailable');
    data.take(TICKS);
    await settle();
    server.write = 'answers';
    await vi.advanceTimersByTimeAsync(RETRY_MS[0]);
    expect(keptFor('s1')).toEqual(TICKS);
    expect(data.notice()).toBeNull();
  });

  it('what the server refuses is not sent again as it is; the widget\'s next change is', async () => {
    const { data } = await drawn();
    server.write = new ApiError(413, '{"detail":"chat_full"}');
    data.take(TICKS);
    await settle();
    expect(data.notice()).toBe('unsaved');
    await vi.advanceTimersByTimeAsync(600_000);
    expect(server.writes).toHaveLength(1);
    server.write = 'answers';
    data.take([['ticks', '[]']]);
    await settle();
    expect(server.writes).toHaveLength(2);
    expect(keptFor('s1')).toEqual([['ticks', '[]']]);
    expect(data.notice()).toBeNull();
  });

  it('nothing more is sent for a chat that is gone', async () => {
    const { data } = await drawn();
    server.write = new ApiError(404, '{"detail":"session_not_found"}');
    data.take(TICKS);
    await settle();
    data.take([['ticks', '[]']]);
    data.close();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(server.writes).toHaveLength(1);
    expect(data.notice()).toBe('unsaved');
  });
});

describe('what waits to be written', () => {
  it('is written when the widget\'s frame goes', async () => {
    const { data } = await drawn();
    data.take([['n', '1']]);
    data.take([['n', '2']]);
    await settle();
    expect(keptFor('s1')).toEqual([['n', '1']]);
    data.close();
    await settle();
    expect(keptFor('s1')).toEqual([['n', '2']]);
    // And nothing is taken from a frame that is gone.
    data.take([['n', '3']]);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(keptFor('s1')).toEqual([['n', '2']]);
  });

  it.each([
    ['is left', () => window.dispatchEvent(new Event('pagehide'))],
    ['is hidden', () => {
      vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
      document.dispatchEvent(new Event('visibilitychange'));
    }],
  ])('goes out when the page %s, as a request the browser still sends then', async (_what, leave) => {
    const one = await drawn(CHAT, SOURCE);
    const two = await drawn(OTHER_CHAT, OTHER_SOURCE);
    one.data.take([['n', '1']]);
    one.data.take([['n', '2']]);
    two.data.take([['m', '1']]);
    two.data.take([['m', '2']]);
    await settle();
    server.writes.length = 0;
    leave();
    expect(server.writes).toEqual([
      { session: 's1', id: ID, items: [['n', '2']], keepalive: true },
      { session: 's2', id: widgetId(OTHER_SOURCE), items: [['m', '2']], keepalive: true },
    ]);
  });

  it('goes out for a page that is left while an older write is still on its way', async () => {
    server.write = 'waits';
    const { data } = await drawn();
    data.take([['n', '1']]);
    data.take([['n', '2']]);
    window.dispatchEvent(new Event('pagehide'));
    expect(server.writes.map(w => [w.items, w.keepalive])).toEqual([[[['n', '1']], false], [[['n', '2']], true]]);
  });

  it('goes out once more when two writes were on their way at once, so that the server ends with the newest', async () => {
    server.write = 'waits';
    const { data } = await drawn();
    data.take([['n', '1']]);
    data.take([['n', '2']]);
    // The page is hidden, not gone: it lives on and hears the answers.
    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    expect(server.waiting).toHaveLength(2);
    // The server takes the newer write first, and the older one after it.
    server.waiting[1].resolve();
    await settle();
    expect(server.writes).toHaveLength(2);
    server.waiting[0].resolve();
    await settle();
    expect(keptFor('s1')).toEqual([['n', '1']]);
    // Both are answered: what the widget holds goes out again.
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    expect(server.writes.map(w => w.items)).toEqual([[['n', '1']], [['n', '2']], [['n', '2']]]);
    server.waiting[2].resolve();
    await settle();
    expect(keptFor('s1')).toEqual([['n', '2']]);
    // And that is all: nothing more goes out.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.writes).toHaveLength(3);
    expect(data.notice()).toBeNull();
  });

  it('goes out as an ordinary request when it is more than a page that is left may send', async () => {
    const small = await drawn(CHAT, SOURCE);
    const large = await drawn(CHAT, OTHER_SOURCE);
    const third = await drawn(CHAT, 'a third');
    for (const { data } of [small, large, third]) data.take([['first', 'write']]);
    await settle();
    small.data.take([['fill', 'x'.repeat(40_000)]]);
    large.data.take([['fill', 'x'.repeat(40_000)]]);
    third.data.take([['n', '2']]);
    server.writes.length = 0;
    window.dispatchEvent(new Event('pagehide'));
    // 60,000 bytes for all of them together: the second large one does not fit, the small one after it does.
    expect(server.writes.map(w => w.keepalive)).toEqual([true, false, true]);
    expect(LEAVING_BYTES).toBeLessThan(64 * 1024);
  });

  it('is nothing when the widget saved all it had', async () => {
    const { data } = await drawn();
    data.take(TICKS);
    await settle();
    server.writes.length = 0;
    window.dispatchEvent(new Event('pagehide'));
    data.close();
    expect(server.writes).toEqual([]);
  });
});

describe('forgetting', () => {
  it('Reset deletes what the widget saved on the server, and the widget starts with nothing', async () => {
    keeps('s1', SOURCE, TICKS);
    keeps('s1', OTHER_SOURCE, [['n', '3']]);
    const { data } = await drawn();
    data.close();
    forgetWidgetData(CHAT, SOURCE);
    expect(server.writes).toEqual([{ session: 's1', id: ID, items: null, keepalive: false }]);
    await settle();
    expect(keptFor('s1')).toBeNull();
    expect(keptFor('s1', OTHER_SOURCE)).toEqual([['n', '3']]);
    const again = await drawn();
    expect(again.saved).toEqual([]);
    expect(again.data.has()).toBe(false);
  });

  it('Reset comes after a write that is still on its way, never before it', async () => {
    server.write = 'waits';
    const { data } = await drawn();
    data.take(TICKS);
    data.take([...TICKS, ['count', '2']]);
    data.close();
    forgetWidgetData(CHAT, SOURCE);
    expect(server.writes).toHaveLength(1);
    server.waiting.shift()!.resolve();
    await vi.advanceTimersByTimeAsync(STORE_INTERVAL_MS);
    // What waited was overtaken by the Reset: the delete is the next and last request.
    expect(server.writes.map(w => w.items)).toEqual([TICKS, null]);
    server.waiting.shift()!.resolve();
    await settle();
    expect(keptFor('s1')).toBeNull();
  });

  it('there is nothing to forget outside a chat', async () => {
    forgetWidgetData(widgetScope('s1', '/tasks/t-17'), SOURCE);
    await settle();
    expect(server.reads).toEqual([]);
    expect(server.writes).toEqual([]);
  });

  it('a chat is asked about again once its last widget has been gone for a while', async () => {
    const first = await drawn();
    first.data.take(TICKS);
    await settle();
    first.data.close();
    // Another device changes the widget meanwhile.
    keeps('s1', SOURCE, [['ticks', '["stove"]']]);
    await vi.advanceTimersByTimeAsync(FORGET_MS - 1);
    expect((await drawn()).saved).toEqual(TICKS);
    expect(server.reads).toEqual(['s1']);
  });

  it('and then starts with what the server has, another device\'s changes among it', async () => {
    const first = await drawn();
    first.data.take(TICKS);
    await settle();
    first.data.close();
    keeps('s1', SOURCE, [['ticks', '["stove"]']]);
    await vi.advanceTimersByTimeAsync(FORGET_MS);
    expect((await drawn()).saved).toEqual([['ticks', '["stove"]']]);
    expect(server.reads).toEqual(['s1', 's1']);
  });

  it('a chat is not forgotten while a write is on its way', async () => {
    server.write = 'waits';
    const first = await drawn();
    first.data.take(TICKS);
    first.data.close();
    await vi.advanceTimersByTimeAsync(FORGET_MS * 3);
    // Drawn again: the page knows what the widget saved, though the server does not yet.
    expect((await drawn()).saved).toEqual(TICKS);
    expect(server.reads).toEqual(['s1']);
  });
});

// The type of the server's answer, as the page reads it.
void (null as unknown as WidgetStates);
