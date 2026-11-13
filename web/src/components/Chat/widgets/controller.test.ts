import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WidgetHostOptions, WidgetSnapshot } from './host';

/** A frame the test steers: it records what the controller does with it. */
class FakeHost {
  static made: FakeHost[] = [];
  options: WidgetHostOptions;
  started: HTMLElement | null = null;
  disposed = false;
  themes: string[] = [];

  constructor(options: WidgetHostOptions) {
    this.options = options;
    FakeHost.made.push(this);
  }

  start(container: HTMLElement) { this.started = container; }
  setTheme(theme: string) { this.themes.push(theme); }
  dispose() { this.disposed = true; }
  say(change: Partial<WidgetSnapshot>) {
    this.options.onChange({ status: 'loading', error: null, ...change });
  }
}

vi.mock('./host', () => ({ WidgetHost: FakeHost }));

type Items = ReadonlyArray<readonly [string, string]>;

/**
 * The saved data of a widget, as the controller uses it: a stand-in the test
 * steers. `storage.test.ts` covers the real one, against a server.
 */
class FakeData {
  static made: FakeData[] = [];
  /** What the widgets saved: place and source to items. */
  static kept = new Map<string, Items>();
  /** The read that new data waits for; null when what was saved is known already. */
  static reading: { promise: Promise<void>; answer: () => void } | null = null;
  static says: 'unavailable' | 'unsaved' | null = null;
  static limit = 65536;
  /** What happened to the data of widgets, in order. */
  static log: string[] = [];

  readonly key: string;
  readonly ready: Promise<void>;
  isReady: boolean;
  started = false;
  closed = false;
  private readonly onChange: () => void;

  constructor(scope: string, source: string, onChange: () => void) {
    this.key = `${scope}|${source}`;
    this.onChange = onChange;
    const reading = FakeData.reading;
    this.isReady = reading === null;
    this.ready = reading ? reading.promise.then(() => { this.isReady = true; }) : Promise.resolve();
    FakeData.made.push(this);
  }

  start() {
    this.started = true;
    return { saved: FakeData.kept.get(this.key) ?? [], limit: FakeData.limit };
  }

  has() { return (FakeData.kept.get(this.key) ?? []).length > 0; }
  notice() { return FakeData.says; }

  take(items: Items) {
    if (this.closed) return;
    if (items.length > 0) FakeData.kept.set(this.key, items);
    else FakeData.kept.delete(this.key);
    this.onChange();
  }

  /** The data says something new of itself: what `notice` says changed. */
  changed() { this.onChange(); }

  close() {
    this.closed = true;
    FakeData.log.push(`close ${this.key}`);
  }

  /** A read that the next data waits for, answered when the test says. */
  static wait() {
    let answer = () => {};
    const promise = new Promise<void>((resolve) => { answer = resolve; });
    FakeData.reading = { promise, answer: () => { FakeData.reading = null; answer(); } };
    return FakeData.reading;
  }
}

vi.mock('./storage', () => ({
  WidgetData: FakeData,
  forgetWidgetData: (scope: string, source: string) => {
    FakeData.kept.delete(`${scope}|${source}`);
    FakeData.log.push(`forget ${scope}|${source}`);
  },
}));

const { MAX_LIVE_WIDGETS, liveWidgets, resetWidgetSlots } = await import('./budget');
const { WidgetController } = await import('./controller');
const { resetWidgetHeights, widgetHeight, widgetKey } = await import('./heights');

const SOURCE = '<p>widget</p>';

type Inputs = Parameters<InstanceType<typeof WidgetController>['sync']>[0];
const SCOPE = 'chat/s1';
const inputs = (overrides: Partial<Inputs> = {}): Inputs => ({
  source: SOURCE, near: true, theme: 'dark', scope: SCOPE, ...overrides,
});

/** A block's controller with a place for its frame. */
function block(first: Partial<Inputs> | null = {}) {
  const controller = new WidgetController();
  const place = document.createElement('div');
  const seen: string[] = [];
  controller.subscribe(() => seen.push(controller.getSnapshot().phase));
  controller.place(place);
  if (first) controller.sync(inputs(first));
  return { controller, place, seen, phase: () => controller.getSnapshot().phase };
}

const lastHost = () => FakeHost.made.at(-1)!;
const lastData = () => FakeData.made.at(-1)!;

beforeEach(() => {
  FakeHost.made = [];
  FakeData.made = [];
  FakeData.kept.clear();
  FakeData.reading = null;
  FakeData.says = null;
  FakeData.limit = 65536;
  FakeData.log = [];
  resetWidgetSlots();
  resetWidgetHeights();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('when a widget gets a frame', () => {
  it('has none until the block knows what to draw and where', () => {
    const b = block(null);
    expect(b.phase()).toBe('idle');
    b.controller.sync(inputs({ source: null }));
    expect(FakeHost.made).toEqual([]);
    expect(liveWidgets()).toBe(0);
  });

  it('has none while the block is far from the screen', () => {
    const b = block({ near: false });
    expect(b.phase()).toBe('idle');
    expect(FakeHost.made).toEqual([]);
    b.controller.sync(inputs({ near: true }));
    expect(b.phase()).toBe('loading');
    expect(FakeHost.made).toHaveLength(1);
  });

  it('opens one in the block\'s place, for the source, the sandbox and the theme it was told', () => {
    const b = block({ theme: 'light' });
    const host = lastHost();
    expect(host.started).toBe(b.place);
    expect(host.options).toMatchObject({ source: SOURCE, theme: 'light' });
    expect(b.phase()).toBe('loading');
    host.say({ status: 'shown' });
    expect(b.phase()).toBe('shown');
    expect(b.seen).toEqual(['loading', 'shown']);
  });

  it('has none without a place for it', () => {
    const controller = new WidgetController();
    controller.sync(inputs());
    expect(FakeHost.made).toEqual([]);
    expect(controller.getSnapshot().phase).toBe('idle');
  });

  it('keeps its frame when the block leaves the screen', () => {
    const b = block();
    lastHost().say({ status: 'shown' });
    b.controller.sync(inputs({ near: false }));
    expect(b.phase()).toBe('shown');
    expect(lastHost().disposed).toBe(false);
    expect(FakeHost.made).toHaveLength(1);
  });

  it('passes a new theme on, without a new frame', () => {
    const b = block();
    b.controller.sync(inputs({ theme: 'light' }));
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().themes).toEqual(['light']);
  });

  it('starts at the height the widget had before, and remembers the one it reports', () => {
    block();
    expect(lastHost().options.height).toBeUndefined();
    lastHost().options.onHeight!(345);
    expect(widgetHeight(widgetKey(SOURCE))).toBe(345);
    block();
    expect(lastHost().options.height).toBe(345);
  });
});

describe('when a widget loses its frame', () => {
  it('for another source: the old frame goes, a new one opens', () => {
    const b = block();
    const first = lastHost();
    first.say({ status: 'shown' });
    b.controller.sync(inputs({ source: '<p>more text</p>' }));
    expect(first.disposed).toBe(true);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.source).toBe('<p>more text</p>');
    expect(b.phase()).toBe('loading');
    // One slot, not two.
    expect(liveWidgets()).toBe(1);
  });

  it('when there is nothing to draw any more: the feature went off', () => {
    const b = block();
    b.controller.sync(inputs({ source: null }));
    expect(lastHost().disposed).toBe(true);
    expect(b.phase()).toBe('idle');
    expect(liveWidgets()).toBe(0);
  });

  it('when the block goes, and the block can come back', () => {
    const b = block();
    b.controller.stop();
    expect(lastHost().disposed).toBe(true);
    expect(liveWidgets()).toBe(0);
    expect(b.phase()).toBe('idle');
    b.controller.sync(inputs());
    expect(FakeHost.made).toHaveLength(2);
    expect(b.phase()).toBe('loading');
  });

  it('when its place goes', () => {
    const b = block();
    b.controller.place(null);
    expect(lastHost().disposed).toBe(true);
    expect(liveWidgets()).toBe(0);
    expect(b.phase()).toBe('idle');
  });

  it('ignores what a frame that is gone still says', () => {
    const b = block();
    const first = lastHost();
    b.controller.sync(inputs({ source: '<p>next</p>' }));
    first.say({ status: 'failed', error: 'late' });
    expect(b.phase()).toBe('loading');
  });
});

describe('a widget that failed', () => {
  function failed() {
    const b = block();
    lastHost().say({ status: 'failed', error: 'The widget sandbox does not answer.' });
    return b;
  }

  it('shows why, and holds no slot', () => {
    const b = failed();
    expect(b.controller.getSnapshot()).toEqual({
      phase: 'failed', error: 'The widget sandbox does not answer.', saved: false, notice: null,
    });
    expect(liveWidgets()).toBe(0);
  });

  it('is not tried again on its own', () => {
    const b = failed();
    b.controller.sync(inputs({ near: false }));
    b.controller.sync(inputs({ near: true }));
    b.controller.sync(inputs({ theme: 'light' }));
    expect(FakeHost.made).toHaveLength(1);
    expect(b.phase()).toBe('failed');
  });

  it('stays failed when its place goes and comes back: the block shows the source meanwhile', () => {
    const b = failed();
    b.controller.place(null);
    expect(b.phase()).toBe('failed');
    b.controller.place(b.place);
    expect(b.phase()).toBe('failed');
    expect(FakeHost.made).toHaveLength(1);
  });

  it('is tried again when the person asks', () => {
    const b = failed();
    b.controller.load();
    expect(FakeHost.made).toHaveLength(2);
    expect(b.controller.getSnapshot()).toEqual({
      phase: 'loading', error: null, saved: false, notice: null,
    });
  });

  it('is tried again for another source', () => {
    const b = failed();
    b.controller.sync(inputs({ source: '<p>other</p>' }));
    expect(FakeHost.made).toHaveLength(2);
    expect(b.phase()).toBe('loading');
  });
});

describe('Reload', () => {
  it('draws the widget again from its source, in a new frame', () => {
    const b = block();
    const first = lastHost();
    first.say({ status: 'shown' });
    b.controller.load();
    expect(first.disposed).toBe(true);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.source).toBe(SOURCE);
    expect(b.phase()).toBe('loading');
    expect(liveWidgets()).toBe(1);
  });
});

describe('what the widget saves', () => {
  const TICKS: Items = [['ticks', '["tent"]'], ['tab', 'gear']];
  const key = (scope = SCOPE, source = SOURCE) => `${scope}|${source}`;

  it('starts with nothing the first time, and with the room its data gives it', () => {
    FakeData.limit = 4000;
    const b = block();
    expect(lastHost().options).toMatchObject({ saved: [], limit: 4000 });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'loading', saved: false, notice: null });
    expect(lastData()).toMatchObject({ key: key(), started: true });
  });

  it('starts with what it saved in the place where the block is', () => {
    FakeData.kept.set(key(), TICKS);
    const b = block();
    expect(lastHost().options.saved).toEqual(TICKS);
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'loading', saved: true });
    lastHost().say({ status: 'shown' });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', saved: true });
  });

  it('is not what the same source starts with in another chat, nor another source in this chat', () => {
    FakeData.kept.set(key(), TICKS);
    block({ scope: 'chat/s2' });
    expect(lastHost().options.saved).toEqual([]);
    expect(lastData().key).toBe(key('chat/s2'));
    block({ source: '<p>another widget</p>' });
    expect(lastHost().options.saved).toEqual([]);
  });

  it('waits for what the chat\'s widgets saved before it opens the frame', async () => {
    const reading = FakeData.wait();
    FakeData.kept.set(key(), TICKS);
    const b = block();
    // The widget has its place and its slot, and no frame yet.
    expect(b.phase()).toBe('loading');
    expect(FakeHost.made).toEqual([]);
    expect(liveWidgets()).toBe(1);
    expect(lastData().started).toBe(false);
    reading.answer();
    await lastData().ready;
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().started).toBe(b.place);
    expect(lastHost().options.saved).toEqual(TICKS);
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'loading', saved: true });
  });

  it('asks once while it waits, and opens the frame in the theme the chat has by then', async () => {
    const reading = FakeData.wait();
    const b = block();
    b.controller.sync(inputs({ theme: 'light' }));
    b.controller.sync(inputs({ theme: 'light', near: false }));
    expect(FakeData.made).toHaveLength(1);
    reading.answer();
    await lastData().ready;
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().options.theme).toBe('light');
    expect(lastHost().themes).toEqual([]);
  });

  it('opens no frame for what the block wanted before it changed its mind', async () => {
    const reading = FakeData.wait();
    const b = block();
    const first = lastData();
    b.controller.sync(inputs({ source: '<p>another widget</p>' }));
    expect(first.closed).toBe(true);
    expect(FakeData.made).toHaveLength(2);
    reading.answer();
    await Promise.all(FakeData.made.map(data => data.ready));
    expect(FakeHost.made).toHaveLength(1);
    expect(lastHost().options.source).toBe('<p>another widget</p>');
    expect(first.started).toBe(false);
  });

  it.each([
    ['the block went', (b: ReturnType<typeof block>) => b.controller.stop()],
    ['its place went', (b: ReturnType<typeof block>) => b.controller.place(null)],
    ['there is nothing to draw any more', (b: ReturnType<typeof block>) => b.controller.sync(inputs({ source: null }))],
  ])('opens no frame when %s while it waited', async (_what, leave) => {
    const reading = FakeData.wait();
    const b = block();
    const data = lastData();
    leave(b);
    expect(data.closed).toBe(true);
    expect(liveWidgets()).toBe(0);
    reading.answer();
    await data.ready;
    expect(FakeHost.made).toEqual([]);
    expect(b.phase()).toBe('idle');
  });

  it('passes on what the widget reports, and shows that it has saved', () => {
    const b = block();
    lastHost().say({ status: 'shown' });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', saved: false });
    lastHost().options.onStorage!(TICKS);
    expect(FakeData.kept.get(key())).toEqual(TICKS);
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', saved: true });
    lastHost().options.onStorage!([]);
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', saved: false });
  });

  it.each(['unavailable', 'unsaved'] as const)('shows when what the widget holds is not kept: %s', (says) => {
    const b = block();
    lastHost().say({ status: 'shown' });
    FakeData.says = says;
    lastData().changed();
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', notice: says });
    FakeData.says = null;
    lastData().changed();
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', notice: null });
  });

  it('shows from the start that the saved state could not be read, and runs the widget', () => {
    FakeData.says = 'unavailable';
    const b = block();
    expect(FakeHost.made).toHaveLength(1);
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'loading', saved: false, notice: 'unavailable' });
    lastHost().say({ status: 'shown' });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'shown', notice: 'unavailable' });
  });

  it('is there after Reload, and stays when the theme changes', () => {
    const b = block();
    lastHost().options.onStorage!(TICKS);
    b.controller.sync(inputs({ theme: 'light' }));
    expect(FakeData.made).toHaveLength(1);
    b.controller.load();
    expect(FakeData.made[0].closed).toBe(true);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.saved).toEqual(TICKS);
  });

  it('is left behind when the block shows up in another place', () => {
    const b = block();
    const first = lastHost();
    first.options.onStorage!(TICKS);
    b.controller.sync(inputs({ scope: 'chat/s2' }));
    expect(first.disposed).toBe(true);
    expect(FakeData.made[0].closed).toBe(true);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options.saved).toEqual([]);
    expect(liveWidgets()).toBe(1);
  });

  it.each([
    ['the block goes', (b: ReturnType<typeof block>) => b.controller.stop()],
    ['its place goes', (b: ReturnType<typeof block>) => b.controller.place(null)],
    ['the frame fails', () => lastHost().say({ status: 'failed', error: 'The widget sandbox does not answer.' })],
  ])('is closed, so that what waits is written, when %s', (_what, leave) => {
    const b = block();
    lastHost().options.onStorage!(TICKS);
    leave(b);
    expect(lastData().closed).toBe(true);
    expect(FakeData.kept.get(key())).toEqual(TICKS);
  });

  it('is not taken from a frame that is gone', () => {
    const b = block();
    const first = lastHost();
    first.options.onStorage!(TICKS);
    b.controller.load();
    first.options.onStorage!([['late', 'word']]);
    expect(FakeData.kept.get(key())).toEqual(TICKS);
    expect(b.controller.getSnapshot().saved).toBe(true);
  });

  it('is forgotten by Reset, which draws the widget again as the model wrote it', () => {
    const b = block();
    const first = lastHost();
    first.say({ status: 'shown' });
    first.options.onStorage!(TICKS);
    b.controller.reset();
    expect(first.disposed).toBe(true);
    // The frame's data is closed first: what it still had to write goes before the forgetting.
    expect(FakeData.log).toEqual([`close ${key()}`, `forget ${key()}`]);
    expect(FakeHost.made).toHaveLength(2);
    expect(lastHost().options).toMatchObject({ source: SOURCE, saved: [] });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'loading', saved: false });
    expect(liveWidgets()).toBe(1);
  });

  it('is forgotten by Reset for this widget only', () => {
    const other = block({ source: '<p>another widget</p>' });
    lastHost().options.onStorage!([['who', 'another']]);
    const b = block();
    lastHost().options.onStorage!(TICKS);
    b.controller.reset();
    expect(FakeData.kept.has(key())).toBe(false);
    expect(FakeData.kept.get(key(SCOPE, '<p>another widget</p>'))).toEqual([['who', 'another']]);
    expect(other.controller.getSnapshot().saved).toBe(true);
  });

  it('does not show for a widget that failed', () => {
    const b = block();
    lastHost().options.onStorage!(TICKS);
    lastHost().say({ status: 'failed', error: 'The widget sandbox does not answer.' });
    expect(b.controller.getSnapshot()).toMatchObject({ phase: 'failed', saved: false, notice: null });
    // What it saved is still there for the next try.
    b.controller.load();
    expect(lastHost().options.saved).toEqual(TICKS);
  });
});

describe('the mount budget', () => {
  it('leaves one widget too many waiting for a click while all the others are on screen', () => {
    const blocks = Array.from({ length: MAX_LIVE_WIDGETS }, () => block());
    const late = block();
    expect(late.phase()).toBe('waiting');
    expect(FakeHost.made).toHaveLength(MAX_LIVE_WIDGETS);
    expect(blocks.every(b => b.phase() === 'loading')).toBe(true);
  });

  it('loads it when the person clicks, and stops the widget used longest ago', () => {
    const blocks = Array.from({ length: MAX_LIVE_WIDGETS }, () => block());
    const hosts = [...FakeHost.made];
    const late = block();
    late.controller.load();
    expect(late.phase()).toBe('loading');
    expect(hosts[0].disposed).toBe(true);
    // The one that gave way is on screen too: it waits for a click now.
    expect(blocks[0].phase()).toBe('waiting');
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
  });

  it('takes the frame of a widget that is off the screen, which comes back when it is on screen again', () => {
    const blocks = Array.from({ length: MAX_LIVE_WIDGETS }, () => block());
    const hosts = [...FakeHost.made];
    blocks[1].controller.sync(inputs({ near: false }));
    const late = block();
    expect(late.phase()).toBe('loading');
    expect(hosts[1].disposed).toBe(true);
    expect(blocks[1].phase()).toBe('idle');
    // The reader scrolls back: another widget has left the screen by then.
    blocks[4].controller.sync(inputs({ near: false }));
    blocks[1].controller.sync(inputs({ near: true }));
    expect(blocks[1].phase()).toBe('loading');
    expect(hosts[4].disposed).toBe(true);
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
  });

  it('never has more frames than the limit, in a chat full of widgets', () => {
    const blocks = Array.from({ length: 30 }, () => block({ near: false }));
    for (const [index, b] of blocks.entries()) {
      b.controller.sync(inputs({ near: true }));
      blocks[index - 2]?.controller.sync(inputs({ near: false }));
      expect(FakeHost.made.filter(h => !h.disposed).length).toBeLessThanOrEqual(MAX_LIVE_WIDGETS);
    }
    expect(FakeHost.made.filter(h => !h.disposed)).toHaveLength(MAX_LIVE_WIDGETS);
    expect(blocks.filter(b => b.phase() === 'loading')).toHaveLength(MAX_LIVE_WIDGETS);
  });
});
