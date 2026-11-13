import { api, type WidgetStateLimits } from '../../../api/client';
import { ApiError } from '../../../api/hosted';
import { sha256 } from './sha256';

/**
 * What a widget keeps: its `localStorage`, held for it by Nerve, with its
 * chat.
 *
 * The frame of a widget has no origin, so the browser gives it no storage.
 * The head of its document (`wrap.ts`) puts a `localStorage` of Nerve's in
 * that place, filled with what the widget saved before, and tells the page
 * of every change. The page keeps nothing of it in the browser: it reads
 * what the widgets of the chat saved from the server, once for the chat,
 * before the first of them is drawn, and writes a widget's keys and values
 * to the server when they change (`docs/widgets.md`, `docs/api.md`).
 *
 * - Whose data. A widget is its source in its chat (`widgetId`). The same
 *   text drawn again finds its data: after a reload of the page, on another
 *   device, when the stored message replaces the streamed one, after Reload.
 *   The same text in another chat does not, and another text in this chat
 *   does not. A widget outside a chat (the text of a task, a plan, a file)
 *   keeps nothing beyond its frame.
 * - How much. MAX_WIDGET_DATA_CHARS and MAX_WIDGET_DATA_KEYS for one widget,
 *   and what the server allows the chat's widgets together: the widget's
 *   own script is refused more (`limit`), and the server refuses what gets
 *   past that.
 * - How often. A widget's data is written at most every STORE_INTERVAL_MS,
 *   one request at a time, the newest report winning; what waits is written
 *   when the widget's frame goes and when the page is left.
 * - When the server does not answer. The widget runs all the same: without
 *   what it saved when the read fails (and nothing is written then, so that
 *   what the server has is not written over unseen), and with its changes
 *   written later when a write fails.
 *
 * The data is text of the widget's choosing. The page checks its shape and
 * its size and passes it on; it reads nothing in it and shows none of it.
 */

/** What one widget may keep: the characters of its keys and values together. */
export const MAX_WIDGET_DATA_CHARS = 64 * 1024;
/** How many keys one widget may have. */
export const MAX_WIDGET_DATA_KEYS = 1024;
/** A widget's data is written at most this often, whatever the widget reports. */
export const STORE_INTERVAL_MS = 250;
/** How long a widget waits for what its chat's widgets saved, before it runs without. */
export const READ_WAIT_MS = 4_000;
/** A write the server did not answer is tried again after these waits, and then with the next change. */
export const RETRY_MS: readonly number[] = [2_000, 10_000];
/** What a chat's widgets saved stays known to the page this long after the last of them went. */
export const FORGET_MS = 5_000;
/**
 * The bytes of the requests a browser still sends for a page that is being
 * left (`keepalive`): 64 KiB for all of them together. A larger write goes
 * out as an ordinary request, which the browser may drop.
 */
export const LEAVING_BYTES = 60_000;

/** A widget's data: its keys and values, in the order the keys were first set. */
export type WidgetItems = ReadonlyArray<readonly [string, string]>;

/**
 * Why what a widget holds is not kept: what its chat's widgets saved could
 * not be read (`unavailable`), or the server did not take its last change
 * (`unsaved`).
 */
export type WidgetNotice = 'unavailable' | 'unsaved' | null;

const NO_ITEMS: WidgetItems = [];
const DONE = Promise.resolve();
const SERVER_LIMITS: WidgetStateLimits = {
  widget_chars: MAX_WIDGET_DATA_CHARS,
  widget_keys: MAX_WIDGET_DATA_KEYS,
  chat_chars: 1024 * 1024,
  chat_widgets: 200,
};

/**
 * Where a widget lives: the chat on screen or, outside a chat (the text of
 * a task, a plan, a file), the page it is on.
 */
export function widgetScope(session: string, path: string): string {
  return session && /^\/chat(\/|$)/.test(path)
    ? `chat/${encodeURIComponent(session)}`
    : `page/${encodeURIComponent(path)}`;
}

/** The session of a scope that is a chat; null for any other place. */
function chatOf(scope: string): string | null {
  return scope.startsWith('chat/') ? decodeURIComponent(scope.slice('chat/'.length)) : null;
}

/** The name of a widget in its chat: the first 32 hex digits of the SHA-256 of its source. */
export function widgetId(source: string): string {
  return sha256(source).slice(0, 32);
}

/** The characters a widget's data counts for. */
export function widgetDataSize(items: WidgetItems): number {
  return items.reduce((sum, [key, text]) => sum + key.length + text.length, 0);
}

/**
 * A widget's data as a message or the server carries it, checked: a list of
 * pairs of texts, each key once, within what one widget may keep. Null for
 * anything else. The result is a list of its own: nothing of what was read
 * is kept.
 */
export function readWidgetItems(value: unknown): WidgetItems | null {
  if (!Array.isArray(value) || value.length > MAX_WIDGET_DATA_KEYS) return null;
  const items: Array<[string, string]> = [];
  const keys = new Set<string>();
  let chars = 0;
  for (const pair of value as unknown[]) {
    if (!Array.isArray(pair) || pair.length !== 2) return null;
    const [key, text] = pair as unknown[];
    if (typeof key !== 'string' || typeof text !== 'string' || keys.has(key)) return null;
    chars += key.length + text.length;
    if (chars > MAX_WIDGET_DATA_CHARS) return null;
    keys.add(key);
    items.push([key, text]);
  }
  return items;
}

/** The limits the server names, each a count; the ones this page knows where it names none. */
function readLimits(value: unknown): WidgetStateLimits {
  const named = (typeof value === 'object' && value !== null ? value : {}) as Record<string, unknown>;
  const limits = { ...SERVER_LIMITS };
  for (const name of Object.keys(limits) as Array<keyof WidgetStateLimits>) {
    const count = named[name];
    if (typeof count === 'number' && Number.isFinite(count) && count >= 0) limits[name] = Math.floor(count);
  }
  return limits;
}

/** The bytes the items take in the body of a write. */
function bodyBytes(items: WidgetItems): number {
  return new TextEncoder().encode(JSON.stringify({ items })).length;
}

/** What has gone out with `keepalive` since the page began to be left. */
let leavingBytes = 0;

/** How one widget's data gets to the server. */
interface Sync {
  /** What the widget holds, which the server should have. */
  wanted: WidgetItems;
  /** The server may have something else than `wanted`. */
  dirty: boolean;
  /** The requests that are on their way. */
  inFlight: number;
  /**
   * Two requests were on their way at once (a page that is left does not
   * wait for its turn): the server may have taken the older one last.
   */
  crossed: boolean;
  /** The server did not take the last write. */
  failed: boolean;
  /** When the last request went out. */
  sentAt: number;
  /** How many times the write was tried again by itself. */
  retries: number;
  timer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * What the widgets of one chat saved, as far as this page knows: what the
 * server said when the page asked, and what the widgets here saved since.
 */
class Chat {
  readonly session: string;
  /** `reading`: the page is asking. `unknown`: the read failed, and nothing is written. */
  status: 'reading' | 'known' | 'unknown' = 'reading';
  private limits = SERVER_LIMITS;
  private readonly kept = new Map<string, WidgetItems>();
  private readonly syncs = new Map<string, Sync>();
  private readonly listeners = new Map<string, Set<() => void>>();
  private reading: Promise<void> | null = null;
  /** The chat is gone, or widgets are off on the server: nothing more is sent. */
  private gone = false;
  private users = 0;
  private forgetTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(session: string) {
    this.session = session;
  }

  /** Ask the server what the chat's widgets saved, once; again when the last answer was none. */
  read(): Promise<void> {
    if (this.status === 'known') return DONE;
    this.reading ??= this.ask().finally(() => {
      this.reading = null;
    });
    return this.reading;
  }

  private async ask(): Promise<void> {
    this.status = 'reading';
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(), READ_WAIT_MS);
    try {
      const answer = await api.getWidgetStates(this.session, deadline.signal);
      this.limits = readLimits(answer?.limits);
      this.kept.clear();
      for (const widget of Array.isArray(answer?.widgets) ? answer.widgets : []) {
        const items = readWidgetItems(widget?.items);
        if (typeof widget?.id === 'string' && items && items.length > 0) this.kept.set(widget.id, items);
      }
      this.status = 'known';
    } catch {
      this.status = 'unknown';
    } finally {
      clearTimeout(timer);
    }
    for (const id of [...this.listeners.keys()]) this.tell(id);
  }

  items(id: string): WidgetItems {
    return this.kept.get(id) ?? NO_ITEMS;
  }

  /** The characters the widget `id` may keep: its own limit, within what the chat has room for. */
  limit(id: string): number {
    if (!this.kept.has(id) && this.kept.size >= this.limits.chat_widgets) return 0;
    let others = 0;
    for (const [other, items] of this.kept) if (other !== id) others += widgetDataSize(items);
    return Math.max(0, Math.min(this.limits.widget_chars, MAX_WIDGET_DATA_CHARS, this.limits.chat_chars - others));
  }

  notice(id: string): WidgetNotice {
    if (this.status === 'unknown') return 'unavailable';
    return this.syncs.get(id)?.failed ? 'unsaved' : null;
  }

  subscribe(id: string, listener: () => void): () => void {
    let listeners = this.listeners.get(id);
    if (!listeners) this.listeners.set(id, listeners = new Set());
    const set = listeners;
    set.add(listener);
    return () => {
      set.delete(listener);
      if (set.size === 0) this.listeners.delete(id);
    };
  }

  private tell(id: string): void {
    for (const listener of [...this.listeners.get(id) ?? []]) listener();
  }

  /** The widget `id` holds `items` now. No items: it holds nothing, and the server forgets it. */
  write(id: string, items: WidgetItems): void {
    // What the server has is not known: nothing is written over it.
    if (this.status !== 'known') return;
    const had = this.kept.has(id);
    if (items.length > 0) this.kept.set(id, items);
    else this.kept.delete(id);
    let sync = this.syncs.get(id);
    if (!sync) {
      sync = {
        wanted: items, dirty: true, inFlight: 0, crossed: false, failed: false, sentAt: 0, retries: 0, timer: undefined,
      };
      this.syncs.set(id, sync);
    }
    sync.wanted = items;
    sync.dirty = true;
    sync.retries = 0;
    // A write that waited to be tried again goes with this one.
    clearTimeout(sync.timer);
    sync.timer = undefined;
    this.schedule(id, sync);
    if (had !== this.kept.has(id)) this.tell(id);
  }

  /** Send what the widget holds: now, or when the last request is STORE_INTERVAL_MS old. */
  private schedule(id: string, sync: Sync): void {
    if (sync.inFlight > 0 || sync.timer !== undefined) return;
    const wait = sync.sentAt + STORE_INTERVAL_MS - Date.now();
    if (wait <= 0) this.send(id);
    else this.later(id, sync, wait);
  }

  private later(id: string, sync: Sync, wait: number): void {
    sync.timer = setTimeout(() => {
      sync.timer = undefined;
      this.send(id);
    }, wait);
  }

  /** Write now what waits for the widget `id`. `leaving`: the page is being left. */
  flush(id: string, leaving = false): void {
    if (this.syncs.get(id)?.dirty) this.send(id, leaving);
  }

  flushAll(leaving: boolean): void {
    for (const id of [...this.syncs.keys()]) this.flush(id, leaving);
  }

  private send(id: string, leaving = false): void {
    const sync = this.syncs.get(id);
    if (!sync || !sync.dirty || this.gone) return;
    // One request at a time, so that the server takes them in order. A page
    // that is being left cannot wait for its turn.
    if (sync.inFlight > 0 && !leaving) return;
    clearTimeout(sync.timer);
    sync.timer = undefined;
    const items = sync.wanted;
    sync.dirty = false;
    if (sync.inFlight > 0) sync.crossed = true;
    sync.inFlight += 1;
    sync.sentAt = Date.now();
    let keepalive = false;
    if (leaving) {
      const bytes = bodyBytes(items);
      keepalive = leavingBytes + bytes <= LEAVING_BYTES;
      if (keepalive) leavingBytes += bytes;
    }
    const request: Promise<unknown> = items.length > 0
      ? api.saveWidgetState(this.session, id, items, keepalive)
      : api.deleteWidgetState(this.session, id, keepalive);
    request.then(() => this.sent(id, sync), error => this.refused(id, sync, error));
  }

  private sent(id: string, sync: Sync): void {
    this.answered(sync);
    sync.retries = 0;
    if (sync.failed) {
      sync.failed = false;
      this.tell(id);
    }
    if (sync.dirty) this.schedule(id, sync);
  }

  /**
   * A request has its answer. When two were on their way at once and both
   * are answered, what the widget holds goes out once more: whichever of
   * the two the server took last, it ends with the newest.
   */
  private answered(sync: Sync): void {
    sync.inFlight -= 1;
    if (sync.inFlight > 0 || !sync.crossed) return;
    sync.crossed = false;
    sync.dirty = true;
  }

  private refused(id: string, sync: Sync, error: unknown): void {
    this.answered(sync);
    const status = error instanceof ApiError ? error.status : 0;
    // The chat is gone, or widgets are off: there is nobody to write to.
    if (status === 404) this.gone = true;
    if (sync.dirty) {
      // The widget has changed since: its newest data goes next.
      this.schedule(id, sync);
    } else {
      sync.dirty = true;
      // No answer, or a fault of the server's: tried again by itself, a few
      // times. What the server refuses is not sent again as it is; the next
      // change of the widget is.
      const temporary = status === 0 || status >= 500;
      if (temporary && !this.gone && sync.retries < RETRY_MS.length) {
        this.later(id, sync, RETRY_MS[sync.retries]);
        sync.retries += 1;
      }
    }
    if (!sync.failed) {
      sync.failed = true;
      this.tell(id);
    }
  }

  /** A widget of the chat is drawn. */
  enter(): void {
    this.users += 1;
    clearTimeout(this.forgetTimer);
  }

  /** A widget of the chat went. A while after the last one, the chat is forgotten. */
  leave(): void {
    this.users -= 1;
    if (this.users <= 0) this.forgetLater();
  }

  /**
   * Forget the chat when no widget of it has come back and nothing waits to
   * be written. Until then a widget that is drawn again (the stored message
   * in the place of the streamed one, Reload) starts with what this page
   * knows, also while its last write is still on its way.
   */
  private forgetLater(): void {
    clearTimeout(this.forgetTimer);
    this.forgetTimer = setTimeout(() => {
      if (this.users > 0) return;
      const busy = [...this.syncs.values()].some(sync => sync.inFlight > 0 || sync.timer !== undefined);
      if (busy) this.forgetLater();
      else if (chats.get(this.session) === this) chats.delete(this.session);
    }, FORGET_MS);
  }
}

/** The chats that have a widget on this page, or had one a moment ago. */
const chats = new Map<string, Chat>();

function chatFor(session: string): Chat {
  let chat = chats.get(session);
  if (!chat) chats.set(session, chat = new Chat(session));
  return chat;
}

/**
 * The data of one widget that is drawn: what its document starts with, and
 * where its reports go.
 */
export class WidgetData {
  /** Kept when the widget may be drawn: what its chat's widgets saved is read, or could not be. */
  readonly ready: Promise<void>;
  private readonly id: string;
  private readonly chat: Chat | null;
  private readonly unsubscribe: (() => void) | null;
  /** The widget runs without what its chat's widgets saved: the read failed. Nothing of it is written. */
  private blind = false;
  private closed = false;

  /**
   * `scope` is where the widget is (`widgetScope`). `onChange` hears when
   * the widget has data that it had not, has none any more, or when what
   * `notice` says changes.
   */
  constructor(scope: string, source: string, onChange: () => void = () => {}) {
    this.id = widgetId(source);
    const session = chatOf(scope);
    // Outside a chat nothing is kept: the widget's storage lasts as long as its frame.
    this.chat = session === null ? null : chatFor(session);
    if (this.chat) {
      this.chat.enter();
      this.unsubscribe = this.chat.subscribe(this.id, () => {
        if (!this.closed) onChange();
      });
      this.ready = this.chat.read();
    } else {
      this.unsubscribe = null;
      this.ready = DONE;
    }
  }

  /** True when the widget can be drawn at once: nothing has to be read first. */
  get isReady(): boolean {
    return !this.chat || this.chat.status !== 'reading';
  }

  /**
   * The widget is drawn now. What its document starts with: what the widget
   * saved, and the characters it may keep (its `setItem` is refused more).
   * A widget that is drawn while what its chat's widgets saved could not be
   * read starts with nothing, and stays apart from the server for as long
   * as it runs: what it reports would be written over what it never saw.
   */
  start(): { saved: WidgetItems; limit: number } {
    if (!this.chat) return { saved: NO_ITEMS, limit: MAX_WIDGET_DATA_CHARS };
    this.blind = this.chat.status !== 'known';
    return this.blind
      ? { saved: NO_ITEMS, limit: MAX_WIDGET_DATA_CHARS }
      : { saved: this.chat.items(this.id), limit: this.chat.limit(this.id) };
  }

  /** True when the widget has saved something. */
  has(): boolean {
    return !this.blind && !!this.chat && this.chat.items(this.id).length > 0;
  }

  notice(): WidgetNotice {
    if (this.blind) return 'unavailable';
    return this.chat?.notice(this.id) ?? null;
  }

  /** The widget reports what it holds now, checked already (`readWidgetItems`). */
  take(items: WidgetItems): void {
    if (!this.closed && !this.blind) this.chat?.write(this.id, items);
  }

  /** The widget is gone: what waits is written, and nothing more is taken. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.unsubscribe?.();
    this.chat?.flush(this.id);
    this.chat?.leave();
  }
}

/** Forget what the widget `source` saved in the place `scope`: the server deletes it. */
export function forgetWidgetData(scope: string, source: string): void {
  const session = chatOf(scope);
  if (session === null) return;
  const chat = chatFor(session);
  const id = widgetId(source);
  chat.write(id, NO_ITEMS);
  chat.flush(id);
}

/** Write what waits, for a page that is being left or hidden. */
function leaving(): void {
  leavingBytes = 0;
  for (const chat of [...chats.values()]) chat.flushAll(true);
}

if (typeof window !== 'undefined') {
  window.addEventListener('pagehide', leaving);
  // A phone that puts the browser away may never say that the page is left.
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') leaving();
  });
}

/** Forget every chat, for tests. */
export function resetWidgetChats(): void {
  chats.clear();
  leavingBytes = 0;
}
