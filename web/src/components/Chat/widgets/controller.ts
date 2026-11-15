import type { ResolvedTheme } from '../../../stores/themeStore';
import { claimWidgetSlot, type WidgetSlot } from './budget';
import { rememberWidgetHeight, widgetHeight, widgetKey } from './heights';
import { WidgetHost, type WidgetSnapshot } from './host';
import { WidgetData, forgetWidgetData, type WidgetNotice } from './storage';

/**
 * When a widget runs: the part of a ```widget block that is not React.
 *
 * A block tells the controller what it knows (`sync`): the source to draw,
 * whether the block is near the screen, the theme, the place it is in. The
 * controller decides from that whether the widget has a frame, and says what
 * the block shows (`getSnapshot`):
 *
 * - `idle`: nothing to draw, or far from the screen. No frame.
 * - `waiting`: near the screen, but every slot is held by a widget that is
 *   near it too (`budget.ts`). No frame until the person asks (`load`).
 * - `loading`, `shown`: the frame is there (`host.ts`).
 * - `failed`: the sandbox did not take the widget. No frame, until the
 *   person asks again or the source changes.
 *
 * A widget that runs has its saved data with it (`storage.ts`): what its
 * `localStorage` starts with, and where its changes go. The data belongs to
 * the source in the place where the block is (`scope`), so it is there again
 * whenever the same widget is drawn again, and `reset` forgets it. The
 * server keeps it, so the frame of the first widget of a chat waits for the
 * page's one read of what the chat's widgets saved.
 */

export type WidgetPhase = 'idle' | 'waiting' | 'loading' | 'shown' | 'failed';

/** What a block shows of its widget. A new object for each change. */
export interface WidgetView {
  phase: WidgetPhase;
  /** Why it failed. */
  error: string | null;
  /** The widget that runs has saved something. */
  saved: boolean;
  /** Why what the widget holds is not kept, when it is not. */
  notice: WidgetNotice;
}

export interface WidgetInputs {
  /** The text to draw; null while there is none (the feature is off, the block is still being written). */
  source: string | null;
  /** The block is on the screen, or close to it. */
  near: boolean;
  theme: ResolvedTheme;
  /** Where the block is: the chat, or the page (`widgetScope`). A widget's saved data does not leave it. */
  scope: string;
}

function view(
  phase: WidgetPhase, error: string | null = null, saved = false, notice: WidgetNotice = null,
): WidgetView {
  return { phase, error, saved, notice };
}

export class WidgetController {
  private shown: WidgetView = view('idle');
  private readonly listeners = new Set<() => void>();
  private inputs: WidgetInputs = { source: null, near: false, theme: 'dark', scope: '' };
  private container: HTMLElement | null = null;
  private slot: WidgetSlot | null = null;
  private host: WidgetHost | null = null;
  /** The saved data of the widget that runs. */
  private data: WidgetData | null = null;
  /** What the last frame was opened for: the one that runs, or the one that failed. */
  private drawn: { source: string; scope: string } | null = null;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = (): WidgetView => this.shown;

  /** Where the frame goes; null when the block has no place for it (any more). */
  place(container: HTMLElement | null): void {
    if (container === this.container) return;
    // A frame cannot follow its place: it would load again anyway.
    this.end();
    this.container = container;
    this.settle();
  }

  /** What the block knows now. */
  sync(inputs: WidgetInputs): void {
    this.inputs = inputs;
    this.settle();
  }

  /**
   * Draw the widget from its source, now: again (Reload, Retry), or for the
   * first time when it waits for a slot. The person asked, so a slot is found.
   */
  load(): void {
    this.end();
    this.drawn = null;
    this.settle(true);
  }

  /**
   * Forget what the widget saved and draw it again, as it was when the model
   * wrote it.
   */
  reset(): void {
    const { source, scope } = this.inputs;
    // The frame first: what it still has to say is said before the data goes.
    this.end();
    if (source !== null) forgetWidgetData(scope, source);
    this.drawn = null;
    this.settle(true);
  }

  /** The block is gone: no frame, no slot. A later `sync` starts over. */
  stop(): void {
    this.end();
    this.drawn = null;
    this.inputs = { ...this.inputs, source: null };
    this.show(view('idle'));
  }

  private show(next: WidgetView): void {
    const now = this.shown;
    if (now.phase === next.phase && now.error === next.error
      && now.saved === next.saved && now.notice === next.notice) return;
    this.shown = next;
    for (const listener of [...this.listeners]) listener();
  }

  /** Remove the frame, with what it still has to save written, and give the slot back. */
  private end(): void {
    const { host, slot, data } = this;
    this.host = null;
    this.slot = null;
    this.data = null;
    host?.dispose();
    data?.close();
    slot?.release();
  }

  /** The frame of another widget goes; the slot stays for the one that comes. */
  private leave(): void {
    const { host, data } = this;
    this.host = null;
    this.data = null;
    host?.dispose();
    data?.close();
  }

  private settle(asked = false): void {
    const { source, near, theme, scope } = this.inputs;
    if (source === null) {
      this.end();
      this.drawn = null;
      this.show(view('idle'));
      return;
    }
    // Another source, or another place: what runs, or failed, is not what is wanted.
    if (this.drawn && (this.drawn.source !== source || this.drawn.scope !== scope)) {
      this.leave();
      this.drawn = null;
    }
    // A widget that failed stays as it is until the person asks again: with
    // or without a place, it is not tried again on its own.
    if (this.shown.phase === 'failed' && this.drawn && !asked) return;
    if (!this.container) {
      this.end();
      this.drawn = null;
      this.show(view('idle'));
      return;
    }
    if (this.slot) {
      this.slot.seen(near);
    } else {
      if (!near && !asked) {
        this.show(view('idle'));
        return;
      }
      this.slot = claimWidgetSlot(this.evicted, asked);
      if (!this.slot) {
        this.show(view('waiting'));
        return;
      }
    }
    if (this.host) this.host.setTheme(theme);
    // No frame and no data yet: open them. (Data without a frame waits for its read.)
    else if (!this.data) this.open(this.container, source, scope);
  }

  private open(container: HTMLElement, source: string, scope: string): void {
    const data: WidgetData = new WidgetData(scope, source, () => {
      if (this.data === data) this.show({ ...this.shown, saved: data.has(), notice: data.notice() });
    });
    this.data = data;
    this.drawn = { source, scope };
    this.show(view('loading'));
    // The widget's document starts with what the widget saved. For a chat
    // the page has not asked about yet, the frame waits for the answer.
    const draw = (): void => {
      // The block changed its mind meanwhile: another source, no place, gone.
      if (this.data !== data || this.container !== container) return;
      const key = widgetKey(source);
      const { saved, limit } = data.start();
      const host: WidgetHost = new WidgetHost({
        source,
        theme: this.inputs.theme,
        height: widgetHeight(key) || undefined,
        saved,
        limit,
        onChange: (snapshot) => {
          if (this.host === host) this.follow(snapshot);
        },
        onHeight: height => rememberWidgetHeight(key, height),
        onStorage: items => data.take(items),
      });
      this.host = host;
      this.show(view('loading', null, data.has(), data.notice()));
      host.start(container);
    };
    if (data.isReady) draw();
    else void data.ready.then(draw);
  }

  /** What the frame says of itself. */
  private follow(snapshot: WidgetSnapshot): void {
    if (snapshot.status === 'failed') {
      // The frame is gone already; the slot goes too.
      this.end();
      this.show(view('failed', snapshot.error));
    } else {
      this.show(view(snapshot.status, null, this.data?.has() ?? false, this.data?.notice() ?? null));
    }
  }

  /** Another widget took the slot (`budget.ts`): the frame goes, the place stays. */
  private readonly evicted = (): void => {
    this.slot = null;
    this.end();
    this.drawn = null;
    this.show(view(this.inputs.near ? 'waiting' : 'idle'));
  };
}
