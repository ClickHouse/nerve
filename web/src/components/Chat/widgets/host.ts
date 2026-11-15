import type { ResolvedTheme } from '../../../stores/themeStore';
import { hostStyleVariables } from '../mcpApps/hostStyles';
import { KeyboardGuard, TAKEN_REASON } from '../mcpApps/keyboard';
import { clampFrameHeight } from '../mcpApps/policy';
import {
  PROXY_PRESSED, SANDBOX_TARGET, isFromSandbox, isWithinFrame, sandboxPressed, sandboxRefusal, sandboxUrl,
} from '../mcpApps/sandbox';
import { readWidgetItems, type WidgetItems } from './storage';
import { HOST_CONTEXT_CHANGED, SIZE_CHANGED, STORAGE_CHANGED, WIDGET_SANDBOX, wrapWidget } from './wrap';

/**
 * The host of one widget: the chat's side of the frame that shows a
 * ```widget block.
 *
 * A widget runs where the view of an MCP App runs: in the frame of the
 * sandbox proxy, a page of this address that has no origin, inside a frame
 * of its own that has none either. But it is no MCP App. It has no server, no tool call and no
 * handshake, and the chat does nothing for it. This host does five things:
 *
 * 1. opens the frame of the proxy and, when the proxy says it is ready,
 *    hands it the widget's document (`wrap.ts`), once;
 * 2. takes the height the document reports, within bounds and not too
 *    often, as the height of the frame;
 * 3. tells the document when the chat's theme changes;
 * 4. passes on what the widget's `localStorage` holds, checked for its
 *    shape and its size, to whoever keeps it (`storage.ts`);
 * 5. keeps the keyboard where the person put it (`mcpApps/keyboard.ts`): a
 *    frame that takes it without the person having pressed in it is removed.
 *
 * Every other message from the frame is dropped unread. The MCP Apps host
 * (`mcpApps/host.ts`) speaks a protocol over the same proxy with the same
 * method names: a request for a tool call, for the composer, for a link.
 * None of that is here to answer. A frame that sends messages without end
 * is removed.
 */

/** The proxy document's word that it is ready (`public/mcp-app-sandbox.html`). It never passes it on from the frame inside it. */
const PROXY_READY = 'ui/notifications/sandbox-proxy-ready';
const RESOURCE_READY = 'ui/notifications/sandbox-resource-ready';

/** The frame's height until the widget reports one, when it was not drawn before. */
export const DEFAULT_WIDGET_HEIGHT = 180;
/** After this long without a word from the proxy, the sandbox counts as out of reach. */
export const SANDBOX_WAIT_MS = 8_000;
/** A widget that has its document but reports no height is shown as it is after this long. */
export const REVEAL_MS = 1_500;
/** The frame changes its height at most this often, whatever the widget reports. */
export const SIZE_INTERVAL_MS = 100;
/**
 * A frame that sends more messages than this in a second is stopped. A
 * widget has one thing to say, its height, and says it when the height
 * changes: a few times a second, sixty while something animates. Every
 * message from a frame costs the chat's page a moment, whatever it says, so
 * a frame that sends them without end would hold the page up.
 */
export const MAX_MESSAGES_PER_SECOND = 300;

/** Why a frame that sent too many messages was stopped. */
const FLOODED = 'The widget was stopped: it sent messages without end.';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** What the chat hears from the frame of a widget. */
export type WidgetMessage =
  | { kind: 'proxy-ready' }
  /** The height of the frame for what the widget reports: already within bounds. */
  | { kind: 'size'; height: number }
  /** What the widget's `localStorage` holds: texts, within what one widget may keep. */
  | { kind: 'storage'; items: WidgetItems };

/**
 * What a message from the frame of a widget says, of the three things the
 * chat hears there; null for everything else. A request (a message with an `id`)
 * is one of the "everything else": it asks for an answer, and a widget gets
 * none.
 */
export function readWidgetMessage(data: unknown): WidgetMessage | null {
  if (!isRecord(data) || data.jsonrpc !== '2.0' || 'id' in data) return null;
  const params = isRecord(data.params) ? data.params : {};
  switch (data.method) {
    case PROXY_READY:
      return { kind: 'proxy-ready' };
    case SIZE_CHANGED: {
      const height = clampFrameHeight(params.height);
      return height === null ? null : { kind: 'size', height };
    }
    case STORAGE_CHANGED: {
      const items = readWidgetItems(params.items);
      return items === null ? null : { kind: 'storage', items };
    }
    default:
      return null;
  }
}

/** What a block shows of its widget. A new object for each change. */
export interface WidgetSnapshot {
  /** `loading`: the frame is on its way. `shown`: the widget is on screen. */
  status: 'loading' | 'shown' | 'failed';
  /** Why it failed. */
  error: string | null;
}

export interface WidgetHostOptions {
  /** The text of the ```widget block. */
  source: string;
  theme: ResolvedTheme;
  /** The height to start at: the one the widget had when it was drawn before, if it was. */
  height?: number;
  /** What the widget saved before: what its `localStorage` starts with. */
  saved?: WidgetItems;
  /** The characters the widget may keep: its `setItem` is refused more. */
  limit?: number;
  onChange: (snapshot: WidgetSnapshot) => void;
  /** The frame has a new height. */
  onHeight?: (height: number) => void;
  /** The widget's `localStorage` holds something else now. */
  onStorage?: (items: WidgetItems) => void;
}

export class WidgetHost {
  private readonly options: WidgetHostOptions;
  private theme: ResolvedTheme;
  private frame: HTMLIFrameElement | null = null;
  private keyboard: KeyboardGuard | null = null;
  private snapshot: WidgetSnapshot = { status: 'loading', error: null };
  private started = false;
  /** The proxy of this frame has been given the document. */
  private handedOver = false;
  private disposed = false;

  private height: number;
  /** The newest height the widget reported that the frame has not taken yet. */
  private pendingHeight: number | null = null;
  private sizedAt = 0;
  /** The messages of the frame in the second that began at `countedFrom`. */
  private messages = 0;
  private countedFrom = 0;

  private waitTimer: ReturnType<typeof setTimeout> | undefined;
  private revealTimer: ReturnType<typeof setTimeout> | undefined;
  private sizeTimer: ReturnType<typeof setTimeout> | undefined;
  private themeTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: WidgetHostOptions) {
    this.options = options;
    this.theme = options.theme;
    this.height = clampFrameHeight(options.height) ?? DEFAULT_WIDGET_HEIGHT;
  }

  getSnapshot = (): WidgetSnapshot => this.snapshot;

  /** Open the frame in `container`. Once: a widget that failed is started again as a new host. */
  start(container: HTMLElement): void {
    if (this.disposed || this.started) return;
    this.started = true;
    // The frame of the proxy, as MCP Apps opens it: no `sandbox` attribute,
    // the gateway's header takes the proxy page's origin away (see
    // mcpApps/sandbox.ts). What a widget may do is the `sandbox` of the
    // frame inside it (WIDGET_SANDBOX).
    const frame = document.createElement('iframe');
    // The proxy and the gateway learn which origin embeds the frame, and
    // nothing of the chat's address.
    frame.setAttribute('referrerpolicy', 'origin');
    frame.title = 'Widget';
    frame.style.cssText = 'display:block;width:100%;border:0;background:transparent;';
    // The same colour scheme all the way in, so that the frames stay see-through.
    frame.style.colorScheme = this.theme;
    frame.style.height = `${this.height}px`;
    this.frame = frame;
    // Listening before the proxy loads, so its first word is not missed.
    window.addEventListener('message', this.onMessage);
    container.appendChild(frame);
    this.keyboard = new KeyboardGuard(frame, container, {
      onTaken: () => this.fail(`The widget was stopped: ${TAKEN_REASON}`),
      // The proxy page knows whether the person has just pressed in the widget.
      ask: () => this.post({ jsonrpc: '2.0', method: PROXY_PRESSED }),
    });
    this.keyboard.start();
    // No `csp` in the address: the gateway's default policy, which names no origin.
    frame.src = sandboxUrl(null);
    this.waitTimer = setTimeout(() => {
      if (!this.handedOver) this.fail('The widget sandbox does not answer.');
    }, SANDBOX_WAIT_MS);
  }

  setTheme(theme: ResolvedTheme): void {
    if (theme === this.theme || this.disposed) return;
    this.theme = theme;
    if (this.frame) this.frame.style.colorScheme = theme;
    // Told in a moment, not now. A block calls this while React applies the
    // new theme, and for that moment the page carries no theme at all; the
    // colours read from the stylesheet then would not be the new theme's.
    clearTimeout(this.themeTimer);
    this.themeTimer = setTimeout(() => {
      // A widget that has no document yet gets the theme with it.
      if (!this.handedOver) return;
      this.post({
        jsonrpc: '2.0',
        method: HOST_CONTEXT_CHANGED,
        params: { theme: this.theme, styles: { variables: hostStyleVariables() } },
      });
    }, 0);
  }

  /** Remove the frame and stop listening. */
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.close();
  }

  private update(change: Partial<WidgetSnapshot>): void {
    this.snapshot = { ...this.snapshot, ...change };
    this.options.onChange(this.snapshot);
  }

  private close(): void {
    window.removeEventListener('message', this.onMessage);
    clearTimeout(this.waitTimer);
    clearTimeout(this.revealTimer);
    clearTimeout(this.sizeTimer);
    clearTimeout(this.themeTimer);
    this.keyboard?.stop();
    this.keyboard = null;
    this.frame?.remove();
    this.frame = null;
  }

  private fail(error: string): void {
    if (this.disposed) return;
    this.close();
    this.update({ status: 'failed', error });
  }

  /** The window of the frame, while the frame is in the page. */
  private target(): Window | null {
    return this.frame?.isConnected ? this.frame.contentWindow : null;
  }

  /**
   * To the window of the frame, whatever document it holds: one without an
   * origin cannot be named. Only the chat puts a document there, and what
   * goes out is the widget's own: its document and the theme.
   */
  private post(message: unknown): void {
    this.target()?.postMessage(message, SANDBOX_TARGET);
  }

  private readonly onMessage = (event: MessageEvent): void => {
    // Every window can post to this page, and every sandboxed frame has the
    // origin "null". Only the window of this widget's frame counts, and only
    // while it holds a document without an origin.
    if (!isFromSandbox(event, this.target())) {
      // A proxy page that has an origin runs nothing, and says why.
      const reason = sandboxRefusal(event, this.target());
      if (reason !== null) this.onProxyRefused(reason);
      // What the widget says to this page itself, past the proxy, is never
      // read. It costs the page a moment each time all the same, so it
      // counts for the limit like everything else of the frame.
      else if (isWithinFrame(event.source, this.target()) && this.flooded()) this.fail(FLOODED);
      return;
    }
    if (this.flooded()) {
      this.fail(FLOODED);
      return;
    }
    const refused = sandboxRefusal(event, this.target());
    if (refused !== null) {
      this.onProxyRefused(refused);
      return;
    }
    // The proxy page's word of a press in the widget, with the browser's.
    const pressed = sandboxPressed(event, this.target());
    if (pressed !== null) {
      this.keyboard?.answer(pressed);
      return;
    }
    const message = readWidgetMessage(event.data);
    if (!message) return;
    if (message.kind === 'proxy-ready') this.onProxyReady();
    else if (message.kind === 'size') this.onSize(message.height);
    // No document, no widget: nothing in the frame has anything to keep yet.
    else if (this.handedOver) this.options.onStorage?.(message.items);
  };

  /** True when this message is one too many for its second (MAX_MESSAGES_PER_SECOND). Every message counts, read or not. */
  private flooded(): boolean {
    const now = Date.now();
    if (now - this.countedFrom >= 1000) {
      this.countedFrom = now;
      this.messages = 0;
    }
    this.messages += 1;
    return this.messages > MAX_MESSAGES_PER_SECOND;
  }

  /**
   * The proxy is ready: give it the document. Once. A frame that says it is
   * ready again was loaded again by somebody else, at an address the chat
   * did not choose; a document handed to it would run under the policy of
   * that address.
   */
  private onProxyReady(): void {
    if (this.handedOver) {
      this.fail('The frame of the widget loaded a second time, which it may not do.');
      return;
    }
    this.handedOver = true;
    clearTimeout(this.waitTimer);
    this.post({
      jsonrpc: '2.0',
      method: RESOURCE_READY,
      params: {
        html: wrapWidget(
          this.options.source, { theme: this.theme, variables: hostStyleVariables() },
          this.options.saved, this.options.limit,
        ),
        sandbox: WIDGET_SANDBOX,
      },
    });
    this.revealTimer = setTimeout(() => this.reveal(), REVEAL_MS);
  }

  /** The proxy does not work where it was served: a fault of the setup, shown as it is. */
  private onProxyRefused(reason: string): void {
    // Only before the proxy is ready: it has nothing to refuse after that.
    if (this.handedOver) return;
    this.fail(`The widget sandbox refused to start${reason ? `: ${reason}` : '.'}`);
  }

  private reveal(): void {
    clearTimeout(this.revealTimer);
    if (this.snapshot.status === 'loading') this.update({ status: 'shown' });
  }

  /**
   * The widget reports the height of its content. The newest report wins,
   * and the frame takes it at most every SIZE_INTERVAL_MS: a widget that
   * reports without end moves the page ten times a second at most.
   */
  private onSize(height: number): void {
    // No document, no widget: nothing in the frame has a height to report yet.
    if (!this.handedOver) return;
    this.pendingHeight = height;
    if (this.sizeTimer !== undefined) return;
    const wait = this.sizedAt + SIZE_INTERVAL_MS - Date.now();
    if (wait <= 0) this.applySize();
    else this.sizeTimer = setTimeout(() => this.applySize(), wait);
  }

  private applySize(): void {
    this.sizeTimer = undefined;
    this.sizedAt = Date.now();
    const height = this.pendingHeight;
    this.pendingHeight = null;
    if (height !== null && height !== this.height && this.frame) {
      this.height = height;
      this.frame.style.height = `${height}px`;
      this.options.onHeight?.(height);
    }
    // Its first height is the sign that the widget has drawn itself.
    this.reveal();
  }
}
