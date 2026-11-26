import { KeyboardGuard, TAKEN_REASON } from '../Chat/mcpApps/keyboard';
import {
  PROXY_PRESSED, SANDBOX_TARGET, isFromSandbox, isWithinFrame, sandboxPressed, sandboxRefusal, sandboxUrl,
} from '../Chat/mcpApps/sandbox';
import { hostMessage, readContentMessage, type ContentMessage, type HostMessage } from './messages';

/**
 * The host of one preview: the Preview tab's side of the sandbox frame.
 *
 * A preview runs where a widget runs (`Chat/widgets/host.ts`): in the frame
 * of the sandbox proxy, a page of this address that has no origin, and
 * inside it a frame that has none either and may only run scripts
 * (`PREVIEW_SANDBOX`). The proxy is loaded with no `csp` in its address, so
 * its policy allows no origin. The frame of the proxy has no `sandbox`
 * attribute of its own (see `Chat/mcpApps/sandbox.ts`).
 *
 * The host:
 *
 * 1. opens the frame of the proxy and, when the proxy says it is ready,
 *    hands it the document of the preview; a new document (after a change
 *    of a file) goes to the same proxy, which makes a new inner frame;
 * 2. reads the messages of the content page (`messages.ts`) and sends its
 *    own; nothing else from the frame is read. After a new document is
 *    handed over, a message of the page before it can still be on its way:
 *    the host asks the proxy a question (the press question of the
 *    keyboard guard) and takes no content message until the answer. The
 *    proxy answers in order, so every message of the page before comes
 *    first;
 * 3. keeps the keyboard where the person put it (`Chat/mcpApps/keyboard.ts`);
 * 4. stops a frame that sends messages without end.
 */

const PROXY_READY = 'ui/notifications/sandbox-proxy-ready';
const RESOURCE_READY = 'ui/notifications/sandbox-resource-ready';

/** What the frame of a preview may do: run scripts. No forms, no popups, no origin. */
export const PREVIEW_SANDBOX = 'allow-scripts';
/** After this long without a word from the proxy, the sandbox counts as out of reach. */
export const SANDBOX_WAIT_MS = 8_000;
/** A page that has not said `ready` this long after its document is asked once with `hello`. */
export const HELLO_MS = 1_000;
/** A frame that sends more messages than this in a second is stopped. */
export const MAX_MESSAGES_PER_SECOND = 300;
/** Content messages count again this long after a hand-over when the proxy has not answered by then. */
export const SETTLE_MS = 500;

const FLOODED = 'The preview was stopped: it sent messages without end.';

export interface PreviewHostOptions {
  /** The frame failed, with a reason for the person. The host is closed. */
  onFail: (reason: string) => void;
  /** A checked message of the content page. */
  onContent: (message: ContentMessage) => void;
  /** A document went to the proxy: what the page before said counts no more. */
  onHandOver?: () => void;
}

export class PreviewHost {
  private readonly options: PreviewHostOptions;
  private frame: HTMLIFrameElement | null = null;
  private keyboard: KeyboardGuard | null = null;
  private proxyReady = false;
  /** The document that the proxy holds or gets once it is ready. */
  private html: string | null = null;
  private closed = false;
  private messages = 0;
  private countedFrom = 0;
  private waitTimer: ReturnType<typeof setTimeout> | undefined;
  private helloTimer: ReturnType<typeof setTimeout> | undefined;
  private settleTimer: ReturnType<typeof setTimeout> | undefined;
  /** The content page said `ready` for the document that the proxy holds. */
  private ready = false;
  /** Who asked the proxy about a press, in order: the proxy answers in the same order. */
  private readonly askers: Array<'keyboard' | 'hand-over'> = [];
  /** Hand-overs whose question the proxy has not answered: no content message counts. */
  private settling = 0;

  constructor(options: PreviewHostOptions) {
    this.options = options;
  }

  /** Open the frame in `place`, an element that holds nothing else. Once. */
  start(place: HTMLElement): void {
    if (this.frame || this.closed) return;
    const frame = document.createElement('iframe');
    frame.setAttribute('referrerpolicy', 'origin');
    frame.title = 'Preview';
    frame.style.cssText = 'display:block;width:100%;height:100%;border:0;background:transparent;';
    this.frame = frame;
    window.addEventListener('message', this.onMessage);
    place.appendChild(frame);
    this.keyboard = new KeyboardGuard(frame, place, {
      onTaken: () => this.fail(`The preview was stopped: ${TAKEN_REASON}`),
      ask: () => this.ask('keyboard'),
    });
    this.keyboard.start();
    // No `csp` in the address: the policy that allows no origin.
    frame.src = sandboxUrl(null);
    this.waitTimer = setTimeout(() => {
      if (!this.proxyReady) this.fail('The preview sandbox does not answer.');
    }, SANDBOX_WAIT_MS);
  }

  /** Show `html`: at once when the proxy is ready, else as soon as it is. */
  show(html: string): void {
    if (this.closed) return;
    this.html = html;
    if (this.proxyReady) this.handOver();
  }

  /** Send a message to the content page. */
  send(message: HostMessage): void {
    this.post(hostMessage(message));
  }

  /** Give the keyboard to the frame (the page's own act, see KeyboardGuard). */
  focus(): void {
    this.frame?.focus();
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    window.removeEventListener('message', this.onMessage);
    clearTimeout(this.waitTimer);
    clearTimeout(this.helloTimer);
    clearTimeout(this.settleTimer);
    this.keyboard?.stop();
    this.keyboard = null;
    this.frame?.remove();
    this.frame = null;
  }

  private fail(reason: string): void {
    if (this.closed) return;
    this.dispose();
    this.options.onFail(reason);
  }

  private target(): Window | null {
    return this.frame?.isConnected ? this.frame.contentWindow : null;
  }

  private post(message: unknown): void {
    this.target()?.postMessage(message, SANDBOX_TARGET);
  }

  /** Ask the proxy whether the person just pressed in the content. */
  private ask(who: 'keyboard' | 'hand-over'): void {
    this.askers.push(who);
    this.post({ jsonrpc: '2.0', method: PROXY_PRESSED });
  }

  private handOver(): void {
    if (this.html === null) return;
    this.ready = false;
    this.post({
      jsonrpc: '2.0',
      method: RESOURCE_READY,
      params: { html: this.html, sandbox: PREVIEW_SANDBOX },
    });
    // The answer comes after every message of the page before (see the class comment).
    this.settling += 1;
    this.ask('hand-over');
    clearTimeout(this.settleTimer);
    this.settleTimer = setTimeout(() => { this.settling = 0; }, SETTLE_MS);
    this.options.onHandOver?.();
    clearTimeout(this.helloTimer);
    this.helloTimer = setTimeout(() => {
      if (!this.ready) this.send({ type: 'hello' });
    }, HELLO_MS);
  }

  private flooded(): boolean {
    const now = Date.now();
    if (now - this.countedFrom >= 1000) {
      this.countedFrom = now;
      this.messages = 0;
    }
    this.messages += 1;
    return this.messages > MAX_MESSAGES_PER_SECOND;
  }

  private readonly onMessage = (event: MessageEvent): void => {
    const target = this.target();
    if (!isFromSandbox(event, target)) {
      const reason = sandboxRefusal(event, target);
      if (reason !== null && !this.proxyReady) {
        this.fail(`The preview sandbox refused to start${reason ? `: ${reason}` : '.'}`);
      } else if (isWithinFrame(event.source, target) && this.flooded()) {
        // What the content says to this page itself, past the proxy, is never read.
        this.fail(FLOODED);
      }
      return;
    }
    if (this.flooded()) {
      this.fail(FLOODED);
      return;
    }
    const pressed = sandboxPressed(event, target);
    if (pressed !== null) {
      if (this.askers.shift() === 'hand-over') {
        this.settling = Math.max(0, this.settling - 1);
        if (this.settling === 0) clearTimeout(this.settleTimer);
        return;
      }
      this.keyboard?.answer(pressed);
      return;
    }
    const data: unknown = event.data;
    if (typeof data === 'object' && data !== null && (data as { method?: unknown }).method === PROXY_READY
      && (data as { jsonrpc?: unknown }).jsonrpc === '2.0') {
      if (this.proxyReady) {
        // Loaded again, by someone else, at an address the tab did not choose.
        this.fail('The frame of the preview loaded a second time, which it may not do.');
        return;
      }
      this.proxyReady = true;
      clearTimeout(this.waitTimer);
      this.handOver();
      return;
    }
    // Content messages count only for a document that the tab handed over,
    // and only once the messages of the page before it have arrived.
    if (!this.proxyReady || this.html === null || this.settling > 0) return;
    const message = readContentMessage(data);
    if (!message) return;
    if (message.type === 'ready') this.ready = true;
    this.options.onContent(message);
  };
}
