import type { JSONRPCMessage, Transport } from '@modelcontextprotocol/client';
import { JSONRPCMessageSchema } from '@modelcontextprotocol/core';
import { PROXY_PRESSED, PROXY_REFUSED, SANDBOX_TARGET, isFromSandbox, sandboxPressed, sandboxRefusal } from './sandbox';

/**
 * The channel between the chat and one view: JSON-RPC messages over
 * `postMessage`, through the frame of the sandbox proxy.
 *
 * Every window can post a message to this page, and every sandboxed frame
 * has the same origin, "null". So a message counts only when the window of
 * this view's frame sent it and that window holds a document without an
 * origin (`isFromSandbox`): a view that posts to this page itself, or a
 * frame that a view made, comes from another window and is left alone.
 *
 * What goes out goes to the window of the frame, whatever document it holds
 * (`SANDBOX_TARGET`): a document without an origin cannot be named. The
 * chat alone puts a document into that frame.
 *
 * One exchange is the proxy page's own and no part of the view's protocol:
 * whether the person has just pressed in the view (`askPressed`,
 * `onpressed`; see `keyboard.ts`).
 */
export class SandboxTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;
  /** The proxy page's answer to `askPressed`. */
  onpressed?: (pressed: boolean | 'unknown') => void;

  private readonly frame: HTMLIFrameElement;
  private listening = false;

  constructor(frame: HTMLIFrameElement) {
    this.frame = frame;
  }

  /** The window of the frame, while the frame is in the page. */
  private window(): Window | null {
    return this.frame.isConnected ? this.frame.contentWindow : null;
  }

  private readonly listener = (event: MessageEvent) => {
    if (!isFromSandbox(event, this.window())) {
      // A proxy page that has an origin runs nothing and says why. That one
      // word is passed on, as the proxy's refusal and nothing else.
      const reason = sandboxRefusal(event, this.window());
      if (reason !== null) this.onmessage?.({ jsonrpc: '2.0', method: PROXY_REFUSED, params: { reason } });
      return;
    }
    // The proxy page's word of a press, with the browser's: not the view's to hear or to say.
    const pressed = sandboxPressed(event, this.window());
    if (pressed !== null) {
      this.onpressed?.(pressed);
      return;
    }
    const parsed = JSONRPCMessageSchema.safeParse(event.data);
    if (parsed.success) {
      this.onmessage?.(parsed.data);
    } else if ((event.data as { jsonrpc?: unknown } | null)?.jsonrpc === '2.0') {
      // JSON-RPC by its own word, but not a valid message: a protocol error.
      this.onerror?.(new Error(`Invalid JSON-RPC message from the app: ${parsed.error.message}`));
    }
    // Anything else in the frame is not for the chat.
  };

  async start(): Promise<void> {
    if (this.listening) return;
    this.listening = true;
    window.addEventListener('message', this.listener);
  }

  async send(message: JSONRPCMessage): Promise<void> {
    const target = this.window();
    if (!target) throw new Error('The frame of the app is gone.');
    target.postMessage(message, SANDBOX_TARGET);
  }

  /** Ask the proxy page whether the person has just pressed in the view. Nothing is asked of a frame that is gone. */
  askPressed(): void {
    this.window()?.postMessage({ jsonrpc: '2.0', method: PROXY_PRESSED }, SANDBOX_TARGET);
  }

  async close(): Promise<void> {
    if (!this.listening) return;
    this.listening = false;
    window.removeEventListener('message', this.listener);
    this.onclose?.();
  }
}
