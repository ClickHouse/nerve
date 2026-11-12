import type { McpUiCsp, McpUiPermissions } from '../../../types/mcpApps';

/**
 * The sandbox of MCP Apps: where its page is, how the chat tells its frame
 * from every other, and what the chat lets a view do there.
 *
 * A view is third-party HTML. It runs in a frame that reaches nothing of the
 * chat: not its DOM, not the session token in its storage. The frame's
 * document is the sandbox proxy (`public/mcp-app-sandbox.html`), which the
 * gateway serves from the chat's own address with a Content-Security-Policy
 * header: `sandbox`, which takes the document's origin away, then what the
 * view may load (from the `?csp=` query), then `frame-ancestors 'self'`.
 *
 * The frame has no `sandbox` attribute of the chat's. It would make the
 * frame's document one without an origin whatever the gateway sends, and the
 * proxy page takes having no origin as its proof that the header arrived,
 * with the limits on what a view may load. Without the header the page has
 * the chat's origin, says so, and runs nothing.
 */

/** The path of the proxy document, at the address of this page. */
export const SANDBOX_PATH = '/mcp-app-sandbox.html';

/** What a document without an origin says its origin is, and what its messages carry. */
export const NO_ORIGIN = 'null';

/**
 * How the chat addresses the frame. A document without an origin cannot be
 * named as the target of a message, so the target is "whatever document the
 * frame holds". Only the chat puts a document into that frame, and nothing
 * the chat posts is a secret of its own: the view's HTML, its theme, the
 * arguments and results of its tool call, the answers to its own requests.
 */
export const SANDBOX_TARGET = '*';

/** The proxy document's word that it does nothing, with the reason (not in the specification). */
export const PROXY_REFUSED = 'ui/notifications/sandbox-proxy-refused';

/**
 * The chat's question to the proxy document whether the person has just
 * pressed in the content of its frame, and the document's answer (not in the
 * specification). The proxy keeps the word to itself: it passes it on
 * neither to the content nor from it. See `keyboard.ts`.
 */
export const PROXY_PRESSED = 'ui/notifications/sandbox-pressed';

/**
 * What the frame of a view, inside the proxy, may do. Without
 * `allow-same-origin`: the view's document gets an origin of its own (an
 * opaque one), which it shares with nothing. So a view reaches neither the
 * proxy nor another view, and has no cookie and no storage. The proxy
 * document holds to this whatever it is sent.
 */
export const VIEW_SANDBOX = 'allow-scripts allow-forms';

const CSP_LISTS = ['connectDomains', 'resourceDomains', 'frameDomains', 'baseUriDomains'] as const;
/** A character that would end a source, a directive or the policy in the header. */
const UNSAFE_IN_POLICY = /[;,'"\s]/;
/** The longest `csp` that goes into the address of the proxy document. */
const MAX_CSP_CHARS = 4000;

/** A source of a policy: `[scheme://]host[:port][/path]`, the host maybe `*` or `*.name`. */
const HOST_SOURCE = /^(?:([a-z][a-z0-9+.-]*):\/\/)?(\*|(?:\*\.)?[a-z0-9.-]+|\[[0-9a-f:.]+\])(?::(\d+|\*))?(?:\/.*)?$/i;

/** True for a name of this machine as only this machine reaches it. */
function isLoopback(host: string): boolean {
  const name = host.toLowerCase().replace(/\.$/, '');
  return name === 'localhost' || name.endsWith('.localhost')
    || /^127(\.\d{1,3}){3}$/.test(name) || name === '0.0.0.0' || name === '[::1]' || name === '::1';
}

/**
 * True when the policy source `source` is on the host `hostname`: that host
 * itself, whatever the scheme and the port, or a pattern that covers it. On
 * a loopback host every loopback name counts (`localhost` and `127.0.0.1`
 * are two origins and one machine). Decided on the safe side: what is no
 * host source (a bare scheme like `https:`, `*`, anything unreadable) counts
 * as on the host.
 */
export function namesHost(source: string, hostname: string): boolean {
  const match = HOST_SOURCE.exec(source);
  if (!match) return true;
  const host = match[2].toLowerCase().replace(/\.$/, '');
  const own = hostname.toLowerCase().replace(/\.$/, '');
  if (host === '*') return true;
  if (isLoopback(own) && isLoopback(host.replace(/^\*\./, ''))) return true;
  return host === own || (host.startsWith('*.') && own.endsWith(host.slice(1)));
}

/**
 * The origins of a view's `_meta.ui.csp` that go into its policy.
 *
 * - Only strings with no character that would add a source or a directive.
 * - No source on the chat's own host (`chatOrigin`), on any port. The
 *   sandbox document is a page of that host: with such a source a view
 *   could ask the chat and the API for things (the sign-in is there), hold
 *   the chat in a frame of its own, or nest the sandbox document with a
 *   policy of its choosing.
 *
 * The gateway builds the header and applies the same rules there, with more
 * of its own; this is the same check on the way out. Here the host is the
 * one the browser really shows, also behind a proxy that the gateway cannot
 * see through.
 *
 * Null when nothing is left, or when the whole is too long to send: the view
 * then gets the restrictive default, which loads nothing from outside.
 */
export function sanitizeCsp(
  csp: McpUiCsp | null | undefined,
  chatOrigin: string = window.location.origin,
): McpUiCsp | null {
  if (!csp || typeof csp !== 'object') return null;
  const hostname = new URL(chatOrigin).hostname;
  const clean: McpUiCsp = {};
  for (const key of CSP_LISTS) {
    const list: unknown = csp[key];
    if (!Array.isArray(list)) continue;
    const domains = list.filter((d): d is string => typeof d === 'string' && d !== '' && !UNSAFE_IN_POLICY.test(d)
      && !namesHost(d, hostname));
    if (domains.length > 0) clean[key] = domains;
  }
  if (Object.keys(clean).length === 0) return null;
  return JSON.stringify(clean).length > MAX_CSP_CHARS ? null : clean;
}

/**
 * The address of the proxy document, relative to this page: the chat's own
 * address, whatever it is. It carries the origins of the view and nothing
 * else: the session token is never in it.
 */
export function sandboxUrl(csp: McpUiCsp | null): string {
  return csp ? `${SANDBOX_PATH}?csp=${encodeURIComponent(JSON.stringify(csp))}` : SANDBOX_PATH;
}

/**
 * The browser features the chat passes on to a view, of those it asks for.
 * Only writing to the clipboard (a Copy button). Camera, microphone and
 * location are not passed on: the browser would ask in the name of the chat's
 * own address, and an answer given for one view would hold for every other.
 */
export function grantedPermissions(asked: McpUiPermissions | null | undefined): McpUiPermissions | null {
  return asked && typeof asked === 'object' && asked.clipboardWrite ? { clipboardWrite: {} } : null;
}

/**
 * The `allow` attribute of the frame for the granted features.
 *
 * With `*`: the browser gives a feature to the origin of the frame's
 * address unless told otherwise, and the proxy page at that address has no
 * origin, so nothing would reach it. `*` gives the feature to whatever
 * document the frame holds, which only the chat chooses. The proxy page
 * hands it on to the view's frame by name.
 */
export function allowAttribute(granted: McpUiPermissions | null): string {
  return granted?.clipboardWrite ? 'clipboard-write *' : '';
}

/**
 * True for a message that the proxy page in the frame `frameWindow` sent to
 * this page.
 *
 * The frame is known by its window. Every sandboxed frame has the origin
 * "null", a view and anything a view makes among them, so the origin names
 * nobody: a message counts when it comes from the window of the frame that
 * the chat made, and that window holds a document without an origin. A view
 * that posts to this page itself comes from another window.
 */
export function isFromSandbox(
  event: Pick<MessageEvent, 'origin' | 'source'>,
  frameWindow: MessageEventSource | null | undefined,
): boolean {
  return !!frameWindow && event.source === frameWindow && event.origin === NO_ORIGIN;
}

/**
 * True when the window `source` is the frame's own or a window inside it:
 * the content's frame, or a frame the content made, however deep.
 *
 * Nothing such a window says to this page itself is ever read (only the
 * proxy page in the frame speaks for it). This tells whose doing a message
 * is all the same, so that a frame whose content floods the page with
 * messages can be stopped. A window's parent can be asked from any origin.
 */
export function isWithinFrame(
  source: MessageEventSource | null | undefined,
  frameWindow: MessageEventSource | null | undefined,
): boolean {
  if (!frameWindow || !source) return false;
  try {
    let current = source as Window;
    for (let depth = 0; depth < 16; depth += 1) {
      if (current === frameWindow) return true;
      const above = current.parent;
      // Not a window (a port, a worker), or the top of its page.
      if (!above || above === current) return false;
      current = above;
    }
  } catch {
    // A window that is gone.
  }
  return false;
}

/**
 * The reason a proxy page gives for doing nothing, when `event` is that
 * word from the frame `frameWindow`; else null.
 *
 * A proxy page that was served without its policy has the chat's own origin
 * and refuses to run. That one word is taken from a frame with an origin:
 * all it does is end the view with a reason.
 */
export function sandboxRefusal(
  event: Pick<MessageEvent, 'origin' | 'source' | 'data'>,
  frameWindow: MessageEventSource | null | undefined,
  chatOrigin: string = window.location.origin,
): string | null {
  if (!frameWindow || event.source !== frameWindow) return null;
  if (event.origin !== NO_ORIGIN && event.origin !== chatOrigin) return null;
  const data: unknown = event.data;
  if (typeof data !== 'object' || data === null) return null;
  const { jsonrpc, method, params, id } = data as Record<string, unknown>;
  if (jsonrpc !== '2.0' || method !== PROXY_REFUSED || id !== undefined) return null;
  const reason = typeof params === 'object' && params !== null ? (params as Record<string, unknown>).reason : null;
  return typeof reason === 'string' ? reason.slice(0, 200) : '';
}

/** What a MessageEvent carries in a browser that adds its own word of a press to a message. */
type Vouched = { userActivation?: { isActive?: unknown } | null };

/**
 * What the proxy page in `frameWindow` says of a press in its content, when
 * `event` is its answer to PROXY_PRESSED; else null.
 *
 * `true`: the browser counts a press of the person's in the content as just
 * done. `false`: it counts none. `'unknown'`: the browser there cannot tell.
 *
 * The answer is the proxy page's own, about its own document: a press in
 * the content counts for the frames around it, and a press anywhere else
 * does not count there. Where the browser adds its own word of the same to
 * the message (Chrome does), a "yes" counts only with that word.
 */
export function sandboxPressed(
  event: Pick<MessageEvent, 'origin' | 'source' | 'data'>,
  frameWindow: MessageEventSource | null | undefined,
): boolean | 'unknown' | null {
  if (!isFromSandbox(event, frameWindow)) return null;
  const data: unknown = event.data;
  if (typeof data !== 'object' || data === null) return null;
  const { jsonrpc, method, params, id } = data as Record<string, unknown>;
  if (jsonrpc !== '2.0' || method !== PROXY_PRESSED || id !== undefined) return null;
  const said = typeof params === 'object' && params !== null ? (params as Record<string, unknown>).pressed : undefined;
  if (said === null) return 'unknown';
  if (said !== true) return false;
  // A browser that adds no word of its own to a message: the page's is all there is.
  if (!('userActivation' in event)) return true;
  return (event as Vouched).userActivation?.isActive === true;
}
