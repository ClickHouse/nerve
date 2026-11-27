import {
  ARTIFACT_CHANNEL,
  ARTIFACT_PROTOCOL_VERSION,
  MAX_SLIDES,
  type ContentMessage,
  type HostMessage,
} from '../components/Preview/messages';

/**
 * The content side of the messages between a kit page and the page that
 * shows it (the host: the viewer, or the Preview tab of Nerve). See
 * `components/Preview/messages.ts` for the host side.
 *
 * The page has no origin, so it posts to the target `*`. It takes a message
 * only from its parent window, and only with a known type and values in
 * range. No value of a message is used as markup or as an address.
 */

type Ready = Extract<ContentMessage, { type: 'ready' }>;

const STEPS: ReadonlySet<string> = new Set(['next', 'prev', 'first', 'last']);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** What a message from the host says, or null for anything else. */
export function readHostMessage(data: unknown): HostMessage | null {
  if (!isRecord(data) || data.channel !== ARTIFACT_CHANNEL || data.v !== ARTIFACT_PROTOCOL_VERSION) return null;
  switch (data.type) {
    case 'hello':
      return { type: 'hello' };
    case 'print':
      return { type: 'print' };
    case 'present':
      return typeof data.on === 'boolean' ? { type: 'present', on: data.on } : null;
    case 'slide': {
      const to = data.to;
      if (typeof to === 'string' && STEPS.has(to)) return { type: 'slide', to: to as 'next' | 'prev' | 'first' | 'last' };
      if (typeof to === 'number' && Number.isInteger(to) && to >= 1 && to <= MAX_SLIDES) return { type: 'slide', to };
      return null;
    }
    default:
      return null;
  }
}

/** True when the page is in a frame, so it has a host to talk to. */
function framed(win: Window): boolean {
  return win.parent !== win;
}

/** Post `message` to the host. Nothing happens at the top level. */
export function postToHost(message: ContentMessage, win: Window = window): void {
  if (!framed(win)) return;
  win.parent.postMessage({ channel: ARTIFACT_CHANNEL, v: ARTIFACT_PROTOCOL_VERSION, ...message }, '*');
}

export interface PageConnection {
  /** Say `ready` again, with the current state (a slide change of a deck). */
  ready(): void;
  close(): void;
}

/**
 * Connect a kit page to its host: post `ready` now and as the answer to
 * each `hello`, call `window.print()` on `print` when the page can print,
 * and give each other host message to `onMessage`.
 */
export function connectPage(
  state: () => Ready,
  onMessage: (message: HostMessage) => void = () => {},
  win: Window = window,
): PageConnection {
  const listener = (event: MessageEvent) => {
    if (!framed(win) || event.source !== win.parent) return;
    const message = readHostMessage(event.data);
    if (!message) return;
    if (message.type === 'hello') postToHost(state(), win);
    else if (message.type === 'print') {
      if (state().canPrint) win.print();
    } else onMessage(message);
  };
  win.addEventListener('message', listener);
  postToHost(state(), win);
  return {
    ready: () => postToHost(state(), win),
    close: () => win.removeEventListener('message', listener),
  };
}
