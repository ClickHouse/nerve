/**
 * The messages between a content page and the page that shows it (the host).
 *
 * The content page is a page of a bundle, in a frame without an origin; the
 * host is the Preview tab. Every message is an object with
 * `channel: "nerve-artifact"` and `v: 1`. The content has no origin, so
 * both sides post to the target `*`, and nothing in a message is a secret.
 *
 * The host takes a message only from the frame it made (see `host.ts`),
 * checks its type and the range of each value, and uses no value as markup
 * or as an address. A page that sends no `ready` is a plain page.
 */

export const ARTIFACT_CHANNEL = 'nerve-artifact';
export const ARTIFACT_PROTOCOL_VERSION = 1;

/** What a content page says it is. */
export type ContentKind = 'deck' | 'table' | 'markdown';
const CONTENT_KINDS: ReadonlySet<string> = new Set<ContentKind>(['deck', 'table', 'markdown']);

/** The most slides that a deck can say it has. */
export const MAX_SLIDES = 10_000;

/**
 * From the content: `ready` when it has loaded and as the answer to
 * `hello`; `slide` when the slide changes. Slides count from 1.
 */
export type ContentMessage =
  | { type: 'ready'; kind: ContentKind; canPrint: boolean; slides?: number; slide?: number }
  | { type: 'slide'; slide: number; slides: number };

/** To the content. */
export type HostMessage =
  | { type: 'hello' }
  | { type: 'print' }
  | { type: 'slide'; to: 'next' | 'prev' | 'first' | 'last' | number }
  | { type: 'present'; on: boolean };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isSlideCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 1 && value <= MAX_SLIDES;
}

function isSlideOf(slide: unknown, slides: number): slide is number {
  return typeof slide === 'number' && Number.isInteger(slide) && slide >= 1 && slide <= slides;
}

/**
 * What a message from the content says, or null for anything that is not a
 * message of this channel with values in range. The result is a new object
 * that holds only the checked values.
 */
export function readContentMessage(data: unknown): ContentMessage | null {
  if (!isRecord(data) || data.channel !== ARTIFACT_CHANNEL || data.v !== ARTIFACT_PROTOCOL_VERSION) return null;
  if (data.type === 'ready') {
    if (typeof data.kind !== 'string' || !CONTENT_KINDS.has(data.kind)) return null;
    if (typeof data.canPrint !== 'boolean') return null;
    const message: ContentMessage = { type: 'ready', kind: data.kind as ContentKind, canPrint: data.canPrint };
    if (data.slides !== undefined || data.slide !== undefined) {
      if (!isSlideCount(data.slides) || !isSlideOf(data.slide, data.slides)) return null;
      message.slides = data.slides;
      message.slide = data.slide;
    }
    return message;
  }
  if (data.type === 'slide') {
    if (!isSlideCount(data.slides) || !isSlideOf(data.slide, data.slides)) return null;
    return { type: 'slide', slide: data.slide, slides: data.slides };
  }
  return null;
}

/** The message that goes to the content. */
export function hostMessage(message: HostMessage): Record<string, unknown> {
  return { channel: ARTIFACT_CHANNEL, v: ARTIFACT_PROTOCOL_VERSION, ...message };
}
