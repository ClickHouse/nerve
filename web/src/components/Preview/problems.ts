import { apiErrorDetail } from '../../api/client';
import { formatMiB, PreviewTimedOut, PreviewTooLarge } from './document';

/**
 * Why a preview cannot be shown, in words for the person. The Preview tab
 * and the inline preview of the chat use the same sentences.
 */

/** How often one refresh starts again from a new description after a stale answer. */
export const MAX_RESTARTS = 3;

/** The file route's word that the files changed after the description the preview builds from. */
export function isStale(e: unknown): boolean {
  return apiErrorDetail(e)?.code === 'stale_description';
}

/** What the Preview tab does after a build that took too long. */
export const TAB_RETRY = 'It tries again when a file changes, or with Reload.';

/**
 * The sentence of a failed read or build. `retry` follows the sentence of a
 * build that took too long: what the place of the preview does next.
 */
export function problemText(e: unknown, retry: string = TAB_RETRY): string {
  if (e instanceof PreviewTimedOut) {
    return retry ? `${e.message} ${retry}` : e.message;
  }
  if (e instanceof PreviewTooLarge) {
    return e.what === 'files'
      ? `This preview needs ${formatMiB(e.bytes)} of files or more. The limit is ${formatMiB(e.limit)}.`
      : `This preview is too large to show: its document would be more than ${formatMiB(e.limit)}`
        + ' (a file that the page uses many times counts each time).';
  }
  const message = apiErrorDetail(e)?.message;
  if (typeof message === 'string' && message) return message;
  return 'The preview cannot be read.';
}
