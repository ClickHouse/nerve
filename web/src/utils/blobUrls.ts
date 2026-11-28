/**
 * Blob URLs that the image viewer (ImageViewerHost) can show, with the
 * number of holds on each. The viewer stays open when the card that made a
 * URL goes (a tool burst folds, the list of messages moves), so the card
 * revokes its URLs through `revokeBlobUrl`, which waits for the last hold.
 */
const holds = new Map<string, number>();
/** Held URLs whose card is gone: revoked when the last hold ends. */
const orphans = new Set<string>();

export function holdBlobUrls(urls: string[]): void {
  for (const url of urls) {
    if (url.startsWith('blob:')) holds.set(url, (holds.get(url) ?? 0) + 1);
  }
}

export function releaseBlobUrls(urls: string[]): void {
  for (const url of urls) {
    const left = (holds.get(url) ?? 0) - 1;
    if (left > 0) {
      holds.set(url, left);
      continue;
    }
    holds.delete(url);
    if (orphans.delete(url)) URL.revokeObjectURL(url);
  }
}

/** Revoke `url` now, or when the viewer no longer holds it. */
export function revokeBlobUrl(url: string): void {
  if (holds.has(url)) orphans.add(url);
  else URL.revokeObjectURL(url);
}
