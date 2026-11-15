/**
 * The height each widget last had, by its source, so that a block that is
 * drawn again holds its place while its frame loads instead of jumping: the
 * finished message replacing the streamed one, a widget that gave up its
 * slot, the page reloaded. Kept in sessionStorage, the newest MAX_HEIGHTS;
 * the in-memory copy serves where storage is unavailable.
 */

const STORAGE_KEY = 'nerve-widget-heights';
const MAX_HEIGHTS = 100;
const memory = new Map<string, number>();

/** A short name for a source: its length and a hash of its text. */
export function widgetKey(source: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < source.length; i++) {
    hash ^= source.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return `${source.length}:${(hash >>> 0).toString(36)}`;
}

/** The stored heights, oldest first; null when this page has no usable storage. */
function stored(): Map<string, number> | null {
  try {
    const raw = window.sessionStorage.getItem(STORAGE_KEY);
    const entries: unknown = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(entries)) return new Map();
    return new Map(entries.filter((entry): entry is [string, number] =>
      Array.isArray(entry) && typeof entry[0] === 'string' && typeof entry[1] === 'number' && entry[1] > 0));
  } catch {
    return null;
  }
}

function newest(heights: Map<string, number>, key: string, height: number): void {
  heights.delete(key);
  heights.set(key, height);
  while (heights.size > MAX_HEIGHTS) heights.delete(heights.keys().next().value!);
}

/** The height the widget `key` last had; 0 when it was never drawn. */
export function widgetHeight(key: string): number {
  return memory.get(key) ?? stored()?.get(key) ?? 0;
}

export function rememberWidgetHeight(key: string, height: number): void {
  newest(memory, key, height);
  const kept = stored();
  if (!kept) return;
  newest(kept, key, height);
  try {
    window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify([...kept]));
  } catch {
    // Storage full or refused: the in-memory heights serve this page.
  }
}

/** Forget every height, for tests. */
export function resetWidgetHeights(): void {
  memory.clear();
  try {
    window.sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    // No storage to clear.
  }
}
