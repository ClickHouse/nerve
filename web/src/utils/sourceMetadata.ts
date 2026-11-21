/**
 * The text of one metadata value in the header of a source message.
 *
 * A value is shown as it is, and a list or any other object as JSON. One key
 * is the exception: `commits`, the list of a repository watch's commit
 * record. When it is too long for a chip it is shown as its length, and the
 * message's content holds its items. The lists of every other key, of any
 * source, are shown in full.
 */
const COMMITS_KEY = 'commits';
const MAX_COMMITS_TEXT = 80;

export function metadataValueText(key: string, value: unknown): string {
  if (key === COMMITS_KEY && Array.isArray(value)) {
    const text = JSON.stringify(value);
    return text.length > MAX_COMMITS_TEXT ? `${value.length} items` : text;
  }
  return typeof value === 'object' ? JSON.stringify(value) : String(value);
}
