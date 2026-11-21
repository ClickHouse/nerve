/**
 * The text of one metadata value in the header of a source message.
 *
 * A list too long for a chip is shown as its length: the message's content
 * holds its items. The commits of a repository watch's commit record are such
 * a list.
 */
const MAX_LIST_TEXT = 80;

export function metadataValueText(value: unknown): string {
  if (Array.isArray(value)) {
    const text = JSON.stringify(value);
    return text.length > MAX_LIST_TEXT ? `${value.length} items` : text;
  }
  return typeof value === 'object' ? JSON.stringify(value) : String(value);
}
