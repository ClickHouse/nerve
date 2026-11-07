import type { ChatMessage } from '../../types/chat';

/** The text of a message: its text blocks, joined. */
function textOf(message: ChatMessage): string {
  return message.blocks
    .map(b => (b.type === 'text' ? b.content : ''))
    .join('');
}

/**
 * The server stores a message with attached images with a note at the end,
 * for example "\n[2 image(s) attached]". Remove it for the comparison.
 */
function storedText(message: ChatMessage): string {
  return textOf(message).replace(/\n?\[\d+ image\(s\) attached\]$/, '');
}

/**
 * Merge the stored messages of the open chat with the messages on screen,
 * after the socket was down. The stored messages replace the rows on screen,
 * with one exception: a message that a tab sent (it has a client_msg_id and
 * no stored id) stays when the server has not stored it yet. The server
 * stores a message only when its turn starts, so the read can come first.
 *
 * The server does not store the client_msg_id. A sent message counts as
 * stored when a stored user message that this tab did not show before has
 * the same text.
 */
export function mergeCatchUp(stored: ChatMessage[], shown: ChatMessage[]): ChatMessage[] {
  const shownIds = new Set(shown.flatMap(m => (m.id != null ? [m.id] : [])));
  const newTexts = stored
    .filter(m => m.role === 'user' && m.id != null && !shownIds.has(m.id))
    .map(storedText);
  const notStored: ChatMessage[] = [];
  for (const m of shown) {
    if (m.id != null || m.role !== 'user' || !m.client_msg_id) continue;
    const i = newTexts.indexOf(textOf(m));
    if (i >= 0) newTexts.splice(i, 1);
    else notStored.push(m);
  }
  return notStored.length > 0 ? [...stored, ...notStored] : stored;
}

/**
 * True when the merge would show the same rows as now: the same stored ids
 * in the same order and no rows without an id. Then the view stays as it is.
 */
export function sameRows(merged: ChatMessage[], shown: ChatMessage[]): boolean {
  return merged.length === shown.length
    && shown.every((m, i) => m.id != null && m.id === merged[i].id);
}
