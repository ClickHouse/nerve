import { describe, expect, it } from 'vitest';
import type { ChatMessage } from '../../types/chat';
import { mergeCatchUp, sameRows } from './catchUp';

const user = (text: string, extra: Partial<ChatMessage> = {}): ChatMessage =>
  ({ role: 'user', blocks: [{ type: 'text', content: text }], ...extra });
const reply = (text: string, extra: Partial<ChatMessage> = {}): ChatMessage =>
  ({ role: 'assistant', blocks: [{ type: 'text', content: text }], ...extra });

describe('mergeCatchUp', () => {
  it('replaces the rows on screen with the stored rows', () => {
    const shown = [user('hi', { id: 1 }), reply('partial answer')];
    const stored = [user('hi', { id: 1 }), reply('full answer', { id: 2 })];

    expect(mergeCatchUp(stored, shown)).toBe(stored);
  });

  it('keeps a sent message that the server has not stored yet', () => {
    const sent = user('run the tests', { client_msg_id: 'c-1' });
    const stored = [user('hi', { id: 1 }), reply('hello', { id: 2 })];

    expect(mergeCatchUp(stored, [user('hi', { id: 1 }), sent])).toEqual([...stored, sent]);
  });

  it('drops a sent message once its stored row is in the read', () => {
    const shown = [user('hi', { id: 1 }), user('run the tests', { client_msg_id: 'c-1' })];
    const stored = [user('hi', { id: 1 }), user('run the tests', { id: 2 })];

    expect(mergeCatchUp(stored, shown)).toBe(stored);
  });

  it('matches a stored row only once, and not a row the screen showed before', () => {
    // The same text twice: the earlier one was on screen with its id, the
    // later one was sent and is not stored yet.
    const sent = user('again', { client_msg_id: 'c-2' });
    const shown = [user('again', { id: 1 }), sent];
    const stored = [user('again', { id: 1 })];

    expect(mergeCatchUp(stored, shown)).toEqual([...stored, sent]);
  });

  it('matches a message with images by its text without the stored note', () => {
    const image = { type: 'image' as const, url: '/f/1', filename: 'a.png', media_type: 'image/png' };
    const shown = [
      user('look at this', { client_msg_id: 'c-1', blocks: [{ type: 'text', content: 'look at this' }, image] }),
      { role: 'user' as const, client_msg_id: 'c-2', blocks: [image] },
    ];
    const stored = [
      user('look at this\n[1 image(s) attached]', { id: 5 }),
      user('[1 image(s) attached]', { id: 6 }),
    ];

    expect(mergeCatchUp(stored, shown)).toBe(stored);
  });

  it('drops the rows that only this tab had', () => {
    const shown = [user('hi', { id: 1 }), reply('Error: Nerve is shutting down')];
    const stored = [user('hi', { id: 1 })];

    expect(mergeCatchUp(stored, shown)).toBe(stored);
  });
});

describe('sameRows', () => {
  it('is true only for the same stored ids in the same order', () => {
    const shown = [user('hi', { id: 1 }), reply('hello', { id: 2 })];

    expect(sameRows([user('hi', { id: 1 }), reply('hello', { id: 2 })], shown)).toBe(true);
    expect(sameRows([user('hi', { id: 1 })], shown)).toBe(false);
    expect(sameRows([user('hi', { id: 1 }), reply('hello', { id: 3 })], shown)).toBe(false);
  });

  it('is false when the screen has a row without an id', () => {
    const shown = [user('hi', { id: 1 }), reply('streamed')];

    expect(sameRows([user('hi', { id: 1 }), reply('stored', { id: 2 })], shown)).toBe(false);
  });
});
