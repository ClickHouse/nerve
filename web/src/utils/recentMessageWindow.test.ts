import { describe, expect, it } from 'vitest';
import type { ChatMessage } from '../types/chat';
import { recentMessageStart } from './recentMessageWindow';

function message(blocks: number): Pick<ChatMessage, 'blocks'> {
  return {
    blocks: Array.from({ length: blocks }, () => ({ type: 'text' as const, content: 'x' })),
  };
}

describe('recentMessageStart', () => {
  it('bounds the initially mounted blocks, then reveals older blocks in batches', () => {
    const history = Array.from({ length: 100 }, () => message(100));
    expect(recentMessageStart(history)).toBe(96);
    expect(recentMessageStart(history, 96)).toBe(92);
    expect(recentMessageStart(history, 92)).toBe(88);
  });

  it('also bounds histories with many small messages', () => {
    const history = Array.from({ length: 50 }, () => message(1));
    expect(recentMessageStart(history)).toBe(20);
    expect(recentMessageStart(history, 20)).toBe(0);
  });

  it('always shows the newest message even when its turn exceeds the budget', () => {
    const history = [message(10), message(5000)];
    expect(recentMessageStart(history)).toBe(1);
    expect(recentMessageStart(history, 1)).toBe(0);
  });

  it('handles empty histories and zero-block messages', () => {
    expect(recentMessageStart([])).toBe(0);
    expect(recentMessageStart([message(0)])).toBe(0);
  });
});
