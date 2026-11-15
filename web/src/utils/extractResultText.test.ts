import { describe, expect, it } from 'vitest';
import { extractResultText } from './extractResultText';

const blocks = [
  { type: 'text', text: 'Sent file: chart.png (2,048 bytes)' },
  { type: 'image', data: 'iVBORw0KGgo=' },
  { type: 'text', text: 'agentId: a1b2' },
  { type: 'text', text: 'Second line' },
];

describe('extractResultText', () => {
  it('reads the text blocks of a Claude result, a JSON list', () => {
    expect(extractResultText(JSON.stringify(blocks))).toBe('Sent file: chart.png (2,048 bytes)\nSecond line');
  });

  it('reads the text blocks of a Codex result, a JSON object with the list in `content`', () => {
    expect(extractResultText(JSON.stringify({ content: blocks, isError: false }))).toBe(
      'Sent file: chart.png (2,048 bytes)\nSecond line',
    );
  });

  it('gives other results unchanged', () => {
    const object = JSON.stringify({ scheduledFor: 1767225600000, clampedDelaySeconds: 60 });
    for (const result of ['Next wakeup scheduled for 10:00:00 (in 60s).', object, '{"content": "plain"}', 'null', '42']) {
      expect(extractResultText(result)).toBe(result);
    }
  });
});
