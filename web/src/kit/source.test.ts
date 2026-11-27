import { describe, expect, it } from 'vitest';
import { MAX_SOURCE_BYTES, sourceTooLarge, utf8Length } from './source';

describe('utf8Length', () => {
  it.each([
    ['', 0], ['abc', 3], ['é', 2], ['€', 3], ['\u{1F600}', 4], ['a\uD800b', 5], ['\uDC00', 3],
  ])('%j has %i bytes', (text, bytes) => {
    expect(utf8Length(text)).toBe(bytes);
    expect(utf8Length(text)).toBe(text.includes('\uD800') || text.includes('\uDC00') ? bytes : new TextEncoder().encode(text).length);
  });

  it('stops counting just past the limit', () => {
    expect(utf8Length('x'.repeat(100), 10)).toBe(11);
  });
});

describe('sourceTooLarge', () => {
  const at = (text: string) => sourceTooLarge({ name: 'a.csv', text });

  it('takes a source of exactly the limit and refuses one byte more', () => {
    expect(at('x'.repeat(MAX_SOURCE_BYTES))).toBeNull();
    expect(at('x'.repeat(MAX_SOURCE_BYTES + 1))).toBe('The file is larger than 5 MiB. A page shows at most that.');
  });

  it('counts characters of several bytes', () => {
    // 1,747,626 characters of 3 bytes and 2 of one: exactly the limit.
    const exact = `a\n${'€'.repeat((MAX_SOURCE_BYTES - 2) / 3)}`;
    expect(exact.length).toBeLessThan(MAX_SOURCE_BYTES / 2);
    expect(at(exact)).toBeNull();
    expect(at(`${exact}x`)).not.toBeNull();
  });
});
