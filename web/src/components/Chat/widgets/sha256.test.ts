import { describe, expect, it } from 'vitest';
import { sha256 } from './sha256';

/**
 * The digests here were computed with Node:
 * `createHash('sha256').update(Buffer.from(text, 'utf16le')).digest('hex')`,
 * the SHA-256 of the text's UTF-16 code units, low byte first.
 */

describe('sha256', () => {
  it('is the SHA-256 of nothing for an empty text', () => {
    expect(sha256('')).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  });

  it.each([
    ['one letter', 'a', 'ffe9aaeaa2a2d5048174df0b80599ef0197ec024c4b051bc9860cff58ef7f9f3'],
    ['a short text', 'abc', '13e228567e8249fce53337f25d7970de3bd68ab2653424c7b8f9fd05e33caedf'],
    [
      'a widget',
      '<button id="b">Hi</button>\n<script>document.getElementById("b").onclick = () => {}</script>',
      '5b5f1f9c827124893554bae0737b13d55b7ed059450a79fef85b26dd295ec84e',
    ],
    ['text outside ASCII', 'Größe — 尺寸 — размер 🧳', 'ae225229e3807b5b3c7dbf55913086d020de43a35ce49fc09041c40905850bfb'],
    ['a text that ends a block exactly', 'x'.repeat(32), 'b1ace41792b7d34bb21f76022ed2263d078456bf5ca6a4cb31e5d6a228e9d929'],
    ['a text whose length just fits its block', 'x'.repeat(27), 'a190a82abecd05b3dea26e1de631b57dc958b2742feec97be363ab29add780ca'],
    ['a text whose length needs a block of its own', 'x'.repeat(28), 'fe6013a359a72f21bddaaa5cf4f0536574441454000ca3a63c8e35dfe7948426'],
    [
      'a long text',
      'The quick brown fox jumps over the lazy dog. '.repeat(5000),
      'fff0eb0a5860386502899f5507cffe87431348e8ab592582d72bfe410ae926cb',
    ],
  ])('is the reference digest for %s', (_what, text, digest) => {
    expect(sha256(text)).toBe(digest);
  });

  it('is the reference digest for every length around the block size', () => {
    // The digests of 201 texts, of 0 to 200 characters, hashed together: one wrong among them changes the whole.
    const digests = [];
    for (let length = 0; length <= 200; length++) {
      digests.push(sha256(Array.from({ length }, (_, i) => String.fromCharCode(33 + ((i * 7) % 90))).join('')));
    }
    expect(new Set(digests).size).toBe(201);
    expect(sha256(digests.join(''))).toBe('c188b0eab790c1f9ea34a7261cdb21848dfa32e8946266794a76e41c4d33e748');
  });

  it('tells texts apart that differ in half a surrogate pair only', () => {
    expect(sha256('a\ud83d')).toBe('b0e6f334ac974a699d7a07e2c5820f51533e18e2b43574799a40815bf9a84dec');
    expect(sha256('a\udc00')).toBe('2bfb965e1d4bf610579e5a5ddb3307749d2c29027c7cb83a551cfa5a7c45d737');
    expect(sha256('a\ud83d')).not.toBe(sha256('a�'));
  });

  it('is 64 hex digits, the same for the same text and another for any other', () => {
    expect(sha256('widget')).toMatch(/^[0-9a-f]{64}$/);
    expect(sha256('widget')).not.toBe(sha256('widget '));
    expect(sha256('widget')).toBe(sha256('widget'));
  });
});
