import { describe, it, expect } from 'vitest';
import { totalInputTokens } from './usageTokens';

describe('totalInputTokens', () => {
  it('adds cache reads and cache writes to the uncached input', () => {
    expect(totalInputTokens({ input_tokens: 12, cache_read: 90_000, cache_creation: 4_000 }))
      .toBe(94_012);
  });

  it('treats missing and null fields as zero', () => {
    expect(totalInputTokens({ input_tokens: 5 })).toBe(5);
    expect(totalInputTokens({ input_tokens: null, cache_read: 7, cache_creation: null })).toBe(7);
    expect(totalInputTokens({})).toBe(0);
  });
});
