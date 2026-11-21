import { describe, it, expect } from 'vitest';
import { metadataValueText } from './sourceMetadata';

describe('metadataValueText', () => {
  it('shows a scalar as it is', () => {
    expect(metadataValueText('acme/widgets')).toBe('acme/widgets');
    expect(metadataValueText(12)).toBe('12');
    expect(metadataValueText(false)).toBe('false');
  });

  it('shows a short list in full', () => {
    expect(metadataValueText(['alice', 'bob'])).toBe('["alice","bob"]');
    expect(metadataValueText([])).toBe('[]');
  });

  it('shows a list too long for a chip as its length', () => {
    const commits = [
      '5c4d3e2f1a0b99887766554433221100ffeeddcc Cache rendered widgets',
      '1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d Add the widget cache interface',
    ];
    expect(metadataValueText(commits)).toBe('2 items');
  });

  it('shows any other object as JSON', () => {
    expect(metadataValueText({ a: 1 })).toBe('{"a":1}');
    expect(metadataValueText(null)).toBe('null');
  });
});
