import { describe, it, expect } from 'vitest';
import { metadataValueText } from './sourceMetadata';

const COMMITS = [
  '5c4d3e2f1a0b99887766554433221100ffeeddcc Cache rendered widgets',
  '1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d Add the widget cache interface',
];

describe('metadataValueText', () => {
  it('shows a scalar as it is', () => {
    expect(metadataValueText('repo_name', 'acme/widgets')).toBe('acme/widgets');
    expect(metadataValueText('number', 12)).toBe('12');
    expect(metadataValueText('truncated', false)).toBe('false');
    expect(metadataValueText('commits', 'none')).toBe('none');
  });

  it('shows the commits of a commit record as their number when they are too long for a chip', () => {
    expect(metadataValueText('commits', COMMITS)).toBe('2 items');
  });

  it('shows a short list of commits in full', () => {
    expect(metadataValueText('commits', ['5c4d3e2 Cache rendered widgets'])).toBe('["5c4d3e2 Cache rendered widgets"]');
    expect(metadataValueText('commits', [])).toBe('[]');
  });

  it('shows the list of every other key in full, however long', () => {
    const labels = Array.from({ length: 12 }, (_, index) => `area/component-${index}`);
    expect(metadataValueText('labels', labels)).toBe(JSON.stringify(labels));
    expect(metadataValueText('actors', ['alice', 'bob'])).toBe('["alice","bob"]');
    // The same list under another key is not the commits of a commit record.
    expect(metadataValueText('changes', COMMITS)).toBe(JSON.stringify(COMMITS));
    expect(metadataValueText('Commits', COMMITS)).toBe(JSON.stringify(COMMITS));
  });

  it('shows any other object as JSON', () => {
    expect(metadataValueText('extra', { a: 1 })).toBe('{"a":1}');
    expect(metadataValueText('extra', null)).toBe('null');
    expect(metadataValueText('commits', { a: 1 })).toBe('{"a":1}');
  });
});
