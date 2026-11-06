import { describe, expect, it } from 'vitest';
import { httpLink } from './httpLink';

describe('httpLink', () => {
  it('keeps an absolute http or https address', () => {
    expect(httpLink('https://loom.example.test/memories?namespace=agent-home'))
      .toBe('https://loom.example.test/memories?namespace=agent-home');
    expect(httpLink('http://192.0.2.1:3000/')).toBe('http://192.0.2.1:3000/');
  });

  it('refuses anything a link must not carry', () => {
    for (const value of ['', null, undefined, 'javascript:alert(1)', '/memories', 'loom.example.test', 'data:text/html,x']) {
      expect(httpLink(value)).toBeNull();
    }
  });
});
