import { describe, expect, it } from 'vitest';
import { ARTIFACT_CHANNEL, MAX_SLIDES, hostMessage, readContentMessage } from './messages';

const base = { channel: ARTIFACT_CHANNEL, v: 1 };

describe('readContentMessage', () => {
  it('reads a ready of a deck and of a plain kind', () => {
    expect(readContentMessage({ ...base, type: 'ready', kind: 'deck', canPrint: true, slides: 12, slide: 3 }))
      .toEqual({ type: 'ready', kind: 'deck', canPrint: true, slides: 12, slide: 3 });
    expect(readContentMessage({ ...base, type: 'ready', kind: 'table', canPrint: false }))
      .toEqual({ type: 'ready', kind: 'table', canPrint: false });
  });

  it('reads a slide change', () => {
    expect(readContentMessage({ ...base, type: 'slide', slide: 2, slides: 5 })).toEqual({ type: 'slide', slide: 2, slides: 5 });
  });

  it('keeps only the checked values', () => {
    const read = readContentMessage({ ...base, type: 'slide', slide: 2, slides: 5, html: '<img src=x>' });
    expect(read).toEqual({ type: 'slide', slide: 2, slides: 5 });
  });

  it.each([
    ['another channel', { channel: 'other', v: 1, type: 'slide', slide: 1, slides: 1 }],
    ['another version', { channel: ARTIFACT_CHANNEL, v: 2, type: 'slide', slide: 1, slides: 1 }],
    ['no object', 'ready'],
    ['a list', [base]],
    ['null', null],
    ['an unknown type', { ...base, type: 'navigate', url: 'https://example.com' }],
    ['an unknown kind', { ...base, type: 'ready', kind: 'app', canPrint: false }],
    ['canPrint as text', { ...base, type: 'ready', kind: 'deck', canPrint: 'yes' }],
    ['slides without slide', { ...base, type: 'ready', kind: 'deck', canPrint: false, slides: 4 }],
    ['a slide past the end', { ...base, type: 'slide', slide: 6, slides: 5 }],
    ['slide zero', { ...base, type: 'slide', slide: 0, slides: 5 }],
    ['a fraction', { ...base, type: 'slide', slide: 1.5, slides: 5 }],
    ['too many slides', { ...base, type: 'slide', slide: 1, slides: MAX_SLIDES + 1 }],
    ['a slide as text', { ...base, type: 'slide', slide: '1', slides: 5 }],
    ['an endless count', { ...base, type: 'slide', slide: 1, slides: Infinity }],
  ])('drops %s', (_what, data) => {
    expect(readContentMessage(data)).toBeNull();
  });
});

describe('hostMessage', () => {
  it('carries the channel and the version', () => {
    expect(hostMessage({ type: 'slide', to: 'next' })).toEqual({ channel: ARTIFACT_CHANNEL, v: 1, type: 'slide', to: 'next' });
    expect(hostMessage({ type: 'present', on: true })).toEqual({ channel: ARTIFACT_CHANNEL, v: 1, type: 'present', on: true });
  });
});
