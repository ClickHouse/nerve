import { describe, it, expect } from 'vitest';
import { findSessionById } from './findSession';
import type { Session } from '../types/chat';

const mk = (id: string, extra: Partial<Session> = {}): Session => ({
  id, title: id, source: 'web', updated_at: '', ...extra,
});

describe('findSessionById', () => {
  const feed = [mk('a'), mk('b')];
  const archived = [mk('c'), mk('shared', { title: 'archived' })];
  const system = [mk('d'), mk('shared', { title: 'system' })];

  it('finds a row in the conversation feed', () => {
    expect(findSessionById('a', feed, archived, system)?.id).toBe('a');
  });

  it('finds a row in the archived group', () => {
    expect(findSessionById('c', feed, archived, system)?.id).toBe('c');
  });

  it('finds a row in the system group', () => {
    expect(findSessionById('d', feed, archived, system)?.id).toBe('d');
  });

  it('prefers the conversation feed on collision', () => {
    const feedShared = [...feed, mk('shared', { title: 'feed' })];
    expect(findSessionById('shared', feedShared, archived, system)?.title).toBe('feed');
    // archived beats system when the feed lacks it
    expect(findSessionById('shared', feed, archived, system)?.title).toBe('archived');
  });

  it('handles null lazy groups', () => {
    expect(findSessionById('a', feed, null, null)?.id).toBe('a');
    expect(findSessionById('z', feed, null, null)).toBeUndefined();
  });

  it('returns undefined when absent', () => {
    expect(findSessionById('z', feed, archived, system)).toBeUndefined();
  });

  it('returns undefined for a null/empty id', () => {
    expect(findSessionById(null, feed, archived, system)).toBeUndefined();
    expect(findSessionById('', feed, archived, system)).toBeUndefined();
  });
});
