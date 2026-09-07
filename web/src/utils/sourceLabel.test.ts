import { describe, it, expect } from 'vitest';
import { sourceLabel } from './sourceLabel';

describe('sourceLabel', () => {
  it('shows a bare source name as-is', () => {
    expect(sourceLabel('github')).toBe('github');
    expect(sourceLabel('telegram')).toBe('telegram');
  });

  it('shows just the account for an account-qualified source', () => {
    expect(sourceLabel('gmail:someone@example.com')).toBe('someone@example.com');
  });

  it('truncates a long account to 18 characters plus an ellipsis', () => {
    expect(sourceLabel('gmail:a-very-long-address@example.com'))
      .toBe('a-very-long-addres..');
  });

  it('keeps the transport for a channel source', () => {
    // The bug this pins: the account rule returned "observed" on its own,
    // which names neither the transport nor the feed.
    expect(sourceLabel('slack:observed')).toBe('slack (observed)');
    expect(sourceLabel('telegram:observed')).toBe('telegram (observed)');
  });

  it('does not mistake an account for the channel-source qualifier', () => {
    expect(sourceLabel('gmail:observed@example.com')).toBe('observed@example.com');
  });

  it('splits on the first colon only, so an account may contain one', () => {
    expect(sourceLabel('imap:user:993')).toBe('user:993');
  });
});
