import { describe, expect, it } from 'vitest';
import type { Notification } from '../../stores/notificationStore';
import { notificationOptions, parseAskOptions, questionOptions } from './askOptions';

describe('parseAskOptions', () => {
  it('takes a list as it is', () => {
    expect(parseAskOptions(['Yes, all of these', ' No ', ''])).toEqual(['Yes, all of these', 'No']);
  });

  it('keeps a comma inside an option of a JSON array string', () => {
    expect(parseAskOptions('["Yes, all of these","No"]')).toEqual(['Yes, all of these', 'No']);
  });

  it('splits a plain string at commas', () => {
    expect(parseAskOptions('Yes, No , Maybe,')).toEqual(['Yes', 'No', 'Maybe']);
  });

  it('splits JSON that is not an array at commas', () => {
    expect(parseAskOptions('{"a": 1, "b": 2}')).toEqual(['{"a": 1', '"b": 2}']);
  });

  it('gives no options for nothing', () => {
    expect(parseAskOptions(undefined)).toEqual([]);
    expect(parseAskOptions(null)).toEqual([]);
    expect(parseAskOptions('  ')).toEqual([]);
    expect(parseAskOptions(3)).toEqual([]);
  });
});

describe('questionOptions', () => {
  it('puts the recommended options first, with the mark off the label', () => {
    expect(questionOptions(['Keep', 'Delete all (recommended)', 'Ask later', 'Archive (Recommended)'])).toEqual([
      { value: 'Delete all (recommended)', label: 'Delete all', recommended: true },
      { value: 'Archive (Recommended)', label: 'Archive', recommended: true },
      { value: 'Keep', label: 'Keep', recommended: false },
      { value: 'Ask later', label: 'Ask later', recommended: false },
    ]);
  });

  it('keeps a mark that is not at the end', () => {
    expect(questionOptions(['(recommended) first'])[0]).toEqual(
      { value: '(recommended) first', label: '(recommended) first', recommended: false },
    );
  });
});

function approval(overrides: Partial<Notification>): Notification {
  return {
    id: 'approval-1', session_id: 's1', session_title: null, type: 'approval', title: 'Run it?',
    body: '', priority: 'high', status: 'pending', options: ['approve', 'decline', 'snooze_24h', 'run_twice'],
    answer: null, answered_by: null, answered_at: null, created_at: '2026-01-01T00:00:00Z', expires_at: null,
    ...overrides,
  };
}

describe('notificationOptions', () => {
  it('labels approval values from the metadata, then the defaults, then the value', () => {
    const notif = approval({ metadata: JSON.stringify({ option_labels: { approve: 'Ship it' } }) });
    expect(notificationOptions(notif).map(o => o.label)).toEqual(['Ship it', 'Decline', 'Snooze 24h', 'Run Twice']);
    expect(notificationOptions(notif).map(o => o.value)).toEqual(['approve', 'decline', 'snooze_24h', 'run_twice']);
  });

  it('marks the known approval values by value, also with the agent labels', () => {
    const notif = approval({ option_labels: { approve: 'Ship it', decline: 'Not now' } });
    expect(notificationOptions(notif).map(o => o.mark)).toEqual(['approve', 'decline', 'snooze', undefined]);
  });

  it('prefers the live label map', () => {
    const notif = approval({ option_labels: { decline: 'Not now' }, metadata: '{not json' });
    expect(notificationOptions(notif)[1].label).toBe('Not now');
  });

  it('reads a question row whose options are a JSON string', () => {
    const notif = approval({ type: 'question', options: '["A, B", "C (recommended)"]' as unknown as string[] });
    expect(notificationOptions(notif).map(o => o.label)).toEqual(['C', 'A, B']);
  });
});
