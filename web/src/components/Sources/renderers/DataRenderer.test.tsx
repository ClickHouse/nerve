import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MessageContent } from '../MessageContent';
import { getRendererType, hasRecordData, showsRecordData } from './index';

describe('The record data of a generic hosted source', () => {
  it('is nested JSON in the metadata, which the flat metadata of other sources is not', () => {
    expect(hasRecordData({ note: { key: 'NOTE-7' } })).toBe(true);
    expect(hasRecordData({ changes: [{ field: 'state' }] })).toBe(true);
    expect(hasRecordData({ matrix: [[1, 2]] })).toBe(true);
    // GitHub actors, Gmail labels, Telegram ids, IMAP flags.
    expect(hasRecordData({ actors: ['alice'], labels: [], chat_id: 42, matched: true, from: '' })).toBe(false);
    expect(hasRecordData({ value: null })).toBe(false);
    expect(hasRecordData(undefined)).toBe(false);
    expect(hasRecordData(null)).toBe(false);
  });

  it('chooses the data view for a source without a renderer of its own', () => {
    expect(getRendererType('example', { note: { key: 'NOTE-7' } })).toBe('data');
    expect(getRendererType('example', { event: 'created' })).toBe('default');
    expect(getRendererType('telegram', { chat_id: 42 })).toBe('default');
    // A source with its own renderer keeps it.
    expect(getRendererType('github', { nested: { a: 1 } })).toBe('github');
    expect(getRendererType('gmail:me@example.com', { nested: { a: 1 } })).toBe('email');
    expect(showsRecordData('data')).toBe(true);
    expect(showsRecordData('default')).toBe(false);
    expect(showsRecordData('github')).toBe(false);
  });

  it('shows the content and every member of the data at once', () => {
    render(
      <MessageContent
        source="example"
        summary="[NOTE-7] New note: Plan"
        content="A **new** note"
        metadata={{ event: 'created', note: { key: 'NOTE-7', labels: ['planning'] } }}
      />,
    );

    expect(screen.getByText('new', { selector: 'strong' })).toBeTruthy();
    const toggle = screen.getByRole('button', { name: 'Record data' });
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByText(/"key": "NOTE-7"/)).toBeTruthy();
    expect(screen.getByText(/"planning"/)).toBeTruthy();
  });

  it('leaves the flat metadata of other sources to the header chips', () => {
    render(<MessageContent source="telegram" summary="Hi" content="Hello" metadata={{ chat_id: 42 }} />);

    expect(screen.getByText('Hello')).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Record data' })).toBeNull();
  });
});
