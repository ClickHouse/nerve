import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MessageContent } from '../MessageContent';
import { getRendererType, recordData, showsRecordData } from './index';

describe('The record data of a generic hosted source', () => {
  it('is the metadata of exactly a schema and a JSON object', () => {
    expect(recordData({ schema: 'example.note/1', data: { event: 'created' } })).toEqual({
      schema: 'example.note/1', data: { event: 'created' },
    });
    expect(recordData({ schema: 'example.note/1', data: {} })).toEqual({ schema: 'example.note/1', data: {} });
    // The metadata of other sources: GitHub, Telegram, a channel drain.
    expect(recordData({ repo_name: 'owner/repo', actors: ['alice'] })).toBeNull();
    expect(recordData({ schema: 'x', data: {}, chat_id: 42 })).toBeNull();
    expect(recordData({ schema: 7, data: {} })).toBeNull();
    expect(recordData({ schema: 'x', data: ['not', 'an', 'object'] })).toBeNull();
    expect(recordData(undefined)).toBeNull();
    expect(recordData(null)).toBeNull();
  });

  it('chooses the data view by the schema, also for flat data', () => {
    expect(getRendererType('example', { schema: 'example.note/1', data: { event: 'created' } })).toBe('data');
    expect(getRendererType('example', { event: 'created', note: { key: 'NOTE-7' } })).toBe('default');
    expect(getRendererType('telegram', { chat_id: 42 })).toBe('default');
    expect(getRendererType('github', { repo_name: 'owner/repo' })).toBe('github');
    expect(getRendererType('gmail:me@example.com', {})).toBe('email');
    expect(showsRecordData('data')).toBe(true);
    expect(showsRecordData('default')).toBe(false);
    expect(showsRecordData('github')).toBe(false);
  });

  it('shows the content and every member of the data at once, with its schema', () => {
    render(
      <MessageContent
        source="example"
        summary="[NOTE-7] New note: Plan"
        content="A **new** note"
        metadata={{ schema: 'example.note/1', data: { event: 'created', note: { key: 'NOTE-7', labels: ['planning'] } } }}
      />,
    );

    expect(screen.getByText('new', { selector: 'strong' })).toBeTruthy();
    const toggle = screen.getByRole('button', { name: /Record data/ });
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(toggle.textContent).toContain('example.note/1');
    expect(screen.getByText(/"key": "NOTE-7"/)).toBeTruthy();
    expect(screen.getByText(/"planning"/)).toBeTruthy();
  });

  it('leaves the flat metadata of other sources to the header chips', () => {
    render(<MessageContent source="telegram" summary="Hi" content="Hello" metadata={{ chat_id: 42 }} />);

    expect(screen.getByText('Hello')).toBeTruthy();
    expect(screen.queryByRole('button', { name: /Record data/ })).toBeNull();
  });
});
