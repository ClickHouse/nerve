import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import { MessageContent } from '../MessageContent';
import { getRendererType, LINEAR_ISSUE_EVENT_SCHEMA, showsRecordData } from './index';
// The data of linear.issue_event/1 records as the gateway sends them (test
// values only). The backend tests validate this file against the schema.
import events from '../../../test/linearIssueEvents.json';

/** The metadata of a stored Linear record: its schema and its data. */
function stored(data: Record<string, unknown>, schema = LINEAR_ISSUE_EVENT_SCHEMA) {
  return { schema, data };
}

function show(metadata: Record<string, unknown>) {
  render(
    <MessageContent
      source="linear"
      summary="[ENG-12] Alice Example moved Fix the login page to In Progress"
      content="**Alice Example** moved the issue to In Progress."
      metadata={metadata}
    />,
  );
}

describe('Linear records', () => {
  it('render with the Linear renderer by their schema, and keep the metadata chips out of the header', () => {
    expect(getRendererType('linear', stored(events.state_changed))).toBe('linear');
    expect(getRendererType('anything', stored(events.state_changed))).toBe('linear');
    expect(showsRecordData('linear')).toBe(true);
  });

  it('of another major version get the record data view', () => {
    expect(getRendererType('linear', stored(events.state_changed, 'linear.issue_event/2'))).toBe('data');
  });

  it('show the issue card: identifier, title, event, state, actor, and a link to the issue', () => {
    show(stored(events.state_changed));

    expect(screen.getByText('ENG-12')).toBeTruthy();
    expect(screen.getByText('Fix the login page')).toBeTruthy();
    expect(screen.getByText('state changed')).toBeTruthy();
    expect(screen.getByText('In Progress')).toBeTruthy();
    expect(screen.getByText('by Alice Example')).toBeTruthy();
    const link = screen.getByRole('link', { name: /View in Linear/ });
    expect(link.getAttribute('href')).toBe('https://linear.example.test/acme/issue/ENG-12');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
    expect(screen.getByText('Alice Example', { selector: 'strong' })).toBeTruthy();
  });

  it('link a comment event to the comment', () => {
    show(stored(events.commented));

    expect(screen.getByText('commented')).toBeTruthy();
    expect(screen.getByRole('link', { name: /View in Linear/ }).getAttribute('href')).toBe(
      'https://linear.example.test/acme/issue/ENG-12#comment-9e2f6a4b',
    );
  });

  it('name a change without an actor by its kind', () => {
    show(stored(events.system));

    expect(screen.getByText('by system')).toBeTruthy();
    expect(screen.getByText('Canceled')).toBeTruthy();
  });

  it('read the data tolerantly: missing members, other types, and links that are not http(s)', () => {
    show(stored({
      event: 7,
      issue: { identifier: 'ENG-13', title: ['not text'], url: 'javascript:alert(1)', state: 'Done' },
      actor: 'Bob Example',
    }));

    expect(screen.getByText('ENG-13')).toBeTruthy();
    expect(screen.getByText('7')).toBeTruthy();
    expect(screen.getByText('Done')).toBeTruthy();
    expect(screen.getByText('by Bob Example')).toBeTruthy();
    expect(screen.queryByRole('link', { name: /View in Linear/ })).toBeNull();
  });

  it('leave the card out without issue fields, and still show the content', () => {
    show(stored({ event: 'mentioned' }));

    expect(screen.queryByText('mentioned')).toBeNull();
    expect(screen.getByText('Alice Example', { selector: 'strong' })).toBeTruthy();
  });

  it('show every member of the data behind the record data toggle, unknown ones too', async () => {
    show(stored({ ...events.state_changed, estimate: { points: 3 } }));
    const toggle = screen.getByRole('button', { name: /Record data/ });

    expect(toggle.textContent).toContain(LINEAR_ISSUE_EVENT_SCHEMA);
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(screen.queryByText(/"points": 3/)).toBeNull();
    await userEvent.click(toggle);

    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByText(/"points": 3/)).toBeTruthy();
    expect(screen.getByText(/"kind": "user"/)).toBeTruthy();
  });
});
