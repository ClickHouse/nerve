import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import { MessageContent } from '../MessageContent';
import { getRendererType, showsRecordData } from './index';

/** The data of a linear.issue_event/1 record, as the gateway sends it (test values only). */
function issueEvent(changes: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    event: 'state_changed',
    via: 'watch',
    server: { id: '3c9a7e15-2b4d-4f60-8e1a-5d7c9b2f4a68', name: 'Linear' },
    issue: {
      id: '5f0c1b9e-3c2a-4d7e-9b1f-8a6d2e4c7b30',
      identifier: 'ENG-12',
      title: 'Fix the login page',
      url: 'https://linear.example.test/acme/issue/ENG-12',
      team: { key: 'ENG' },
      state: { name: 'In Progress', type: 'started' },
      priority: 2,
    },
    actor: { id: '0b7d4c2a-6e1f-4a3b-9c8d-2f5e7a1b3c49', name: 'Alice Example', kind: 'user' },
    changes: [{ field: 'state', from: 'Todo', to: 'In Progress' }],
    ...changes,
  };
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
  it('render with the Linear renderer and keep the metadata chips out of the header', () => {
    expect(getRendererType('linear')).toBe('linear');
    expect(showsRecordData('linear')).toBe(true);
  });

  it('show the issue card: identifier, title, event, state, actor, and a link to the issue', () => {
    show(issueEvent());

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
    show(issueEvent({
      event: 'commented',
      comment: { id: 'c1', url: 'https://linear.example.test/acme/issue/ENG-12#comment-c1', body: 'Looks good' },
    }));

    expect(screen.getByText('commented')).toBeTruthy();
    expect(screen.getByRole('link', { name: /View in Linear/ }).getAttribute('href')).toBe(
      'https://linear.example.test/acme/issue/ENG-12#comment-c1',
    );
  });

  it('read the data tolerantly: missing members, other types, and links that are not http(s)', () => {
    show({
      event: 7,
      issue: { identifier: 'ENG-13', title: ['not text'], url: 'javascript:alert(1)', state: 'Done' },
      actor: 'Bob Example',
    });

    expect(screen.getByText('ENG-13')).toBeTruthy();
    expect(screen.getByText('7')).toBeTruthy();
    expect(screen.getByText('Done')).toBeTruthy();
    expect(screen.getByText('by Bob Example')).toBeTruthy();
    expect(screen.queryByRole('link', { name: /View in Linear/ })).toBeNull();
  });

  it('leave the card out without issue fields, and still show the content', () => {
    show({ event: 'mentioned' });

    expect(screen.queryByText('mentioned')).toBeNull();
    expect(screen.getByText('Alice Example', { selector: 'strong' })).toBeTruthy();
  });

  it('show every member of the data behind the record data toggle, unknown ones too', async () => {
    show(issueEvent({ estimate: { points: 3 } }));
    const toggle = screen.getByRole('button', { name: 'Record data' });

    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(screen.queryByText(/"points": 3/)).toBeNull();
    await userEvent.click(toggle);

    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByText(/"points": 3/)).toBeTruthy();
    expect(screen.getByText(/"kind": "user"/)).toBeTruthy();
  });
});
