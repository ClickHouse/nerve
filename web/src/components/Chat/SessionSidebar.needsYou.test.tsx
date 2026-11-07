import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Session, SessionAttention } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= key === 'listActors'
      ? vi.fn(async () => ({ actors: [] }))
      : vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(), disconnect: vi.fn(), onMessage: vi.fn(() => () => {}),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { useChatStore } = await import('../../stores/chatStore');
const { SessionSidebar } = await import('./SessionSidebar');

const NONE: SessionAttention = { questions: 0, approvals: 0, plans: 0, awaiting_input: false, review: false };

function session(id: string, extra: Partial<Session> = {}, attention: Partial<SessionAttention> = {}): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(), is_running: false,
    attention: { ...NONE, ...attention }, ...extra,
  };
}

function renderSidebar(sessions: Session[]) {
  return render(
    <MemoryRouter>
      <SessionSidebar
        sessions={sessions} activeSession="" agentStatus={{ state: 'idle' }}
        onCreate={() => {}} onDelete={() => {}}
      />
    </MemoryRouter>,
  );
}

/** The session titles listed under one group header, in order. */
function groupTitles(label: string): string[] {
  const group = screen.getByRole('button', { name: label }).parentElement!;
  return within(group).getAllByRole('link').map(a => a.getAttribute('href')!.replace('/chat/', ''));
}

beforeEach(() => {
  localStorage.clear();
  useChatStore.setState({
    sessions: [], searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
  });
});

describe('the Needs you group', () => {
  it('lists each session that waits for the user once, above Running', () => {
    renderSidebar([
      session('asks', { is_running: true }, { questions: 1 }),
      session('plan', { source: 'cron', starred: true }, { plans: 1 }),
      session('busy', { is_running: true }),
      session('quiet'),
    ]);

    const headers = screen.getAllByRole('button', { expanded: true }).map(b => b.textContent);
    expect(headers.indexOf('Needs you')).toBeLessThan(headers.indexOf('Running'));
    expect(groupTitles('Needs you').sort()).toEqual(['asks', 'plan']);
    expect(groupTitles('Running')).toEqual(['busy']);
    expect(screen.queryByRole('button', { name: 'Starred' })).toBeNull();
  });

  it('gives each row a short reason', () => {
    renderSidebar([
      session('q', {}, { questions: 1 }),
      session('a', {}, { approvals: 2 }),
      session('p', {}, { plans: 1 }),
      session('w', { awaiting_input: true }),
      session('r', { review_loop: { id: 'rvl-1', status: 'awaiting_user', iteration: 1, max_iterations: 3 } }),
      session('two', {}, { questions: 2, plans: 3 }),
    ]);

    for (const text of ['Question', '2 approvals', 'Plan to review', 'Waiting for input', 'Review loop', '2 questions · 3 plans to review']) {
      expect(screen.getByText(text)).toBeInTheDocument();
    }
  });

  it('follows the live session fields over the fetched booleans', () => {
    renderSidebar([
      // Answered since the fetch: the session no longer waits.
      session('answered', { awaiting_input: false }, { awaiting_input: true }),
      session('decided', { review_loop: { id: 'rvl-1', status: 'implementing', iteration: 2, max_iterations: 3 } }, { review: true }),
    ]);

    expect(screen.queryByRole('button', { name: 'Needs you' })).toBeNull();
    expect(screen.getByText('Nothing needs you')).toBeInTheDocument();
  });

  it('shows a waiting child at the top level, not inside its parent', () => {
    renderSidebar([
      session('parent'),
      session('child', { parent_session_id: 'parent' }, { questions: 1 }),
    ]);

    expect(groupTitles('Needs you')).toEqual(['child']);
    expect(screen.getByRole('link', { name: /parent/ })).toBeInTheDocument();
  });
});

describe('the summary line', () => {
  it('counts the sessions that need you and the running ones', () => {
    renderSidebar([
      session('q1', {}, { questions: 1 }),
      session('q2', {}, { plans: 1 }),
      session('busy', { is_running: true }),
    ]);
    expect(screen.getByText('2 need you').parentElement).toHaveTextContent('2 need you · 1 running');
  });

  it('uses the singular for one session and leaves out a zero', () => {
    renderSidebar([session('q1', {}, { approvals: 1 }), session('quiet')]);
    expect(screen.getByText('1 needs you').parentElement).toHaveTextContent(/^1 needs you$/);
  });

  it('says so when nothing needs you', () => {
    renderSidebar([session('quiet')]);
    expect(screen.getByText('Nothing needs you')).toBeInTheDocument();
  });

  it('is not shown on an empty list', () => {
    renderSidebar([]);
    expect(screen.queryByText('Nothing needs you')).toBeNull();
    expect(screen.getByText('No conversations yet')).toBeInTheDocument();
  });
});
