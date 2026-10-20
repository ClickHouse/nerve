import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Session } from '../../types/chat';

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

function parked(id: string, backgroundCount: number): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(), starred: true, is_running: false,
    has_background_tasks: backgroundCount > 0, background_count: backgroundCount,
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

beforeEach(() => {
  useChatStore.setState({
    sessions: [], searchResults: null, archivedSessions: null, systemSessions: null,
    archivedCount: 0, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
    virtualSession: null, activeSession: '',
  });
});

describe('the parked dot of a session whose agents run between turns', () => {
  it('says how many agents run and shows the count from two up', () => {
    const { container } = renderSidebar([parked('wave', 5), parked('single', 1)]);
    const titles = [...container.querySelectorAll('[title]')].map(el => el.getAttribute('title'));
    expect(titles).toContain('5 agents running');
    expect(titles).toContain('1 agent running');
    expect(screen.getByText('5')).toHaveClass('tabular-nums');
    expect(screen.queryByText('1', { selector: '.tabular-nums' })).toBeNull();
  });

  it('pings only when motion is welcome', () => {
    const { container } = renderSidebar([parked('wave', 3)]);
    const pings = [...container.querySelectorAll('[class*="animate-ping"]')];
    expect(pings.length).toBeGreaterThan(0);
    for (const ping of pings) expect(ping.className).toMatch(/(^|\s)motion-safe:animate-ping(\s|$)/);
  });

  it('keeps the old wording for background work without agent records', () => {
    const { container } = renderSidebar([{ ...parked('legacy', 0), has_background_tasks: true }]);
    const titles = [...container.querySelectorAll('[title]')].map(el => el.getAttribute('title'));
    expect(titles).toContain('Background job running');
  });
});
