import { act, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AgentStatus, Session } from '../../types/chat';

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

function running(id: string, backgroundCount: number): Session {
  return {
    id, title: id, source: 'web', updated_at: new Date().toISOString(), starred: false, is_running: true,
    has_background_tasks: backgroundCount > 0, background_count: backgroundCount,
  };
}

function renderSidebar(sessions: Session[], opts: { activeSession?: string; agentStatus?: AgentStatus } = {}) {
  const { activeSession = '', agentStatus = { state: 'idle' } } = opts;
  return render(
    <MemoryRouter>
      <SessionSidebar
        sessions={sessions} activeSession={activeSession} agentStatus={agentStatus}
        onCreate={() => {}} onDelete={() => {}}
      />
    </MemoryRouter>,
  );
}

/** The sidebar row of the session titled `title`. */
function rowOf(title: string): HTMLElement {
  const row = screen.getByText(title).closest('a');
  if (!row) throw new Error(`no row for ${title}`);
  return row;
}

/** The agent number drawn in a row, or null when there is none. */
const agentNumber = (row: HTMLElement) => row.querySelector<HTMLElement>('.tabular-nums');
const titlesIn = (el: HTMLElement) => [...el.querySelectorAll('[title]')].map(e => e.getAttribute('title'));

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

  it('draws the number in violet, from two agents up', () => {
    renderSidebar([parked('pair', 2), parked('lone', 1)]);
    expect(agentNumber(rowOf('pair'))).toHaveTextContent(/^2$/);
    expect(agentNumber(rowOf('pair'))).toHaveClass('text-hue-violet');
    expect(agentNumber(rowOf('lone'))).toBeNull();
  });
});

describe('the agent count of a session whose turn is running', () => {
  it('follows the green dot from one agent up', () => {
    renderSidebar([running('quiet', 0), running('solo', 1), running('trio', 3)]);

    expect(agentNumber(rowOf('quiet'))).toBeNull();
    expect(titlesIn(rowOf('quiet')).filter(t => /running/.test(t ?? ''))).toEqual([]);

    expect(agentNumber(rowOf('solo'))).toHaveTextContent(/^1$/);
    expect(agentNumber(rowOf('solo'))).toHaveClass('text-hue-emerald');
    expect(titlesIn(rowOf('solo'))).toContain('1 agent running');

    expect(agentNumber(rowOf('trio'))).toHaveTextContent(/^3$/);
    expect(agentNumber(rowOf('trio'))).toHaveClass('text-hue-emerald');
    expect(titlesIn(rowOf('trio'))).toContain('3 agents running');
  });

  it('follows the spinner of the open session', () => {
    renderSidebar([running('open', 2)], { activeSession: 'open', agentStatus: { state: 'thinking' } });
    const row = rowOf('open');
    expect(row.querySelector('.animate-spin')).not.toBeNull();
    expect(agentNumber(row)).toHaveTextContent(/^2$/);
    expect(agentNumber(row)).toHaveClass('text-accent');
    expect(titlesIn(row)).toContain('2 agents running');
  });

  it('leaves the spinner alone while the turn runs no agents', () => {
    renderSidebar([running('open', 0)], { activeSession: 'open', agentStatus: { state: 'thinking' } });
    const row = rowOf('open');
    expect(row.querySelector('.animate-spin')).not.toBeNull();
    expect(agentNumber(row)).toBeNull();
  });

  it('gives way to a question that waits for you', () => {
    renderSidebar([{ ...running('asks', 3), awaiting_input: true }]);
    const row = rowOf('asks');
    expect(agentNumber(row)).toBeNull();
    expect(titlesIn(row)).not.toContain('3 agents running');
  });

  it('follows the live session_agents push without a reload', () => {
    function Live() {
      const sessions = useChatStore(s => s.sessions);
      return (
        <SessionSidebar
          sessions={sessions} activeSession="" agentStatus={{ state: 'idle' }}
          onCreate={() => {}} onDelete={() => {}}
        />
      );
    }
    useChatStore.setState({ sessions: [running('live', 0)] });
    render(<MemoryRouter><Live /></MemoryRouter>);
    expect(agentNumber(rowOf('live'))).toBeNull();

    const push = (count: number) => act(() => useChatStore.getState().handleWSMessage({
      type: 'session_agents', session_id: 'live', background_count: count, has_background_tasks: count > 0,
    }));
    push(2);
    expect(agentNumber(rowOf('live'))).toHaveTextContent(/^2$/);
    push(0);
    expect(agentNumber(rowOf('live'))).toBeNull();
  });
});
