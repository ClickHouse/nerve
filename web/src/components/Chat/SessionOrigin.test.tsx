// @vitest-environment jsdom
import { ClickUIProvider } from '@clickhouse/click-ui';
import { fireEvent, render, screen, within } from '@testing-library/react';
import type { ReactNode } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../../api/client';
import type { Session } from '../../types/chat';

vi.mock('../../api/client', () => ({
  api: {
    listActors: vi.fn(async () => ({ actors: [] })),
    listSessions: vi.fn(async () => ({
      sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: 0,
    })),
    listSystemSessions: vi.fn(),
  },
  getToken: vi.fn(() => 'tok'), setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    disconnect: vi.fn(), onMessage: vi.fn(() => () => {}),
  },
}));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { useAuthStore } = await import('../../stores/authStore');
const { useChatStore } = await import('../../stores/chatStore');
const { SessionOriginIcon, SessionOriginLine } = await import('./SessionOrigin');
const { SessionSidebar } = await import('./SessionSidebar');

const ALICE = 'actor-alice';
const BOB = 'actor-bob';

function actor(id: string, display_name: string, kind = 'human'): ActorRef {
  return { id, kind, display_name, username: null } as ActorRef;
}

function session(overrides: Partial<Session>): Session {
  return {
    id: 's1', title: 'A session', source: 'web', updated_at: new Date().toISOString(),
    ...overrides,
  };
}

const slack = session({
  id: 'slack-1', source: 'slack', trigger: 'slack', created_by_actor_id: BOB,
  origin_label: 'Started from Slack #general',
  origin_detail: { channel: 'C1', channel_name: 'general' },
});

function wrap(children: ReactNode) {
  return render(
    <ClickUIProvider theme="dark" persistTheme={false}>
      <MemoryRouter>{children}</MemoryRouter>
    </ClickUIProvider>,
  );
}

function openSession(row: Session) {
  useChatStore.setState({
    activeSession: row.id, sessions: [row], archivedSessions: null, systemSessions: null,
  });
}

beforeEach(() => {
  useAuthStore.setState({
    viewer: actor(ALICE, 'Alice'), account: { id: 'account-alice', username: null },
  } as never);
  useActorStore.setState({
    actors: { [ALICE]: actor(ALICE, 'Alice'), [BOB]: actor(BOB, 'Bob') },
  });
});

describe('SessionOriginIcon', () => {
  it('names the origin of a channel session', () => {
    wrap(<SessionOriginIcon session={slack} />);
    expect(screen.getByRole('img', { name: 'Started from Slack #general' })).toBeTruthy();
  });

  it('keeps a plain web chat quiet', () => {
    wrap(<SessionOriginIcon session={session({
      trigger: 'chat', origin_label: 'Started in the web chat',
    })} />);
    expect(screen.queryByRole('img')).toBeNull();
  });
});

describe('session rows', () => {
  it('lead with the origin icon of each session, also in the System group', async () => {
    const cron = session({
      id: 'cron:nightly:1', title: 'Cron: nightly', source: 'cron', trigger: 'cron',
      origin_label: 'Started by cron job nightly', origin_detail: { cron_job: 'nightly' },
    });
    const web = session({ id: 'web-1', title: 'A chat', trigger: 'chat', origin_label: 'Started in the web chat' });
    (api.listSystemSessions as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      sessions: [cron], has_more: false, next_offset: 1,
    });
    useChatStore.setState({
      sessions: [slack, web], searchResults: null, archivedSessions: null,
      systemSessions: null, systemCount: 1, drafts: {}, reads: {}, readsBaseline: 0,
      activeSession: '',
    });
    wrap(
      <SessionSidebar
        sessions={[slack, web]} activeSession="" agentStatus={{ state: 'idle' }}
        onCreate={() => {}} onDelete={() => {}}
      />,
    );
    expect(screen.getByRole('img', { name: 'Started from Slack #general' })).toBeTruthy();
    expect(screen.queryByRole('img', { name: 'Started in the web chat' })).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: /System \(1\)/ }));
    expect(await screen.findByRole('img', { name: 'Started by cron job nightly' })).toBeTruthy();
  });

  it('keep the origin icon next to the expand control and the review loop icon', () => {
    const parent = { ...slack, id: 'parent-1', title: 'Parent thread' };
    const child = session({ id: 'child-1', title: 'Child', parent_session_id: 'parent-1' });
    const observer = session({
      id: 'loop-1', title: 'Loop observer', trigger: 'api', origin_label: 'Started through the API',
      review_loop: { id: 'rvl-1', status: 'implementing', iteration: 1, max_iterations: 3 },
    });
    useChatStore.setState({
      sessions: [parent, child, observer], searchResults: null, archivedSessions: null,
      systemSessions: null, systemCount: 0, drafts: {}, reads: {}, readsBaseline: 0,
      activeSession: '',
    });
    wrap(
      <SessionSidebar
        sessions={[parent, child, observer]} activeSession="" agentStatus={{ state: 'idle' }}
        onCreate={() => {}} onDelete={() => {}}
      />,
    );
    const parentRow = screen.getByText('Parent thread').closest('a')!;
    expect(within(parentRow).getByTitle(/children/)).toBeTruthy();
    expect(within(parentRow).getByRole('img', { name: 'Started from Slack #general' })).toBeTruthy();
    const loopRow = screen.getByText('Loop observer').closest('a')!;
    expect(within(loopRow).getByRole('img', { name: 'Started through the API' })).toBeTruthy();
  });
});

describe('SessionOriginLine', () => {
  it('says where the open session comes from and who wrote to it', () => {
    openSession(slack);
    wrap(<SessionOriginLine />);
    expect(screen.getByTestId('session-origin').textContent)
      .toBe('Started from Slack #general by Bob');
    expect(screen.queryByRole('link')).toBeNull();
  });

  it('does not name the viewer', () => {
    openSession({ ...slack, created_by_actor_id: ALICE });
    wrap(<SessionOriginLine />);
    expect(screen.getByTestId('session-origin').textContent).toBe('Started from Slack #general');
  });

  it('links a plan and names no approver', () => {
    openSession(session({
      id: 'impl-1', trigger: 'plan', created_by_actor_id: BOB,
      origin_label: 'Started by a plan', origin_detail: { plan_id: 'plan-1', task_id: 't' },
    }));
    wrap(<SessionOriginLine />);
    expect(screen.getByTestId('session-origin').textContent).toBe('Started by a plan·Open plan');
    expect(screen.getByRole('link', { name: 'Open plan' }).getAttribute('href')).toBe('/plans/plan-1');
  });

  it('shows nothing for a plain web chat or a fork', () => {
    openSession(session({ trigger: 'chat', origin_label: 'Started in the web chat' }));
    const { unmount } = wrap(<SessionOriginLine />);
    expect(screen.queryByTestId('session-origin')).toBeNull();
    unmount();

    openSession(session({
      trigger: 'chat', origin_label: 'Forked from another chat', origin_detail: { fork_of: 'p1' },
    }));
    wrap(<SessionOriginLine />);
    expect(screen.queryByTestId('session-origin')).toBeNull();
  });
});
