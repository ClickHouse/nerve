import { act, fireEvent, render, screen } from '@testing-library/react';
import type { ChatMessage } from '../../types/chat';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(),
      listActors: vi.fn(async () => ({ actors: [] })),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllDrafts: vi.fn(),
}));
vi.mock('../../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllReads: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: { sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn() },
}));

const { useSetupStore } = await import('../../stores/setupStore');
const { useChatStore } = await import('../../stores/chatStore');
const { useAgentStore } = await import('../../stores/agentStore');
const { makeState, step, withStep } = await import('../../test/setupFixtures');
const { makeQueueEntry } = await import('../../test/agentFixtures');
const { MessageList } = await import('./MessageList');

function renderEmpty() {
  return render(
    <MemoryRouter>
      <MessageList messages={[]} isStreaming={false} />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  useSetupStore.getState().reset();
  useAgentStore.setState({ queue: [] });
});

describe('empty transcript', () => {
  it('invites a conversation while the setup state is unknown', () => {
    renderEmpty();
    expect(screen.getByText('Start a conversation')).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Finish setup' })).not.toBeInTheDocument();
  });

  it('points at the provider step while no model provider is configured', () => {
    useSetupStore.setState({ state: makeState() });
    renderEmpty();
    const status = screen.getByRole('status');
    expect(status).toHaveTextContent('Connect a model to start a conversation');
    expect(screen.getByRole('link', { name: 'Finish setup' })).toHaveAttribute('href', '/setup/provider');
    expect(screen.queryByText('Start a conversation')).not.toBeInTheDocument();
  });

  it('gives the reason and no link when a lockdown closed the Model step', () => {
    const reason = 'Configuration is managed from the config repository on this instance; changes arrive by PR and sync. Configure this step from the config repository.';
    useSetupStore.setState({
      state: withStep(makeState({ lockdown: true, writable: false }), step('provider', {
        status: 'blocked', blocked_kind: 'lockdown', blocked_reason: reason, read_only: true, read_only_reason: reason,
      })),
    });
    renderEmpty();
    const status = screen.getByRole('status');
    expect(status).toHaveTextContent('No model is connected');
    expect(status).toHaveTextContent(reason);
    expect(screen.queryByRole('link', { name: 'Finish setup' })).not.toBeInTheDocument();
    expect(screen.queryByText('Connect a model to start a conversation')).not.toBeInTheDocument();
  });

  it('goes back to the default once a provider is configured', () => {
    useSetupStore.setState({ state: withStep(makeState(), step('provider', { status: 'done' })) });
    renderEmpty();
    expect(screen.getByText('Start a conversation')).toBeInTheDocument();
  });
});

describe('origin line', () => {
  it('leads the transcript of a session that a cron job started', () => {
    useChatStore.setState({
      activeSession: 'cron:nightly:1', archivedSessions: null, systemSessions: null,
      sessions: [{
        id: 'cron:nightly:1', title: 'Cron: nightly', source: 'cron', updated_at: '2026-01-01T00:00:00Z',
        trigger: 'cron', origin_label: 'Started by cron job nightly', origin_detail: { cron_job: 'nightly' },
      }],
    });
    render(
      <MemoryRouter>
        <MessageList
          messages={[{ role: 'user', blocks: [{ type: 'text', content: 'tidy up' }] }]}
          isStreaming={false}
        />
      </MemoryRouter>,
    );
    const line = screen.getByTestId('session-origin');
    expect(line).toHaveTextContent('From cron job nightly');
    expect(line.compareDocumentPosition(screen.getByText('tidy up')))
      .toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  it('uses the row that the page read for a session that no list has', () => {
    useChatStore.setState({
      activeSession: 'cron:nightly:2', sessions: [], archivedSessions: null, systemSessions: null,
    });
    render(
      <MemoryRouter>
        <MessageList
          messages={[{ role: 'user', blocks: [{ type: 'text', content: 'tidy up' }] }]}
          isStreaming={false}
          unlistedSession={{
            id: 'cron:nightly:2', title: 'Cron: nightly', source: 'cron',
            updated_at: '2026-01-01T00:00:00Z', trigger: 'cron',
            origin_label: 'Started by cron job nightly', origin_detail: { cron_job: 'nightly' },
          }}
        />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('session-origin')).toHaveTextContent('From cron job nightly');
  });
});

describe('a message waiting in the turn queue', () => {
  const bubble = (content: string, client_msg_id: string | null) => ({
    role: 'user' as const, blocks: [{ type: 'text' as const, content }],
    created_at: '2026-02-27T02:06:00Z', client_msg_id,
  });

  function renderList(messages: ReturnType<typeof bubble>[]) {
    return render(
      <MemoryRouter>
        <MessageList messages={messages} isStreaming />
      </MemoryRouter>,
    );
  }

  it('has no bubble while it waits (another tab sent it), and one when it starts', () => {
    useAgentStore.setState({ queue: [makeQueueEntry({ client_msg_id: 'c-other-tab' })] });
    renderList([bubble('earlier message', null), bubble('from the other tab', 'c-other-tab')]);
    expect(screen.getByText('earlier message')).toBeInTheDocument();
    expect(screen.queryByText('from the other tab')).toBeNull();

    act(() => useAgentStore.setState({ queue: [] }));
    expect(screen.getByText('from the other tab')).toBeInTheDocument();
  });

  it('always shows history, which has no client id', () => {
    useAgentStore.setState({ queue: [makeQueueEntry({ client_msg_id: null })] });
    renderList([bubble('stored message', null)]);
    expect(screen.getByText('stored message')).toBeInTheDocument();
  });
});

describe('long history rendering', () => {
  it('mounts a bounded recent window and reveals earlier messages on request', () => {
    const messages: ChatMessage[] = Array.from({ length: 35 }, (_, i) => ({
      id: i + 1,
      role: 'user',
      blocks: [{ type: 'text', content: `Test turn ${i}` }],
    }));
    const view = (history: ChatMessage[]) => (
      <MemoryRouter>
        <MessageList messages={history} isStreaming={false} />
      </MemoryRouter>
    );
    const { rerender } = render(view(messages));

    expect(screen.queryByText('Test turn 0')).not.toBeInTheDocument();
    expect(screen.getByText('Test turn 5')).toBeInTheDocument();
    expect(screen.getByText('Test turn 34')).toBeInTheDocument();

    // A new turn must not evict an existing row while the reader is scrolled up.
    const appended: ChatMessage[] = [
      ...messages, { id: 36, role: 'user', blocks: [{ type: 'text', content: 'Test turn 35' }] },
    ];
    rerender(view(appended));
    expect(screen.getByText('Test turn 5')).toBeInTheDocument();
    expect(screen.getByText('Test turn 35')).toBeInTheDocument();
    expect(screen.queryByText('Test turn 4')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Show earlier messages (5 loaded)' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Show earlier messages (5 loaded)' }));
    expect(screen.getByText('Test turn 0')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Show earlier messages/ })).not.toBeInTheDocument();

    // Revealed history stays available as more turns arrive too.
    rerender(view([
      ...appended, { id: 37, role: 'user', blocks: [{ type: 'text', content: 'Test turn 36' }] },
    ]));
    expect(screen.getByText('Test turn 0')).toBeInTheDocument();
    expect(screen.getByText('Test turn 36')).toBeInTheDocument();
  });
});
