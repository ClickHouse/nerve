import { render, screen } from '@testing-library/react';
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
const { makeState, step, withStep } = await import('../../test/setupFixtures');
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
    expect(line).toHaveTextContent('Started by cron job nightly');
    expect(line.compareDocumentPosition(screen.getByText('tidy up')))
      .toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });
});
