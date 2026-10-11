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
const { makeState, step, withStep } = await import('../../test/setupFixtures');
const { MessageList } = await import('./MessageList');

function renderEmpty() {
  return render(
    <MemoryRouter>
      <MessageList messages={[]} streamingBlocks={[]} isStreaming={false} />
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

  it('goes back to the default once a provider is configured', () => {
    useSetupStore.setState({ state: withStep(makeState(), step('provider', { status: 'done' })) });
    renderEmpty();
    expect(screen.getByText('Start a conversation')).toBeInTheDocument();
  });
});
