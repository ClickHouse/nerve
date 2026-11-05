import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '../../types/chat';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { useAgentStore } = await import('../../stores/agentStore');
const { FX_WAVE, makeAgentStoreSeed, makeQueueEntry } = await import('../../test/agentFixtures');
const { UserMessage } = await import('./UserMessage');

const message = (client_msg_id: string | null): ChatMessage => ({
  role: 'user', blocks: [{ type: 'text', content: 'also run the e2e suite' }],
  created_at: '2026-02-27T02:06:00Z', client_msg_id,
});

beforeEach(() => {
  useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
});

describe('UserMessage', () => {
  it('marks a bubble whose message waits in the turn queue', () => {
    const entry = makeQueueEntry();
    useAgentStore.setState({ queue: [entry] });
    render(<UserMessage message={message(entry.client_msg_id)} />);
    expect(screen.getByText('Queued')).toBeInTheDocument();
  });

  it('drops the mark once the message left the queue', () => {
    useAgentStore.setState({ queue: [makeQueueEntry()] });
    render(<UserMessage message={message('another-id')} />);
    expect(screen.queryByText('Queued')).toBeNull();
    expect(screen.getByText('also run the e2e suite')).toBeInTheDocument();
  });

  it('never marks history, which has no client id', () => {
    useAgentStore.setState({ queue: [makeQueueEntry({ client_msg_id: null })] });
    render(<UserMessage message={message(null)} />);
    expect(screen.queryByText('Queued')).toBeNull();
  });
});
