import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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
    answerInteraction: vi.fn(),
  },
}));

const { useChatStore } = await import('../../stores/chatStore');
const { ApprovalCard } = await import('./ApprovalCard');

beforeEach(() => {
  useChatStore.setState({ pendingInteraction: null });
});

describe('ApprovalCard', () => {
  it('names a GitHub write and shows its command and reason', () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i1', interactionType: 'command_approval', toolName: 'GitHubWrite',
        toolInput: {
          title: 'Agent wants to write to GitHub',
          item: { command: 'git push seva seva/x' },
          reason: 'push: git push seva seva/x',
          github: true,
        },
        agent: null,
      },
    });
    render(<ApprovalCard />);
    expect(screen.getByText('Agent wants to write to GitHub')).toBeInTheDocument();
    expect(screen.getByText('git push seva seva/x')).toBeInTheDocument();
    expect(screen.getByText('push: git push seva seva/x')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Approve/ })).toBeInTheDocument();
  });

  it('keeps the command title for other approvals', () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i2', interactionType: 'command_approval', toolName: 'Bash',
        toolInput: { item: { command: 'make test' } }, agent: null,
      },
    });
    render(<ApprovalCard />);
    expect(screen.getByText('Agent wants to run a command')).toBeInTheDocument();
  });
});
