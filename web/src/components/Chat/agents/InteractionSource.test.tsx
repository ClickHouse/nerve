import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    answerInteraction: vi.fn(),
  },
}));

const { useChatStore } = await import('../../../stores/chatStore');
const { FX_RUNNING } = await import('../../../test/agentFixtures');
const { ApprovalCard } = await import('../ApprovalCard');
const { InteractiveQuestionCard } = await import('../InteractiveQuestionCard');

const openAgentTab = vi.fn();
const fromAgent = { agent_id: FX_RUNNING.id, task_id: 'a1b2c3d4e5f6a7b8c', label: 'build-api' };

beforeEach(() => {
  openAgentTab.mockClear();
  useChatStore.setState({ openAgentTab, pendingInteraction: null });
});

describe('interaction cards raised by a sub-agent', () => {
  it('name the agent on an approval and open it', async () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i1', interactionType: 'command_approval', toolName: 'Bash',
        toolInput: { item: { command: ['make', 'test'] } }, agent: fromAgent,
      },
    });
    render(<ApprovalCard />);
    expect(screen.getByText('build-api')).toBeInTheDocument();
    expect(screen.getByText(/from/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open build-api' }));
    expect(openAgentTab).toHaveBeenCalledWith(FX_RUNNING.id);
  });

  it('say nothing extra for the orchestrator\'s own approvals', () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i1', interactionType: 'command_approval', toolName: 'Bash',
        toolInput: { item: { command: 'make test' } }, agent: null,
      },
    });
    render(<ApprovalCard />);
    expect(screen.queryByText(/from/)).toBeNull();
  });

  it('point at an agent whose own question waits in its transcript', () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i2', interactionType: 'question', toolName: 'AskUserQuestion',
        toolInput: { questions: [{ question: 'Which branch?', options: [{ label: 'main' }] }] }, agent: fromAgent,
      },
    });
    render(<InteractiveQuestionCard />);
    expect(screen.getByRole('status')).toHaveTextContent('Waiting for your answer');
    expect(screen.getByRole('button', { name: 'Open build-api' })).toBeInTheDocument();
  });

  it('offer no Open without an agent id', () => {
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i3', interactionType: 'question', toolName: 'request_user_input',
        toolInput: { outOfBand: true, message: 'Pick one', questions: [] },
        agent: { agent_id: null, task_id: 't1', label: 'worker' },
      },
    });
    render(<InteractiveQuestionCard />);
    expect(screen.getByText('worker')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Open worker' })).toBeNull();
  });
});
