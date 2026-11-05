import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AutoTurnTrigger } from '../../../types/agents';

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
  },
}));

const { useAgentStore } = await import('../../../stores/agentStore');
const { useChatStore } = await import('../../../stores/chatStore');
const { FX_COMPLETED, FX_WAVE, makeAgentStoreSeed } = await import('../../../test/agentFixtures');
const { AutoTurnDivider } = await import('./AutoTurnDivider');

const trigger = (label: string, status: AutoTurnTrigger['status'], agent_id = `ag_${label}`): AutoTurnTrigger => ({
  agent_id, label, kind: 'agent', status,
});

const openAgentTab = vi.fn();

beforeEach(() => {
  openAgentTab.mockClear();
  useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
  useChatStore.setState({ openAgentTab });
});

describe('AutoTurnDivider', () => {
  it('reads "Background continuation" for old markers without triggers', () => {
    render(<AutoTurnDivider block={{ type: 'auto' }} />);
    expect(screen.getByText('Background continuation')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('names the one agent that settled and opens it', async () => {
    render(<AutoTurnDivider block={{ type: 'auto', triggers: [trigger('build-chart', 'completed', FX_COMPLETED.id)] }} />);
    const pill = screen.getByRole('button', { name: /Background · build-chart finished/ });
    await userEvent.click(pill);
    expect(openAgentTab).toHaveBeenCalledWith(FX_COMPLETED.id);
  });

  it('is plain text when the one agent is not loaded', () => {
    render(<AutoTurnDivider block={{ type: 'auto', triggers: [trigger('review-api', 'failed')] }} />);
    expect(screen.getByText('Background · review-api failed')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('counts several settled agents and their failures', () => {
    render(
      <AutoTurnDivider
        block={{ type: 'auto', triggers: [trigger('a', 'completed'), trigger('b', 'failed')], more: 1 }}
      />,
    );
    expect(screen.getByText('Background · 3 agents settled (1 failed)')).toBeInTheDocument();
  });
});
