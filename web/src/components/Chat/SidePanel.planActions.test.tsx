import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PanelTab } from '../../types/chat';

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

const { ws } = await import('../../api/websocket');
const { useChatStore } = await import('../../stores/chatStore');
const { SidePanel } = await import('./SidePanel');

const PLAN = '# Plan for org/repo-a\n\n1. Read the open pull requests.\n2. Draft review comments for alice.';

function planTab(): PanelTab {
  return {
    id: 'plan-1', type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content: PLAN,
    prompt: '', streaming: false, status: 'complete', startedAt: 0, completedAt: 1, blocks: [],
  };
}

const sendMessage = vi.fn();

/**
 * The plan tab open in the panel. `waiting`: the agent's turn waits on
 * `ExitPlanMode` (and so is running); `streaming`: a turn runs.
 */
function open({ waiting, streaming = waiting, session = 'session-1' }: {
  waiting: boolean; streaming?: boolean; session?: string;
}) {
  useChatStore.setState({
    activeSession: session,
    panels: [planTab()], activePanelId: 'plan-1', panelVisible: true,
    isStreaming: streaming,
    pendingInteraction: waiting
      ? { interactionId: 'i-plan', interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput: { plan: PLAN } }
      : null,
    sendMessage,
  });
  return render(<SidePanel />);
}

beforeEach(() => {
  vi.mocked(ws.answerInteraction).mockClear();
  sendMessage.mockClear();
});

describe("The plan panel's footer", () => {
  it('offers the same decision as the card while the agent waits, and Approve closes the tab', async () => {
    open({ waiting: true });
    expect(screen.getByRole('button', { name: 'Request changes…' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', null);
    expect(sendMessage).not.toHaveBeenCalled();
    expect(useChatStore.getState().panels).toHaveLength(0);
  });

  it('sends a change request with the reason and keeps the tab open for the revised plan', async () => {
    open({ waiting: true });
    await userEvent.click(screen.getByRole('button', { name: 'Request changes…' }));
    const field = screen.getByRole('textbox', { name: 'What should change?' });
    expect(field).toHaveFocus();
    expect(screen.getByRole('button', { name: 'Send' })).toBeDisabled();
    await userEvent.type(field, 'Skip draft pull requests');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));

    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', null, true, 'Skip draft pull requests');
    expect(useChatStore.getState().panels.map(p => p.id)).toEqual(['plan-1']);
    // The decision is made: no buttons until the agent proposes again.
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Request changes…' })).not.toBeInTheDocument();
  });

  it('uses the onboarding words in the onboarding conversation', () => {
    open({ waiting: true, session: 'onboarding-1a2b3c4d' });
    expect(screen.getByRole('button', { name: 'Apply setup' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Change something…' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
  });

  it('approves by message when no plan decision waits, with nothing to turn down', async () => {
    open({ waiting: false });
    expect(screen.queryByRole('button', { name: 'Request changes…' })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(sendMessage).toHaveBeenCalledWith('Looks good — implement it.');
    expect(ws.answerInteraction).not.toHaveBeenCalled();
    expect(useChatStore.getState().panels).toHaveLength(0);
  });

  it('offers nothing while a turn runs that does not wait on the plan', () => {
    open({ waiting: false, streaming: true });
    expect(screen.getByRole('heading', { name: 'Plan for org/repo-a' })).toBeInTheDocument();
    expect(screen.queryAllByRole('button', { name: /Approve|Request changes/ })).toHaveLength(0);
  });

  it('edits the plan the agent waits on, applies it, and closes the tab', async () => {
    open({ waiting: true });
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const editor = screen.getByRole('textbox', { name: 'Edit the plan' });
    expect(editor).toHaveValue(PLAN);
    expect(editor).toHaveFocus();
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    const apply = screen.getByRole('button', { name: 'Apply edited plan' });
    expect(apply).toBeDisabled();

    await userEvent.type(editor, '\n3. Skip draft pull requests.');
    await userEvent.click(apply);
    expect(ws.answerInteraction).toHaveBeenCalledWith(
      'session-1', 'i-plan', { plan: `${PLAN}\n3. Skip draft pull requests.` },
    );
    expect(useChatStore.getState().panels).toHaveLength(0);
  });

  it('goes back to the decision on Cancel', async () => {
    open({ waiting: true, session: 'onboarding-1a2b3c4d' });
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByRole('textbox', { name: 'Edit the setup proposal' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Apply setup' })).toBeInTheDocument();
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('offers no Edit when no plan decision waits', () => {
    open({ waiting: false });
    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
  });
});
