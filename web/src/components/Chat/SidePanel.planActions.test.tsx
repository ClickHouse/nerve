import { act, render, screen, within } from '@testing-library/react';
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
const { PlanApprovalBlock } = await import('./tools/PlanApprovalBlock');

const PLAN = '# Plan for org/repo-a\n\n1. Read the open pull requests.\n2. Draft review comments for alice.';

function planTab(extra: Partial<PanelTab> = {}): PanelTab {
  return {
    id: 'plan-1', type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content: PLAN,
    prompt: '', streaming: false, status: 'complete', startedAt: 0, completedAt: 1, blocks: [], ...extra,
  };
}

const sendMessage = vi.fn();

/**
 * The plan tab open in the panel. `waiting`: the agent's turn waits on
 * `ExitPlanMode` (and so is running); `streaming`: a turn runs.
 */
function open({ waiting, streaming = waiting, session = 'session-1', tab = {} }: {
  waiting: boolean; streaming?: boolean; session?: string; tab?: Partial<PanelTab>;
}) {
  useChatStore.setState({
    activeSession: session,
    panels: [planTab(tab)], activePanelId: 'plan-1', panelVisible: true, planEditRequest: null,
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

  it('offers nothing in the onboarding conversation once no plan decision waits', () => {
    open({ waiting: false, session: 'onboarding-1a2b3c4d' });
    expect(screen.getByRole('heading', { name: 'Plan for org/repo-a' })).toBeInTheDocument();
    expect(screen.queryAllByRole('button', { name: /Apply setup|Approve|Change something|Edit/ })).toHaveLength(0);
  });

  it('opens in edit mode from the card\'s Edit, applies the edit, and leaves no pre-edit plan behind', async () => {
    // The panel is closed and holds no plan: it opens on request only.
    useChatStore.setState({
      activeSession: 'onboarding-1a2b3c4d', panels: [], activePanelId: null, panelVisible: false, isStreaming: true,
      pendingInteraction: {
        interactionId: 'i-plan', interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput: { plan: PLAN },
      },
      planEditRequest: null, sendMessage,
    });
    const panel = render(<SidePanel />);
    const card = render(<PlanApprovalBlock block={{
      type: 'tool_call', toolUseId: 'tu-exit', tool: 'ExitPlanMode', input: { plan: PLAN }, status: 'running',
    }} />);
    expect(within(panel.container).queryByRole('textbox')).not.toBeInTheDocument();

    await userEvent.click(within(card.container).getByRole('button', { name: 'Edit' }));
    const inPanel = within(panel.container);
    const editor = inPanel.getByRole('textbox', { name: 'Edit the setup proposal' });
    expect(editor).toHaveValue(PLAN);
    expect(useChatStore.getState()).toMatchObject({ panelVisible: true, planEditRequest: null });
    expect(inPanel.getByRole('button', { name: 'Apply edited plan' })).toBeDisabled();

    await userEvent.type(editor, '\n3. Skip draft pull requests.');
    await userEvent.click(inPanel.getByRole('button', { name: 'Apply edited plan' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith(
      'onboarding-1a2b3c4d', 'i-plan', { plan: `${PLAN}\n3. Skip draft pull requests.` },
    );
    // The CLI writes the edit to disk without a plan update, so the tab
    // would keep the old text: it closes instead.
    expect(useChatStore.getState().panels).toHaveLength(0);
    expect(within(card.container).getByText('Resolving…')).toBeInTheDocument();
  });

  it('takes the card\'s Edit once: back on the tab later, it shows the decision, not the editor', async () => {
    useChatStore.setState({
      activeSession: 'session-1', panels: [], activePanelId: null, panelVisible: false, isStreaming: true,
      pendingInteraction: {
        interactionId: 'i-plan', interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput: { plan: PLAN },
      },
      planEditRequest: null, sendMessage,
    });
    const panel = render(<SidePanel />);
    const card = render(<PlanApprovalBlock block={{
      type: 'tool_call', toolUseId: 'tu-exit', tool: 'ExitPlanMode', input: { plan: PLAN }, status: 'running',
    }} />);
    await userEvent.click(within(card.container).getByRole('button', { name: 'Edit' }));
    await userEvent.click(within(panel.container).getByRole('button', { name: 'Cancel' }));
    // Away from the tab and back: the footer remounts with no editor.
    act(() => useChatStore.setState({ panelVisible: false }));
    await userEvent.click(within(card.container).getByRole('button', { name: 'Read the plan' }));
    expect(within(panel.container).queryByRole('textbox')).not.toBeInTheDocument();
    expect(within(panel.container).getByRole('button', { name: 'Approve' })).toBeInTheDocument();
  });

  it('has no footer on a plan opened to read from a decided card', () => {
    open({ waiting: true, tab: { readOnly: true } });
    expect(screen.getByRole('heading', { name: 'Plan for org/repo-a' })).toBeInTheDocument();
    expect(screen.queryAllByRole('button', { name: /Approve|Request changes|Edit/ })).toHaveLength(0);
  });

  it('never offers "Apply setup" again after the card applied it and the turn ended', async () => {
    const panel = open({ waiting: true, session: 'onboarding-1a2b3c4d' });
    const card = render(<PlanApprovalBlock block={{
      type: 'tool_call', toolUseId: 'tu-exit', tool: 'ExitPlanMode', input: { plan: PLAN }, status: 'running',
    }} />);
    await userEvent.click(within(card.container).getByRole('button', { name: 'Apply setup' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('onboarding-1a2b3c4d', 'i-plan', null);
    // The card's decision closed the plan tab as well.
    expect(useChatStore.getState().panels).toHaveLength(0);

    // The apply turn ends and the plan tab is back (the agent touched the plan
    // file again): still nothing to apply a second time.
    act(() => useChatStore.setState({ isStreaming: false, panels: [planTab()], activePanelId: 'plan-1', panelVisible: true }));
    expect(within(panel.container).getByRole('heading', { name: 'Plan for org/repo-a' })).toBeInTheDocument();
    expect(within(panel.container).queryByRole('button', { name: 'Apply setup' })).not.toBeInTheDocument();
    expect(sendMessage).not.toHaveBeenCalled();
  });
});
