import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PanelTab, ToolCallBlockData } from '../../../types/chat';

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

const { ws } = await import('../../../api/websocket');
const { useChatStore } = await import('../../../stores/chatStore');
const { PlanApprovalBlock } = await import('./PlanApprovalBlock');

const PLAN = [
  '# Setup proposal — review pull requests in org/repo-a',
  '',
  '## Jobs',
  '',
  '- `pr-review`: every 30 minutes, drafts review comments for alice to approve',
].join('\n');
const HEADING = 'Setup proposal — review pull requests in org/repo-a';
const APPROVED_RESULT = 'User has approved your plan. You can now start coding.';

function call(tool: string, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return { type: 'tool_call', toolUseId: `tu-${tool}`, tool, input: {}, status: 'running', ...overrides };
}

/** The agent waits on its plan; the pause carries `toolInput`. */
function pause(toolInput: Record<string, unknown> = { plan: PLAN }) {
  useChatStore.setState({
    pendingInteraction: { interactionId: 'i-plan', interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput },
  });
}

function planTab(content: string | null): PanelTab {
  return {
    id: 'plan-1', type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content,
    prompt: '', streaming: false, status: 'complete', startedAt: 0, completedAt: 1, blocks: [],
  };
}

beforeEach(() => {
  vi.mocked(ws.answerInteraction).mockClear();
  useChatStore.setState({
    activeSession: 'session-1', pendingInteraction: null, panels: [], activePanelId: null, panelVisible: false,
    isStreaming: false, streamingBlocks: [],
  });
});

describe('EnterPlanMode', () => {
  it('is a status line without buttons: entering plan mode while it runs, entered once done', () => {
    const view = render(<PlanApprovalBlock block={call('EnterPlanMode')} />);
    expect(screen.getByText('Entering plan mode…')).toBeInTheDocument();
    expect(screen.queryAllByRole('button')).toHaveLength(0);

    view.rerender(<PlanApprovalBlock block={call('EnterPlanMode', { status: 'complete', result: 'Entered plan mode.' })} />);
    expect(screen.getByText('Entered plan mode')).toBeInTheDocument();
    expect(screen.queryByText(/Planned/)).not.toBeInTheDocument();
    expect(screen.queryAllByRole('button')).toHaveLength(0);
  });

  it('says so when plan mode was refused', () => {
    render(<PlanApprovalBlock block={call('EnterPlanMode', {
      status: 'complete', isError: true, result: 'Plan mode is not available in non-web sessions.',
    })} />);
    expect(screen.getByText('Plan mode declined')).toBeInTheDocument();
  });

  it('lets an older server that still pauses here go on with one click', async () => {
    useChatStore.setState({
      pendingInteraction: { interactionId: 'i-enter', interactionType: 'plan_enter', toolName: 'EnterPlanMode', toolInput: {} },
    });
    render(<PlanApprovalBlock block={call('EnterPlanMode')} />);
    expect(screen.getByText('Waiting to start planning')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-enter', null);
  });
});

describe('ExitPlanMode asks for a decision', () => {
  it('in an ordinary session: Approve or Request changes', () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Plan ready for your decision')).toBeInTheDocument();
    expect(screen.getByText('Read it below or in the panel. Approve it, or say what should change.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Request changes…' })).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('button', { name: 'Apply setup' })).not.toBeInTheDocument();
  });

  it('in the onboarding conversation: a setup proposal to apply or change', () => {
    useChatStore.setState({ activeSession: 'onboarding-1a2b3c4d' });
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Setup proposal')).toBeInTheDocument();
    expect(screen.getByText('Read it below or in the panel. Apply it, or say what should change.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Apply setup' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Change something…' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    expect(screen.queryByText('Plan ready for your decision')).not.toBeInTheDocument();
  });

  it('approves, then says Approved', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(ws.answerInteraction).toHaveBeenCalledTimes(1);
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', null);
    expect(screen.getByText('Approved')).toBeInTheDocument();
    expect(screen.queryAllByRole('button')).toHaveLength(0);
  });

  it('sends a change request only with a reason, and shows its first line', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Request changes…' }));
    expect(screen.getByRole('button', { name: 'Request changes…' })).toHaveAttribute('aria-expanded', 'true');

    const field = screen.getByRole('textbox', { name: 'What should change?' });
    expect(field).toHaveFocus();
    expect(field).toBeRequired();
    const send = screen.getByRole('button', { name: 'Send' });
    expect(send).toBeDisabled();
    await userEvent.type(field, '   ');
    expect(send).toBeDisabled();
    await userEvent.click(send);
    expect(ws.answerInteraction).not.toHaveBeenCalled();

    await userEvent.clear(field);
    await userEvent.type(field, 'Run the review hourly{Shift>}{Enter}{/Shift}and skip drafts');
    expect(send).toBeEnabled();
    await userEvent.click(send);
    expect(ws.answerInteraction).toHaveBeenCalledTimes(1);
    expect(ws.answerInteraction).toHaveBeenCalledWith(
      'session-1', 'i-plan', null, true, 'Run the review hourly\nand skip drafts',
    );
    expect(screen.getByText('Changes requested — Run the review hourly')).toBeInTheDocument();
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  });

  it.each(['Control', 'Meta'])('sends with %s+Enter; Enter alone starts a new line', async (modifier) => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Request changes…' }));
    const field = screen.getByRole('textbox', { name: 'What should change?' });
    await userEvent.type(field, 'Hourly{Enter}please');
    expect(field).toHaveValue('Hourly\nplease');
    expect(ws.answerInteraction).not.toHaveBeenCalled();

    await userEvent.keyboard(`{${modifier}>}{Enter}{/${modifier}}`);
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', null, true, 'Hourly\nplease');
  });

  it('closes the field with Escape, keeps the text, and does not stop the waiting turn', async () => {
    // The app-wide Escape (a document listener) stops a running turn; the
    // turn is running while it waits for this decision.
    const onDocumentKey = vi.fn();
    document.addEventListener('keydown', onDocumentKey);
    try {
      pause();
      render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
      const toggle = screen.getByRole('button', { name: 'Request changes…' });
      await userEvent.click(toggle);
      await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'Hourly');
      await userEvent.keyboard('{Escape}');

      expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
      expect(toggle).toHaveFocus();
      expect(toggle).toHaveAttribute('aria-expanded', 'false');
      expect(onDocumentKey.mock.calls.some(([e]) => (e as KeyboardEvent).key === 'Escape')).toBe(false);
      expect(ws.answerInteraction).not.toHaveBeenCalled();

      await userEvent.click(toggle);
      expect(screen.getByRole('textbox', { name: 'What should change?' })).toHaveValue('Hourly');
    } finally {
      document.removeEventListener('keydown', onDocumentKey);
    }
  });
});

describe('ExitPlanMode shows the plan in the card', () => {
  it('from the pause, as markdown, in a box with its own scroll that expands', async () => {
    pause({ plan: PLAN });
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    const box = screen.getByRole('region', { name: 'Plan' });
    expect(within(box).getByRole('heading', { level: 1, name: HEADING })).toBeInTheDocument();
    expect(within(box).getByRole('heading', { level: 2, name: 'Jobs' })).toBeInTheDocument();
    // The height cap is a layout fact only the class can express (jsdom has no layout).
    expect(box.className).toMatch(/\bmax-h-96\b/);
    expect(box.className).toMatch(/\boverflow-y-auto\b/);

    const toggle = screen.getByRole('button', { name: 'Show all' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(toggle).toHaveAttribute('aria-controls', box.id);
    await userEvent.click(toggle);
    expect(screen.getByRole('button', { name: 'Show less' })).toHaveAttribute('aria-expanded', 'true');
    expect(box.className).not.toMatch(/\bmax-h-96\b/);
  });

  it('names the box after the setup proposal in the onboarding conversation', () => {
    useChatStore.setState({ activeSession: 'onboarding-1a2b3c4d' });
    pause({ plan: PLAN });
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(within(screen.getByRole('region', { name: 'Setup proposal' }))
      .getByRole('heading', { level: 1, name: HEADING })).toBeInTheDocument();
  });

  it('from the plan panel when the pause carries no plan text', () => {
    useChatStore.setState({ panels: [planTab(PLAN)] });
    pause({ plan: '  ' });
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(within(screen.getByRole('region', { name: 'Plan' }))
      .getByRole('heading', { level: 1, name: HEADING })).toBeInTheDocument();
  });

  it('points to the side panel when there is no plan text to show', async () => {
    useChatStore.setState({ panels: [planTab(null)] });
    pause({});
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.queryByRole('region')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open the side panel to read the plan' }));
    expect(useChatStore.getState()).toMatchObject({ activePanelId: 'plan-1', panelVisible: true });
    view.unmount();

    useChatStore.setState({ panels: [] });
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Open the side panel to read the plan.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /side panel/ })).not.toBeInTheDocument();
  });
});

describe('ExitPlanMode after the decision', () => {
  it('waits for the pause before offering anything', () => {
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Waiting for the plan…')).toBeInTheDocument();
    expect(screen.queryAllByRole('button')).toHaveLength(0);
  });

  it('settles on Resolving… when another client decides, then on what was decided', () => {
    pause();
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    act(() => useChatStore.setState({ pendingInteraction: null }));
    expect(screen.getByText('Resolving…')).toBeInTheDocument();
    expect(screen.queryAllByRole('button')).toHaveLength(0);

    view.rerender(<PlanApprovalBlock block={call('ExitPlanMode', { status: 'complete', result: APPROVED_RESULT })} />);
    expect(screen.getByText('Approved')).toBeInTheDocument();
  });

  it('reads a change request back from the tool result, as after a reload', () => {
    render(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', isError: true, result: '\nRun the review hourly\nand skip drafts',
    })} />);
    expect(screen.getByText('Changes requested — Run the review hourly')).toBeInTheDocument();
  });

  it('tells an old fixed decline and a pause that ended without a decision from feedback', () => {
    const cases: [Partial<ToolCallBlockData>, string][] = [
      [{ isError: true, result: 'User declined the plan.' }, 'Declined'],
      [{ isError: true, result: 'Session stopped by user.' }, 'Closed — Session stopped by user.'],
      [{ isError: true, result: 'No response received after 1 hour — timed out.' }, 'Closed — No response received after 1 hour — timed out.'],
      [{}, 'Closed'],
    ];
    for (const [overrides, text] of cases) {
      const view = render(<PlanApprovalBlock block={call('ExitPlanMode', { status: 'complete', ...overrides })} />);
      expect(screen.getByText(text)).toBeInTheDocument();
      view.unmount();
    }
  });

  it('keeps what this client decided when the tool result lands', async () => {
    pause();
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Request changes…' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'Use the alice account');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    view.rerender(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', isError: true, result: 'Use the alice account',
    })} />);
    expect(screen.getByText('Changes requested — Use the alice account')).toBeInTheDocument();
  });
});

describe('ExitPlanMode: edit, then approve', () => {
  const EDITED = PLAN.replace('every 30 minutes', 'every hour');

  it('turns the plan into a text field and applies the edited text', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));

    const editor = screen.getByRole('textbox', { name: 'Edit the plan' });
    expect(editor).toHaveValue(PLAN);
    expect(editor).toHaveFocus();
    expect(screen.queryByRole('region', { name: 'Plan' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Request changes…' })).not.toBeInTheDocument();
    const apply = screen.getByRole('button', { name: 'Apply edited plan' });
    expect(apply).toBeDisabled();

    await userEvent.clear(editor);
    await userEvent.type(editor, EDITED.replace(/[{[]/g, '$&$&'));
    expect(apply).toBeEnabled();
    await userEvent.click(apply);
    expect(ws.answerInteraction).toHaveBeenCalledTimes(1);
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', { plan: EDITED });
    expect(screen.getByText('Approved with your edits')).toBeInTheDocument();
  });

  it('applies only a text that differs from the plan and is not empty', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const editor = screen.getByRole('textbox', { name: 'Edit the plan' });
    const apply = screen.getByRole('button', { name: 'Apply edited plan' });

    await userEvent.type(editor, '\n\n');
    expect(apply).toBeDisabled();
    await userEvent.clear(editor);
    expect(apply).toBeDisabled();
    await userEvent.type(editor, '   ');
    expect(apply).toBeDisabled();
    await userEvent.click(apply);
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('drops the edits on Cancel and shows the plan again', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'Edit the plan' }), ' and more');
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(screen.getByRole('region', { name: 'Plan' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByRole('textbox', { name: 'Edit the plan' })).toHaveValue(PLAN);
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('applies with Cmd/Ctrl+Enter', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'Edit the plan' }), '\n- also skip drafts');
    await userEvent.keyboard('{Meta>}{Enter}{/Meta}');
    expect(ws.answerInteraction).toHaveBeenCalledWith('session-1', 'i-plan', { plan: `${PLAN}\n- also skip drafts` });
  });

  it('cancels with Escape only while nothing changed, and never stops the waiting turn', async () => {
    const onDocumentKey = vi.fn();
    document.addEventListener('keydown', onDocumentKey);
    try {
      pause();
      render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
      await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
      await userEvent.keyboard('{Escape}');
      expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
      expect(screen.getByRole('region', { name: 'Plan' })).toBeInTheDocument();

      await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
      await userEvent.type(screen.getByRole('textbox', { name: 'Edit the plan' }), ' more');
      await userEvent.keyboard('{Escape}');
      expect(screen.getByRole('textbox', { name: 'Edit the plan' })).toHaveValue(`${PLAN} more`);
      expect(onDocumentKey.mock.calls.some(([e]) => (e as KeyboardEvent).key === 'Escape')).toBe(false);
    } finally {
      document.removeEventListener('keydown', onDocumentKey);
    }
  });

  it('names the field after the setup proposal in the onboarding conversation', async () => {
    useChatStore.setState({ activeSession: 'onboarding-1a2b3c4d' });
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByRole('textbox', { name: 'Edit the setup proposal' })).toHaveValue(PLAN);
    expect(screen.getByRole('button', { name: 'Apply edited plan' })).toBeDisabled();
  });

  it('offers no Edit when the plan text did not come with the pause', () => {
    useChatStore.setState({ panels: [planTab(PLAN)] });
    pause({});
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByRole('region', { name: 'Plan' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
  });

  it('reads an edited approval back from the tool result, by its heading', () => {
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', result: `${APPROVED_RESULT}\n\n## Approved Plan (edited by user):\n${PLAN}`,
    })} />);
    expect(screen.getByText('Approved with your edits')).toBeInTheDocument();
    view.unmount();

    // A plan that only mentions the words is a plain approval.
    render(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', result: `${APPROVED_RESULT}\n\n## Approved Plan:\n- keep notes (edited by user): yes`,
    })} />);
    expect(screen.getByText('Approved')).toBeInTheDocument();
  });
});

describe('ExitPlanMode while the approved work runs', () => {
  /** The call belongs to the turn that is still streaming. */
  function inRunningTurn(block: ToolCallBlockData) {
    useChatStore.setState({ isStreaming: true, streamingBlocks: [block] });
  }

  it('reads "Approved — applying…" until its turn ends, then "Approved"', async () => {
    const block = call('ExitPlanMode');
    inRunningTurn(block);
    pause();
    render(<PlanApprovalBlock block={block} />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(screen.getByText('Approved — applying…')).toBeInTheDocument();

    act(() => useChatStore.setState({ isStreaming: false, streamingBlocks: [] }));
    expect(screen.getByText('Approved')).toBeInTheDocument();
    expect(screen.queryByText(/applying/)).not.toBeInTheDocument();
  });

  it('reads "Applying the setup…" in the onboarding conversation, with or without edits', async () => {
    useChatStore.setState({ activeSession: 'onboarding-1a2b3c4d' });
    const block = call('ExitPlanMode');
    inRunningTurn(block);
    pause();
    const view = render(<PlanApprovalBlock block={block} />);
    await userEvent.click(screen.getByRole('button', { name: 'Apply setup' }));
    expect(screen.getByText('Applying the setup…')).toBeInTheDocument();
    view.unmount();

    const edited = call('ExitPlanMode', {
      status: 'complete', result: `${APPROVED_RESULT}\n## Approved Plan (edited by user):\n${PLAN}`,
    });
    inRunningTurn(edited);
    render(<PlanApprovalBlock block={edited} />);
    expect(screen.getByText('Applying the setup…')).toBeInTheDocument();
  });

  it('says the edits were applied while the work runs', () => {
    const block = call('ExitPlanMode', {
      status: 'complete', result: `${APPROVED_RESULT}\n## Approved Plan (edited by user):\n${PLAN}`,
    });
    inRunningTurn(block);
    render(<PlanApprovalBlock block={block} />);
    expect(screen.getByText('Approved with your edits — applying…')).toBeInTheDocument();
  });

  it('stays "Approved" for a call from an earlier turn while a new one runs', () => {
    useChatStore.setState({ isStreaming: true, streamingBlocks: [call('Bash', { toolUseId: 'tu-other' })] });
    render(<PlanApprovalBlock block={call('ExitPlanMode', { status: 'complete', result: APPROVED_RESULT })} />);
    expect(screen.getByText('Approved')).toBeInTheDocument();
  });

  it('shows no applying state for a change request', () => {
    const block = call('ExitPlanMode', { status: 'complete', isError: true, result: 'Run it hourly' });
    inRunningTurn(block);
    render(<PlanApprovalBlock block={block} />);
    expect(screen.getByText('Changes requested — Run it hourly')).toBeInTheDocument();
  });
});

describe('ExitPlanMode after a change request', () => {
  it('gives the revised plan its own card, while the earlier one keeps its outcome', () => {
    const REVISED = '# Setup proposal — review pull requests in org/repo-a, hourly\n\n## Jobs\n\n- `pr-review`: every hour';
    const first = call('ExitPlanMode', {
      toolUseId: 'tu-first', status: 'complete', isError: true, result: 'Run the review hourly',
      input: { plan: PLAN },
    });
    const second = call('ExitPlanMode', { toolUseId: 'tu-second', input: { plan: REVISED } });
    useChatStore.setState({
      pendingInteraction: {
        interactionId: 'i-plan-2', interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput: { plan: REVISED },
      },
    });
    render(<><PlanApprovalBlock block={first} /><PlanApprovalBlock block={second} /></>);

    expect(screen.getByText('Changes requested — Run the review hourly')).toBeInTheDocument();
    const box = screen.getByRole('region', { name: 'Plan' });
    expect(within(box).getByRole('heading', { level: 1, name: /hourly$/ })).toBeInTheDocument();
    expect(screen.getAllByRole('region')).toHaveLength(1);
  });
});
