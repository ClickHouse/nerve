import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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
  '## What I understood',
  '',
  '- Review new pull requests in org/repo-a and say which need a human.',
  '- Act as `alice`; every public comment is a draft for you to approve.',
  '- Scan every 30 minutes.',
].join('\n');
const APPROVED_RESULT = 'User has approved your plan. You can now start coding.';
const BULLETS = [
  'Reviews new pull requests in `org/repo-a` every 30 minutes.',
  'Drafts every public comment for you to approve; reads and clones on its own.',
  'Needs a token with read access to org/repo-a.',
];

function call(tool: string, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return { type: 'tool_call', toolUseId: `tu-${tool}`, tool, input: {}, status: 'running', ...overrides };
}

/** The agent waits on its plan; the pause carries `toolInput`. */
function pause(toolInput: Record<string, unknown> = { plan: PLAN }, interactionId = 'i-plan') {
  useChatStore.setState({
    pendingInteraction: { interactionId, interactionType: 'plan_exit', toolName: 'ExitPlanMode', toolInput },
  });
}

/** The server's summary of the pause's plan arrived. */
function summarized(bullets = BULLETS, status: 'ok' | 'unavailable' = 'ok', interactionId = 'i-plan') {
  useChatStore.setState(s => ({ planSummaries: { ...s.planSummaries, [interactionId]: { bullets, status } } }));
}

function planTab(content: string | null, extra: Partial<PanelTab> = {}): PanelTab {
  return {
    id: 'plan-1', type: 'plan', label: 'Plan', subagentType: 'Plan', description: '', content,
    prompt: '', streaming: false, status: 'complete', startedAt: 0, completedAt: 1, blocks: [], ...extra,
  };
}

const summaryBox = () => within(screen.getByRole('group', { name: 'Summary' }));

beforeEach(() => {
  vi.mocked(ws.answerInteraction).mockClear();
  useChatStore.setState({
    activeSession: 'session-1', pendingInteraction: null, panels: [], activePanelId: null, panelVisible: false,
    isStreaming: false, streamingBlocks: [], planSummaries: {}, planEditRequest: null,
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
  it('in an ordinary session: Approve, Request changes, Read the plan, Edit', () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Plan ready for your decision')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Request changes…' })).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getByRole('button', { name: 'Read the plan' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Apply setup' })).not.toBeInTheDocument();
  });

  it('in the onboarding conversation: a setup proposal to apply or change', () => {
    useChatStore.setState({ activeSession: 'onboarding-1a2b3c4d' });
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByText('Setup proposal')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Apply setup' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Change something…' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Read the plan' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    expect(screen.queryByText('Plan ready for your decision')).not.toBeInTheDocument();
  });

  it('shows no plan text in the card, only its summary', () => {
    pause();
    summarized();
    render(<PlanApprovalBlock block={call('ExitPlanMode', { input: { plan: PLAN } })} />);
    expect(screen.queryByRole('heading')).not.toBeInTheDocument();
    expect(screen.queryByText(/Act as/)).not.toBeInTheDocument();
    expect(screen.queryByRole('region')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Show all' })).not.toBeInTheDocument();
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

describe('ExitPlanMode: the summary', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('shows the server\'s summary as a list, with inline markdown', () => {
    pause();
    summarized();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    const items = summaryBox().getAllByRole('listitem');
    expect(items).toHaveLength(3);
    expect(items[0]).toHaveTextContent('Reviews new pull requests in org/repo-a every 30 minutes.');
    expect(within(items[0]).getByText('org/repo-a').tagName).toBe('CODE');
    expect(summaryBox().queryByText('Summarizing…')).not.toBeInTheDocument();
  });

  it('reads "Summarizing…" until the summary arrives', () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(summaryBox().getByRole('status')).toHaveTextContent('Summarizing…');
    expect(summaryBox().queryByRole('listitem')).not.toBeInTheDocument();

    act(() => summarized());
    expect(summaryBox().queryByText('Summarizing…')).not.toBeInTheDocument();
    expect(summaryBox().getAllByRole('listitem')).toHaveLength(3);
  });

  it('falls back to the plan\'s first heading and two lines when no summary can be made', () => {
    pause();
    summarized([], 'unavailable');
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    const box = summaryBox();
    expect(box.getByText('Setup proposal — review pull requests in org/repo-a')).toBeInTheDocument();
    expect(box.getByText('Review new pull requests in org/repo-a and say which need a human.')).toBeInTheDocument();
    // Plain text: no list marker, no backticks.
    expect(box.getByText('Act as alice; every public comment is a draft for you to approve.')).toBeInTheDocument();
    expect(box.queryByText(/Scan every 30 minutes/)).not.toBeInTheDocument();
    expect(box.queryByRole('status')).not.toBeInTheDocument();
  });

  it('falls back after waiting for a summary that never comes', () => {
    vi.useFakeTimers();
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    act(() => { vi.advanceTimersByTime(24_000); });
    expect(summaryBox().getByRole('status')).toHaveTextContent('Summarizing…');
    act(() => { vi.advanceTimersByTime(1_000); });
    expect(summaryBox().queryByRole('status')).not.toBeInTheDocument();
    expect(summaryBox().getByText('Setup proposal — review pull requests in org/repo-a')).toBeInTheDocument();
  });

  it('starts a new wait for a revised plan\'s summary', () => {
    pause();
    summarized();
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(summaryBox().getAllByRole('listitem')).toHaveLength(3);
    // The same card can only see one pause, but a new pause id is a new wait.
    act(() => pause({ plan: PLAN }, 'i-plan-2'));
    view.rerender(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(summaryBox().getByRole('status')).toHaveTextContent('Summarizing…');
  });
});

describe('ExitPlanMode: Read the plan and Edit open the side panel', () => {
  it('opens a plan tab from the pause when there is none', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(useChatStore.getState().panelVisible).toBe(false);
    await userEvent.click(screen.getByRole('button', { name: 'Read the plan' }));
    const { panels, activePanelId, panelVisible } = useChatStore.getState();
    expect(panels).toHaveLength(1);
    expect(panels[0]).toMatchObject({ type: 'plan', content: PLAN });
    expect(panels[0].readOnly).toBeFalsy();
    expect(activePanelId).toBe(panels[0].id);
    expect(panelVisible).toBe(true);
  });

  it('focuses the plan tab that is there, and fills it in when it is empty', async () => {
    useChatStore.setState({ panels: [planTab(null)], panelVisible: false });
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Read the plan' }));
    const { panels, activePanelId, panelVisible } = useChatStore.getState();
    expect(panels).toHaveLength(1);
    expect(panels[0].content).toBe(PLAN);
    expect(activePanelId).toBe('plan-1');
    expect(panelVisible).toBe(true);
  });

  it('opens the plan for editing: the panel shows and its footer is asked to edit', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const { panels, panelVisible, planEditRequest } = useChatStore.getState();
    expect(panels[0]).toMatchObject({ type: 'plan', content: PLAN });
    expect(panelVisible).toBe(true);
    expect(planEditRequest).toBe('i-plan');
    expect(ws.answerInteraction).not.toHaveBeenCalled();
  });

  it('offers no Edit when the plan text did not come with the pause', () => {
    useChatStore.setState({ panels: [planTab(PLAN)] });
    pause({});
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.getByRole('button', { name: 'Read the plan' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
  });

  it('offers no Read the plan when there is no plan text anywhere', () => {
    pause({});
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    expect(screen.queryByRole('button', { name: 'Read the plan' })).not.toBeInTheDocument();
    expect(summaryBox().queryByRole('status')).toBeInTheDocument();
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

describe('ExitPlanMode in history: Read the plan', () => {
  it('opens the approved plan, from the result, as its own tab to read', async () => {
    const edited = PLAN.replace('every 30 minutes', 'every hour');
    render(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', input: { plan: PLAN },
      result: `${APPROVED_RESULT}\n\n## Approved Plan (edited by user):\n${edited}`,
    })} />);
    await userEvent.click(screen.getByRole('button', { name: 'Read the plan' }));
    const { panels, activePanelId, panelVisible } = useChatStore.getState();
    expect(panels).toHaveLength(1);
    // The person's edits, which the call's input never saw.
    expect(panels[0]).toMatchObject({ type: 'plan', content: edited, readOnly: true, description: 'Approved with your edits' });
    expect(activePanelId).toBe(panels[0].id);
    expect(panelVisible).toBe(true);

    // A second click focuses the same tab.
    act(() => useChatStore.setState({ panelVisible: false }));
    await userEvent.click(screen.getByRole('button', { name: 'Read the plan' }));
    expect(useChatStore.getState().panels).toHaveLength(1);
    expect(useChatStore.getState().panelVisible).toBe(true);
  });

  it('opens the plan a change request was about, from the call\'s input', async () => {
    render(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', isError: true, result: 'Run it hourly', input: { plan: PLAN },
    })} />);
    expect(screen.getByText('Changes requested — Run it hourly')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Read the plan' }));
    expect(useChatStore.getState().panels[0]).toMatchObject({ content: PLAN, readOnly: true, description: 'Changes requested' });
  });

  it('offers no link without any plan text', () => {
    render(<PlanApprovalBlock block={call('ExitPlanMode', { status: 'complete', result: APPROVED_RESULT })} />);
    expect(screen.queryByRole('button', { name: 'Read the plan' })).not.toBeInTheDocument();
  });

  it('leaves a read-only plan alone when the live plan is approved', async () => {
    useChatStore.setState({
      panels: [planTab(PLAN), planTab('# An earlier plan', { id: 'plan-read-tu-old', readOnly: true })],
    });
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(useChatStore.getState().panels.map(p => p.id)).toEqual(['plan-read-tu-old']);
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
  it('gives the revised plan its own card, while the earlier one keeps its outcome', async () => {
    const REVISED = '# Setup proposal — review pull requests in org/repo-a, hourly\n\n- Scan every hour.';
    const first = call('ExitPlanMode', {
      toolUseId: 'tu-first', status: 'complete', isError: true, result: 'Run the review hourly',
      input: { plan: PLAN },
    });
    const second = call('ExitPlanMode', { toolUseId: 'tu-second', input: { plan: REVISED } });
    pause({ plan: REVISED }, 'i-plan-2');
    summarized(['Reviews pull requests in org/repo-a every hour.'], 'ok', 'i-plan-2');
    render(<><PlanApprovalBlock block={first} /><PlanApprovalBlock block={second} /></>);

    expect(screen.getByText('Changes requested — Run the review hourly')).toBeInTheDocument();
    expect(summaryBox().getByText('Reviews pull requests in org/repo-a every hour.')).toBeInTheDocument();
    expect(screen.getAllByRole('group', { name: 'Summary' })).toHaveLength(1);
    await userEvent.click(screen.getAllByRole('button', { name: 'Read the plan' })[1]);
    expect(useChatStore.getState().panels[0]).toMatchObject({ content: REVISED });
  });
});

describe('ExitPlanMode: the plan tab after a decision from the card', () => {
  beforeEach(() => {
    useChatStore.setState({ panels: [planTab(PLAN)], activePanelId: 'plan-1', panelVisible: true });
  });

  it('closes on Approve, as it does from the footer', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect(useChatStore.getState().panels).toHaveLength(0);
  });

  it('stays open on a change request, for the revised plan', async () => {
    pause();
    render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    await userEvent.click(screen.getByRole('button', { name: 'Request changes…' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'Hourly');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    expect(useChatStore.getState().panels.map(p => p.id)).toEqual(['plan-1']);
  });
});

describe('ExitPlanMode: an error result that is no one\'s feedback', () => {
  it('reads as closed when this client saw the call run and no decision was offered', () => {
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    view.rerender(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', isError: true, result: 'Plan file could not be read',
    })} />);
    expect(screen.getByText('Closed — Plan file could not be read')).toBeInTheDocument();
  });

  it('reads as feedback when the decision was offered and someone decided', () => {
    pause();
    const view = render(<PlanApprovalBlock block={call('ExitPlanMode')} />);
    act(() => useChatStore.setState({ pendingInteraction: null }));
    view.rerender(<PlanApprovalBlock block={call('ExitPlanMode', {
      status: 'complete', isError: true, result: 'Run the review hourly',
    })} />);
    expect(screen.getByText('Changes requested — Run the review hourly')).toBeInTheDocument();
  });

  it('tells CLI errors from feedback in history by the result text alone', () => {
    const cases: [string, string][] = [
      [
        '<tool_use_error>You are not in plan mode. To enter plan mode, call the EnterPlanMode tool first.</tool_use_error>',
        'Closed — You are not in plan mode. To enter plan mode, call the EnterPlanMode tool first.',
      ],
      ['<tool_use_error>InputValidationError: unexpected key</tool_use_error>', 'Closed — InputValidationError: unexpected key'],
      // Feedback that only ends like a server denial stays feedback.
      ['The nightly job timed out.', 'Changes requested — The nightly job timed out.'],
      ['Session stopped before, so retry the clone', 'Changes requested — Session stopped before, so retry the clone'],
    ];
    for (const [result, text] of cases) {
      const view = render(<PlanApprovalBlock block={call('ExitPlanMode', { status: 'complete', isError: true, result })} />);
      expect(screen.getByText(text)).toBeInTheDocument();
      view.unmount();
    }
  });
});
