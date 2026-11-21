import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SessionContext } from '../../api/client';
import { ApiError } from '../../api/hosted';
import { COMPACT_CONFIRM_TEXT, type ContextUsage } from './contextUsage';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: { getSessionContext: vi.fn(), compactSession: vi.fn() },
}));

const { api } = await import('../../api/client');
const { ContextBar } = await import('./ContextBar');

const getContext = vi.mocked(api.getSessionContext);
const compactSession = vi.mocked(api.compactSession);

// 66,010 input tokens over 2 model calls: about 33k in context.
const USAGE: ContextUsage = {
  input_tokens: 10, output_tokens: 400, cache_read_input_tokens: 60000,
  cache_creation_input_tokens: 6000, max_context_tokens: 200000, num_turns: 2,
};

// A Codex turn with three calls whose inputs grow: 100k, 140k and 180k.
const CODEX_USAGE: ContextUsage = {
  input_tokens: 200_000, output_tokens: 1_500, cache_read_input_tokens: 220_000,
  cache_creation_input_tokens: 0, max_context_tokens: 272_000, num_turns: 3,
};

const LIVE: SessionContext = {
  session_id: 's-1', live: true, can_compact: true,
  total_tokens: 20237, max_tokens: 200000, auto_compact_threshold: 167000,
  categories: [
    { name: 'System prompt', tokens: 170 },
    { name: 'System tools', tokens: 15149 },
    { name: 'Skills', tokens: 1715 },
    { name: 'Messages', tokens: 3203 },
  ],
};
const TOTALS: SessionContext = {
  session_id: 's-1', live: false, can_compact: false,
  total_tokens: 33005, max_tokens: 200000, auto_compact_threshold: null, categories: [],
};

function renderBar(props: Partial<Parameters<typeof ContextBar>[0]> = {}) {
  render(
    <ContextBar usage={USAGE} sessionId="s-1" backend="claude" busy={false} {...props} />,
  );
  return userEvent.setup();
}

const trigger = () => screen.getByRole('button', { name: /^Context window:/ });
const popover = () => screen.getByRole('dialog', { name: 'Context window' });

beforeEach(() => {
  getContext.mockReset();
  compactSession.mockReset();
});

describe('ContextBar', () => {
  it('shows the estimate from the last turn until it is opened', () => {
    renderBar();
    expect(trigger()).toHaveAccessibleName('Context window: about 33.0k of 200.0k tokens');
    expect(trigger()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(getContext).not.toHaveBeenCalled();
  });

  it('shows the input of the last call when the backend reports it', () => {
    renderBar({ usage: { ...CODEX_USAGE, context_tokens: 180_000 }, backend: 'codex' });
    expect(trigger()).toHaveAccessibleName('Context window: 180.0k of 272.0k tokens');
  });

  it('divides the turn input by the number of calls otherwise', () => {
    renderBar({ usage: CODEX_USAGE, backend: 'codex' });
    expect(trigger()).toHaveAccessibleName('Context window: about 140.0k of 272.0k tokens');
  });

  it('opens the categories of a live session, with the threshold', async () => {
    getContext.mockResolvedValue(LIVE);
    const user = renderBar();
    await user.click(trigger());
    const list = await screen.findByRole('list', { name: 'Context by category' });
    const rows = within(list).getAllByRole('listitem').map(li => li.textContent);
    expect(rows).toEqual(['System prompt170', 'System tools15.1k', 'Skills1.7k', 'Messages3.2k']);
    expect(within(popover()).getByText('Auto-compact at 167.0k')).toBeInTheDocument();
    // The meter takes the measured use.
    expect(trigger()).toHaveAccessibleName('Context window: 20.2k of 200.0k tokens');
    expect(within(popover()).getByRole('button', { name: 'Compact…' })).toBeEnabled();
  });

  it('shows the totals and no Compact for a Codex session', async () => {
    getContext.mockResolvedValue(TOTALS);
    const user = renderBar({ backend: 'codex' });
    await user.click(trigger());
    expect(await screen.findByText('Codex does not report a breakdown.')).toBeInTheDocument();
    expect(screen.queryByRole('list', { name: 'Context by category' })).toBeNull();
    expect(screen.queryByRole('button', { name: /Compact/ })).toBeNull();
    expect(within(popover()).getByText(/of 200\.0k tokens/).parentElement)
      .toHaveTextContent('~33.0k of 200.0k tokens');
  });

  it('compacts after the confirmation and shows what it freed', async () => {
    getContext.mockResolvedValue(LIVE);
    compactSession.mockResolvedValue({
      ...LIVE, total_tokens: 17862,
      categories: [...LIVE.categories.slice(0, 3), { name: 'Messages', tokens: 828 }],
    });
    const user = renderBar();
    await user.click(trigger());
    await user.click(await screen.findByRole('button', { name: 'Compact…' }));
    expect(within(popover()).getByText(COMPACT_CONFIRM_TEXT)).toBeInTheDocument();
    expect(compactSession).not.toHaveBeenCalled();

    await user.click(within(popover()).getByRole('button', { name: 'Compact' }));
    expect(compactSession).toHaveBeenCalledWith('s-1');
    expect(await screen.findByText('Freed 2.4k tokens.')).toBeInTheDocument();
    expect(screen.getByText('Messages').nextSibling).toHaveTextContent('828');
    expect(trigger()).toHaveAccessibleName('Context window: 17.9k of 200.0k tokens');
  });

  it('shows a refused compact as a message, never as a reply', async () => {
    getContext.mockResolvedValue(LIVE);
    compactSession.mockRejectedValue(new ApiError(409, JSON.stringify({
      detail: { code: 'nothing_to_compact', message: 'Nothing to compact yet.' },
    })));
    const user = renderBar();
    await user.click(trigger());
    await user.click(await screen.findByRole('button', { name: 'Compact…' }));
    await user.click(within(popover()).getByRole('button', { name: 'Compact' }));
    expect(await within(popover()).findByRole('status')).toHaveTextContent('Nothing to compact yet.');
  });

  it('waits for the turn to end before it compacts', async () => {
    getContext.mockResolvedValue(LIVE);
    const user = renderBar({ busy: true });
    await user.click(trigger());
    expect(await screen.findByRole('button', { name: 'Compact…' })).toBeDisabled();
    expect(screen.getByText('Available when the agent is idle.')).toBeInTheDocument();
  });

  it('closes on Escape and gives the focus back', async () => {
    getContext.mockResolvedValue(LIVE);
    const user = renderBar();
    await user.click(trigger());
    await screen.findByRole('list', { name: 'Context by category' });
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(trigger()).toHaveFocus();
  });
});

describe('ContextBar live cost', () => {
  const live = {
    usage: { input_tokens: 2, output_tokens: 3, cache_read_input_tokens: 100_000, cache_creation_input_tokens: 5_000 },
    context_tokens: 105_002, max_context_tokens: 1_048_576, calls: 1,
    turn_cost_usd: 0.045, agents_cost_usd: 0, session_cost_usd: 10.045,
  };

  it('shows the billed session cost next to the context size', () => {
    renderBar({ sessionCostUsd: 3.1 });
    expect(screen.getByLabelText('Session cost: $3.10')).toHaveTextContent('$3.10');
  });

  it('shows the running estimate during the first turn, with no usage yet', async () => {
    getContext.mockResolvedValue(LIVE);
    const user = renderBar({ usage: null, live, sessionCostUsd: 10, busy: true });
    expect(trigger()).toHaveAccessibleName('Context window: 105.0k of 1.0M tokens');
    expect(screen.getByLabelText('Session cost: about $10.04')).toHaveTextContent('~$10.04');
    await user.click(trigger());
    await screen.findByRole('list', { name: 'Context by category' });
    expect(within(popover()).getByText('This turn so far')).toBeInTheDocument();
    expect(within(popover()).getByText('This turn, 1 call')).toBeInTheDocument();
    expect(within(popover()).getByText('Session (live)').nextSibling).toHaveTextContent('~$10.04');
    // The meter keeps the latest call's context, not the breakdown loaded after the last turn.
    expect(trigger()).toHaveAccessibleName('Context window: 105.0k of 1.0M tokens');
  });

  it('adds what the agents spent since the last turn', async () => {
    getContext.mockResolvedValue(LIVE);
    const user = renderBar({ sessionCostUsd: 3, agentsCostUsd: 5.5, agentsCostBase: 5 });
    expect(screen.getByLabelText('Session cost: about $3.50')).toHaveTextContent('~$3.50');
    await user.click(trigger());
    expect(within(popover()).getByText('Agents since the last turn').nextSibling).toHaveTextContent('~$0.50');
  });

  it('shows nothing without usage', () => {
    const { container } = render(<ContextBar usage={null} sessionId="s-1" backend="claude" busy={false} />);
    expect(container).toBeEmptyDOMElement();
  });
});
