import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WSMessage } from '../../api/websocket';
import type { ImplSession, Plan } from '../../stores/planStore';

const listeners = new Set<(msg: WSMessage) => void>();
const openers = new Set<() => void>();
vi.mock('../../api/websocket', () => ({
  ws: {
    onMessage: (handler: (msg: WSMessage) => void) => {
      listeners.add(handler);
      return () => listeners.delete(handler);
    },
    onOpen: (handler: () => void) => {
      openers.add(handler);
      return () => { openers.delete(handler); };
    },
  },
}));
vi.mock('../../api/client', () => ({ api: { getPlan: vi.fn() } }));

const { api } = await import('../../api/client');
const { usePlanStore } = await import('../../stores/planStore');
const { PlanProgressCard } = await import('./PlanProgressCard');
const { implementationState } = await import('./planProgress');

const getPlan = vi.mocked(api.getPlan);
const APPROVED_AT = '2026-01-02T10:00:00+00:00';

function session(overrides: Partial<ImplSession> = {}): ImplSession {
  return {
    id: 'impl-1', title: 'Implement: Add dark mode', updated_at: APPROVED_AT,
    is_running: true, awaiting_input: false, parked: false, ...overrides,
  };
}

function plan(overrides: Partial<Plan> = {}): Plan {
  return {
    id: 'plan-1', task_id: 'task-1', task_title: 'Add dark mode', session_id: null,
    impl_session_id: 'impl-1', status: 'implementing', content: 'Step one', feedback: null, version: 1,
    parent_plan_id: null, model: null, plan_type: 'generic', created_at: APPROVED_AT,
    reviewed_at: APPROVED_AT, impl_session: session(), ...overrides,
  };
}

/** The card as the plan page renders it: bound to the open plan. */
function OpenPlan() {
  const open = usePlanStore(s => s.selectedPlan);
  return open ? <PlanProgressCard plan={open} /> : null;
}

function renderCard(initial: Plan) {
  usePlanStore.setState({ selectedPlan: initial });
  return render(
    <MemoryRouter initialEntries={['/plans/plan-1']}>
      <Routes>
        <Route path="/plans/:planId" element={<OpenPlan />} />
        <Route path="/chat/:sessionId" element={<div>Chat page</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

/** Deliver a WS event, and let the plan load that it starts settle. */
const emit = (msg: WSMessage) => act(async () => { for (const listener of [...listeners]) listener(msg); });

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(new Date('2026-01-02T10:12:00+00:00'));
});

afterEach(() => {
  vi.useRealTimers();
});

describe('implementationState', () => {
  it.each([
    ['done', plan({ status: 'done' })],
    ['failed', plan({ status: 'failed', impl_session: session({ is_running: false }) })],
    ['failed', plan({ status: 'failed', impl_session: null })],
    // A later turn in the session of a failed plan.
    ['running', plan({ status: 'failed' })],
    ['waiting', plan({ impl_session: session({ awaiting_input: true }) })],
    ['running', plan()],
    ['parked', plan({ impl_session: session({ is_running: false, parked: true }) })],
    ['waiting', plan({ impl_session: session({ is_running: false }) })],
    [null, plan({ impl_session: null })],
  ])('reads %s', (state, value) => {
    expect(implementationState(value)).toBe(state);
  });
});

describe('PlanProgressCard', () => {
  it('shows the session title, the state and the time since the approval', () => {
    renderCard(plan());
    const card = screen.getByRole('region', { name: 'Implementation' });
    expect(card).toHaveTextContent('Implement: Add dark mode');
    expect(card).toHaveTextContent('Running · 12m');
  });

  it('shows how long a finished implementation took', () => {
    renderCard(plan({
      status: 'done', impl_session: session({ is_running: false, updated_at: '2026-01-02T11:05:00+00:00' }),
    }));
    expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Done after 1h 5m');
  });

  it('opens the session', async () => {
    vi.useRealTimers();
    renderCard(plan());
    await userEvent.click(screen.getByRole('button', { name: 'Open session' }));
    expect(screen.getByText('Chat page')).toBeInTheDocument();
  });

  it('follows the session state live', async () => {
    renderCard(plan());
    getPlan.mockResolvedValue(plan({ impl_session: session({ awaiting_input: true }) }));

    await emit({ type: 'session_running', session_id: 'other', is_running: false });
    expect(getPlan).not.toHaveBeenCalled();

    await emit({ type: 'session_awaiting_input', session_id: 'impl-1', awaiting: true });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' }))
      .toHaveTextContent('Waiting for you'));
    expect(getPlan).toHaveBeenCalledWith('plan-1');
  });

  it('ignores comments on the task', async () => {
    renderCard(plan());
    await emit({ type: 'task_updated', session_id: null, event: 'commented', task: { id: 'task-1' } as never });
    expect(getPlan).not.toHaveBeenCalled();
  });

  it('shows a failed implementation when the server marks the plan failed', async () => {
    renderCard(plan({ impl_session: session({ is_running: false }) }));
    getPlan.mockResolvedValue(plan({ status: 'failed', impl_session: session({ is_running: false }) }));
    await emit({ type: 'task_updated', session_id: null, event: 'updated', task: { id: 'task-1' } as never });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Failed'));
    // A later turn in the session can still finish the work.
    expect(listeners.size).toBe(1);
  });

  it('follows a done plan until the last turn ends, for the final time', async () => {
    renderCard(plan());
    // task_done marks the plan done while the turn still writes its reply.
    getPlan.mockResolvedValue(plan({ status: 'done', impl_session: session({ updated_at: '2026-01-02T10:05:00+00:00' }) }));
    await emit({ type: 'task_updated', session_id: null, event: 'done', task: { id: 'task-1' } as never });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Done after 5m'));
    expect(listeners.size).toBe(1);

    getPlan.mockResolvedValue(plan({
      status: 'done', impl_session: session({ is_running: false, updated_at: '2026-01-02T10:09:00+00:00' }),
    }));
    await emit({ type: 'session_running', session_id: 'impl-1', is_running: false });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Done after 9m'));
    expect(listeners.size).toBe(0);
    expect(openers.size).toBe(0);
  });

  it('keeps a finished plan when an older load answers last', async () => {
    renderCard(plan());
    let answerOld!: (value: Plan) => void;
    getPlan
      .mockReturnValueOnce(new Promise(r => { answerOld = r; }))
      .mockResolvedValueOnce(plan({ status: 'done', impl_session: session({ is_running: false }) }));
    await emit({ type: 'session_agents', session_id: 'impl-1', background_count: 0, has_background_tasks: false } as WSMessage);
    await emit({ type: 'task_updated', session_id: null, event: 'done', task: { id: 'task-1' } as never });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Done'));
    await act(async () => { answerOld(plan()); });
    expect(screen.getByRole('region', { name: 'Implementation' })).toHaveTextContent('Done');
  });

  it('loads the plan again after a reconnect', async () => {
    renderCard(plan());
    getPlan.mockResolvedValue(plan({ impl_session: session({ is_running: false, parked: true }) }));
    await act(async () => { for (const open of [...openers]) open(); });
    await waitFor(() => expect(screen.getByRole('region', { name: 'Implementation' }))
      .toHaveTextContent('Waiting on background work'));
  });
});
