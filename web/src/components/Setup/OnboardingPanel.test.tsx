import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useParams } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  return {
    ...actual,
    api: {
      authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(), listActors: vi.fn(), setupSkipOnboarding: vi.fn(),
      setupFinish: vi.fn(),
    },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../../api/client');
const { useSetupStore } = await import('../../stores/setupStore');
const { makeState, step } = await import('../../test/setupFixtures');
const { OnboardingPanel } = await import('./OnboardingPanel');

const withOnboarding = (onboarding: 'pending' | 'running' | 'skipped', id: string | null = null) => makeState({
  steps: [step('mode', { status: 'done' }, { mode: 'personal', onboarding, onboarding_session_id: id })],
});

function ChatProbe() {
  return <p>chat page {useParams().sessionId}</p>;
}

function Panel() {
  const state = useSetupStore((s) => s.state);
  return state ? <OnboardingPanel state={state} /> : null;
}

function renderPanel() {
  return render(
    <MemoryRouter initialEntries={['/setup']}>
      <Routes>
        <Route path="/setup" element={<Panel />} />
        <Route path="/chat/:sessionId" element={<ChatProbe />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useSetupStore.getState().reset();
});

describe('OnboardingPanel', () => {
  it('leads into the waiting conversation', async () => {
    useSetupStore.setState({ state: withOnboarding('running', 'onboarding-1') });
    renderPanel();
    await userEvent.click(screen.getByRole('button', { name: 'Continue the conversation' }));
    expect(await screen.findByText('chat page onboarding-1')).toBeInTheDocument();
  });

  it('starts the conversation once setup is complete, for an install that never had one', async () => {
    const pending = withOnboarding('pending');
    useSetupStore.setState({ state: { ...pending, finished: true, steps_left: 0 } });
    vi.mocked(api.setupFinish).mockResolvedValue({
      state: withOnboarding('running', 'onboarding-5'), action: 'reloaded',
      onboarding: { status: 'running', session_id: 'onboarding-5' },
    });
    renderPanel();
    expect(screen.getByText('Your agent is ready for its onboarding conversation.')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Start the conversation' }));
    expect(await screen.findByText('chat page onboarding-5')).toBeInTheDocument();
    expect(api.setupFinish).toHaveBeenCalledTimes(1);
  });

  it('links to the session of a worker that sets itself up', async () => {
    useSetupStore.setState({ state: makeState({
      steps: [step('mode', { status: 'done' }, {
        mode: 'worker', onboarding: 'running', onboarding_session_id: 'onboarding-6', onboarding_kind: 'autonomous',
      })],
    }) });
    renderPanel();
    expect(screen.getByText('Your worker is setting itself up from its task description.')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Open the session' }));
    expect(await screen.findByText('chat page onboarding-6')).toBeInTheDocument();
  });

  it('skips onboarding only after a confirmation', async () => {
    useSetupStore.setState({ state: withOnboarding('pending') });
    vi.mocked(api.setupSkipOnboarding).mockResolvedValue(withOnboarding('skipped'));
    renderPanel();
    expect(screen.getByText('The onboarding conversation starts when you finish setup.')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Skip onboarding' }));
    expect(screen.getByText(/Nothing is set up for you/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Keep it' })).toHaveFocus();
    expect(api.setupSkipOnboarding).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Keep it' }));
    expect(screen.queryByText(/Nothing is set up for you/)).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Skip onboarding' }));
    await userEvent.click(screen.getByRole('button', { name: 'Skip onboarding' }));
    await waitFor(() => expect(api.setupSkipOnboarding).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Onboarding' })).not.toBeInTheDocument());
  });
});
