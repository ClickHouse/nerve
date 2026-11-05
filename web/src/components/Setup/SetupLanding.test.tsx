import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Link, MemoryRouter, useLocation, useNavigationType } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { SetupState } from '../../api/client';

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  return {
    ...actual,
    api: { authStatus: vi.fn(), getViewer: vi.fn(), login: vi.fn(), listActors: vi.fn() },
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { useSetupStore } = await import('../../stores/setupStore');
const { makeExternalState, makeFreshState, makeState, step, withStep } = await import('../../test/setupFixtures');
const { SetupLanding } = await import('./SetupLanding');

/** Where the router is, how it got there, and a way out of the wizard. */
function Probe() {
  const { pathname } = useLocation();
  const how = useNavigationType();
  return (
    <>
      <span data-testid="where">{pathname}</span>
      <span data-testid="how">{how}</span>
      <Link to="/chat">Chat</Link>
    </>
  );
}

function renderAt(path: string) {
  return render(<MemoryRouter initialEntries={[path]}><SetupLanding /><Probe /></MemoryRouter>);
}

const where = () => screen.getByTestId('where').textContent;

/** The checklist arrives, or is read again. */
function checklist(state: SetupState) {
  act(() => useSetupStore.setState({ state, loading: false }));
}

const LOCKED_MODE = step('mode', {
  status: 'blocked', blocked_kind: 'lockdown',
  blocked_reason: 'Configuration is managed from the config repository on this instance.',
});

beforeEach(() => {
  useSetupStore.getState().reset();
});

describe('SetupLanding', () => {
  it('sends an instance that has never chosen a mode to /setup once the checklist is known', () => {
    renderAt('/chat');
    expect(where()).toBe('/chat');   // unknown: nothing moves
    checklist(makeFreshState());
    expect(where()).toBe('/setup');
    // The page it arrived on was never seen, so Back must not return to it.
    expect(screen.getByTestId('how')).toHaveTextContent('REPLACE');
  });

  it('does the same behind a front gateway, from whatever page was opened', () => {
    renderAt('/tasks');
    checklist(makeExternalState({}, makeFreshState()));
    expect(where()).toBe('/setup');
  });

  it("leaves a partly set-up instance on its page: Mode chosen with steps left is the banner's case", () => {
    renderAt('/chat');
    checklist(makeState());
    expect(where()).toBe('/chat');
  });

  it('leaves a finished instance alone', () => {
    renderAt('/chat');
    checklist(makeState({ finished: true, steps_left: 0 }));
    expect(where()).toBe('/chat');
  });

  it('leaves a locked instance alone: nothing on the page can answer Mode there', () => {
    renderAt('/chat');
    checklist(withStep(makeFreshState({ lockdown: true }), LOCKED_MODE));
    expect(where()).toBe('/chat');
  });

  it('leaves a deep link into the wizard where it points', () => {
    renderAt('/setup/provider');
    checklist(makeFreshState());
    expect(where()).toBe('/setup/provider');
  });

  it('decides once: the person may leave, and a re-read does not bring them back', async () => {
    renderAt('/chat');
    checklist(makeFreshState());
    expect(where()).toBe('/setup');

    await userEvent.click(screen.getByRole('link', { name: 'Chat' }));
    expect(where()).toBe('/chat');

    // The wizard re-reads the checklist on every step change and whenever
    // the tab becomes visible again; the same answer must not move anyone.
    checklist(makeFreshState());
    expect(where()).toBe('/chat');
  });
});
