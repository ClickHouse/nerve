import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    approvePlan: vi.fn(async () => ({ impl_session_id: 'impl-1' })),
    updatePlan: vi.fn(async () => ({})),
    revisePlan: vi.fn(async () => ({})),
    getPlan: vi.fn(async () => ({})),
    listPlans: vi.fn(async () => ({ plans: [] })),
  },
}));
vi.mock('./helpers/sessionsReload', () => ({ reloadSessionsSoon: vi.fn() }));

const { api } = await import('../api/client');
const { reloadSessionsSoon } = await import('./helpers/sessionsReload');
const { usePlanStore } = await import('./planStore');

beforeEach(() => vi.clearAllMocks());

describe('a plan decision in this tab', () => {
  it('reloads the session list after an approval and a decline', async () => {
    await usePlanStore.getState().approvePlan('plan-1');
    expect(reloadSessionsSoon).toHaveBeenCalledTimes(1);

    await usePlanStore.getState().updatePlan('plan-2', 'declined');
    expect(reloadSessionsSoon).toHaveBeenCalledTimes(2);
  });

  it('does not reload when the request fails', async () => {
    vi.mocked(api.approvePlan).mockRejectedValueOnce(new Error('409: taken'));
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await usePlanStore.getState().approvePlan('plan-1');
    expect(reloadSessionsSoon).not.toHaveBeenCalled();
  });
});
