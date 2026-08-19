import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CronJob } from './cronStore';

vi.mock('../api/client', () => ({
  api: {
    setCronJobEnabled: vi.fn(),
    listCronJobs: vi.fn(),
    getCronLogs: vi.fn(),
  },
}));

const { api } = await import('../api/client');
const { useCronStore } = await import('./cronStore');

function job(id: string, enabled = true, extra: Partial<CronJob> = {}): CronJob {
  return {
    id, type: 'cron', schedule: '1h', description: '', enabled,
    next_run: enabled ? '2026-08-19T12:00:00Z' : null,
    toggle_refusal: null,
    ...extra,
  };
}

/** What `request()` throws for a non-2xx response: "<status>: <raw body>". */
function httpError(status: number, detail: string): Error {
  return new Error(`${status}: ${JSON.stringify({ detail })}`);
}

beforeEach(() => {
  // clearAllMocks resets calls but keeps implementations, so a rejection set by
  // one test would leak into the next. Re-arm both mocks explicitly.
  vi.clearAllMocks();
  useCronStore.setState({
    jobs: [job('planner'), job('sweeper', false)],
    toggling: null,
    toggleErrors: {},
  });
  vi.mocked(api.listCronJobs).mockResolvedValue({ jobs: [] });
  vi.mocked(api.setCronJobEnabled).mockResolvedValue({
    job_id: 'planner', enabled: false, changed: true,
    file: '/ws/config/cron/jobs.yaml',
    reload: { added: [], removed: [], updated: [] },
  });
});

describe('setJobEnabled', () => {
  it('calls the API with the requested state and reloads the list', async () => {
    vi.mocked(api.listCronJobs).mockResolvedValue({
      jobs: [job('planner', false), job('sweeper', false)],
    });

    await useCronStore.getState().setJobEnabled('planner', false);

    expect(api.setCronJobEnabled).toHaveBeenCalledWith('planner', false);
    // Reloaded rather than patched locally: the server also recomputed
    // next_run, which a local flip would leave pointing at a stale time.
    expect(api.listCronJobs).toHaveBeenCalled();
    expect(useCronStore.getState().jobs.find(j => j.id === 'planner')!.enabled)
      .toBe(false);
    expect(useCronStore.getState().toggling).toBeNull();
  });

  it('records the server detail when the toggle is refused', async () => {
    vi.mocked(api.setCronJobEnabled).mockRejectedValue(
      httpError(403, 'Cannot toggle cron job in /ws/config/cron/jobs.yaml: tracked config'),
    );

    await useCronStore.getState().setJobEnabled('planner', false);

    // The detail, not the raw "403: {...}" envelope — a lockdown refusal is an
    // expected answer and has to be readable.
    expect(useCronStore.getState().toggleErrors.planner)
      .toBe('Cannot toggle cron job in /ws/config/cron/jobs.yaml: tracked config');
    expect(useCronStore.getState().toggling).toBeNull();
    // The switch must not appear to have moved.
    expect(api.listCronJobs).not.toHaveBeenCalled();
  });

  it('clears a previous error once a retry succeeds', async () => {
    useCronStore.setState({ toggleErrors: { planner: 'old failure' } });

    await useCronStore.getState().setJobEnabled('planner', false);

    expect(useCronStore.getState().toggleErrors.planner).toBeUndefined();
  });

  it('keeps other jobs’ errors when one job is retried', async () => {
    useCronStore.setState({ toggleErrors: { planner: 'mine', sweeper: 'theirs' } });

    await useCronStore.getState().setJobEnabled('planner', true);

    const { toggleErrors } = useCronStore.getState();
    expect(toggleErrors.planner).toBeUndefined();
    expect(toggleErrors.sweeper).toBe('theirs');
  });

  it('falls back to the whole message when the body is not FastAPI JSON', async () => {
    vi.mocked(api.setCronJobEnabled).mockRejectedValue(
      new Error('502: <html>gateway</html>'),
    );

    await useCronStore.getState().setJobEnabled('planner', false);

    expect(useCronStore.getState().toggleErrors.planner).toBe('<html>gateway</html>');
  });

  it('surfaces a non-HTTP failure rather than swallowing it', async () => {
    vi.mocked(api.setCronJobEnabled).mockRejectedValue(new TypeError('network down'));

    await useCronStore.getState().setJobEnabled('planner', false);

    expect(useCronStore.getState().toggleErrors.planner).toBe('network down');
  });
});
