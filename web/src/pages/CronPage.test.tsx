import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api, type CronLog, type WorkflowRun } from '../api/client';
import { ws, type WSMessage } from '../api/websocket';
import { useCronStore } from '../stores/cronStore';
import { LogsTable } from '../components/Cron/LogsTable';
import { CronPage } from './CronPage';

vi.mock('../api/client', () => ({ api: {
  getCronLogs: vi.fn(), listCronJobs: vi.fn(), getCronSchedule: vi.fn(),
} }));
vi.mock('../api/websocket', () => ({ ws: {
  onMessage: vi.fn(), onOpen: vi.fn(),
} }));

const log = (overrides: Partial<CronLog> = {}): CronLog => ({
  id: 1, job_id: 'batch', started_at: '2025-01-01 10:00:00',
  finished_at: null, status: 'pending', output: null, error: null,
  session_id: 'workflow:wfr-test', workflow_id: 'wfr-test',
  dispatch: { status: 'success', finished_at: '2025-01-01T10:00:00.050Z', output: 'queued', error: null },
  ...overrides,
});
const response = (logs: CronLog[], total = logs.length) => ({ logs, total, limit: 50, offset: 0 });
const event = (createdBy = 'cron:batch'): WSMessage => ({
  type: 'workflow_run_update', session_id: 'workflow:wfr-test',
  run: { id: 'wfr-test', created_by: createdBy } as WorkflowRun,
});
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}

beforeEach(() => {
  vi.resetAllMocks();
  useCronStore.setState(useCronStore.getInitialState());
  vi.mocked(api.getCronLogs).mockResolvedValue(response([]));
  vi.mocked(api.listCronJobs).mockResolvedValue({ jobs: [] });
  vi.mocked(api.getCronSchedule).mockResolvedValue({ next: null, jobs: [] });
  vi.mocked(ws.onMessage).mockReturnValue(vi.fn());
  vi.mocked(ws.onOpen).mockReturnValue(vi.fn());
});
afterEach(() => { vi.useRealTimers(); });

describe('workflow rows', () => {
  it.each([
    ['pending', 'pending', 'waiting'], ['running', 'running', 'running'],
    ['done', 'done', '2m 3s'], ['failed', 'failed', '2m 3s'],
    ['killed', 'killed', '2m 3s'], ['budget_exhausted', 'budget exhausted', '2m 3s'],
    ['unavailable', 'unavailable', '—'],
  ] as const)('shows %s instead of dispatch success', (status, label, duration) => {
    useCronStore.setState({ logs: [log({
      status, finished_at: ['pending', 'running', 'unavailable'].includes(status) ? null : '2025-01-01T10:02:03Z',
    })] });
    render(<MemoryRouter><LogsTable showJobColumn /></MemoryRouter>);
    const cells = within(screen.getAllByRole('row')[1]).getAllByRole('cell');
    expect(cells[2]).toHaveTextContent(duration);
    expect(cells[3]).toHaveTextContent(label);
    expect(screen.queryByText('ok')).not.toBeInTheDocument();
    expect(screen.queryByText('queued')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open chat' })).toHaveAttribute('href', '/chat/workflow%3Awfr-test');
  });

  it('expands the actual workflow failure', () => {
    const error = 'initialize timeout: ' + 'test worker did not respond. '.repeat(5);
    useCronStore.setState({ logs: [log({ status: 'failed', error, finished_at: '2025-01-01T10:02:03Z' })] });
    render(<MemoryRouter><LogsTable showJobColumn /></MemoryRouter>);
    fireEvent.click(screen.getAllByRole('row')[1]);
    expect(screen.getByText(/^Error: initialize timeout:/).textContent).toBe(`Error: ${error}`);
  });

  it('keeps ordinary success and missed runs', () => {
    useCronStore.setState({ logs: [
      log({ id: 1, status: 'success', finished_at: '2025-01-01T10:00:01Z', workflow_id: undefined }),
      log({ id: 2, status: 'missed', finished_at: '2025-01-01T10:02:03Z', workflow_id: undefined }),
    ] });
    render(<MemoryRouter><LogsTable showJobColumn /></MemoryRouter>);
    expect(screen.getByText('ok')).toBeInTheDocument();
    expect(screen.getByText('1.0s')).toBeInTheDocument();
    expect(screen.getByText('missed')).toBeInTheDocument();
    expect(within(screen.getAllByRole('row')[2]).getAllByRole('cell')[2]).toHaveTextContent('—');
  });
});

it('refreshes the visible page on workflow changes and reconnect, then unsubscribes', async () => {
  useCronStore.setState({ selectedJobId: 'batch', logsOffset: 50 });
  vi.mocked(api.getCronLogs).mockResolvedValue(response([log()], 100));
  const view = render(<MemoryRouter><CronPage /></MemoryRouter>);
  await act(async () => {});
  const onMessage = vi.mocked(ws.onMessage).mock.calls[0][0];
  const onOpen = vi.mocked(ws.onOpen).mock.calls[0][0];
  const offMessage = vi.mocked(ws.onMessage).mock.results[0].value;
  const offOpen = vi.mocked(ws.onOpen).mock.results[0].value;
  vi.mocked(api.getCronLogs).mockClear();
  await act(async () => { onMessage(event('session:batch')); onMessage(event('cron:other')); });
  expect(api.getCronLogs).not.toHaveBeenCalled();
  vi.mocked(api.getCronLogs).mockResolvedValue(response([log({
    status: 'failed', error: 'worker failed', finished_at: '2025-01-01T10:02:03Z',
  })], 100));
  await act(async () => { onMessage(event()); });
  expect(api.getCronLogs).toHaveBeenLastCalledWith('batch', 50, 50);
  expect(screen.getByText('failed')).toBeInTheDocument();
  expect(screen.getByText('worker failed')).toBeInTheDocument();
  expect(screen.getByText('2m 3s')).toBeInTheDocument();
  await act(async () => { onOpen(); });
  expect(api.getCronLogs).toHaveBeenCalledTimes(2);
  view.unmount();
  expect(offMessage).toHaveBeenCalledOnce();
  expect(offOpen).toHaveBeenCalledOnce();
});

it('refreshes all jobs for a cron workflow event', async () => {
  render(<MemoryRouter><CronPage /></MemoryRouter>);
  await act(async () => {});
  vi.mocked(api.getCronLogs).mockClear();
  await act(async () => { vi.mocked(ws.onMessage).mock.calls[0][0](event()); });
  expect(api.getCronLogs).toHaveBeenCalledWith(undefined, 50, 0);
});

it('polls active history if an event is lost and stops when it completes', async () => {
  vi.useFakeTimers();
  vi.mocked(api.getCronLogs).mockResolvedValue(response([log()]));
  const view = render(<MemoryRouter><CronPage /></MemoryRouter>);
  await act(async () => {});
  vi.mocked(api.getCronLogs).mockResolvedValue(response([log({ status: 'done', finished_at: '2025-01-01T10:02:03Z' })]));
  await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(screen.getByText('done')).toBeInTheDocument();
  expect(api.getCronLogs).toHaveBeenCalledTimes(2);
  await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
  expect(api.getCronLogs).toHaveBeenCalledTimes(2);
  view.unmount();
});

it('ignores a stale response after changing filters', async () => {
  const old = deferred<ReturnType<typeof response>>();
  vi.mocked(api.getCronLogs).mockReturnValueOnce(old.promise);
  const request = useCronStore.getState().loadLogs();
  useCronStore.getState().selectJob('other');
  await Promise.resolve();
  old.resolve(response([log()]));
  await request;
  expect(useCronStore.getState()).toMatchObject({ selectedJobId: 'other', logs: [], loading: false });
});

it('refreshes the requested page during navigation and ignores the older response', async () => {
  useCronStore.setState({ logs: [log({ id: 99 })] });
  const old = deferred<ReturnType<typeof response>>();
  vi.mocked(api.getCronLogs).mockReturnValueOnce(old.promise);
  const request = useCronStore.getState().loadLogs(50);
  expect(useCronStore.getState().logs).toEqual([]);
  vi.mocked(api.getCronLogs).mockResolvedValue(response([log({ status: 'done' })]));
  await useCronStore.getState().loadLogs();
  expect(api.getCronLogs).toHaveBeenLastCalledWith(undefined, 50, 50);
  old.resolve(response([log()]));
  await request;
  expect(useCronStore.getState().logs[0].status).toBe('done');
  expect(useCronStore.getState().logsOffset).toBe(50);
});
