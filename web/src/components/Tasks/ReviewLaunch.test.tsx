import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { ReviewLaunch } from './ReviewLaunch';
import { codeReview, type ReviewPR } from '../../api/codeReview';
import { ApiError } from '../../api/hosted';

vi.mock('../../api/codeReview', () => ({ codeReview: { launch: vi.fn() } }));
afterEach(() => { cleanup(); vi.resetAllMocks(); });
const pr: ReviewPR = {
  id: 'pr-1', profile: 'example', task_id: 'pr-1', repository: 'alice/example', number: 1,
  title: 'Synthetic PR', url: 'https://example.com', state: 'open', draft: 0, head: 'head', base: 'base',
  current_version: 'version-1', reviewed_version: null, review_state: 'queued', severity: 'none', ci: '{}',
  content_updated_at: '', last_polled_at: '', error: '',
  review_action: { enabled: true, state: 'ready', reason: '', review_mode: 'manual' },
};
const pending = { id: 'request-1', version_id: 'version-1', state: 'pending', reason: '' };

it('one click sends the exact version; concurrent clicks and a delayed refresh cannot send another', async () => {
  let finish!: (value: { request: typeof pending }) => void;
  vi.mocked(codeReview.launch).mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const onChange = vi.fn();
  const { rerender } = render(<ReviewLaunch pr={pr} onChange={onChange} />);
  const button = screen.getByRole('button');
  fireEvent.click(button);
  fireEvent.click(button);
  expect(codeReview.launch).toHaveBeenCalledTimes(1);
  expect(codeReview.launch).toHaveBeenCalledWith('pr-1', 'version-1', expect.any(String));
  expect(button).toBeDisabled();
  await act(async () => finish({ request: pending }));
  expect(screen.getByRole('button', { name: 'В очереди' })).toBeDisabled();
  expect(onChange).toHaveBeenCalledOnce();
  rerender(<ReviewLaunch pr={{ ...pr, review_action: { enabled: false, state: 'running', reason: 'Проверяется', last_request: { ...pending, state: 'claimed' } } }} onChange={onChange} />);
  expect(screen.getByRole('button', { name: 'Проверяется' })).toBeDisabled();
  rerender(<ReviewLaunch pr={{ ...pr, review_action: { enabled: true, state: 'ready', reason: '', last_request: { ...pending, id: 'newer-tab-request', state: 'finished' } } }} onChange={onChange} />);
  expect(screen.getByRole('button', { name: 'Запустить проверку' })).toBeEnabled();
});

it('reuses the idempotency key after a lost response', async () => {
  vi.mocked(codeReview.launch).mockRejectedValueOnce(new Error('Connection lost')).mockResolvedValueOnce({ request: pending });
  render(<ReviewLaunch pr={pr} onChange={vi.fn()} />);
  fireEvent.click(screen.getByRole('button'));
  await screen.findByRole('alert');
  fireEvent.click(screen.getByRole('button'));
  await screen.findByRole('button', { name: 'В очереди' });
  const calls = vi.mocked(codeReview.launch).mock.calls;
  expect(calls[0][2]).toBe(calls[1][2]);
});

it('shows an explicit budget rejection and uses a new key for a deliberate retry', async () => {
  vi.mocked(codeReview.launch).mockRejectedValue(new ApiError(409, JSON.stringify({ detail: 'Суточный бюджет исчерпан' })));
  render(<ReviewLaunch pr={pr} onChange={vi.fn()} />);
  fireEvent.click(screen.getByRole('button'));
  expect(await screen.findByRole('alert')).toHaveTextContent('Суточный бюджет исчерпан');
  expect(screen.getByRole('alert')).not.toHaveTextContent('409');
  fireEvent.click(screen.getByRole('button'));
  await waitFor(() => expect(codeReview.launch).toHaveBeenCalledTimes(2));
  const calls = vi.mocked(codeReview.launch).mock.calls;
  expect(calls[0][2]).not.toBe(calls[1][2]);
});

it.each(['Профиль на полной паузе', 'Черновики не проверяются', 'PR закрыт', 'Эта версия уже проверена', 'Лимит расходов на эту версию исчерпан'])('explains why launch is disabled: %s', reason => {
  render(<ReviewLaunch pr={{ ...pr, review_action: { enabled: false, state: 'ready', reason } }} onChange={vi.fn()} />);
  expect(screen.getByText(reason)).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button'));
  expect(codeReview.launch).not.toHaveBeenCalled();
});
