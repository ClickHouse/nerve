import { useRef, useState } from 'react';
import { codeReview, type ReviewPR, type ManualReviewRequest } from '../../api/codeReview';
import { ApiError } from '../../api/hosted';

/** Mount keyed by PR/version so an in-flight click cannot authorize new input. */
export function ReviewLaunch({ pr, onChange }: { pr: ReviewPR; onChange: () => void }) {
  const [sending, setSending] = useState(false);
  const [accepted, setAccepted] = useState<ManualReviewRequest>();
  const [error, setError] = useState('');
  const busy = useRef(false);
  const key = useRef('');
  const previousRequest = useRef('');
  const action = pr.review_action;
  const awaitingRefresh = accepted && ['pending', 'claimed'].includes(accepted.state) && (action?.last_request?.id ?? '') === previousRequest.current;
  const state = awaitingRefresh ? (accepted.state === 'pending' ? 'queued' : 'running') : action?.state;
  const disabled = sending || !!awaitingRefresh || !pr.current_version || !action?.enabled;
  const launch = async () => {
    if (busy.current || disabled || !pr.current_version) return;
    busy.current = true;
    setSending(true);
    setError('');
    key.current ||= crypto.randomUUID();
    previousRequest.current = action?.last_request?.id ?? '';
    try {
      const result = await codeReview.launch(pr.id, pr.current_version, key.current);
      setAccepted(result.request);
      key.current = '';
    } catch (e) {
      let message = e instanceof Error ? e.message : String(e);
      if (e instanceof ApiError) {
        try {
          const body: unknown = JSON.parse(message.slice(message.indexOf(':') + 1));
          if (body && typeof body === 'object' && 'detail' in body && typeof body.detail === 'string') message = body.detail;
        } catch { /* A non-JSON response still carries its original message. */ }
      }
      setError(message);
      // A lost response may already have accepted the request. Reuse its key.
      if (e instanceof ApiError && e.status >= 400 && e.status < 500) key.current = '';
    } finally {
      busy.current = false;
      setSending(false);
      onChange();
    }
  };
  return <div className="mt-3 space-y-1 text-sm">
    <button type="button" disabled={disabled} onClick={() => void launch()}
      className="rounded border border-border px-3 py-1.5 text-accent disabled:cursor-not-allowed disabled:opacity-60">
      {sending ? 'Отправка…' : state === 'queued' ? 'В очереди' : state === 'running' ? 'Проверяется' : 'Запустить проверку'}
    </button>
    {action?.reason && <p className="text-text-muted">{action.reason}</p>}
    {!action && <p className="text-text-muted">Состояние запуска недоступно</p>}
    {action?.last_request?.reason && <p className="text-text-muted">{action.last_request.reason}</p>}
    {error && <p role="alert">{error}</p>}
  </div>;
}
