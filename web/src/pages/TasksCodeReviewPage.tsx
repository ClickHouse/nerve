import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { codeReview, type ReviewPR, type ReviewStatus } from '../api/codeReview';

import { reviewTime } from '../utils/codeReview';

function ReviewColumn({ column, title }: { column: 'active' | 'archive'; title: string }) {
  const [data, setData] = useState<{ items: ReviewPR[]; total: number }>();
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true;
    const load = () => codeReview.board(column, offset).then(value => { if (active) { setData(value); setError(''); } }).catch(e => { if (active) setError(String(e)); });
    void load();
    const timer = setInterval(() => { void load(); }, 30_000);
    return () => { active = false; clearInterval(timer); };
  }, [column, offset]);
  return <section className="min-w-0 flex-1 rounded-xl bg-surface-raised p-4">
    <h2 className="mb-3 font-semibold">{title} · {data?.total ?? '…'}</h2>
    {error && <p role="alert">{error}</p>}
    <div className="space-y-3">
      {data?.items.map(pr => <article key={pr.id} className="rounded-lg border border-border bg-surface p-3">
        <Link className="font-medium text-accent" to={`/tasks-code-review/${pr.id}`}>#{pr.number} {pr.title}</Link>
        <p className="mt-2 text-sm">{pr.repository} · {pr.state}{pr.draft ? ' · draft' : ''}</p>
        <p className="text-sm">{pr.review_state} · {pr.severity} · <code>{pr.head.slice(0, 12)}</code></p>
        <p className="text-xs text-text-muted">Updated {reviewTime(pr.content_updated_at)}</p>
        <p className="text-xs text-text-muted">Polled {reviewTime(pr.last_polled_at)}</p>
        {pr.error && <p role="alert" className="text-sm">Observation error: {pr.error}</p>}
        <a className="text-xs underline" href={pr.url} target="_blank" rel="noreferrer">GitHub PR</a>
      </article>)}
      {data?.total === 0 && <p className="text-text-muted">No pull requests.</p>}
    </div>
    <div className="mt-4 flex gap-4">
      <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 50))}>Previous</button>
      <span>{(data?.total ? offset + 1 : 0)}–{Math.min(offset + 50, data?.total ?? 0)}</span>
      <button disabled={offset + 50 >= (data?.total ?? 0)} onClick={() => setOffset(offset + 50)}>Next</button>
    </div>
  </section>;
}

export function TasksCodeReviewPage() {
  const [status, setStatus] = useState<ReviewStatus>();
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true;
    const load = () => codeReview.status().then(value => {
      if (active) { setStatus(value); setError(''); }
    }).catch(e => { if (active) setError(String(e)); });
    void load();
    const timer = setInterval(() => { void load(); }, 30_000);
    return () => { active = false; clearInterval(timer); };
  }, []);
  const control = async (id: string, paused: boolean) => {
    try { setStatus(await (paused ? codeReview.resume(id) : codeReview.pause(id))); } catch (e) { setError(String(e)); }
  };
  return <div className="h-full overflow-auto p-4 md:p-6">
    <h1 className="mb-4 text-2xl font-semibold">{status?.board_name ?? 'Tasks Code Review'}</h1>
    {error && <p role="alert">{error}</p>}
    {status && !status.running && <p role="alert">Code Review service is stopped or not configured.</p>}
    {status?.worker_error && <p role="alert">Worker error: {status.worker_error}</p>}
    {status && !status.worker_available && <p role="alert">Review worker is unavailable. No reviews will start.</p>}
    <div className="mb-5 space-y-3">
      {status?.profiles.map(p => <div key={p.id} className="rounded-lg border border-border p-3 text-sm">
        <p className="font-medium">{p.id} · {!p.enabled ? 'Disabled' : p.paused ? 'Paused' : p.registered ? 'Scheduled' : 'Schedule unavailable'}</p>
        <p>{p.schedule} · {p.timezone} · Next: {reviewTime(p.next_run_at)}</p>
        <p>Last successful poll: {reviewTime(p.last_success_at)}</p>
        {p.enabled && !p.paused && p.overdue && <p role="alert">Polling is overdue or has not completed.</p>}
        <p>${p.day_committed_usd.toFixed(2)} committed / ${p.daily_budget_usd.toFixed(2)} daily · ${p.version_budget_usd.toFixed(2)} per version · {p.day}</p>
        {p.day_committed_usd > p.daily_budget_usd && <p role="alert">Recorded spend exceeds the daily cap; new reviews are blocked.</p>}
        {p.error && <p role="alert">{p.error}</p>}
        {p.enabled && <button className="mt-2 underline" onClick={() => void control(p.id, !!p.paused)}>{p.paused ? 'Resume profile' : 'Pause profile and stop its reviews'}</button>}
      </div>)}
    </div>
    <div className="flex flex-col gap-4 lg:flex-row">
      <ReviewColumn column="active" title="Open PRs" />
      <ReviewColumn column="archive" title="Archive" />
    </div>
  </div>;
}
