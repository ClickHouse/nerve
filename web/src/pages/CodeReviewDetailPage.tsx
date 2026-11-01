import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { codeReview, type ReviewDetail, type VersionReport } from '../api/codeReview';
import { MarkdownContent } from '../components/Chat/MarkdownContent';
import { reviewTime } from '../utils/codeReview';

export function CodeReviewHistory({ prId }: { prId: string }) {
  const [detail, setDetail] = useState<ReviewDetail>();
  const [selected, setSelected] = useState('');
  const [offset, setOffset] = useState(0);
  const [report, setReport] = useState<VersionReport>();
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true;
    codeReview.detail(prId, offset).then(data => {
      if (!active) return;
      setDetail(data);
      setSelected(offset === 0 ? data.pr.current_version ?? data.versions[0]?.id ?? '' : data.versions[0]?.id ?? '');
    }).catch(e => { if (active) setError(String(e)); });
    return () => { active = false; };
  }, [prId, offset]);
  useEffect(() => {
    if (!selected) return;
    let active = true;
    codeReview.version(prId, selected).then(data => {
      if (active) { setReport(data); setError(''); }
    }).catch(e => { if (active) setError(String(e)); });
    return () => { active = false; };
  }, [prId, selected]);
  const currentReport = report?.version.id === selected ? report : undefined;
  return <section className="space-y-4">
    {error && <p role="alert">{error}</p>}
    {detail && <>
      <p><a href={detail.pr.url} target="_blank" rel="noreferrer">{detail.pr.title}</a> · {detail.pr.review_state}</p>
      <p className="text-sm">Head <code>{detail.pr.head}</code> · Base <code>{detail.pr.base}</code></p>
      {detail.pr.reviewed_version !== detail.pr.current_version && <p role="status">Current input version has no completed review. Earlier reports apply to their recorded SHA.</p>}
      {detail.pr.draft !== 0 && <p role="status">Draft: review is paused.</p>}
      <label className="block">Review version
        <select className="ml-3 max-w-full rounded border border-border bg-surface p-2" value={selected} onChange={e => setSelected(e.target.value)}>
          {detail.pr.current_version && !detail.versions.some(v => v.id === detail.pr.current_version) &&
            <option value={detail.pr.current_version}>Current · {detail.pr.head.slice(0, 12)}</option>}
          {detail.versions.map(v => <option key={v.id} value={v.id}>{reviewTime(v.created_at)} · {v.head.slice(0, 12)} · {v.status} · ${v.spent_usd.toFixed(2)}</option>)}
        </select>
      </label>
      {detail.total > 50 && <div className="flex gap-4">
        <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 50))}>Newer versions</button>
        <button disabled={offset + 50 >= detail.total} onClick={() => setOffset(offset + 50)}>Older versions</button>
      </div>}
      <details><summary>Latest CI evidence</summary><pre className="overflow-auto">{JSON.stringify(JSON.parse(detail.pr.ci), null, 2)}</pre></details>
    </>}
    {selected && !currentReport && !error && <p>Loading version…</p>}
    {currentReport && <>
      <p>Status: {currentReport.version.status} · SHA <code>{currentReport.version.head}</code></p>
      {currentReport.version.error && <p role="alert">{currentReport.version.error}</p>}
      {currentReport.reports.length === 0 && <p>No saved review output yet.</p>}
      {currentReport.reports.map((attempt, index) => <article className="space-y-4 rounded-lg border border-border p-4" key={attempt.run}>
        <p>Attempt {index + 1} · {attempt.admission} · ${attempt.spent_usd.toFixed(2)}</p>
        {!!attempt.uncertain_usd && <p role="status">Additional ${attempt.uncertain_usd.toFixed(2)} reserved for unreported usage after interruption.</p>}
        {attempt.error && <p role="alert">{attempt.error}</p>}
        {['description.md', 'diagrams.md', 'review.md'].map(stage => attempt.output[stage] && <MarkdownContent key={stage} content={attempt.output[stage]} />)}
        <details><summary>Structured review and scope</summary><pre className="overflow-auto whitespace-pre-wrap">{attempt.output['verdict.json']}</pre></details>
      </article>)}
    </>}
  </section>;
}

export function CodeReviewDetailPage() {
  const { prId } = useParams();
  return <div className="h-full overflow-auto p-4 md:p-6">
    <Link to="/tasks-code-review">← Tasks Code Review</Link>
    <h1 className="my-4 text-2xl font-semibold">Pull request review</h1>
    {prId && <><p className="mb-4"><Link to={`/tasks/${prId}`}>Open task</Link></p><CodeReviewHistory prId={prId} /></>}
  </div>;
}
