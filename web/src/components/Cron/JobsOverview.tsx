import { useCronStore, type CronJob } from '../../stores/cronStore';
import { chatPath, formatRelativeTime, formatSchedule, jobLabel } from './utils';
import { ChatLink, StatusBadge, TriggerButton, JobTypeIcon } from './controls';

/** Overview table for the "All Jobs" view — one row per registered job.
 *  Plain click selects the job; cmd/ctrl/middle-click opens its chat. */
export function JobsOverview() {
  const { jobs, selectJob } = useCronStore();

  if (jobs.length === 0) return null;

  return (
    <div className="mx-4 mt-4 border border-border-subtle rounded-lg overflow-y-auto max-h-[45vh] shrink-0">
      <table className="w-full text-[13px]">
        <thead className="sticky top-0 bg-surface z-10">
          <tr className="text-text-muted">
            <th className="text-left px-3 py-2 font-medium">Job</th>
            <th className="text-left px-3 py-2 font-medium">Schedule</th>
            <th className="text-left px-3 py-2 font-medium hidden md:table-cell">Last Run</th>
            <th className="text-left px-3 py-2 font-medium">Next Run</th>
            <th className="w-20" aria-label="Actions" />
          </tr>
        </thead>
        <tbody>
          {jobs.map(job => <JobRow key={job.id} job={job} onSelect={selectJob} />)}
        </tbody>
      </table>
    </div>
  );
}

function JobRow({ job, onSelect }: { job: CronJob; onSelect: (id: string) => void }) {
  const openChat = () => {
    if (job.last_session_id) {
      window.open(chatPath(job.last_session_id), '_blank', 'noopener');
    }
  };

  return (
    <tr
      className={`group border-t border-border-subtle hover:bg-surface cursor-pointer ${!job.enabled ? 'opacity-50' : ''}`}
      title={job.description || job.id}
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey) { openChat(); return; }
        onSelect(job.id);
      }}
      onAuxClick={(e) => { if (e.button === 1) openChat(); }}>
      <td className="px-3 py-2">
        <span className="flex items-center gap-2 min-w-0">
          <JobTypeIcon type={job.type} />
          <span className="truncate text-text-secondary">{jobLabel(job)}</span>
          {!job.enabled && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-border-subtle/50 text-text-muted border border-border-subtle">disabled</span>
          )}
        </span>
      </td>
      <td className="px-3 py-2 text-text-muted whitespace-nowrap">{formatSchedule(job.schedule)}</td>
      <td className="px-3 py-2 hidden md:table-cell">
        {job.last_run ? (
          <span className="flex items-center gap-2">
            <StatusBadge status={job.last_run.status} />
            {job.last_run.started_at && (
              <span className="text-text-dim text-[12px]">{formatRelativeTime(job.last_run.started_at)}</span>
            )}
          </span>
        ) : (
          <span className="text-text-faint">never</span>
        )}
      </td>
      <td className="px-3 py-2 text-text-muted whitespace-nowrap">
        {job.next_run ? formatRelativeTime(job.next_run) : '—'}
      </td>
      <td className="px-1 py-2">
        <span className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          {job.last_session_id && <ChatLink sessionId={job.last_session_id} small />}
          {job.enabled && <TriggerButton jobId={job.id} small />}
        </span>
      </td>
    </tr>
  );
}
