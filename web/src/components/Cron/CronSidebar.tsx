import { Timer } from 'lucide-react';
import { useCronStore } from '../../stores/cronStore';
import { chatPath, jobLabel } from './utils';
import { ChatLink, TriggerButton, JobTypeIcon } from './controls';

export function CronSidebar() {
  const { jobs, selectedJobId, selectJob } = useCronStore();

  return (
    <div className="w-[220px] border-r border-border-subtle flex flex-col shrink-0 overflow-y-auto">
      <div className="p-2 space-y-1">
        <button onClick={() => selectJob(null)}
          className={`w-full flex items-center justify-between px-2 py-1.5 rounded text-[13px] transition-colors cursor-pointer
            ${selectedJobId === null ? 'bg-accent/15 text-accent' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}>
          <span className="flex items-center gap-2"><Timer size={14} /> All Jobs</span>
          <span className="text-[11px] opacity-70">{jobs.length}</span>
        </button>

        {jobs.map(job => (
          <div key={job.id} className={`group ${!job.enabled ? 'opacity-50' : ''}`}>
            <button title={job.description || job.id}
              onClick={(e) => {
                // Cmd/ctrl+click opens the cron's chat in a new tab;
                // plain click keeps the in-page select behaviour.
                if ((e.metaKey || e.ctrlKey) && job.last_session_id) {
                  window.open(chatPath(job.last_session_id), '_blank', 'noopener');
                  return;
                }
                selectJob(job.id);
              }}
              onAuxClick={(e) => {
                // Middle-click → new tab, like a link.
                if (e.button === 1 && job.last_session_id) {
                  window.open(chatPath(job.last_session_id), '_blank', 'noopener');
                }
              }}
              className={`w-full flex items-center justify-between px-2 py-1.5 rounded text-[13px] transition-colors cursor-pointer
                ${selectedJobId === job.id ? 'bg-accent/15 text-accent' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}>
              <span className="flex items-center gap-2 min-w-0 truncate">
                <JobTypeIcon type={job.type} />
                <span className="truncate">{jobLabel(job)}</span>
              </span>
              <span className="flex items-center gap-1 shrink-0">
                {job.last_session_id && (
                  <span className="opacity-0 group-hover:opacity-100 transition-opacity">
                    <ChatLink sessionId={job.last_session_id} small />
                  </span>
                )}
                {job.enabled && (
                  <span className="opacity-0 group-hover:opacity-100 transition-opacity">
                    <TriggerButton jobId={job.id} small />
                  </span>
                )}
              </span>
            </button>
          </div>
        ))}

        {jobs.length === 0 && (
          <div className="text-[12px] text-text-faint text-center py-4">No jobs configured</div>
        )}
      </div>
    </div>
  );
}
