import { Filter, FileText } from 'lucide-react';
import type { CronJob } from '../../stores/cronStore';
import { formatRelativeTime, formatSchedule } from './utils';
import { ChatLink, JobTypeBadge, JobTypeIcon, RotateButton, TriggerButton } from './controls';

export function JobInfoCard({ job }: { job: CronJob }) {
  return (
    <div className="mx-4 mt-4 p-4 bg-surface border border-border-subtle rounded-lg">
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <JobTypeBadge type={job.type} />
            <span className="text-[14px] text-text font-medium">{job.id}</span>
            {!job.enabled && (
              <span className="text-[10px] px-1.5 py-0.5 rounded bg-border-subtle/50 text-text-muted border border-border-subtle">disabled</span>
            )}
          </div>
          {job.description && (
            <div className="text-[13px] text-text-muted mt-1">{job.description}</div>
          )}
          {job.prompt_file && (
            <div className="text-[11px] text-text-dim mt-1 flex items-center gap-1 font-mono">
              <FileText size={11} /> {job.prompt_file}
            </div>
          )}
        </div>
        <div className="flex items-center gap-1">
          {job.last_session_id && <ChatLink sessionId={job.last_session_id} label="Open Chat" />}
          {job.enabled && job.session_mode === 'persistent' && <RotateButton jobId={job.id} />}
          {job.enabled && <TriggerButton jobId={job.id} />}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
        <div>
          <div className="text-[11px] text-text-dim mb-0.5">Schedule</div>
          <div className="text-[13px] text-text-secondary">{formatSchedule(job.schedule)}</div>
          <div className="text-[11px] text-text-faint font-mono">{job.schedule}</div>
        </div>
        <div>
          <div className="text-[11px] text-text-dim mb-0.5">Next Run</div>
          <div className="text-[13px] text-text-secondary">
            {job.next_run ? formatRelativeTime(job.next_run) : '—'}
          </div>
        </div>
        <div>
          <div className="text-[11px] text-text-dim mb-0.5">Type</div>
          <div className="flex items-center gap-1.5 text-[13px] text-text-secondary">
            <JobTypeIcon type={job.type} /> {job.type}
          </div>
        </div>
      </div>

      {job.gates && job.gates.length > 0 && (
        <div className="mt-3 pt-3 border-t border-border-subtle">
          <div className="text-[11px] text-text-dim mb-1.5 flex items-center gap-1">
            <Filter size={11} /> Runs only if{job.gates.length > 1 ? ' (all)' : ''}
          </div>
          <div className="flex flex-wrap gap-1.5">
            {job.gates.map((gate, i) => (
              <span key={i}
                className="text-[12px] px-2 py-0.5 rounded bg-surface-raised text-text-secondary border border-border-subtle">
                {gate}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
