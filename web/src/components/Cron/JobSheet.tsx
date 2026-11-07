import { useId, type ReactNode } from 'react';
import { useCronStore, type CronJob } from '../../stores/cronStore';
import { Badge, Switch } from '../ui';
import { Filter, FileText, Lock } from '../ui/icons';
import {
  describeSchedule, formatDuration, formatForecast, formatRelativeTime, formatRunTime,
  formatSuccess, isCrontab, jobLabel, parseUTC,
} from './utils';
import {
  ChatLink, JobTypeBadge, JobTypeIcon, RotateButton, RunStatusIcon, TriggerButton,
} from './controls';

/** One labelled value of the sheet, with an optional second line. */
function Fact({ label, children, detail }: { label: string; children: ReactNode; detail?: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-text-dim mb-0.5">{label}</dt>
      <dd className="text-sm text-text-secondary">{children}</dd>
      {detail && <dd className="text-xs text-text-faint mt-0.5 break-words">{detail}</dd>}
    </div>
  );
}

function absolute(iso: string): string {
  return new Date(parseUTC(iso)).toLocaleString();
}

/**
 * The sheet of one job: what it is, whether it is on, when it runs, and how
 * its recent runs went. The run history is below it, in `RunHistory`.
 */
export function JobSheet({ job }: { job: CronJob }) {
  const { toggling, toggleError, setJobEnabled } = useCronStore();
  const blockedId = useId();
  const schedule = describeSchedule(job.schedule);
  // For a job without gates the forecast is always "run": it adds nothing.
  const forecast = job.enabled && (job.forecast === 'skip' || (job.gates ?? []).length > 0)
    ? formatForecast(job) : null;
  const last = job.last_run;
  const success = formatSuccess(job.stats_7d);
  const failures = [
    job.stats_7d.error > 0 && `${job.stats_7d.error} failed`,
    job.stats_7d.missed > 0 && `${job.stats_7d.missed} missed`,
  ].filter(Boolean).join(', ');

  return (
    <section aria-labelledby={`${blockedId}-title`}
      className="p-4 bg-surface border border-border-subtle rounded-lg">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <JobTypeIcon kind={job.kind} />
            <h2 id={`${blockedId}-title`} className="text-base font-medium text-text truncate">
              {jobLabel(job)}
            </h2>
            <JobTypeBadge kind={job.kind} />
          </div>
          {job.description && <p className="text-sm text-text-muted mt-1">{job.description}</p>}
          {job.prompt_file && (
            <div className="text-xs text-text-dim mt-1 flex items-center gap-1 font-mono">
              <FileText size={11} /> {job.prompt_file}
            </div>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-1">
          {job.kind !== 'source' && (
            <Switch
              label="Active"
              checked={job.enabled}
              disabled={toggling === job.id || job.toggle_blocked !== null}
              aria-describedby={job.toggle_blocked ? blockedId : undefined}
              onChange={(enabled) => void setJobEnabled(job.id, enabled)}
              className="mr-2 px-1 py-1.5"
            />
          )}
          {job.last_session_id && <ChatLink sessionId={job.last_session_id} label="Open chat" />}
          {job.enabled && job.session_mode === 'persistent' && <RotateButton jobId={job.id} />}
          {job.enabled && <TriggerButton jobId={job.id} />}
        </div>
      </div>

      {job.kind !== 'source' && job.toggle_blocked && (
        <p id={blockedId} className="mt-2 text-xs text-text-dim flex items-start gap-1">
          <Lock size={11} className="mt-0.5 shrink-0" /> {job.toggle_blocked}
        </p>
      )}
      {toggleError?.jobId === job.id && (
        <p role="alert" className="mt-2 text-xs text-error">{toggleError.message}</p>
      )}

      <dl className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-x-4 gap-y-3">
        <Fact label="Schedule" detail={isCrontab(job.schedule) && schedule !== job.schedule
          ? <span className="font-mono">{job.schedule}</span> : undefined}>
          {schedule}
        </Fact>
        <Fact label="Next run" detail={forecast && <span data-testid="cron-forecast">{forecast}</span>}>
          {!job.enabled ? 'Paused' : job.next_run ? (
            <span title={absolute(job.next_run)}>{formatRelativeTime(job.next_run)}</span>
          ) : 'Not scheduled'}
        </Fact>
        <Fact label="Last run" detail={last?.finished_at && last.status !== 'missed'
          ? `Took ${formatDuration(last.started_at, last.finished_at)}` : undefined}>
          {last ? (
            <span className="flex items-center gap-1.5" data-testid="cron-last-run">
              <RunStatusIcon status={last.status} />
              <span title={absolute(last.started_at)}>
                {last.status === null ? 'Running' : formatRunTime(last.started_at)}
              </span>
            </span>
          ) : 'No runs yet'}
        </Fact>
        <Fact label="Last 7 days" detail={failures || undefined}>
          <span data-testid="cron-success">{success ?? 'No runs'}</span>
        </Fact>
      </dl>

      {job.gates && job.gates.length > 0 && (
        <div className="mt-3 pt-3 border-t border-border-subtle">
          <div className="text-xs text-text-dim mb-1.5 flex items-center gap-1">
            <Filter size={11} /> Runs only if{job.gates.length > 1 ? ' (all)' : ''}
          </div>
          <div className="flex flex-wrap gap-1.5">
            {job.gates.map((gate, i) => (
              // Outlined rather than filled: a gate is a condition, and the
              // filled chips beside it already mean "this job is of type X".
              <Badge key={i} size="sm" className="border border-border-subtle">
                {gate}
              </Badge>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
