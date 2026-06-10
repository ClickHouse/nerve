import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  RefreshCw, RotateCw, Play, Loader2, Clock, Inbox,
  CheckCircle2, XCircle, Timer, Filter, MessageSquare, FileText,
} from 'lucide-react';
import { useCronStore, type CronJob, type CronLog } from '../stores/cronStore';

/** Chat-page path for a session id. */
function chatPath(sessionId: string): string {
  return `/chat/${encodeURIComponent(sessionId)}`;
}

// --- Helpers ---

/** Parse a timestamp string as UTC. SQLite CURRENT_TIMESTAMP produces naive
 *  strings like "2026-03-03 05:00:00" without timezone — new Date() would treat
 *  those as local time. We detect the missing indicator and force UTC. */
function parseUTC(iso: string): number {
  if (!iso.includes('Z') && !iso.includes('+') && !iso.match(/T.*-/)) {
    return new Date(iso.replace(' ', 'T') + 'Z').getTime();
  }
  return new Date(iso).getTime();
}

function formatRelativeTime(iso: string): string {
  const diff = Date.now() - parseUTC(iso);
  if (diff < 0) {
    // Future time (e.g. next_run)
    const mins = Math.floor(-diff / 60000);
    if (mins < 1) return 'now';
    if (mins < 60) return `in ${mins}m`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `in ${hours}h`;
    const days = Math.floor(hours / 24);
    return `in ${days}d`;
  }
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatDuration(startIso: string, endIso: string | null): string {
  if (!endIso) return '—';
  const ms = parseUTC(endIso) - parseUTC(startIso);
  if (ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  const secs = ms / 1000;
  if (secs < 60) return `${secs.toFixed(1)}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = Math.floor(secs % 60);
  return `${mins}m ${remSecs}s`;
}

function formatSchedule(schedule: string): string {
  // Try to produce a human-readable label for common patterns
  if (/^\d+[hm]$/.test(schedule)) return `every ${schedule}`;
  if (/^\*\/(\d+) \* \* \* \*$/.test(schedule)) {
    const m = schedule.match(/^\*\/(\d+)/);
    return `every ${m![1]}m`;
  }
  if (/^0 (\d+) \* \* \*$/.test(schedule)) {
    const m = schedule.match(/^0 (\d+)/);
    return `daily at ${m![1]}:00`;
  }
  return schedule;
}

function jobTypeIcon(type: string) {
  switch (type) {
    case 'cron': return <Clock size={14} className="text-hue-amber" />;
    case 'source': return <Inbox size={14} className="text-hue-blue" />;
    default: return <Clock size={14} className="text-text-dim" />;
  }
}

function jobTypeBadge(type: string) {
  const styles: Record<string, string> = {
    cron: 'text-amber-600 bg-amber-500/15',
    source: 'text-blue-600 bg-blue-500/15',
  };
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded ${styles[type] || 'text-text-muted bg-surface-raised'}`}>
      {type}
    </span>
  );
}

/** Sidebar label: always the job name (id) — descriptions go in the tooltip. */
function jobLabel(job: CronJob): string {
  // Source runner ids are prefixed (e.g. "source:gmail:user@x") — strip it.
  return job.type === 'source' ? job.id.replace(/^source:/, '') : job.id;
}

// --- Trigger Button ---

function TriggerButton({ jobId, small = false }: { jobId: string; small?: boolean }) {
  const { triggering, triggerJob } = useCronStore();
  const isTriggering = triggering === jobId;

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (isTriggering) return;
    await triggerJob(jobId);
  };

  return (
    <button onClick={handleClick} disabled={isTriggering}
      onAuxClick={(e) => e.stopPropagation()}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer shrink-0
        ${isTriggering ? 'text-text-faint cursor-not-allowed' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}
        ${small ? 'p-1' : 'px-2 py-1.5 text-[12px]'}`}
      title="Trigger now">
      {isTriggering ? <Loader2 size={small ? 12 : 14} className="animate-spin" /> : <Play size={small ? 12 : 14} />}
      {!small && !isTriggering && <span>Run</span>}
    </button>
  );
}

// --- Chat Link ---

/** Link to a cron's chat session. Renders an anchor so cmd/ctrl+click and
 *  middle-click open a new tab natively. */
function ChatLink({ sessionId, small = false, label }: { sessionId: string; small?: boolean; label?: string }) {
  return (
    <Link to={chatPath(sessionId)}
      onClick={(e) => e.stopPropagation()}
      onAuxClick={(e) => e.stopPropagation()}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer shrink-0
        text-text-muted hover:text-text-secondary hover:bg-surface-raised
        ${small ? 'p-1' : 'px-2 py-1.5 text-[12px]'}`}
      title="Open chat">
      <MessageSquare size={small ? 12 : 14} />
      {!small && <span>{label || 'Chat'}</span>}
    </Link>
  );
}

// --- Rotate Button ---

function RotateButton({ jobId }: { jobId: string }) {
  const { rotating, rotateSession } = useCronStore();
  const isRotating = rotating === jobId;

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (isRotating) return;
    await rotateSession(jobId);
  };

  return (
    <button onClick={handleClick} disabled={isRotating}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer px-2 py-1.5 text-[12px]
        ${isRotating ? 'text-text-faint cursor-not-allowed' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}
      title="Rotate session context">
      {isRotating ? <Loader2 size={14} className="animate-spin" /> : <RotateCw size={14} />}
      {!isRotating && <span>Rotate</span>}
    </button>
  );
}

// --- Sidebar ---

function CronSidebar() {
  const { jobs, selectedJobId, selectJob } = useCronStore();

  return (
    <div className="w-[220px] border-r border-border-subtle flex flex-col shrink-0 overflow-y-auto">
      {/* Job list */}
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
                {jobTypeIcon(job.type)}
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

      {/* Schedule summary */}
      <div className="mt-auto border-t border-border-subtle p-3 space-y-1">
        <div className="text-[11px] text-text-dim flex items-center gap-1"><Clock size={10} /> Schedules</div>
        {jobs.slice(0, 6).map(job => (
          <div key={job.id} className="flex items-center justify-between text-[11px]">
            <span className="text-text-dim truncate max-w-[110px]">{job.id}</span>
            <span className="text-text-muted">{formatSchedule(job.schedule)}</span>
          </div>
        ))}
        {jobs.length > 6 && (
          <div className="text-[11px] text-text-faint">+{jobs.length - 6} more</div>
        )}
      </div>
    </div>
  );
}

// --- Job Info Card ---

function JobInfoCard({ job }: { job: CronJob }) {
  return (
    <div className="mx-4 mt-4 p-4 bg-surface border border-border-subtle rounded-lg">
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center gap-2 mb-1">
            {jobTypeBadge(job.type)}
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
            {jobTypeIcon(job.type)} {job.type}
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

// --- Logs Table ---

function LogsTable({ showJobColumn }: { showJobColumn: boolean }) {
  const { logs, loading } = useCronStore();

  if (loading && logs.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-faint">
        <Loader2 size={20} className="animate-spin" />
      </div>
    );
  }

  if (logs.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-faint text-sm">
        No runs recorded
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto">
      <table className="w-full text-[13px]">
        <thead className="sticky top-0 bg-bg">
          <tr className="text-text-muted">
            {showJobColumn && <th className="text-left px-3 py-2 font-medium">Job</th>}
            <th className="text-left px-3 py-2 font-medium">Started</th>
            <th className="text-left px-3 py-2 font-medium">Duration</th>
            <th className="text-left px-3 py-2 font-medium">Status</th>
            <th className="text-left px-3 py-2 font-medium">Output</th>
            <th className="w-8" aria-label="Chat" />
          </tr>
        </thead>
        <tbody>
          {logs.map(log => (
            <LogRow key={log.id} log={log} showJobColumn={showJobColumn} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LogRow({ log, showJobColumn }: { log: CronLog; showJobColumn: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const hasOutput = log.output || log.error;
  const preview = log.error
    ? log.error.slice(0, 80)
    : log.output
      ? log.output.slice(0, 80)
      : '';
  const isLong = (log.output?.length || 0) > 80 || (log.error?.length || 0) > 80;

  return (
    <>
      <tr className={`border-t border-border-subtle hover:bg-surface ${isLong ? 'cursor-pointer' : ''}`}
        onClick={() => isLong && setExpanded(!expanded)}>
        {showJobColumn && (
          <td className="px-3 py-2">
            <span className="text-text-secondary font-mono text-[12px]">{log.job_id}</span>
          </td>
        )}
        <td className="px-3 py-2 text-text-muted">{formatRelativeTime(log.started_at)}</td>
        <td className="px-3 py-2 text-text-dim">
          {!log.finished_at ? (
            <span className="flex items-center gap-1 text-hue-amber">
              <Loader2 size={12} className="animate-spin" /> running
            </span>
          ) : (
            formatDuration(log.started_at, log.finished_at)
          )}
        </td>
        <td className="px-3 py-2">
          {log.status === 'success' ? (
            <span className="flex items-center gap-1 text-hue-emerald"><CheckCircle2 size={12} /> ok</span>
          ) : log.status === 'error' ? (
            <span className="flex items-center gap-1 text-hue-red"><XCircle size={12} /> error</span>
          ) : (
            <span className="text-text-dim">{log.status || '—'}</span>
          )}
        </td>
        <td className="px-3 py-2 text-text-dim max-w-[300px]">
          <span className={`truncate block ${log.error ? 'text-hue-red/70' : ''}`}>
            {preview}{isLong && !expanded ? '…' : ''}
          </span>
        </td>
        <td className="px-1 py-2">
          {log.session_id && <ChatLink sessionId={log.session_id} small />}
        </td>
      </tr>
      {expanded && hasOutput && (
        <tr className="bg-surface">
          <td colSpan={showJobColumn ? 6 : 5} className="px-3 py-2">
            <pre className="text-[12px] text-text-muted whitespace-pre-wrap break-words max-h-[200px] overflow-y-auto font-mono">
              {log.error ? `Error: ${log.error}` : log.output}
            </pre>
          </td>
        </tr>
      )}
    </>
  );
}

// --- Main Page ---

export function CronPage() {
  const { jobs, selectedJobId, loadJobs, loadLogs, refresh } = useCronStore();
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    loadJobs();
    loadLogs();
  }, []);

  const handleRefresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    try { await refresh(); } finally { setRefreshing(false); }
  };

  const selectedJob = selectedJobId ? jobs.find(j => j.id === selectedJobId) : null;

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-border-subtle px-4 py-2.5 flex items-center justify-between bg-bg shrink-0">
        <h1 className="text-lg font-semibold">Cron Jobs</h1>
        <button onClick={handleRefresh} disabled={refreshing}
          className="text-text-dim hover:text-text-muted cursor-pointer p-1.5 hover:bg-surface-raised rounded"
          title="Refresh">
          {refreshing ? <Loader2 size={16} className="animate-spin" /> : <RefreshCw size={16} />}
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 flex min-h-0">
        <CronSidebar />

        <div className="flex-1 flex flex-col min-w-0">
          {selectedJob && <JobInfoCard job={selectedJob} />}
          <div className={`flex-1 flex flex-col min-h-0 ${selectedJob ? 'mt-2' : ''}`}>
            <LogsTable showJobColumn={selectedJobId === null} />
          </div>
        </div>
      </div>
    </div>
  );
}
