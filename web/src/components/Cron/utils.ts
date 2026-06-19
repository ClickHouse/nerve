import type { CronJob } from '../../stores/cronStore';

/** Chat-page path for a session id. */
export function chatPath(sessionId: string): string {
  return `/chat/${encodeURIComponent(sessionId)}`;
}

/** Parse a timestamp string as UTC. SQLite CURRENT_TIMESTAMP produces naive
 *  strings like "2026-03-03 05:00:00" without timezone — new Date() would treat
 *  those as local time. We detect the missing indicator and force UTC. */
export function parseUTC(iso: string): number {
  if (!iso.includes('Z') && !iso.includes('+') && !iso.match(/T.*-/)) {
    return new Date(iso.replace(' ', 'T') + 'Z').getTime();
  }
  return new Date(iso).getTime();
}

export function formatRelativeTime(iso: string): string {
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

export function formatDuration(startIso: string, endIso: string | null): string {
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

export function formatSchedule(schedule: string): string {
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

/** Display label: always the job name (id) — descriptions go in tooltips. */
export function jobLabel(job: CronJob): string {
  // Source runner ids are prefixed (e.g. "source:gmail:user@x") — strip it.
  return job.type === 'source' ? job.id.replace(/^source:/, '') : job.id;
}
