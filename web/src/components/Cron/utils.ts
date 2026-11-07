import cronstrue from 'cronstrue';
import type { CronJob, CronJobStats } from '../../stores/cronStore';

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

/** An interval schedule (`2h`, `30m`, `1h30m`, `0.5h`) in words, or null. */
export function formatSchedule(schedule: string): string | null {
  const compact = schedule.replace(/\s+/g, '');
  if (!/^(\d+(\.\d+)?[hms])+$/i.test(compact)) return null;
  return `Every ${compact.toLowerCase().replace(/([hms])(?=\d)/g, '$1 ')}`;
}

/** Whether a schedule is a 5-field crontab expression (not an interval). */
export function isCrontab(schedule: string): boolean {
  return schedule.trim().split(/\s+/).length === 5;
}

/**
 * A job's schedule in words: `cronstrue` for a crontab expression ("At 08:00"),
 * `formatSchedule` for an interval ("Every 2h"), or the schedule as written
 * when neither can read it.
 */
export function describeSchedule(schedule: string): string {
  if (isCrontab(schedule)) {
    try {
      return cronstrue.toString(schedule, { use24HourTimeFormat: true });
    } catch {
      return schedule;
    }
  }
  return formatSchedule(schedule) ?? schedule;
}

/** What the job's next run is forecast to do, or null when there is no forecast. */
export function formatForecast(job: Pick<CronJob, 'forecast' | 'forecast_reason'>): string | null {
  if (job.forecast === 'run') return 'Will run';
  if (job.forecast === 'skip') return job.forecast_reason ? `Will skip: ${job.forecast_reason}` : 'Will skip';
  return null;
}

/** A run's start as a short local date and time, for example "Nov 7, 8:04 AM". */
export function formatRunTime(iso: string): string {
  return new Date(parseUTC(iso)).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit',
  });
}

/** "6 of 7 succeeded" over the finished runs of the last 7 days, or null without runs. */
export function formatSuccess(stats: CronJobStats): string | null {
  const finished = stats.success + stats.error + stats.missed;
  return finished > 0 ? `${stats.success} of ${finished} succeeded` : null;
}

/** Display label: always the job name (id) — descriptions go in tooltips. */
export function jobLabel(job: CronJob): string {
  // Source runner ids are prefixed (e.g. "source:gmail:user@x") — strip it.
  return job.type === 'source' ? job.id.replace(/^source:/, '') : job.id;
}
