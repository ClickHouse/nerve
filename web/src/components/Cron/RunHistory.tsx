import { useId, useState } from 'react';
import { useCronStore, LOGS_PAGE_SIZE, type CronLog, type CronRunStatus } from '../../stores/cronStore';
import { Checkbox, IconButton } from '../ui';
import { ChevronLeft, ChevronRight, Loader2 } from '../ui/icons';
import { formatDuration, formatRunTime, parseUTC } from './utils';
import { ChatLink, RunStatusIcon } from './controls';

/**
 * The runs of the selected job (or of all jobs), newest first: ✓, ✗ or −,
 * the start, the duration, the start of the output or error, and the run's
 * session. A row with output opens to show all of it.
 *
 * Source runs that found nothing new are left out until "Show runs with no new
 * items" is on: a source that polls every few minutes otherwise fills the
 * first pages with them.
 */
export function RunHistory({ showJob }: { showJob: boolean }) {
  const { logs, loading, logsEmpty, showEmpty, setShowEmpty } = useCronStore();

  let body;
  if (loading && logs.length === 0) {
    body = (
      <div className="py-10 flex justify-center text-text-faint">
        <Loader2 size={20} className="animate-spin" aria-label="Loading runs" />
      </div>
    );
  } else if (logs.length === 0) {
    body = (
      <p className="py-10 text-center text-sm text-text-faint">
        {logsEmpty > 0 && !showEmpty ? 'No runs with new items.' : 'No runs yet.'}
      </p>
    );
  } else {
    body = (
      <ul className="divide-y divide-border-subtle border-y border-border-subtle">
        {logs.map(log => <RunRow key={log.id} log={log} showJob={showJob} />)}
      </ul>
    );
  }

  return (
    <section aria-labelledby="cron-history-title" className="flex flex-col">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 px-1 pb-2">
        <h2 id="cron-history-title" className="text-sm font-medium text-text">History</h2>
        {logsEmpty > 0 && (
          <Checkbox
            label={`Show runs with no new items (${logsEmpty.toLocaleString()})`}
            labelSize="xs" labelTone="muted"
            checked={showEmpty}
            onChange={(e) => setShowEmpty(e.target.checked)}
          />
        )}
      </div>
      {body}
      <HistoryPagination />
    </section>
  );
}

function HistoryPagination() {
  const { logsTotal, logsOffset, loading, setLogsPage } = useCronStore();

  if (logsTotal <= LOGS_PAGE_SIZE) return null;

  const from = logsOffset + 1;
  const to = Math.min(logsOffset + LOGS_PAGE_SIZE, logsTotal);
  const hasPrev = logsOffset > 0;
  const hasNext = to < logsTotal;

  return (
    <div className="px-1 py-1.5 flex items-center justify-end gap-2">
      <span className="text-xs text-text-dim tabular-nums">
        {from}–{to} of {logsTotal}
      </span>
      <IconButton label="Previous page" size="xs" disabled={!hasPrev || loading}
        onClick={() => setLogsPage(logsOffset - LOGS_PAGE_SIZE)}>
        <ChevronLeft size={14} />
      </IconButton>
      <IconButton label="Next page" size="xs" disabled={!hasNext || loading}
        onClick={() => setLogsPage(logsOffset + LOGS_PAGE_SIZE)}>
        <ChevronRight size={14} />
      </IconButton>
    </div>
  );
}

/** A run with a start but no end and no status is still going. */
function runStatus(log: CronLog): CronRunStatus {
  if (log.status === 'success' || log.status === 'error' || log.status === 'missed') return log.status;
  return log.finished_at ? 'missed' : null;
}

function RunRow({ log, showJob }: { log: CronLog; showJob: boolean }) {
  const [open, setOpen] = useState(false);
  const detailId = useId();
  const status = runStatus(log);
  const text = log.error || log.output || '';
  const duration = status === 'missed' || !log.finished_at
    ? '' : formatDuration(log.started_at, log.finished_at);

  // On a phone the job and the start of the output go on a second line,
  // under the time.
  const row = 'flex-1 min-w-0 flex flex-wrap sm:flex-nowrap items-center gap-x-3 gap-y-0.5 px-2 py-2 text-sm';
  const job = (shown: string) => (
    <span className={`${shown} w-32 shrink-0 truncate font-mono text-xs text-text-secondary`}>
      {log.job_id}
    </span>
  );
  const summary = (
    <>
      <RunStatusIcon status={status} />
      <span className="w-32 shrink-0 whitespace-nowrap text-text-muted tabular-nums"
        title={new Date(parseUTC(log.started_at)).toLocaleString()}>
        {formatRunTime(log.started_at)}
      </span>
      {showJob && job('hidden sm:block')}
      <span className="w-14 shrink-0 text-text-dim tabular-nums">{duration}</span>
      {(text || showJob) && (
        <span className="w-full sm:w-auto sm:flex-1 min-w-0 pl-[26px] sm:pl-0 flex items-center gap-3">
          {showJob && job('sm:hidden')}
          <span className={`min-w-0 truncate ${status === 'error' ? 'text-error/80' : 'text-text-dim'}`}>
            {text.split('\n')[0]}
          </span>
        </span>
      )}
    </>
  );

  return (
    <li>
      <div className="flex items-center gap-1 pr-1 hover:bg-surface">
        {text ? (
          <button type="button" onClick={() => setOpen(o => !o)}
            aria-expanded={open} aria-controls={detailId}
            className={`${row} text-left cursor-pointer rounded-sm`}>
            {summary}
          </button>
        ) : (
          <div className={row}>{summary}</div>
        )}
        <span className="w-7 shrink-0 flex justify-center">
          {log.session_id && <ChatLink sessionId={log.session_id} small />}
        </span>
      </div>
      {open && text && (
        <pre id={detailId}
          className="mx-2 mb-2 px-3 py-2 rounded bg-surface text-xs text-text-muted whitespace-pre-wrap break-words max-h-[240px] overflow-y-auto font-mono">
          {log.error ? `Error: ${log.error}` : log.output}
        </pre>
      )}
    </li>
  );
}
