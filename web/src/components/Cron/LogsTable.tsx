import { useState } from 'react';
import { useCronStore, LOGS_PAGE_SIZE, type CronLog } from '../../stores/cronStore';
import { IconButton } from '../ui';
import { ChevronLeft, ChevronRight, Loader2 } from '../ui/icons';
import { formatDuration, formatRelativeTime } from './utils';
import { ChatLink, StatusBadge } from './controls';

export function LogsTable({ showJobColumn }: { showJobColumn: boolean }) {
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
    <div className="flex-1 flex flex-col min-h-0">
      <div className="flex-1 overflow-y-auto">
        {/* `text-sm` carries a 20px line-height, which on `py-2` cells would
            add 4px to every row of a log that is often hundreds long.
            `leading-tight` (17.5px) keeps the run history as dense as the
            13px/normal it replaces. */}
        <table className="w-full text-sm leading-tight">
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
      <LogsPagination />
    </div>
  );
}

function LogsPagination() {
  const { logsTotal, logsOffset, loading, setLogsPage } = useCronStore();

  if (logsTotal <= LOGS_PAGE_SIZE) return null;

  const from = logsOffset + 1;
  const to = Math.min(logsOffset + LOGS_PAGE_SIZE, logsTotal);
  const hasPrev = logsOffset > 0;
  const hasNext = to < logsTotal;

  return (
    <div className="border-t border-border-subtle px-3 py-1.5 flex items-center justify-end gap-2 shrink-0 bg-bg">
      <span className="text-xs text-text-dim tabular-nums">
        {from}–{to} of {logsTotal}
      </span>
      {/* The `btn(enabled)` helper this replaces existed only to hand-roll a
          disabled colour; IconButton carries that treatment itself. */}
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
            <span className="text-text-secondary font-mono text-xs">{log.job_id}</span>
          </td>
        )}
        <td className="px-3 py-2 text-text-muted whitespace-nowrap">{formatRelativeTime(log.started_at)}</td>
        <td className="px-3 py-2 text-text-dim">
          {!log.finished_at ? (
            <span className="flex items-center gap-1 text-warning">
              <Loader2 size={12} className="animate-spin" /> running
            </span>
          ) : (
            formatDuration(log.started_at, log.finished_at)
          )}
        </td>
        <td className="px-3 py-2">
          {log.status ? <StatusBadge status={log.status} /> : <span className="text-text-dim">—</span>}
        </td>
        <td className="px-3 py-2 text-text-dim max-w-[300px]">
          <span className={`truncate block ${log.error ? 'text-error/70' : ''}`}>
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
            <pre className="text-xs text-text-muted whitespace-pre-wrap break-words max-h-[200px] overflow-y-auto font-mono">
              {log.error ? `Error: ${log.error}` : log.output}
            </pre>
          </td>
        </tr>
      )}
    </>
  );
}
