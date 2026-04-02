import { useEffect, useState, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  RefreshCw, Play, Loader2, Mail, Github, MessageCircle, Inbox,
  ExternalLink, Trash2, ChevronDown, ChevronRight,
  CheckCircle2, XCircle, HardDrive, Database, Filter, AlertTriangle,
} from 'lucide-react';
import { useSourcesStore, type SourceOverviewEntry } from '../stores/sourcesStore';
import { MessageContent } from '../components/Sources/MessageContent';

// --- Helpers ---

function formatRelativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function sourceIcon(source: string) {
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return <Mail size={14} className="text-red-400" />;
    case 'github': return <Github size={14} className="text-purple-400" />;
    case 'telegram': return <MessageCircle size={14} className="text-blue-400" />;
    default: return <Inbox size={14} className="text-text-dim" />;
  }
}

function sourceBadgeColor(source: string): string {
  const type = source.split(':')[0];
  switch (type) {
    case 'gmail': return 'text-red-400 bg-red-950/30';
    case 'github': return 'text-purple-400 bg-purple-950/30';
    case 'telegram': return 'text-blue-400 bg-blue-950/30';
    default: return 'text-text-muted bg-surface-raised';
  }
}

function sourceLabel(source: string): string {
  const type = source.split(':')[0];
  // For gmail:<account>, show just the account
  if (source.includes(':')) {
    const rest = source.slice(source.indexOf(':') + 1);
    return rest.length > 20 ? rest.slice(0, 18) + '..' : rest;
  }
  return type;
}

// --- Sync Button ---

function SyncButton({ onClick, small = false }: { onClick: () => Promise<void>; small?: boolean }) {
  const [running, setRunning] = useState(false);
  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (running) return;
    setRunning(true);
    try { await onClick(); } finally { setRunning(false); }
  };
  return (
    <button onClick={handleClick} disabled={running}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer shrink-0
        ${running ? 'text-text-faint cursor-not-allowed' : 'text-text-dim hover:text-text-secondary hover:bg-surface-raised'}
        ${small ? 'p-1' : 'px-2 py-1.5 text-[12px]'}`}
      title="Sync now">
      {running ? <Loader2 size={small ? 12 : 14} className="animate-spin" /> : <Play size={small ? 12 : 14} />}
      {!small && !running && <span>Sync</span>}
    </button>
  );
}

// --- Health Badge ---

function HealthBadge({ state }: { state: 'healthy' | 'degraded' | 'open' | undefined }) {
  if (!state || state === 'healthy') return null;

  const config = {
    degraded: { Icon: AlertTriangle, color: 'text-amber-400', bg: 'bg-amber-950/20', label: 'degraded' },
    open:     { Icon: XCircle,       color: 'text-red-400',   bg: 'bg-red-950/20',   label: 'circuit open' },
  }[state];
  if (!config) return null;

  const { Icon } = config;
  return (
    <span className={`flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded ${config.color} ${config.bg}`}
          title={`Source is ${config.label} — check runs tab for errors`}>
      <Icon size={10} />
      {config.label}
    </span>
  );
}

// --- Sidebar ---

function SourceSidebar() {
  const { overview, activeSource, setActiveSource, activeTab, setActiveTab, syncSource, purgeMessages, consumers, sourceHealth } = useSourcesStore();
  const [purgeConfirm, setPurgeConfirm] = useState<string | null>(null);

  const sources = overview?.sources || {};
  const totalMessages = overview?.total_messages || 0;
  const totalStorage = overview?.total_storage_bytes || 0;

  return (
    <div className="w-[220px] border-r border-border-subtle flex flex-col shrink-0 overflow-y-auto">
      {/* Tab toggle */}
      <div className="flex border-b border-border-subtle">
        {(['inbox', 'runs', 'consumers'] as const).map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)}
            className={`flex-1 py-2 text-[12px] font-medium transition-colors cursor-pointer
              ${activeTab === tab ? 'text-[#6366f1] border-b-2 border-[#6366f1]' : 'text-text-dim hover:text-text-muted'}`}>
            {tab === 'inbox' ? 'Inbox' : tab === 'runs' ? 'Runs' : 'Consumers'}
          </button>
        ))}
      </div>

      {/* Source list */}
      <div className="p-2 space-y-1">
        <button onClick={() => setActiveSource(null)}
          className={`w-full flex items-center justify-between px-2 py-1.5 rounded text-[13px] transition-colors cursor-pointer
            ${activeSource === null ? 'bg-[#6366f1]/15 text-[#6366f1]' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}>
          <span className="flex items-center gap-2"><Inbox size={14} /> All</span>
          <span className="text-[11px] opacity-70 tabular-nums shrink-0">{totalMessages}</span>
        </button>

        {Object.entries(sources).map(([src, info]) => {
          const unread = consumers.find(c => c.consumer === 'inbox' && c.source === src)?.unread || 0;
          return (
            <div key={src} className="group">
              <button onClick={() => setActiveSource(src)}
                className={`w-full flex items-center justify-between px-2 py-1.5 rounded text-[13px] transition-colors cursor-pointer
                  ${activeSource === src ? 'bg-[#6366f1]/15 text-[#6366f1]' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}>
                <span className="flex items-center gap-1.5 min-w-0 truncate">
                  {sourceIcon(src)}
                  <span className="truncate">{sourceLabel(src)}</span>
                  <HealthBadge state={sourceHealth?.[src]?.state} />
                </span>
                <span className="shrink-0 flex items-center gap-1.5">
                  {unread > 0 && (
                    <span className="text-[10px] tabular-nums bg-amber-500/20 text-amber-400 px-1 py-0.5 rounded-full leading-none font-medium">
                      {unread}
                    </span>
                  )}
                  <span className="relative w-6 h-5 flex items-center justify-end">
                    <span className="text-[11px] opacity-70 tabular-nums group-hover:opacity-0 transition-opacity">
                      {info.message_count}
                    </span>
                    <span className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                      <SyncButton onClick={() => syncSource(src)} small />
                    </span>
                  </span>
                </span>
              </button>
            </div>
          );
        })}
      </div>

      {/* Storage section */}
      <div className="mt-auto border-t border-border-subtle p-3 space-y-2">
        <div className="flex items-center justify-between">
          <span className="text-[11px] text-text-dim flex items-center gap-1"><HardDrive size={11} /> Storage</span>
          <span className="text-[12px] text-text-muted">{formatBytes(totalStorage)}</span>
        </div>
        {Object.entries(sources).filter(([, s]) => s.storage_bytes > 0).map(([src, s]) => (
          <div key={src} className="flex items-center justify-between text-[11px]">
            <span className="text-text-dim truncate">{sourceLabel(src)}</span>
            <span className="text-text-muted">{formatBytes(s.storage_bytes)}</span>
          </div>
        ))}

        {/* Purge buttons */}
        <div className="flex gap-1 pt-1">
          {activeSource ? (
            <button onClick={() => {
              if (purgeConfirm === activeSource) { purgeMessages(activeSource); setPurgeConfirm(null); }
              else setPurgeConfirm(activeSource);
            }}
              className={`flex-1 flex items-center justify-center gap-1 px-2 py-1 text-[11px] rounded transition-colors cursor-pointer
                ${purgeConfirm === activeSource ? 'bg-red-900/30 text-red-400 border border-red-900/50' : 'text-text-dim hover:text-red-400 bg-surface border border-border-subtle'}`}>
              <Trash2 size={10} /> {purgeConfirm === activeSource ? 'Confirm?' : 'Purge source'}
            </button>
          ) : (
            <button onClick={() => {
              if (purgeConfirm === '_all') { purgeMessages(); setPurgeConfirm(null); }
              else setPurgeConfirm('_all');
            }}
              className={`flex-1 flex items-center justify-center gap-1 px-2 py-1 text-[11px] rounded transition-colors cursor-pointer
                ${purgeConfirm === '_all' ? 'bg-red-900/30 text-red-400 border border-red-900/50' : 'text-text-dim hover:text-red-400 bg-surface border border-border-subtle'}`}>
              <Trash2 size={10} /> {purgeConfirm === '_all' ? 'Confirm purge all?' : 'Purge all'}
            </button>
          )}
        </div>

        {/* Stats */}
        {activeSource && sources[activeSource] ? (
          <SourceStats info={sources[activeSource]} />
        ) : overview ? (
          <AggregateStats sources={sources} />
        ) : null}

        {/* Health summary */}
        {sourceHealth && Object.values(sourceHealth).some(h => h.state !== 'healthy') && (
          <div className="text-[11px] text-amber-400 flex items-center gap-1 pt-1">
            <AlertTriangle size={10} />
            {Object.values(sourceHealth).filter(h => h.state !== 'healthy').length} source(s) unhealthy
          </div>
        )}
      </div>
    </div>
  );
}

function SourceStats({ info }: { info: SourceOverviewEntry }) {
  return (
    <div className="space-y-1 pt-1">
      <div className="text-[11px] text-text-dim flex items-center gap-1"><Database size={10} /> Stats</div>
      <div className="grid grid-cols-2 gap-x-3 text-[11px]">
        <span className="text-text-dim">1h runs</span><span className="text-text-muted">{info.stats_1h.runs}</span>
        <span className="text-text-dim">1h fetched</span><span className="text-text-muted">{info.stats_1h.fetched}</span>
        <span className="text-text-dim">24h runs</span><span className="text-text-muted">{info.stats_24h.runs}</span>
        <span className="text-text-dim">24h fetched</span><span className="text-text-muted">{info.stats_24h.fetched}</span>
        {info.stats_24h.errors > 0 && <>
          <span className="text-text-dim">24h errors</span><span className="text-red-400">{info.stats_24h.errors}</span>
        </>}
      </div>
    </div>
  );
}

function AggregateStats({ sources }: { sources: Record<string, SourceOverviewEntry> }) {
  const totals = Object.values(sources).reduce(
    (acc, s) => ({
      runs_1h: acc.runs_1h + s.stats_1h.runs,
      fetched_1h: acc.fetched_1h + s.stats_1h.fetched,
      runs_24h: acc.runs_24h + s.stats_24h.runs,
      fetched_24h: acc.fetched_24h + s.stats_24h.fetched,
      errors_24h: acc.errors_24h + s.stats_24h.errors,
    }),
    { runs_1h: 0, fetched_1h: 0, runs_24h: 0, fetched_24h: 0, errors_24h: 0 },
  );
  return (
    <div className="space-y-1 pt-1">
      <div className="text-[11px] text-text-dim flex items-center gap-1"><Database size={10} /> Stats (all)</div>
      <div className="grid grid-cols-2 gap-x-3 text-[11px]">
        <span className="text-text-dim">1h runs</span><span className="text-text-muted">{totals.runs_1h}</span>
        <span className="text-text-dim">1h fetched</span><span className="text-text-muted">{totals.fetched_1h}</span>
        <span className="text-text-dim">24h runs</span><span className="text-text-muted">{totals.runs_24h}</span>
        <span className="text-text-dim">24h fetched</span><span className="text-text-muted">{totals.fetched_24h}</span>
        {totals.errors_24h > 0 && <>
          <span className="text-text-dim">24h errors</span><span className="text-red-400">{totals.errors_24h}</span>
        </>}
      </div>
    </div>
  );
}

// --- Message List ---

function MessageList() {
  const { messages, selectedMessage, loading, hasMore, loadMore, selectMessage } = useSourcesStore();
  const listRef = useRef<HTMLDivElement>(null);

  const handleScroll = useCallback(() => {
    const el = listRef.current;
    if (!el || !hasMore || loading) return;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 100) {
      loadMore();
    }
  }, [hasMore, loading, loadMore]);

  if (loading && messages.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>;
  }

  if (messages.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint text-sm">No messages</div>;
  }

  return (
    <div ref={listRef} onScroll={handleScroll} className="flex-1 overflow-y-auto">
      {messages.map((msg) => {
        const isSelected = selectedMessage?.id === msg.id && selectedMessage?.source === msg.source;
        return (
          <button key={`${msg.source}:${msg.id}`} onClick={() => selectMessage(msg.source, msg.id)}
            className={`w-full text-left px-3 py-2.5 border-b border-border-subtle transition-colors cursor-pointer
              ${isSelected ? 'bg-[#6366f1]/10 border-l-2 border-l-[#6366f1]' : 'hover:bg-surface'}`}>
            <div className="flex items-center gap-2 mb-0.5">
              <span className={`text-[10px] px-1.5 py-0.5 rounded ${sourceBadgeColor(msg.source)}`}>
                {msg.source.split(':')[0]}
              </span>
              <span className="text-[11px] text-text-faint ml-auto">{formatRelativeTime(msg.timestamp)}</span>
            </div>
            <div className="text-[13px] text-text-secondary truncate">{msg.summary}</div>
          </button>
        );
      })}
      {loading && (
        <div className="flex items-center justify-center py-3 text-text-faint">
          <Loader2 size={16} className="animate-spin" />
        </div>
      )}
    </div>
  );
}

// --- Runs List ---

function RunsList() {
  const { runs, selectedRun, selectRun, loading, activeSource, sourceHealth } = useSourcesStore();
  const [hideEmpty, setHideEmpty] = useState(true);

  const filteredRuns = hideEmpty
    ? runs.filter(r => r.records_fetched > 0 || r.error)
    : runs;

  if (loading && runs.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>;
  }

  if (runs.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint text-sm">No runs recorded</div>;
  }

  const healthEntry = activeSource ? sourceHealth?.[activeSource] : null;
  const isUnhealthy = healthEntry && healthEntry.state !== 'healthy';

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Health info card */}
      {isUnhealthy && (
        <div className={`mx-3 mt-2 mb-1 p-2 rounded text-[12px] ${
          healthEntry.state === 'open'
            ? 'bg-red-950/20 border border-red-900/30'
            : 'bg-amber-950/20 border border-amber-900/30'
        }`}>
          <div className={`flex items-center gap-1.5 font-medium mb-1 ${
            healthEntry.state === 'open' ? 'text-red-400' : 'text-amber-400'
          }`}>
            <AlertTriangle size={12} />
            Circuit breaker: {healthEntry.state}
          </div>
          <div className="text-text-muted">
            {healthEntry.consecutive_failures} consecutive failure{healthEntry.consecutive_failures !== 1 ? 's' : ''}
            {healthEntry.backoff_until && (
              <> · Backoff until {new Date(healthEntry.backoff_until).toLocaleTimeString()}</>
            )}
          </div>
          {healthEntry.last_error && (
            <div className="mt-1 text-[11px] text-text-dim truncate">
              Last error: {healthEntry.last_error}
            </div>
          )}
        </div>
      )}

      {/* Filter bar */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b border-border-subtle shrink-0">
        <span className="text-[11px] text-text-dim">
          {filteredRuns.length}{hideEmpty && filteredRuns.length !== runs.length ? ` of ${runs.length}` : ''} runs
        </span>
        <label className="flex items-center gap-1.5 text-[11px] text-text-dim cursor-pointer select-none">
          <Filter size={10} />
          <input
            type="checkbox"
            checked={hideEmpty}
            onChange={(e) => setHideEmpty(e.target.checked)}
            className="accent-[#6366f1] cursor-pointer"
          />
          Hide empty
        </label>
      </div>

      <div className="flex-1 overflow-y-auto">
        <table className="w-full text-[13px]">
          <thead className="sticky top-0 bg-bg">
            <tr className="text-text-muted">
              <th className="text-left px-3 py-2 font-medium">Source</th>
              <th className="text-left px-3 py-2 font-medium">Time</th>
              <th className="text-left px-3 py-2 font-medium">F/P</th>
              <th className="text-left px-3 py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {filteredRuns.map((run) => {
              const isSelected = selectedRun?.id === run.id;
              return (
                <tr key={run.id} onClick={() => selectRun(run)}
                  className={`border-t border-border-subtle cursor-pointer transition-colors
                    ${isSelected ? 'bg-[#6366f1]/10' : 'hover:bg-surface'}`}>
                  <td className="px-3 py-2">
                    <span className="flex items-center gap-1.5">
                      {sourceIcon(run.source)}
                      <span className="text-text-secondary truncate max-w-[120px]">{sourceLabel(run.source)}</span>
                    </span>
                  </td>
                  <td className="px-3 py-2 text-text-dim">{formatRelativeTime(run.ran_at)}</td>
                  <td className="px-3 py-2 text-text-muted">{run.records_fetched}/{run.records_processed}</td>
                  <td className="px-3 py-2">
                    {run.error ? (
                      <span className="flex items-center gap-1 text-red-400"><XCircle size={12} /> error</span>
                    ) : (
                      <span className="flex items-center gap-1 text-emerald-400"><CheckCircle2 size={12} /> ok</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- Run Detail ---

function RunDetail() {
  const { selectedRun, selectedRunMessages, detailLoading } = useSourcesStore();
  const navigate = useNavigate();

  if (detailLoading) {
    return <div className="flex-1 flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>;
  }

  if (!selectedRun) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-faint">
        <div className="text-center">
          <Play size={32} className="mx-auto mb-2 opacity-30" />
          <div className="text-sm">Select a run to view details</div>
        </div>
      </div>
    );
  }

  const run = selectedRun;

  return (
    <div className="flex-1 overflow-y-auto">
      {/* Run header */}
      <div className="px-4 py-3 border-b border-border-subtle bg-bg">
        <div className="flex items-center gap-2 mb-1">
          {sourceIcon(run.source)}
          <span className="text-[14px] text-text-secondary font-medium">{sourceLabel(run.source)}</span>
          {run.error ? (
            <span className="flex items-center gap-1 text-[11px] text-red-400"><XCircle size={11} /> error</span>
          ) : (
            <span className="flex items-center gap-1 text-[11px] text-emerald-400"><CheckCircle2 size={11} /> ok</span>
          )}
        </div>
        <div className="text-[12px] text-text-dim">{new Date(run.ran_at).toLocaleString()}</div>

        {/* Stats */}
        <div className="flex gap-4 mt-2">
          <span className="text-[12px]">
            <span className="text-text-dim">Fetched:</span>{' '}
            <span className="text-text-muted">{run.records_fetched}</span>
          </span>
          <span className="text-[12px]">
            <span className="text-text-dim">Processed:</span>{' '}
            <span className="text-text-muted">{run.records_processed}</span>
          </span>
        </div>

        {/* Error message */}
        {run.error && (
          <div className="mt-2 p-2 bg-red-950/20 border border-red-900/30 rounded text-[12px] text-red-400">
            {run.error}
          </div>
        )}

        {/* Session link */}
        {run.session_id && (
          <button onClick={() => navigate(`/chat/${run.session_id}`)}
            className="flex items-center gap-1.5 mt-2 text-[12px] text-[#6366f1] hover:text-[#818cf8] transition-colors cursor-pointer">
            <ExternalLink size={12} /> View processing session
          </button>
        )}
      </div>

      {/* Consumed messages */}
      <div className="px-4 py-3">
        <div className="text-[12px] text-text-dim mb-2">
          {selectedRunMessages.length > 0
            ? `${selectedRunMessages.length} message${selectedRunMessages.length > 1 ? 's' : ''} consumed`
            : run.session_id ? 'No linked messages' : 'No session linked to this run'}
        </div>
        {selectedRunMessages.map((msg) => (
          <div key={`${msg.source}:${msg.id}`}
            className="flex items-start gap-2 px-2 py-2 rounded hover:bg-surface transition-colors border-b border-border-subtle last:border-0">
            <span className={`text-[10px] px-1.5 py-0.5 rounded shrink-0 mt-0.5 ${sourceBadgeColor(msg.source)}`}>
              {msg.record_type.replace('_', ' ')}
            </span>
            <div className="min-w-0 flex-1">
              <div className="text-[13px] text-text-secondary truncate">{msg.summary}</div>
              <div className="text-[11px] text-text-faint">{formatRelativeTime(msg.timestamp)}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Message Detail ---

function MessageDetail() {
  const { selectedMessage, detailLoading } = useSourcesStore();
  const navigate = useNavigate();
  const [showProcessed, setShowProcessed] = useState(false);

  if (detailLoading) {
    return <div className="flex-1 flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>;
  }

  if (!selectedMessage) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-faint">
        <div className="text-center">
          <Inbox size={32} className="mx-auto mb-2 opacity-30" />
          <div className="text-sm">Select a message to view</div>
        </div>
      </div>
    );
  }

  const msg = selectedMessage;
  const hasProcessedContent = msg.processed_content && msg.processed_content !== msg.content;

  return (
    <div className="flex-1 overflow-y-auto">
      {/* Header */}
      <div className="px-4 py-3 border-b border-border-subtle bg-bg">
        <div className="flex items-center gap-2 mb-1">
          <span className={`text-[10px] px-1.5 py-0.5 rounded ${sourceBadgeColor(msg.source)}`}>
            {msg.record_type}
          </span>
          <span className="text-[11px] text-text-faint">{new Date(msg.timestamp).toLocaleString()}</span>
        </div>
        <div className="text-[14px] text-text-secondary font-medium">{msg.summary}</div>

        {/* Metadata */}
        {msg.metadata && Object.keys(msg.metadata).length > 0 && (
          <div className="flex flex-wrap gap-2 mt-2">
            {Object.entries(msg.metadata).map(([k, v]) => (
              <span key={k} className="text-[11px] px-1.5 py-0.5 bg-surface border border-border-subtle rounded">
                <span className="text-text-dim">{k}:</span>{' '}
                <span className="text-text-muted">{typeof v === 'object' ? JSON.stringify(v) : String(v)}</span>
              </span>
            ))}
          </div>
        )}

        {/* Session link */}
        {msg.run_session_id && (
          <button onClick={() => navigate(`/chat/${msg.run_session_id}`)}
            className="flex items-center gap-1.5 mt-2 text-[12px] text-[#6366f1] hover:text-[#818cf8] transition-colors cursor-pointer">
            <ExternalLink size={12} /> View processing session
          </button>
        )}
      </div>

      {/* Content */}
      <div className="px-4 py-3">
        <MessageContent
          source={msg.source}
          content={msg.content || ''}
          rawContent={msg.raw_content}
          metadata={msg.metadata}
          summary={msg.summary}
        />
      </div>

      {/* Processed content toggle */}
      {hasProcessedContent && (
        <div className="px-4 pb-4">
          <button onClick={() => setShowProcessed(!showProcessed)}
            className="flex items-center gap-1.5 text-[12px] text-text-dim hover:text-text-muted transition-colors cursor-pointer mb-2">
            {showProcessed ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            Processed version (what the agent saw)
          </button>
          {showProcessed && (
            <div className="p-3 bg-surface border border-border-subtle rounded-lg">
              <div className="prose prose-invert prose-sm max-w-none text-text-muted">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {msg.processed_content || ''}
                </ReactMarkdown>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// --- Consumers List ---

function ConsumersList() {
  const { consumers, loading } = useSourcesStore();

  if (loading && consumers.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint"><Loader2 size={20} className="animate-spin" /></div>;
  }

  if (consumers.length === 0) {
    return <div className="flex-1 flex items-center justify-center text-text-faint text-sm">No active consumers</div>;
  }

  // Group by consumer name
  const grouped: Record<string, typeof consumers> = {};
  for (const c of consumers) {
    if (!grouped[c.consumer]) grouped[c.consumer] = [];
    grouped[c.consumer].push(c);
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex items-center px-3 py-1.5 border-b border-border-subtle shrink-0">
        <span className="text-[11px] text-text-dim">
          {consumers.length} cursor{consumers.length !== 1 ? 's' : ''} across {Object.keys(grouped).length} consumer{Object.keys(grouped).length !== 1 ? 's' : ''}
        </span>
      </div>

      <div className="flex-1 overflow-y-auto">
        <table className="w-full text-[13px]">
          <thead className="sticky top-0 bg-bg">
            <tr className="text-text-muted">
              <th className="text-left px-3 py-2 font-medium">Consumer</th>
              <th className="text-left px-3 py-2 font-medium">Source</th>
              <th className="text-right px-3 py-2 font-medium">Position</th>
              <th className="text-right px-3 py-2 font-medium">Unread</th>
              <th className="text-left px-3 py-2 font-medium">Updated</th>
              <th className="text-left px-3 py-2 font-medium">Expires</th>
            </tr>
          </thead>
          <tbody>
            {consumers.map((c) => (
              <tr key={`${c.consumer}-${c.source}`}
                className="border-t border-border-subtle hover:bg-surface transition-colors">
                <td className="px-3 py-2 text-text-secondary font-mono text-[12px]">{c.consumer}</td>
                <td className="px-3 py-2">
                  <span className="flex items-center gap-1.5">
                    {sourceIcon(c.source)}
                    <span className="text-text-secondary truncate max-w-[140px]">{sourceLabel(c.source)}</span>
                  </span>
                </td>
                <td className="px-3 py-2 text-right text-text-muted font-mono tabular-nums text-[12px]">
                  {c.cursor_seq}
                </td>
                <td className="px-3 py-2 text-right">
                  {c.unread > 0 ? (
                    <span className="text-amber-400 font-medium tabular-nums">{c.unread}</span>
                  ) : (
                    <span className="text-text-faint tabular-nums">0</span>
                  )}
                </td>
                <td className="px-3 py-2 text-text-dim">{formatRelativeTime(c.updated_at)}</td>
                <td className="px-3 py-2 text-text-dim">
                  {c.expires_at ? formatRelativeTime(c.expires_at).replace(' ago', '') : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- Consumers Detail (session link) ---

function ConsumersDetail() {
  const { consumers } = useSourcesStore();
  const navigate = useNavigate();

  // Find unique sessions from consumers
  const sessions = [...new Set(consumers.filter(c => c.session_id).map(c => c.session_id!))];

  return (
    <div className="flex-1 flex flex-col overflow-y-auto">
      <div className="px-4 py-3 border-b border-border-subtle bg-bg">
        <h3 className="text-sm font-medium text-text-secondary">Consumer Sessions</h3>
        <p className="text-[11px] text-text-dim mt-0.5">Cron sessions processing the inbox</p>
      </div>
      <div className="p-4 space-y-2">
        {sessions.length === 0 ? (
          <div className="text-[13px] text-text-faint">No active consumer sessions</div>
        ) : sessions.map(sid => {
          const cursorsForSession = consumers.filter(c => c.session_id === sid);
          const totalUnread = cursorsForSession.reduce((sum, c) => sum + c.unread, 0);
          return (
            <button key={sid} onClick={() => navigate(`/chat/${sid}`)}
              className="w-full text-left p-3 rounded border border-border-subtle hover:border-border-subtle hover:bg-surface transition-colors cursor-pointer">
              <div className="flex items-center justify-between">
                <span className="text-[13px] text-text-secondary font-mono">{sid}</span>
                <ExternalLink size={12} className="text-text-faint" />
              </div>
              <div className="flex items-center gap-3 mt-1.5 text-[11px] text-text-dim">
                <span>{cursorsForSession.length} source{cursorsForSession.length !== 1 ? 's' : ''}</span>
                {totalUnread > 0 && <span className="text-amber-400">{totalUnread} unread</span>}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// --- Main Page ---

export function SourcesPage() {
  const { activeTab, loadOverview, loadMessages, loadConsumers, fetchSourceHealth, syncAll } = useSourcesStore();
  const [syncing, setSyncing] = useState(false);

  useEffect(() => {
    loadOverview();
    loadMessages();
    loadConsumers();
    fetchSourceHealth();
    const interval = setInterval(() => {
      loadOverview();
      fetchSourceHealth();
    }, 30_000);
    return () => clearInterval(interval);
  }, []);

  const handleSyncAll = async () => {
    if (syncing) return;
    setSyncing(true);
    try { await syncAll(); } finally { setSyncing(false); }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-border-subtle px-4 py-2.5 flex items-center justify-between bg-bg shrink-0">
        <h1 className="text-lg font-semibold">Sources</h1>
        <div className="flex items-center gap-2">
          <button onClick={handleSyncAll} disabled={syncing}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-[12px] rounded-lg transition-colors cursor-pointer
              ${syncing ? 'text-text-faint cursor-not-allowed bg-surface' : 'text-text-muted hover:text-text-secondary bg-surface hover:bg-surface-raised border border-border-subtle'}`}>
            {syncing ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />}
            {syncing ? 'Syncing...' : 'Sync All'}
          </button>
          <button onClick={() => { loadOverview(); if (activeTab === 'inbox') loadMessages(); }}
            className="text-text-dim hover:text-text-muted cursor-pointer p-1.5 hover:bg-surface-raised rounded"
            title="Refresh">
            <RefreshCw size={16} />
          </button>
        </div>
      </div>

      {/* Body */}
      <div className="flex-1 flex min-h-0">
        <SourceSidebar />

        {/* Main + Detail split */}
        <div className="flex-1 flex min-w-0">
          {/* Main panel: message list, runs, or consumers */}
          <div className="flex-1 flex flex-col min-w-0 border-r border-border-subtle">
            {activeTab === 'inbox' ? <MessageList /> : activeTab === 'runs' ? <RunsList /> : <ConsumersList />}
          </div>

          {/* Detail panel */}
          <div className="flex-1 flex flex-col min-w-0">
            {activeTab === 'inbox' ? <MessageDetail /> : activeTab === 'runs' ? <RunDetail /> : <ConsumersDetail />}
          </div>
        </div>
      </div>
    </div>
  );
}
