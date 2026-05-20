/**
 * External Agents — bootstrap-configured Codex / Claude Code / ...
 *
 * Renders one card per configured agent showing:
 * - CLI install / version (smoke check)
 * - Last sync timestamp + per-file hash
 * - Pause / resume / remove controls
 * - "Sync now" button that triggers an out-of-band sweep
 *
 * Available-but-unconfigured agents are listed at the bottom with a
 * note pointing back to `nerve init` — adding them through the UI is
 * a follow-up because the wizard handles JWT issuance + config write.
 */

import { useEffect, useState, useCallback } from 'react';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  ExternalLink,
  FileText,
  Loader2,
  Pause,
  Play,
  RefreshCw,
  Trash2,
  XCircle,
} from 'lucide-react';
import { api } from '../../api/client';

type AgentFile = {
  path: string;
  hash: string;
  written_at: string | null;
  skipped: boolean;
  error: string | null;
};

type ConfiguredAgent = {
  name: string;
  enabled: boolean;
  display_name?: string;
  cli_installed?: boolean;
  cli_version?: string | null;
  last_run_at?: string | null;
  last_error?: string | null;
  files?: AgentFile[];
};

type AvailableAgent = {
  name: string;
  display_name: string;
  cli_command: string | null;
  cli_installed: boolean;
  cli_version: string | null;
  config_paths: string[];
};

type State = {
  enabled: boolean;
  sync_interval_minutes: number;
  conflict_policy: string;
  available: AvailableAgent[];
  configured: ConfiguredAgent[];
};

function relativeTime(iso: string | null | undefined): string {
  if (!iso) return 'never';
  const diff = Date.now() - new Date(iso).getTime();
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return new Date(iso).toLocaleString();
}

export function ExternalAgentsSection() {
  const [state, setState] = useState<State | null>(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [busyAgent, setBusyAgent] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const next = await api.listExternalAgents();
      setState(next as State);
    } catch (e) {
      console.error('Failed to load external agents:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const onSyncAll = async () => {
    if (syncing) return;
    setSyncing(true);
    try {
      await api.triggerExternalAgentsSync();
      await load();
    } catch (e) {
      console.error('Sync failed:', e);
    } finally {
      setSyncing(false);
    }
  };

  const onToggle = async (name: string, enabled: boolean) => {
    setBusyAgent(name);
    try {
      await api.toggleExternalAgent(name, enabled);
      await load();
    } catch (e) {
      console.error('Toggle failed:', e);
    } finally {
      setBusyAgent(null);
    }
  };

  const onRemove = async (name: string) => {
    if (!confirm(`Remove ${name}? Files in ~/.codex / ~/.claude are left intact.`)) {
      return;
    }
    setBusyAgent(name);
    try {
      await api.removeExternalAgent(name);
      await load();
    } catch (e) {
      console.error('Remove failed:', e);
    } finally {
      setBusyAgent(null);
    }
  };

  if (loading) {
    return (
      <section>
        <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
          <ExternalLink size={14} /> External Agents
        </h2>
        <div className="text-text-faint text-sm">Loading...</div>
      </section>
    );
  }

  if (!state) {
    return (
      <section>
        <h2 className="text-[14px] font-medium text-text-muted mb-3 flex items-center gap-2">
          <ExternalLink size={14} /> External Agents
        </h2>
        <div className="text-hue-red text-sm">Failed to load</div>
      </section>
    );
  }

  const configured = state.configured ?? [];
  const configuredNames = new Set(configured.map((a) => a.name));
  const unconfigured = (state.available ?? []).filter((a) => !configuredNames.has(a.name));

  return (
    <section>
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-[14px] font-medium text-text-muted flex items-center gap-2">
          <ExternalLink size={14} /> External Agents
          <span className="text-text-faint text-[11px]">
            (sync every {state.sync_interval_minutes}m, policy: {state.conflict_policy})
          </span>
        </h2>
        {configured.length > 0 && (
          <button
            onClick={onSyncAll}
            disabled={syncing}
            className={`flex items-center gap-1.5 px-3 py-2 text-[12px] bg-surface border border-border-subtle rounded-lg cursor-pointer transition-colors shrink-0 ${
              syncing
                ? 'text-text-faint cursor-not-allowed'
                : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'
            }`}
            title="Re-render every configured agent's memory bundle"
          >
            {syncing ? (
              <>
                <Loader2 size={12} className="animate-spin" /> Syncing...
              </>
            ) : (
              <>
                <RefreshCw size={12} /> Sync now
              </>
            )}
          </button>
        )}
      </div>

      {configured.length === 0 && (
        <div className="text-text-faint text-sm border border-border-subtle rounded-lg p-4">
          No external agents configured. Run <code className="bg-surface px-1 rounded">nerve init</code>{' '}
          to add Codex or Claude Code.
        </div>
      )}

      <div className="space-y-3">
        {configured.map((agent) => (
          <AgentCard
            key={agent.name}
            agent={agent}
            busy={busyAgent === agent.name}
            onToggle={onToggle}
            onRemove={onRemove}
          />
        ))}
      </div>

      {unconfigured.length > 0 && (
        <div className="mt-4 text-[12px] text-text-faint">
          <div className="mb-1">Available but not configured:</div>
          <ul className="list-disc pl-5 space-y-0.5">
            {unconfigured.map((a) => (
              <li key={a.name}>
                {a.display_name}
                {a.cli_installed && (
                  <span className="text-hue-emerald"> (CLI installed)</span>
                )}
              </li>
            ))}
          </ul>
          <div className="mt-1 text-text-faint">
            Run <code className="bg-surface px-1 rounded">nerve init</code> to add one.
          </div>
        </div>
      )}
    </section>
  );
}

function AgentCard({
  agent,
  busy,
  onToggle,
  onRemove,
}: {
  agent: ConfiguredAgent;
  busy: boolean;
  onToggle: (name: string, enabled: boolean) => Promise<void>;
  onRemove: (name: string) => Promise<void>;
}) {
  const enabled = agent.enabled;
  const installed = agent.cli_installed ?? false;
  const files = agent.files ?? [];

  return (
    <div className="border border-border-subtle rounded-lg p-4 bg-surface">
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center gap-2 text-[14px] font-medium text-text-secondary">
            {agent.display_name ?? agent.name}
            {!enabled && (
              <span className="text-[11px] text-text-faint border border-border-subtle px-1.5 py-0.5 rounded">
                paused
              </span>
            )}
          </div>
          <div className="text-[11px] text-text-dim mt-1 flex items-center gap-2 flex-wrap">
            <span className="flex items-center gap-1">
              {installed ? (
                <>
                  <CheckCircle2 size={11} className="text-hue-emerald" />
                  CLI installed
                  {agent.cli_version && (
                    <span className="text-text-faint">({agent.cli_version})</span>
                  )}
                </>
              ) : (
                <>
                  <XCircle size={11} className="text-text-faint" />
                  CLI not on PATH
                </>
              )}
            </span>
            <span className="text-text-faint">·</span>
            <span className="flex items-center gap-1">
              <Activity size={11} /> last sync {relativeTime(agent.last_run_at ?? null)}
            </span>
          </div>
        </div>

        <div className="flex items-center gap-1.5 shrink-0">
          <button
            onClick={() => onToggle(agent.name, !enabled)}
            disabled={busy}
            className={`flex items-center gap-1 px-2 py-1 text-[11px] border border-border-subtle rounded transition-colors ${
              busy
                ? 'text-text-faint cursor-not-allowed'
                : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised cursor-pointer'
            }`}
            title={enabled ? 'Pause sync for this agent' : 'Resume sync'}
          >
            {enabled ? (
              <>
                <Pause size={11} /> Pause
              </>
            ) : (
              <>
                <Play size={11} /> Resume
              </>
            )}
          </button>
          <button
            onClick={() => onRemove(agent.name)}
            disabled={busy}
            className={`flex items-center gap-1 px-2 py-1 text-[11px] border border-border-subtle rounded transition-colors ${
              busy
                ? 'text-text-faint cursor-not-allowed'
                : 'text-hue-red/80 hover:text-hue-red hover:bg-surface-raised cursor-pointer'
            }`}
            title="Remove from sync (files on disk are left intact)"
          >
            <Trash2 size={11} /> Remove
          </button>
        </div>
      </div>

      {agent.last_error && (
        <div className="text-[11px] text-hue-red flex items-start gap-1.5 mb-3">
          <AlertCircle size={11} className="mt-0.5 shrink-0" />
          <span className="break-all">{agent.last_error}</span>
        </div>
      )}

      {files.length > 0 && (
        <div className="border-t border-border-subtle pt-3 space-y-1.5">
          {files.map((f) => (
            <div
              key={f.path}
              className="flex items-center justify-between gap-3 text-[11px]"
            >
              <div className="flex items-center gap-1.5 min-w-0 flex-1">
                <FileText size={11} className="text-text-faint shrink-0" />
                <span className="text-text-secondary truncate">{f.path}</span>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                {f.hash && (
                  <span className="font-mono text-text-faint">{f.hash}</span>
                )}
                {f.skipped ? (
                  <span className="text-text-faint">unchanged</span>
                ) : f.error ? (
                  <span className="text-hue-red flex items-center gap-1">
                    <XCircle size={11} /> error
                  </span>
                ) : f.written_at ? (
                  <span className="text-hue-emerald flex items-center gap-1">
                    <CheckCircle2 size={11} /> {relativeTime(f.written_at)}
                  </span>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
