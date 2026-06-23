import { Loader2, Check, X, Circle } from 'lucide-react';
import { MarkdownContent } from './MarkdownContent';
import type { PanelTab, WorkflowAgent, WorkflowSnapshot } from '../../types/chat';

function fmtTokens(n?: number): string {
  if (!n) return '';
  return n >= 1000 ? `${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}k` : String(n);
}

function fmtDuration(ms?: number): string {
  if (!ms) return '';
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${s % 60}s`;
}

function AgentState({ state }: { state?: string }) {
  if (state === 'running') return <Loader2 size={12} className="text-hue-amber animate-spin shrink-0" />;
  if (state === 'done') return <Check size={12} className="text-hue-emerald shrink-0" />;
  if (state === 'failed') return <X size={12} className="text-hue-red shrink-0" />;
  // queued / pending / unknown
  return <Circle size={9} className="text-text-faint shrink-0 mx-[1.5px]" />;
}

function AgentRow({ agent }: { agent: WorkflowAgent }) {
  return (
    <div className="flex items-center gap-2 px-2 py-1 rounded hover:bg-surface-hover/50">
      <AgentState state={agent.state} />
      <span className="text-[12px] text-text-secondary truncate max-w-[140px]">{agent.label || 'agent'}</span>
      {agent.lastToolName && (
        <span className="text-[11px] text-text-faint font-mono shrink-0">{agent.lastToolName}</span>
      )}
      {agent.lastToolSummary && (
        <span className="text-[11px] text-text-dim truncate flex-1">{agent.lastToolSummary}</span>
      )}
      <div className="ml-auto shrink-0 flex items-center gap-2 text-[10px] text-text-faint font-mono">
        {agent.tokens ? <span>{fmtTokens(agent.tokens)}</span> : null}
        {agent.durationMs ? <span>{fmtDuration(agent.durationMs)}</span> : null}
      </div>
    </div>
  );
}

interface PhaseGroup {
  index: number;
  title: string;
  agents: WorkflowAgent[];
}

function groupByPhase(wf: WorkflowSnapshot): PhaseGroup[] {
  const groups = new Map<number, PhaseGroup>();
  // Seed declared phases so empty/upcoming phases still render.
  for (const p of wf.phases) {
    const idx = p.index ?? 0;
    groups.set(idx, { index: idx, title: p.title || `Phase ${idx}`, agents: [] });
  }
  for (const a of wf.agents) {
    const idx = a.phaseIndex ?? 0;
    if (!groups.has(idx)) {
      groups.set(idx, { index: idx, title: a.phaseTitle || (idx ? `Phase ${idx}` : 'Agents'), agents: [] });
    }
    groups.get(idx)!.agents.push(a);
  }
  return [...groups.values()].sort((x, y) => x.index - y.index);
}

export function WorkflowPanel({ tab }: { tab: PanelTab }) {
  const wf = tab.workflow;

  if (!wf || (wf.phases.length === 0 && wf.agents.length === 0)) {
    return (
      <div className="flex-1 overflow-y-auto px-4 py-4">
        <div className="flex items-center gap-2 text-[13px] text-text-dim">
          <Loader2 size={14} className="animate-spin" /> Starting workflow…
        </div>
      </div>
    );
  }

  const groups = groupByPhase(wf);
  const total = wf.agentCount ?? wf.agents.length;
  const done = wf.agents.filter(a => a.state === 'done').length;
  const running = wf.agents.filter(a => a.state === 'running').length;

  return (
    <div className="flex-1 overflow-y-auto px-4 py-3">
      {/* Totals bar */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-text-faint font-mono mb-3 pb-2 border-b border-border-subtle">
        <span><span className="text-text-secondary">{done}</span>/{total} agents done</span>
        {running > 0 && <span><span className="text-hue-amber">{running}</span> running</span>}
        <span>{wf.phases.length} phases</span>
        {wf.totalTokens ? <span>{fmtTokens(wf.totalTokens)} tokens</span> : null}
        {wf.totalToolCalls ? <span>{wf.totalToolCalls} tool calls</span> : null}
      </div>

      {/* Phase groups */}
      <div className="space-y-3">
        {groups.map(group => {
          const gDone = group.agents.filter(a => a.state === 'done').length;
          return (
            <div key={group.index}>
              <div className="flex items-center gap-2 mb-1">
                <span className="text-[10px] uppercase tracking-wider text-text-faint">
                  Phase {group.index}
                </span>
                <span className="text-[12px] font-medium text-text-secondary">{group.title}</span>
                {group.agents.length > 0 && (
                  <span className="text-[10px] text-text-faint font-mono ml-auto">
                    {gDone}/{group.agents.length}
                  </span>
                )}
              </div>
              {group.agents.length > 0 ? (
                <div className="space-y-0.5">
                  {group.agents.map((a, i) => <AgentRow key={i} agent={a} />)}
                </div>
              ) : (
                <div className="px-2 py-1 text-[11px] text-text-faint italic">pending…</div>
              )}
            </div>
          );
        })}
      </div>

      {/* Final summary / result */}
      {tab.content && (
        <div className="mt-4 pt-3 border-t border-border-subtle text-[13px]">
          <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">Result</div>
          <MarkdownContent content={tab.content} />
        </div>
      )}
    </div>
  );
}
