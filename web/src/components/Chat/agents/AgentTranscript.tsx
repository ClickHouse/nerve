import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { MessageBlock, PanelTab } from '../../../types/chat';
import type { AgentEventItem, AgentView } from '../../../types/agents';
import { useAgentStore, type TranscriptState } from '../../../stores/agentStore';
import { agentTabId, useChatStore } from '../../../stores/chatStore';
import { useAuthStore } from '../../../stores/authStore';
import { clientAttention, isLive, toTranscriptSegments } from '../../../stores/helpers/agentSelectors';
import { useNow } from '../../../hooks/useNow';
import { copyToClipboard } from '../../../utils/clipboard';
import { Badge, Button, IconButton } from '../../ui';
import {
  Check, CheckCircle2, ChevronDown, ChevronRight, CircleDashed, CircleHelp, CircleX, Copy, Loader2, LocateFixed,
  Reply, RotateCcw, RotateCw, Square, Unlink, X, type Icon,
} from '../../ui/icons';
import { BlockRenderer } from '../BlockRenderer';
import { MarkdownContent } from '../MarkdownContent';
import { WorkflowPanel } from '../WorkflowPanel';
import {
  agentDetailText, agentElapsedMs, canRelaunch, followUpText, formatClock, formatCost, formatElapsed, formatTokens,
  kindVisual, relaunchText, statusVisual, typeLabel,
} from './agentVisuals';
import { jumpToSpawn, prefillComposer } from './agentActions';
import { TwoStepStopButton } from './TwoStepStopButton';
import { useTwoStepConfirm } from './useTwoStepConfirm';

/** A shell's output tail is refreshed this often while it runs. */
const OUTPUT_REFRESH_MS = 5000;

/**
 * One agent's tab: a header with its state, model, elapsed time, tokens and
 * cost, and its actions (Stop, Follow up…, Relaunch…, Copy result, Jump to
 * spawn), over a body that depends on what the agent is:
 * - a Claude agent streams its transcript (Load earlier, then the items as
 *   chat blocks with lifecycle dividers), following the bottom while you are
 *   there;
 * - a shell or monitor shows its output tail, refreshed while it runs;
 * - a workflow shows its phase and agent tree;
 * - an agent whose backend streams no transcript (Codex) shows its brief and
 *   result.
 * The tab never closes on its own.
 */
export function AgentTranscript({ agentId }: { agentId: string }) {
  const agent = useAgentStore(s => s.agents[agentId]);
  const transcript = useAgentStore(s => s.transcripts[agentId]);

  if (!agent) {
    return (
      <div className="flex-1 flex flex-col min-h-0">
        <header className="flex items-center justify-between gap-2 px-4 py-2.5 border-b border-border-subtle bg-bg shrink-0">
          <span className="text-sm font-medium text-text-secondary truncate">Agent</span>
          <ClosePanelButton />
        </header>
        <div className="p-5 text-sm text-text-faint">This agent isn't in the session's list (any more).</div>
      </div>
    );
  }

  const shellLike = agent.kind === 'shell' || agent.kind === 'monitor';
  const streams = agent.backend === 'claude' && agent.controls.transcript;
  return (
    <div className="flex-1 flex flex-col min-h-0">
      <TranscriptHeader agent={agent} transcript={transcript} />
      {shellLike
        ? <ShellOutput agent={agent} transcript={transcript} />
        : agent.kind === 'workflow'
          ? <WorkflowBody agent={agent} transcript={transcript} />
          : streams
            ? <StreamedTranscript agent={agent} transcript={transcript} />
            : <BriefAndResult agent={agent} transcript={transcript} />}
    </div>
  );
}

function ClosePanelButton() {
  return (
    <IconButton label="Close the panel" size="xs" onClick={() => useChatStore.getState().togglePanel()}>
      <X size={14} />
    </IconButton>
  );
}

// ------------------------------------------------------------------ //
//  Header                                                              //
// ------------------------------------------------------------------ //

function shortModel(model: string): string {
  return model.replace(/^claude-/, '');
}

function TranscriptHeader({ agent, transcript }: { agent: AgentView; transcript: TranscriptState | undefined }) {
  const live = isLive(agent);
  const quietAfter = useAgentStore(s => s.capabilities.quiet_after_minutes);
  const self = useAuthStore(s => s.viewer?.id ?? null);
  const now = useNow(1000, live);
  const stop = useTwoStepConfirm(3000);
  const [copied, setCopied] = useState(false);
  const [spawnMissing, setSpawnMissing] = useState(false);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const missingTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => {
    if (copiedTimer.current !== null) clearTimeout(copiedTimer.current);
    if (missingTimer.current !== null) clearTimeout(missingTimer.current);
  }, []);

  const attention = clientAttention(agent, now, quietAfter);
  const status = statusVisual(agent.status, attention);
  const identity = kindVisual(agent.kind, agent.agent_type);
  const detail = agentDetailText(agent, now, quietAfter, self);
  const elapsed = formatElapsed(agentElapsedMs(agent, now));
  const cost = formatCost(agent.cost_usd);
  const canStop = live && agent.controls.stop && agent.status !== 'stopping';
  const canRetry = agent.status === 'stopping' && agent.attention === 'stop_unconfirmed' && agent.controls.stop;
  const hasResult = !live && (!!agent.result_preview || !!transcript?.detail?.result_text);

  const model = agent.model_changed?.to ?? agent.model ?? agent.requested_model;
  const modelTitle = [
    `requested ${agent.requested_model ?? 'the default model'}`,
    agent.model_changed
      ? `ran ${agent.model_changed.from} → ${agent.model_changed.to}`
      : agent.model ? `ran ${agent.model}` : null,
  ].filter(Boolean).join(' · ');

  const stopAgent = () => { void useAgentStore.getState().stopAgent(agent.id); };

  const copyResult = async () => {
    const store = useAgentStore.getState();
    if (!store.transcripts[agent.id]?.detail) await store.loadDetail(agent.id);
    const text = useAgentStore.getState().transcripts[agent.id]?.detail?.result_text ?? agent.result_preview ?? '';
    if (!text || !(await copyToClipboard(text))) return;
    setCopied(true);
    if (copiedTimer.current !== null) clearTimeout(copiedTimer.current);
    copiedTimer.current = setTimeout(() => setCopied(false), 1500);
  };

  // Only the orchestrator's own spawns are in the chat; a nested agent was
  // started inside another agent's transcript.
  const canJump = !!agent.spawn_tool_use_id && agent.depth <= 1;
  const jump = () => {
    const found = !!agent.spawn_tool_use_id && jumpToSpawn(agent.spawn_tool_use_id);
    setSpawnMissing(!found);
    if (missingTimer.current !== null) clearTimeout(missingTimer.current);
    // The note gives the status line back after a few seconds.
    if (!found) missingTimer.current = setTimeout(() => setSpawnMissing(false), 4000);
  };

  return (
    <header className="px-4 py-2.5 border-b border-border-subtle bg-bg shrink-0">
      <div className="flex items-center justify-between gap-2 min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          <status.Icon size={14} className={`shrink-0 ${status.className}`} aria-hidden="true" />
          <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
          <h2 className={`text-sm font-medium truncate ${identity.className}`}>{agent.label}</h2>
          <span className="sr-only">, {status.label}</span>
          <Badge tone="neutral" size="xs" className="shrink-0">{typeLabel(agent)}</Badge>
          {model && (
            <span
              title={modelTitle}
              className={`text-2xs px-1.5 py-0.5 rounded shrink-0 truncate max-w-[10rem] ${
                agent.model_changed ? 'text-warning bg-warning-bg' : 'bg-surface-raised text-text-faint'
              }`}
            >
              {shortModel(model)}
            </span>
          )}
        </div>
        <div className="flex items-center gap-0.5 shrink-0">
          {canStop && (
            <TwoStepStopButton
              label={agent.label}
              armed={stop.armed}
              onPress={() => stop.press(stopAgent)}
              onDisarm={stop.disarm}
            />
          )}
          {canRetry && (
            <IconButton size="xs" label={`Retry stopping ${agent.label}`} onClick={stopAgent}>
              <RotateCw size={13} />
            </IconButton>
          )}
          {agent.kind === 'agent' && (
            <IconButton
              size="xs"
              label={agent.controls.message
                ? `Follow up with ${agent.label}…`
                : `${agent.label} can't take a follow-up (it isn't running or resumable)`}
              disabled={!agent.controls.message}
              onClick={() => prefillComposer(followUpText(agent))}
            >
              <Reply size={13} />
            </IconButton>
          )}
          {canRelaunch(agent) && (
            <IconButton size="xs" label={`Relaunch ${agent.label}…`} onClick={() => prefillComposer(relaunchText(agent))}>
              <RotateCcw size={13} />
            </IconButton>
          )}
          {hasResult && (
            <IconButton size="xs" label={copied ? 'Result copied' : 'Copy result'} onClick={() => void copyResult()}>
              {copied ? <Check size={13} className="text-success" /> : <Copy size={13} />}
            </IconButton>
          )}
          {canJump && (
            <IconButton size="xs" label="Jump to spawn" onClick={jump}>
              <LocateFixed size={13} />
            </IconButton>
          )}
          <ClosePanelButton />
        </div>
      </div>
      <div className="mt-1 flex items-center gap-2 text-xs tabular-nums min-w-0">
        <span className={`truncate flex-1 min-w-0 ${stop.armed ? 'text-error' : 'text-text-muted'}`} title={detail}>
          {stop.armed ? `Click again to stop ${agent.label}` : spawnMissing ? 'The spawn point is not on the page (it may be in a collapsed group).' : detail}
        </span>
        {elapsed && <span className="shrink-0 text-text-faint">{elapsed}</span>}
        {agent.tokens > 0 && <span className="shrink-0 text-text-faint">{formatTokens(agent.tokens)} tokens</span>}
        {cost && <span className="shrink-0 text-text-faint">{cost}</span>}
      </div>
    </header>
  );
}

// ------------------------------------------------------------------ //
//  Body states                                                         //
// ------------------------------------------------------------------ //

function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="flex items-center gap-2 px-5 py-4 text-sm text-text-dim" role="status">
      <Loader2 size={14} className="motion-safe:animate-spin" aria-hidden="true" /> {label}
    </div>
  );
}

function LoadError({ agentId, error }: { agentId: string; error?: string }) {
  return (
    <div className="px-5 py-4 space-y-2" role="alert">
      <p className="text-sm text-text-secondary">Couldn't load this agent's activity.</p>
      {error && <p className="text-xs text-text-faint break-all">{error}</p>}
      <Button size="xs" onClick={() => void useAgentStore.getState().openTranscript(agentId)}>
        <RotateCw size={12} /> Retry
      </Button>
    </div>
  );
}

/** Follow the bottom of a scrolling body while the reader is there (the SidePanel rule). */
function useFollowBottom(trigger: unknown) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const nearBottom = useRef(true);
  const onScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    nearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100;
  }, []);
  useEffect(() => {
    if (nearBottom.current) endRef.current?.scrollIntoView?.({ block: 'end' });
  }, [trigger]);
  return { scrollRef, endRef, onScroll };
}

// ------------------------------------------------------------------ //
//  Claude agents: the streamed transcript                              //
// ------------------------------------------------------------------ //

const EMPTY_ITEMS: AgentEventItem[] = [];

function StreamedTranscript({ agent, transcript }: { agent: AgentView; transcript: TranscriptState | undefined }) {
  const items = transcript?.items ?? EMPTY_ITEMS;
  const segments = useMemo(() => toTranscriptSegments(items), [items]);
  const { scrollRef, endRef, onScroll } = useFollowBottom(transcript?.lastSeq);
  const [loadingEarlier, setLoadingEarlier] = useState(false);
  const live = isLive(agent);

  const loadEarlier = async () => {
    const el = scrollRef.current;
    const fromBottom = el ? el.scrollHeight - el.scrollTop : 0;
    setLoadingEarlier(true);
    try {
      await useAgentStore.getState().loadEarlier(agent.id);
    } finally {
      setLoadingEarlier(false);
    }
    // Keep the reader where they were while older items land above.
    requestAnimationFrame(() => {
      const node = scrollRef.current;
      if (node) node.scrollTop = node.scrollHeight - fromBottom;
    });
  };

  if (!transcript || (transcript.status === 'loading' && items.length === 0)) return <Loading />;
  if (transcript.status === 'error' && items.length === 0) return <LoadError agentId={agent.id} error={transcript.error} />;

  return (
    <div ref={scrollRef} onScroll={onScroll} className="flex-1 overflow-y-auto px-5 py-4" data-role="agent-transcript">
      <BriefDisclosure agent={agent} transcript={transcript} />
      {transcript.hasMoreBefore && (
        <div className="flex justify-center mb-3">
          <Button size="xs" onClick={() => void loadEarlier()} disabled={loadingEarlier}>
            {loadingEarlier && <Loader2 size={12} className="motion-safe:animate-spin" aria-hidden="true" />}
            Load earlier
          </Button>
        </div>
      )}
      {segments.length === 0 && (
        <p className="text-sm text-text-faint">{live ? 'No activity yet.' : 'This agent recorded no activity.'}</p>
      )}
      {segments.map(segment => segment.kind === 'blocks'
        ? <SegmentBlocks key={segment.key} blocks={segment.blocks} />
        : <LifecycleDivider key={segment.key} item={segment.item} />)}
      {live && (
        <div className="flex items-center gap-2 mt-2 text-xs text-text-faint">
          <Loader2 size={12} className="motion-safe:animate-spin" aria-hidden="true" />
          <span className="truncate">{agent.status === 'stopping' ? 'Stopping…' : agent.activity || 'Working…'}</span>
        </div>
      )}
      {transcript.error && items.length > 0 && (
        <p className="mt-2 text-xs text-warning">{transcript.error}</p>
      )}
      <div ref={endRef} />
    </div>
  );
}

/** A cheap signature of a block: segments re-render only when what they show changed. */
function blockSignature(block: MessageBlock): string {
  switch (block.type) {
    case 'text':
    case 'thinking':
      return `${block.type}:${block.content.length}`;
    case 'tool_call':
      return `tool:${block.toolUseId}:${block.status}:${block.isError ? 1 : 0}:${block.result?.length ?? -1}`;
    default:
      return block.type;
  }
}

const SegmentBlocks = memo(function SegmentBlocks({ blocks }: { blocks: MessageBlock[] }) {
  return <BlockRenderer blocks={blocks} textClassName="text-sm my-1" />;
}, (prev, next) =>
  prev.blocks.length === next.blocks.length
  && prev.blocks.every((block, i) => blockSignature(block) === blockSignature(next.blocks[i])));

const LIFECYCLE: Record<Extract<AgentEventItem, { kind: 'lifecycle' }>['event'], { Icon: Icon; className: string; text: string }> = {
  started: { Icon: CircleDashed, className: 'text-text-faint', text: 'Started' },
  resumed: { Icon: RotateCw, className: 'text-text-faint', text: 'Resumed' },
  stop_requested: { Icon: Square, className: 'text-text-muted', text: 'Stop requested' },
  completed: { Icon: CheckCircle2, className: 'text-success', text: 'Completed' },
  failed: { Icon: CircleX, className: 'text-error', text: 'Failed' },
  stopped: { Icon: Square, className: 'text-text-muted', text: 'Stopped' },
  interrupted: { Icon: Unlink, className: 'text-warning', text: 'Interrupted' },
  lost: { Icon: CircleHelp, className: 'text-warning', text: 'Lost track' },
};

const STOP_REASON: Record<string, string> = {
  user: 'Stopped by you',
  stop_all: 'Stopped (stop all)',
  turn_stop: 'Stopped with the turn',
  orchestrator: 'Stopped by the orchestrator',
};

const INTERRUPT_REASON: Record<string, string> = {
  gateway_restart: 'Interrupted: gateway restart',
  process_ended: 'Interrupted: session process ended',
  force_stop: 'Interrupted: force stop',
  model_switch: 'Interrupted: model switch',
  archived: 'Interrupted: session archived',
};

function lifecycleText(item: Extract<AgentEventItem, { kind: 'lifecycle' }>): string {
  const reason = item.reason ?? '';
  if (item.event === 'stopped' && STOP_REASON[reason]) return STOP_REASON[reason];
  if (item.event === 'interrupted' && INTERRUPT_REASON[reason]) return INTERRUPT_REASON[reason];
  if (item.event === 'resumed' && item.run) return `Resumed · run ${item.run}`;
  return LIFECYCLE[item.event]?.text ?? item.event;
}

function LifecycleDivider({ item }: { item: Extract<AgentEventItem, { kind: 'lifecycle' }> }) {
  const visual = LIFECYCLE[item.event] ?? LIFECYCLE.started;
  const at = formatClock(item.at);
  return (
    <div className="flex items-center gap-1.5 my-2 text-xs text-text-muted" data-lifecycle={item.event}>
      <span className="h-px flex-1 bg-border" />
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border border-border bg-surface tabular-nums">
        <visual.Icon size={11} className={visual.className} aria-hidden="true" />
        {lifecycleText(item)}
        {at && <span className="text-text-faint">· {at}</span>}
      </span>
      <span className="h-px flex-1 bg-border" />
    </div>
  );
}

/** The orchestrator's brief for the agent, loaded on first open. */
function BriefDisclosure({ agent, transcript }: { agent: AgentView; transcript: TranscriptState }) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const prompt = transcript.detail?.prompt;
  const toggle = async () => {
    const next = !open;
    setOpen(next);
    if (next && !transcript.detail) {
      setLoading(true);
      try {
        await useAgentStore.getState().loadDetail(agent.id);
      } finally {
        setLoading(false);
      }
    }
  };
  return (
    <div className="mb-3">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => void toggle()}
        className="flex items-center gap-1 text-2xs uppercase tracking-wider text-text-faint hover:text-text-muted cursor-pointer"
      >
        {open ? <ChevronDown size={11} aria-hidden="true" /> : <ChevronRight size={11} aria-hidden="true" />}
        Brief
      </button>
      {open && (loading
        ? <Loading />
        : prompt
          ? <pre className="mt-1 text-xs text-text-muted whitespace-pre-wrap max-h-60 overflow-y-auto font-sans">{prompt}</pre>
          : <p className="mt-1 text-xs text-text-faint">No brief was recorded.</p>)}
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Shells and monitors: the output tail                                //
// ------------------------------------------------------------------ //

function ShellOutput({ agent, transcript }: { agent: AgentView; transcript: TranscriptState | undefined }) {
  const live = isLive(agent);
  const output = transcript?.output;
  const { scrollRef, endRef, onScroll } = useFollowBottom(output?.text);
  const wasLive = useRef(live);

  useEffect(() => {
    if (!live) return;
    const timer = setInterval(() => { void useAgentStore.getState().refreshOutput(agent.id); }, OUTPUT_REFRESH_MS);
    return () => clearInterval(timer);
  }, [live, agent.id]);

  // Fetch the final output once when the shell ends.
  useEffect(() => {
    if (wasLive.current && !live) void useAgentStore.getState().refreshOutput(agent.id);
    wasLive.current = live;
  }, [live, agent.id]);

  if (!transcript || (transcript.status === 'loading' && !output)) return <Loading />;
  if (transcript.status === 'error' && !output) return <LoadError agentId={agent.id} error={transcript.error} />;

  return (
    <div ref={scrollRef} onScroll={onScroll} className="flex-1 overflow-y-auto px-4 py-3">
      {agent.command && <div className="mb-2 text-xs font-mono text-text-muted break-all">$ {agent.command}</div>}
      {output && !output.available ? (
        <p className="text-sm text-text-faint">The output file is no longer available.</p>
      ) : (
        <>
          {output?.truncated && (
            <p className="mb-2 text-2xs text-text-faint">
              Showing the last {Math.round(output.text.length / 1024)} KB of {Math.round(output.size / 1024)} KB.
            </p>
          )}
          <pre className="text-xs font-mono whitespace-pre-wrap break-all text-text-secondary">
            {output?.text || (live ? 'No output yet.' : 'No output.')}
          </pre>
        </>
      )}
      {live && (
        <div className="flex items-center gap-2 mt-2 text-2xs text-text-faint">
          <Loader2 size={11} className="motion-safe:animate-spin" aria-hidden="true" /> Running · refreshed every 5 s
        </div>
      )}
      <div ref={endRef} />
    </div>
  );
}

// ------------------------------------------------------------------ //
//  Workflows: the phase and agent tree                                 //
// ------------------------------------------------------------------ //

function WorkflowBody({ agent, transcript }: { agent: AgentView; transcript: TranscriptState | undefined }) {
  const live = isLive(agent);
  const result = live ? null : (transcript?.detail?.result_text ?? agent.result_preview ?? null);
  const tab = useMemo<PanelTab>(() => ({
    id: agentTabId(agent.id),
    type: 'workflow',
    label: agent.label,
    subagentType: 'Workflow',
    description: agent.description,
    content: result,
    prompt: '',
    streaming: live,
    status: live ? 'running' : agent.status === 'completed' ? 'complete' : 'error',
    startedAt: 0,
    blocks: [],
    workflow: agent.workflow ?? undefined,
  }), [agent.id, agent.label, agent.description, agent.status, agent.workflow, live, result]);
  return <WorkflowPanel tab={tab} />;
}

// ------------------------------------------------------------------ //
//  Backends without a transcript stream: the brief and the result      //
// ------------------------------------------------------------------ //

function BriefAndResult({ agent, transcript }: { agent: AgentView; transcript: TranscriptState | undefined }) {
  const detail = transcript?.detail;
  const asked = useRef(false);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (asked.current || detail || !transcript) return;
    asked.current = true;
    void useAgentStore.getState().loadDetail(agent.id).then(() => {
      if (!useAgentStore.getState().transcripts[agent.id]?.detail) setFailed(true);
    });
  }, [agent.id, detail, transcript]);

  const result = detail?.result_text ?? agent.result_preview;
  return (
    <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
      <p className="text-xs text-text-faint">
        {agent.backend === 'codex'
          ? "This backend doesn't stream subagent transcripts."
          : "This agent's transcript isn't available."}
      </p>
      {!detail && !failed ? <Loading /> : (
        <>
          <section>
            <h3 className="text-2xs uppercase tracking-wider text-text-faint mb-1">Brief</h3>
            {detail?.prompt
              ? <pre className="text-xs text-text-muted whitespace-pre-wrap font-sans">{detail.prompt}</pre>
              : <p className="text-xs text-text-faint">{agent.description || 'No brief was recorded.'}</p>}
          </section>
          <section>
            <h3 className="text-2xs uppercase tracking-wider text-text-faint mb-1">Result</h3>
            {result
              ? <div className="text-sm"><MarkdownContent content={result} /></div>
              : <p className="text-xs text-text-faint">{isLive(agent) ? 'The result shows here when the agent finishes.' : 'No result was recorded.'}</p>}
          </section>
        </>
      )}
    </div>
  );
}
