import { useState } from 'react';
import { Link, useInRouterContext } from 'react-router-dom';
import type { ToolCallBlockData } from '../../../types/chat';
import type { AgentView } from '../../../types/agents';
import { useAgentStore } from '../../../stores/agentStore';
import { useAuthStore } from '../../../stores/authStore';
import { useChatStore } from '../../../stores/chatStore';
import { agentForBlock, clientAttention, isLive } from '../../../stores/helpers/agentSelectors';
import { useNow } from '../../../hooks/useNow';
import { extractResultText } from '../../../utils/extractResultText';
import { Badge, Button, IconButton } from '../../ui';
import { ArrowRight, ArrowUpRight, ChevronDown, ChevronRight, CircleDashed, RotateCw } from '../../ui/icons';
import { MarkdownContent } from '../MarkdownContent';
import { SubagentToolBlock } from '../tools/SubagentToolBlock';
import {
  agentAriaLabel, agentDetailText, agentElapsedMs, formatElapsed, formatTokens, identityVisual, kindVisual,
  statusVisual, typeLabel,
} from './agentVisuals';
import { useAgentLookup } from './agentLookup';
import { TwoStepStopButton } from './TwoStepStopButton';
import { useTwoStepConfirm } from './useTwoStepConfirm';

/** Results longer than this are cut in the inline expansion (the transcript tab has everything). */
const RESULT_PREVIEW_CHARS = 3000;

/**
 * The orchestrator's `Agent`/`Task` call at its spawn point: one live line
 * for the agent record it created (status, identity, activity, elapsed,
 * tokens, Open, a two-step Stop), with the brief and result one click away.
 *
 * - No record yet but the call is still running: the agent is starting (the
 *   record follows within moments).
 * - No record and the call is settled: old history, rendered with the legacy
 *   card exactly as before (plus a link when the record lives in another
 *   session, as for a fork's inherited cards).
 */
export function AgentLine({ block }: { block: ToolCallBlockData }) {
  const agent = useAgentStore(s => agentForBlock(s, block));
  const lookup = useAgentLookup(block, agent !== null);

  if (agent) return <RecordLine agent={agent} block={block} />;
  if (block.status === 'running') return <StartingLine block={block} />;
  return (
    <div data-agent-spawn={block.toolUseId}>
      <SubagentToolBlock block={block} />
      {lookup?.kind === 'other' && <OtherSessionLink sessionId={lookup.ownerSessionId} />}
    </div>
  );
}

function OtherSessionLink({ sessionId }: { sessionId: string | null }) {
  const routed = useInRouterContext();
  const text = <>Belongs to another session <ArrowRight size={11} /></>;
  const className = 'inline-flex items-center gap-1 -mt-0.5 mb-1.5 px-1 text-xs text-text-muted';
  if (!sessionId || !routed) return <div className={className}>{text}</div>;
  return (
    <Link to={`/chat/${sessionId}`} className={`${className} hover:text-text-secondary hover:underline underline-offset-2`}>
      {text}
    </Link>
  );
}

function RecordLine({ agent, block }: { agent: AgentView; block: ToolCallBlockData }) {
  const live = isLive(agent);
  const quietAfter = useAgentStore(s => s.capabilities.quiet_after_minutes);
  const self = useAuthStore(s => s.viewer?.id ?? null);
  const now = useNow(1000, live);
  const [expanded, setExpanded] = useState(false);
  const stop = useTwoStepConfirm(3000);

  const attention = clientAttention(agent, now, quietAfter);
  const status = statusVisual(agent.status, attention);
  const identity = kindVisual(agent.kind, agent.agent_type);
  const detail = agentDetailText(agent, now, quietAfter, self);
  const elapsed = formatElapsed(agentElapsedMs(agent, now));
  const canStop = live && agent.controls.stop && agent.status !== 'stopping';
  const canRetry = agent.status === 'stopping' && agent.attention === 'stop_unconfirmed' && agent.controls.stop;

  const open = () => useChatStore.getState().openAgentTab(agent.id);
  const stopAgent = () => { void useAgentStore.getState().stopAgent(agent.id); };

  return (
    <div
      data-agent-spawn={block.toolUseId}
      className="my-1 rounded-lg border border-border bg-surface text-xs"
    >
      <div
        className="flex items-center gap-2 px-3 py-1.5 min-w-0"
        aria-label={agentAriaLabel(agent, now, quietAfter, self)}
        role="group"
      >
        <status.Icon size={14} className={`shrink-0 ${status.className}`} aria-hidden="true" />
        <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
        <span className={`text-sm font-medium truncate max-w-[45%] ${identity.className}`}>{agent.label}</span>
        <Badge tone="neutral" size="xs" className="shrink-0">{typeLabel(agent)}</Badge>
        <span
          className={`truncate flex-1 min-w-0 ${stop.armed ? 'text-error' : 'text-text-muted'}`}
          title={detail}
        >
          {stop.armed ? `Click again to stop ${agent.label}` : detail}
        </span>
        {elapsed && <span className="shrink-0 tabular-nums text-text-faint">{elapsed}</span>}
        {agent.tokens > 0 && (
          <span className="shrink-0 tabular-nums text-text-faint hidden sm:inline">{formatTokens(agent.tokens)}</span>
        )}
        <div className="shrink-0 flex items-center gap-0.5 -mr-1">
          <IconButton size="xs" label={`Open ${agent.label}`} onClick={open}>
            <ArrowUpRight size={14} />
          </IconButton>
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
              <RotateCw size={12} />
            </IconButton>
          )}
          <IconButton
            size="xs"
            label={expanded ? 'Hide the brief and result' : 'Show the brief and result'}
            aria-expanded={expanded}
            onClick={() => setExpanded(v => !v)}
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </IconButton>
        </div>
      </div>
      {expanded && (
        <SpawnDetails
          block={block}
          running={live}
          // A background spawn returns at once with a "launched" note; its
          // outcome is the record's, so show that instead.
          result={agent.background ? (agent.result_preview ?? agent.error ?? '') : undefined}
          isError={agent.background ? agent.status === 'failed' : undefined}
          onOpen={open}
        />
      )}
    </div>
  );
}

/** A spawn whose record has not arrived yet: the call is in flight. */
function StartingLine({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const type = typeof block.input.subagent_type === 'string' ? block.input.subagent_type : 'general-purpose';
  const description = typeof block.input.description === 'string' ? block.input.description : '';
  const identity = identityVisual(type);
  const label = description || type;
  return (
    <div data-agent-spawn={block.toolUseId} className="my-1 rounded-lg border border-border bg-surface text-xs">
      <div className="flex items-center gap-2 px-3 py-1.5 min-w-0">
        <CircleDashed size={14} className="shrink-0 text-text-faint" aria-hidden="true" />
        <identity.Icon size={12} className={`shrink-0 ${identity.className}`} aria-hidden="true" />
        <span className={`text-sm font-medium truncate max-w-[45%] ${identity.className}`}>{label}</span>
        <Badge tone="neutral" size="xs" className="shrink-0">{type === 'general-purpose' ? 'general' : type}</Badge>
        <span className="truncate flex-1 min-w-0 text-text-muted">Starting…</span>
        <IconButton
          size="xs"
          label={expanded ? 'Hide the brief' : 'Show the brief'}
          aria-expanded={expanded}
          onClick={() => setExpanded(v => !v)}
          className="-mr-1"
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </IconButton>
      </div>
      {expanded && <SpawnDetails block={block} running />}
    </div>
  );
}

/**
 * The brief the orchestrator wrote and the result the call returned, from the
 * block itself (`result` overrides the block's result, for background spawns).
 */
function SpawnDetails({ block, running, result, isError, onOpen }: {
  block: ToolCallBlockData;
  running: boolean;
  result?: string;
  isError?: boolean;
  onOpen?: () => void;
}) {
  const prompt = typeof block.input.prompt === 'string' ? block.input.prompt : '';
  const text = result ?? (block.result ? extractResultText(block.result) : '');
  const error = isError ?? !!block.isError;
  const shown = text.length > RESULT_PREVIEW_CHARS ? `${text.slice(0, RESULT_PREVIEW_CHARS)}\n\n…(truncated)` : text;
  return (
    <div className="border-t border-border-subtle px-3 py-2 space-y-2">
      {prompt && (
        <div>
          <div className="text-2xs uppercase tracking-wider text-text-faint mb-1">Brief</div>
          <pre className="text-xs text-text-muted whitespace-pre-wrap max-h-40 overflow-y-auto font-sans">{prompt}</pre>
        </div>
      )}
      {shown ? (
        <div>
          <div className="text-2xs uppercase tracking-wider text-text-faint mb-1">{error ? 'Error' : 'Result'}</div>
          {error
            ? <pre className="text-xs text-error whitespace-pre-wrap max-h-60 overflow-y-auto">{shown}</pre>
            : <div className="text-sm max-h-96 overflow-y-auto"><MarkdownContent content={shown} /></div>}
        </div>
      ) : (
        <div className="text-xs text-text-faint">
          {running ? 'The result shows here when the agent finishes.' : 'No result was recorded.'}
        </div>
      )}
      {onOpen && (
        <Button variant="link" size="xs" onClick={onOpen}>
          Open the full transcript <ArrowRight size={11} />
        </Button>
      )}
    </div>
  );
}
