import { useState } from 'react';
import { ChevronRight, ChevronDown, Clock, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractResultText } from '../../../utils/extractResultText';

/** "5m 30s" / "45s" / "1h 5m" — short relative-duration formatter. */
function formatDelay(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return rem === 0 ? `${m}m` : `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  const mm = m % 60;
  return mm === 0 ? `${h}h` : `${h}h ${mm}m`;
}

/** Format a future epoch-ms timestamp as a short clock time. */
function formatScheduledFor(epochMs: number): string {
  try {
    const d = new Date(epochMs);
    if (Number.isNaN(d.getTime())) return '';
    return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

/**
 * Render a `ScheduleWakeup` tool call. Claude Code uses this to self-pace
 * `/loop` iterations — Nerve doesn't run a timer for it, but the call is
 * still rendered so the user can see what the model planned. If a wakeup is
 * ever past-due at next session resume, the CLI fires it on its own.
 */
export function ScheduleWakeupBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const input = block.input || {};
  const delaySeconds = typeof input.delaySeconds === 'number' ? input.delaySeconds : 0;
  const reason = typeof input.reason === 'string' ? input.reason : '';
  const prompt = typeof input.prompt === 'string' ? input.prompt : '';

  // Parse the result to surface the actual (clamped) delay and the wall-clock
  // time the CLI scheduled the wakeup for.
  let scheduledFor = 0;
  let clampedDelaySeconds = 0;
  let wasClamped = false;
  if (block.result !== undefined && !block.isError) {
    const text = extractResultText(block.result);
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object') {
        if (typeof parsed.scheduledFor === 'number') scheduledFor = parsed.scheduledFor;
        if (typeof parsed.clampedDelaySeconds === 'number') clampedDelaySeconds = parsed.clampedDelaySeconds;
        if (typeof parsed.wasClamped === 'boolean') wasClamped = parsed.wasClamped;
      }
    } catch { /* result wasn't JSON; fall back to input.delaySeconds */ }
  }

  const effectiveDelay = clampedDelaySeconds || delaySeconds;
  const delayLabel = effectiveDelay ? formatDelay(effectiveDelay) : '';
  const timeLabel = scheduledFor ? formatScheduledFor(scheduledFor) : '';

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Clock size={14} className={`shrink-0 ${block.isError ? 'text-hue-red' : 'text-hue-cyan'}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary shrink-0 whitespace-nowrap">
          Schedule wakeup
        </span>
        {delayLabel && (
          <span className="text-[12px] text-text-muted shrink-0">
            in {delayLabel}
            {timeLabel ? ` · ${timeLabel}` : ''}
            {wasClamped ? ' (clamped)' : ''}
          </span>
        )}
        {reason && (
          <span className="text-[12px] text-text-faint truncate">— {reason}</span>
        )}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Note: Nerve doesn't actively trigger wakeups — the CLI persists
              them and fires when the session next resumes. */}
          <div className="px-3 py-2 text-[11px] text-text-faint italic">
            Stored by Claude Code; fires on next session resume if past-due.
            Nerve has no <code>/loop</code> timer.
          </div>

          {prompt && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">Prompt</div>
              <pre className="text-[12px] text-text-muted whitespace-pre-wrap overflow-x-auto max-h-40 overflow-y-auto bg-bg rounded p-2 border border-border-subtle">
                {prompt}
              </pre>
            </div>
          )}

          {block.isError && block.result !== undefined && (
            <div className="px-3 py-2 border-t border-border-subtle">
              <pre className="text-[12px] text-hue-red whitespace-pre-wrap">
                {extractResultText(block.result)}
              </pre>
            </div>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2 border-t border-border-subtle">
              <Loader2 size={12} className="animate-spin" /> Scheduling...
            </div>
          )}
        </div>
      )}
    </div>
  );
}
