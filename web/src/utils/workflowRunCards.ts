import type { MessageBlock, ToolCallBlockData } from '../types/chat';

const WORKFLOW_RUN_ID_RE = /wfr-[0-9a-f]{8}/;

/** True for the tools that render the live workflow-run card. */
export function isWorkflowRunTool(tool: string): boolean {
  return tool.includes('workflow_run_start') || tool.includes('workflow_run_status');
}

/** Readable text of a tool result: MCP results may be JSON content-block arrays. */
export function toolResultText(result: string): string {
  try {
    const parsed: unknown = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter(b => b && b.type === 'text')
        .map(b => String(b.text))
        .join('\n');
    }
  } catch { /* not JSON */ }
  return result;
}

/**
 * First wfr-xxxxxxxx id in a workflow_run_* tool result. Both tools embed it:
 *   workflow_run_start  → "Workflow run wfr-xxxxxxxx created (running). ..."
 *   workflow_run_status → "wfr-xxxxxxxx [status] title — spent $X / budget $Y"
 * Returns null while the call has no result yet.
 */
export function extractWorkflowRunId(result?: string): string | null {
  if (!result) return null;
  const match = toolResultText(result).match(WORKFLOW_RUN_ID_RE);
  return match ? match[0] : null;
}

/**
 * The run id when this block renders as a workflow-run card, else null. The
 * run id only exists in the tool result, so a call that is still running, or
 * that failed (e.g. "no such workflow run"), renders as a generic tool block.
 */
export function workflowRunCardId(block: ToolCallBlockData): string | null {
  if (block.isError || !isWorkflowRunTool(block.tool)) return null;
  return extractWorkflowRunId(block.result);
}

/**
 * Tool-use ids of workflow_run_* calls that refer to a run which already has a
 * card earlier in the same message.
 *
 * The card is live — it reads the run from the store, not from its own tool
 * result — so every card for one run shows the same thing. An agent that
 * starts a run and then polls it would otherwise stack one identical card per
 * call. The first call that renders a card keeps it; the calls returned here
 * render as a compact row instead.
 */
export function findRepeatWorkflowRunCalls(blocks: MessageBlock[]): Set<string> {
  const seenRuns = new Set<string>();
  const repeats = new Set<string>();
  for (const block of blocks) {
    if (block.type !== 'tool_call') continue;
    const runId = workflowRunCardId(block);
    if (!runId) continue;
    if (seenRuns.has(runId)) {
      repeats.add(block.toolUseId);
    } else {
      seenRuns.add(runId);
    }
  }
  return repeats;
}
