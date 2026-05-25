/**
 * Helpers for Claude Code 2.1.x task tools (TaskCreate / TaskUpdate /
 * TaskList / TaskGet).
 *
 * The CLI stores tasks per-session in ~/.claude/tasks/<id>/ as JSON files.
 * Nerve doesn't reach into that directory — it just mirrors what flows
 * through tool calls and results so the in-chat panel reflects the current
 * todo list.
 *
 * Result content is plain text (the CLI's mapToolResultToToolResultBlockParam):
 *   - TaskCreate:  "Task #N created successfully: SUBJECT"
 *   - TaskList:    "#N [STATUS] SUBJECT [(OWNER)] [[blocked by #X, #Y]]"
 *                  one per line, or "No tasks found"
 *   - TaskGet:     multi-line:
 *                    Task #N: SUBJECT
 *                    Status: STATUS
 *                    Description: ...
 *                    [Blocked by: #X, #Y]
 *                    [Blocks: #X, #Y]
 *   - TaskUpdate:  shape varies; we apply the input optimistically instead.
 */
import type { CCTask } from '../chatStore';

type Status = CCTask['status'];

const STATUSES: ReadonlySet<Status> = new Set([
  'pending',
  'in_progress',
  'completed',
]);

function asStatus(s: unknown): Status | undefined {
  return typeof s === 'string' && STATUSES.has(s as Status)
    ? (s as Status)
    : undefined;
}

/** Stable id used for an optimistic TaskCreate row before the CLI assigns one. */
function placeholderId(toolUseId: string): string {
  return `pending:${toolUseId}`;
}

/** Sort by numeric id ascending, with placeholder rows last. */
function sortTasks(tasks: CCTask[]): CCTask[] {
  return [...tasks].sort((a, b) => {
    const aIsPlaceholder = a.id.startsWith('pending:');
    const bIsPlaceholder = b.id.startsWith('pending:');
    if (aIsPlaceholder && !bIsPlaceholder) return 1;
    if (!aIsPlaceholder && bIsPlaceholder) return -1;
    const na = Number(a.id);
    const nb = Number(b.id);
    if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb;
    return a.id.localeCompare(b.id);
  });
}

/**
 * Add a placeholder row for an in-flight TaskCreate. The real numeric ID
 * lands on tool_result and is reconciled by `parseCCTaskCreateResult`.
 */
export function applyCCTaskCreateInput(
  current: CCTask[],
  input: Record<string, unknown>,
  toolUseId: string,
): CCTask[] {
  const subject = typeof input.subject === 'string' ? input.subject : '';
  if (!subject) return current;
  const activeForm = typeof input.activeForm === 'string' ? input.activeForm : undefined;
  const id = placeholderId(toolUseId);
  // If we somehow already have a row for this toolUseId, no-op.
  if (current.some(t => t.id === id)) return current;
  return sortTasks([
    ...current,
    { id, subject, activeForm, status: 'pending' },
  ]);
}

/** Apply a TaskUpdate input optimistically. */
export function applyCCTaskUpdateInput(
  current: CCTask[],
  input: Record<string, unknown>,
): CCTask[] {
  const taskId = typeof input.taskId === 'string' ? input.taskId : '';
  if (!taskId) return current;
  const status = asStatus(input.status);
  const subject = typeof input.subject === 'string' ? input.subject : undefined;
  const activeForm = typeof input.activeForm === 'string' ? input.activeForm : undefined;
  const owner = typeof input.owner === 'string' ? input.owner : undefined;
  // Status === "deleted" removes the row.
  if (input.status === 'deleted') {
    return current.filter(t => t.id !== taskId);
  }
  let found = false;
  const next = current.map(t => {
    if (t.id !== taskId) return t;
    found = true;
    return {
      ...t,
      ...(subject !== undefined ? { subject } : {}),
      ...(activeForm !== undefined ? { activeForm } : {}),
      ...(owner !== undefined ? { owner } : {}),
      ...(status !== undefined ? { status } : {}),
    };
  });
  // Unknown taskId — drop a synthetic row so the panel still surfaces the
  // intent (the next TaskList will replace it cleanly).
  if (!found && (subject || status)) {
    next.push({
      id: taskId,
      subject: subject || `Task #${taskId}`,
      activeForm,
      owner,
      status: status || 'pending',
    });
  }
  return sortTasks(next);
}

/**
 * Parse a TaskList result. Each line looks like:
 *   #N [status] subject (owner) [blocked by #X, #Y]
 * The owner and blocked-by suffixes are optional and may both be absent.
 * Returns null if no usable lines are found (e.g. "No tasks found"),
 * signalling the caller to clear the panel.
 */
export function parseCCTaskListResult(
  text: string,
  current: CCTask[],
): CCTask[] {
  if (!text) return [];
  if (/^\s*No tasks found\s*$/i.test(text)) return [];
  const re = /^#(\d+)\s+\[([a-z_]+)\]\s+(.+?)(?:\s+\(([^)]+)\))?(?:\s+\[blocked by\s+([^\]]+)\])?\s*$/;
  const out: CCTask[] = [];
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    if (!line) continue;
    const m = re.exec(line);
    if (!m) continue;
    const status = asStatus(m[2]) || 'pending';
    const blockedBy = m[5]
      ? m[5].split(',').map(s => s.trim().replace(/^#/, '')).filter(Boolean)
      : undefined;
    const id = m[1];
    // Preserve any activeForm we already had locally — TaskList doesn't return it.
    const known = current.find(t => t.id === id);
    out.push({
      id,
      subject: m[3].trim(),
      activeForm: known?.activeForm,
      status,
      owner: m[4]?.trim() || undefined,
      blockedBy,
    });
  }
  return sortTasks(out);
}

/**
 * Parse a TaskCreate result of the form:
 *   "Task #N created successfully: SUBJECT"
 * On success, swap the placeholder row (keyed by toolUseId) for one with
 * the real numeric id. If parsing fails, leave the placeholder so it's at
 * least visible until the next TaskList.
 */
export function parseCCTaskCreateResult(
  text: string,
  current: CCTask[],
  toolUseId: string,
): CCTask[] {
  const m = /^Task\s+#(\d+)\s+created\s+successfully:\s+(.+)$/m.exec(text || '');
  if (!m) return current;
  const realId = m[1];
  const subject = m[2].trim();
  const placeholder = placeholderId(toolUseId);
  // If the real id already exists (re-broadcast / replay), just drop the placeholder.
  if (current.some(t => t.id === realId)) {
    return sortTasks(current.filter(t => t.id !== placeholder));
  }
  let swapped = false;
  const next = current.map(t => {
    if (t.id === placeholder) {
      swapped = true;
      return { ...t, id: realId, subject };
    }
    return t;
  });
  if (!swapped) {
    next.push({ id: realId, subject, status: 'pending' });
  }
  return sortTasks(next);
}

/**
 * Parse a TaskGet result block. Multi-line; example:
 *   Task #1: Read the file
 *   Status: in_progress
 *   Description: ...
 *   [Blocked by: #2, #3]
 *   [Blocks: #4]
 *
 * Upserts a single task into the existing list.
 */
export function parseCCTaskGetResult(
  text: string,
  current: CCTask[],
): CCTask[] {
  if (!text) return current;
  if (/^\s*Task not found\s*$/i.test(text)) return current;
  const idMatch = /^Task\s+#(\d+):\s+(.+)$/m.exec(text);
  if (!idMatch) return current;
  const id = idMatch[1];
  const subject = idMatch[2].trim();
  const statusMatch = /^Status:\s+([a-z_]+)\s*$/m.exec(text);
  const status = asStatus(statusMatch?.[1]) || 'pending';
  const blockedByMatch = /^Blocked by:\s+(.+)$/m.exec(text);
  const blockedBy = blockedByMatch
    ? blockedByMatch[1].split(',').map(s => s.trim().replace(/^#/, '')).filter(Boolean)
    : undefined;
  let found = false;
  const next = current.map(t => {
    if (t.id !== id) return t;
    found = true;
    return {
      ...t,
      subject,
      status,
      ...(blockedBy !== undefined ? { blockedBy } : {}),
    };
  });
  if (!found) {
    next.push({ id, subject, status, blockedBy });
  }
  return sortTasks(next);
}
