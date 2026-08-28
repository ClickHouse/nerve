import { ShieldQuestion, Terminal, FileDiff, Check, Ban } from '../ui/icons';
import { Button } from '../ui';
import { useChatStore } from '../../stores/chatStore';

/**
 * Floating approval card for backend approval requests (Codex sandbox:
 * command_approval / file_approval / permission_approval).
 *
 * Unlike AskUserQuestion / plan mode — which arrive as tool blocks in the
 * message stream — approval requests are out-of-band server requests, so
 * this card renders directly from `pendingInteraction` above the composer.
 * The agent's turn is paused server-side until Approve/Decline is sent
 * (auto-declines after the server-side timeout).
 */

const APPROVAL_TYPES = new Set(['command_approval', 'file_approval', 'permission_approval']);

interface ChangeEntry {
  path?: string;
  // PatchChangeKind: tagged object in the v2 protocol; legacy strings tolerated.
  kind?: string | { type?: string };
}

function kindLabel(kind: ChangeEntry['kind']): string {
  if (kind && typeof kind === 'object') return kind.type || 'edit';
  return kind || 'edit';
}

export function ApprovalCard() {
  const pendingInteraction = useChatStore(s => s.pendingInteraction);
  const answerInteraction = useChatStore(s => s.answerInteraction);
  const denyInteraction = useChatStore(s => s.denyInteraction);

  if (!pendingInteraction || !APPROVAL_TYPES.has(pendingInteraction.interactionType)) {
    return null;
  }

  const input = pendingInteraction.toolInput || {};
  const item = (input.item as Record<string, unknown>) || {};
  const kind = pendingInteraction.interactionType;
  const reason = (input.reason as string) || '';

  const command = Array.isArray(item.command)
    ? (item.command as unknown[]).join(' ')
    : (item.command as string) || '';
  const cwd = (item.cwd as string) || '';
  const changes: ChangeEntry[] = Array.isArray(item.changes)
    ? (item.changes as ChangeEntry[])
    : [];

  const title =
    kind === 'command_approval' ? 'Agent wants to run a command'
    : kind === 'file_approval' ? 'Agent wants to change files'
    : 'Agent requests elevated permissions';

  const Icon = kind === 'command_approval' ? Terminal
    : kind === 'file_approval' ? FileDiff
    : ShieldQuestion;

  return (
    <div className="mx-4 mb-2 border border-hue-orange/40 rounded-lg bg-surface shadow-lg overflow-hidden">
      <div className="px-3 py-2 flex items-center gap-2 bg-hue-orange/10">
        <Icon size={15} className="text-hue-orange" />
        <span className="text-sm font-medium text-text">{title}</span>
        <span className="ml-auto text-xs text-text-muted">approval required</span>
      </div>

      <div className="px-3 py-2 space-y-1.5">
        {command && (
          <pre className="text-xs font-mono bg-bg-sunken rounded px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
            {command}
          </pre>
        )}
        {cwd && (
          <div className="text-xs text-text-muted font-mono">in {cwd}</div>
        )}
        {changes.length > 0 && (
          <ul className="text-xs font-mono space-y-0.5">
            {changes.map((c, i) => (
              <li key={i} className="text-text-secondary">
                <span className="text-hue-orange mr-1.5">{kindLabel(c.kind)}</span>
                {c.path}
              </li>
            ))}
          </ul>
        )}
        {reason && (
          <div className="text-xs text-text-secondary">{reason}</div>
        )}
      </div>

      <div className="px-3 py-2 flex gap-2 border-t border-border">
        <Button variant="success" size="sm" onClick={() => answerInteraction(null)}>
          <Check size={13} /> Approve
        </Button>
        <Button variant="danger" size="sm" onClick={() => denyInteraction('Declined by user.')}>
          <Ban size={13} /> Decline
        </Button>
      </div>
    </div>
  );
}
