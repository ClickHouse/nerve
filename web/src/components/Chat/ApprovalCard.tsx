import { ShieldQuestion, Terminal, FileDiff, Check, Ban } from 'lucide-react';
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
  kind?: string;
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
        <span className="text-[13px] font-medium text-text-primary">{title}</span>
        <span className="ml-auto text-[11px] text-text-muted">approval required</span>
      </div>

      <div className="px-3 py-2 space-y-1.5">
        {command && (
          <pre className="text-[12px] font-mono bg-surface-deep rounded px-2 py-1.5 overflow-x-auto whitespace-pre-wrap break-all">
            {command}
          </pre>
        )}
        {cwd && (
          <div className="text-[11px] text-text-muted font-mono">in {cwd}</div>
        )}
        {changes.length > 0 && (
          <ul className="text-[12px] font-mono space-y-0.5">
            {changes.map((c, i) => (
              <li key={i} className="text-text-secondary">
                <span className="text-hue-orange mr-1.5">{c.kind || 'edit'}</span>
                {c.path}
              </li>
            ))}
          </ul>
        )}
        {reason && (
          <div className="text-[12px] text-text-secondary">{reason}</div>
        )}
      </div>

      <div className="px-3 py-2 flex gap-2 border-t border-border">
        <button
          onClick={() => answerInteraction(null)}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-hue-green/15 text-hue-green text-[12px] font-medium hover:bg-hue-green/25 transition-colors"
        >
          <Check size={13} /> Approve
        </button>
        <button
          onClick={() => denyInteraction('Declined by user.')}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-hue-red/15 text-hue-red text-[12px] font-medium hover:bg-hue-red/25 transition-colors"
        >
          <Ban size={13} /> Decline
        </button>
      </div>
    </div>
  );
}
