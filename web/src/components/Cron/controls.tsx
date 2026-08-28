import { Link } from 'react-router-dom';
import { useCronStore } from '../../stores/cronStore';
import { Badge, Button, IconButton, type BadgeTone } from '../ui';
import {
  RotateCw, Play, Loader2, Clock, Inbox, MessageSquare,
  CheckCircle2, XCircle,
} from '../ui/icons';
import { chatPath } from './utils';

export function JobTypeIcon({ type }: { type: string }) {
  switch (type) {
    case 'cron': return <Clock size={14} className="text-hue-amber" />;
    case 'source': return <Inbox size={14} className="text-hue-blue" />;
    default: return <Clock size={14} className="text-text-dim" />;
  }
}

/**
 * Which kind of job this is — identity, not status, so it uses the `hue-*`
 * colours (through the Badge tones that carry them) and pairs with the amber /
 * blue of `JobTypeIcon` above. A green badge here would read as "healthy",
 * which is what `StatusBadge` is for.
 */
const JOB_TYPE_TONES: Record<string, BadgeTone> = {
  cron: 'warning',
  source: 'info',
};

export function JobTypeBadge({ type }: { type: string }) {
  return <Badge tone={JOB_TYPE_TONES[type] ?? 'neutral'}>{type}</Badge>;
}

export function StatusBadge({ status }: { status: string | null | undefined }) {
  if (status === 'success') {
    return <span className="flex items-center gap-1 text-success"><CheckCircle2 size={12} /> ok</span>;
  }
  if (status === 'error') {
    return <span className="flex items-center gap-1 text-error"><XCircle size={12} /> error</span>;
  }
  if (!status) {
    return (
      <span className="flex items-center gap-1 text-warning">
        <Loader2 size={12} className="animate-spin" /> running
      </span>
    );
  }
  return <span className="text-text-dim">{status}</span>;
}

/** Link to a cron's chat session. Renders an anchor so cmd/ctrl+click and
 *  middle-click open a new tab natively. */
export function ChatLink({ sessionId, small = false, label }: { sessionId: string; small?: boolean; label?: string }) {
  return (
    <Link to={chatPath(sessionId)}
      onClick={(e) => e.stopPropagation()}
      onAuxClick={(e) => e.stopPropagation()}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer shrink-0
        text-text-muted hover:text-text-secondary hover:bg-surface-raised
        ${small ? 'p-1' : 'px-2 py-1.5 text-xs'}`}
      title="Open chat">
      <MessageSquare size={small ? 12 : 14} />
      {!small && <span>{label || 'Chat'}</span>}
    </Link>
  );
}

export function TriggerButton({ jobId, small = false }: { jobId: string; small?: boolean }) {
  const { triggering, triggerJob } = useCronStore();
  const isTriggering = triggering === jobId;

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (isTriggering) return;
    await triggerJob(jobId);
  };

  const glyph = isTriggering
    ? <Loader2 size={small ? 12 : 14} className="animate-spin" />
    : <Play size={small ? 12 : 14} />;

  if (small) {
    return (
      <IconButton label="Trigger now" size="xs" onClick={handleClick}
        disabled={isTriggering} onAuxClick={(e) => e.stopPropagation()}>
        {glyph}
      </IconButton>
    );
  }

  return (
    <Button variant="subtle" onClick={handleClick} disabled={isTriggering}
      onAuxClick={(e) => e.stopPropagation()} title="Trigger now">
      {glyph}
      {!isTriggering && <span>Run</span>}
    </Button>
  );
}

export function RotateButton({ jobId }: { jobId: string }) {
  const { rotating, rotateSession } = useCronStore();
  const isRotating = rotating === jobId;

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (isRotating) return;
    await rotateSession(jobId);
  };

  return (
    <Button variant="subtle" onClick={handleClick} disabled={isRotating}
      title="Start a fresh chat (current chat is kept)">
      {isRotating ? <Loader2 size={14} className="animate-spin" /> : <RotateCw size={14} />}
      {!isRotating && <span>Rotate</span>}
    </Button>
  );
}
