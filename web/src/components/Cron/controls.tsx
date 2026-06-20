import { Link } from 'react-router-dom';
import {
  RotateCw, Play, Loader2, Clock, Inbox, MessageSquare,
  CheckCircle2, XCircle,
} from 'lucide-react';
import { useCronStore } from '../../stores/cronStore';
import { chatPath } from './utils';

export function JobTypeIcon({ type }: { type: string }) {
  switch (type) {
    case 'cron': return <Clock size={14} className="text-hue-amber" />;
    case 'source': return <Inbox size={14} className="text-hue-blue" />;
    default: return <Clock size={14} className="text-text-dim" />;
  }
}

export function JobTypeBadge({ type }: { type: string }) {
  const styles: Record<string, string> = {
    cron: 'text-amber-600 bg-amber-500/15',
    source: 'text-blue-600 bg-blue-500/15',
  };
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded ${styles[type] || 'text-text-muted bg-surface-raised'}`}>
      {type}
    </span>
  );
}

export function StatusBadge({ status }: { status: string | null | undefined }) {
  if (status === 'success') {
    return <span className="flex items-center gap-1 text-hue-emerald"><CheckCircle2 size={12} /> ok</span>;
  }
  if (status === 'error') {
    return <span className="flex items-center gap-1 text-hue-red"><XCircle size={12} /> error</span>;
  }
  if (!status) {
    return (
      <span className="flex items-center gap-1 text-hue-amber">
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
        ${small ? 'p-1' : 'px-2 py-1.5 text-[12px]'}`}
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

  return (
    <button onClick={handleClick} disabled={isTriggering}
      onAuxClick={(e) => e.stopPropagation()}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer shrink-0
        ${isTriggering ? 'text-text-faint cursor-not-allowed' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}
        ${small ? 'p-1' : 'px-2 py-1.5 text-[12px]'}`}
      title="Trigger now">
      {isTriggering ? <Loader2 size={small ? 12 : 14} className="animate-spin" /> : <Play size={small ? 12 : 14} />}
      {!small && !isTriggering && <span>Run</span>}
    </button>
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
    <button onClick={handleClick} disabled={isRotating}
      className={`flex items-center gap-1 rounded transition-colors cursor-pointer px-2 py-1.5 text-[12px]
        ${isRotating ? 'text-text-faint cursor-not-allowed' : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'}`}
      title="Rotate session context">
      {isRotating ? <Loader2 size={14} className="animate-spin" /> : <RotateCw size={14} />}
      {!isRotating && <span>Rotate</span>}
    </button>
  );
}
