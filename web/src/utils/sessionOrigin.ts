import {
  Bot, Clock, GitBranch, Hammer, Mail, MessageSquare, Plug, Send, Slack, Workflow,
  type Icon,
} from '../components/ui/icons';
import type { Session, SessionTrigger } from '../types/chat';

/** The leading icon of a session row and its colour classes. */
export interface OriginIcon {
  icon: Icon;
  tone: string;
}

/** The icon of a plain web chat, and of a row without a known trigger. */
const CHAT_ICON: OriginIcon = { icon: MessageSquare, tone: 'opacity-50' };
/** A fork shows a branch, whatever the trigger. */
export const FORK_ICON: OriginIcon = { icon: GitBranch, tone: 'text-hue-violet/70' };
/** Neutral colour for the icons that only tell where a session comes from. */
const ORIGIN_TONE = 'text-text-dim';

/** One icon for each trigger. */
export const ORIGIN_ICONS: Record<SessionTrigger, OriginIcon> = {
  chat: CHAT_ICON,
  api: { icon: Plug, tone: ORIGIN_TONE },
  slack: { icon: Slack, tone: ORIGIN_TONE },
  telegram: { icon: Send, tone: ORIGIN_TONE },
  email: { icon: Mail, tone: ORIGIN_TONE },
  cron: { icon: Clock, tone: ORIGIN_TONE },
  plan: { icon: Hammer, tone: 'text-hue-cyan/70' },
  workflow: { icon: Workflow, tone: ORIGIN_TONE },
  external: { icon: Plug, tone: ORIGIN_TONE },
  system: { icon: Bot, tone: ORIGIN_TONE },
  unknown: CHAT_ICON,
};

type OriginFields = Pick<Session, 'trigger' | 'origin_label' | 'origin_detail'>;

export function originIcon(session: OriginFields): OriginIcon {
  if (session.origin_detail?.fork_of) return FORK_ICON;
  return (session.trigger && ORIGIN_ICONS[session.trigger]) || CHAT_ICON;
}

/** Whether a row tells its origin: every origin except a plain web chat. */
export function showsOrigin(session: OriginFields): boolean {
  if (!session.origin_label || !session.trigger) return false;
  return session.trigger !== 'chat' || !!session.origin_detail?.fork_of;
}

/**
 * Whether a person wrote the first message, so that the origin line names
 * the session's creator: channel input, the API, and "Run later".
 */
export function startedByPerson(session: OriginFields): boolean {
  switch (session.trigger) {
    case 'slack':
    case 'telegram':
    case 'email':
    case 'api':
      return true;
    case 'cron':
      return !!session.origin_detail?.run_later;
    default:
      return false;
  }
}

/** The page that the origin line links to, if there is one. */
export function originLink(session: OriginFields): { to: string; label: string } | null {
  const detail = session.origin_detail ?? {};
  if (detail.plan_id) return { to: `/plans/${encodeURIComponent(detail.plan_id)}`, label: 'Open plan' };
  if (detail.cron_job) return { to: '/cron', label: 'Open cron jobs' };
  if (detail.workflow_run) return { to: '/workflow-runs', label: 'Open workflow runs' };
  return null;
}
