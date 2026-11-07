import { Link } from 'react-router-dom';
import type { Session } from '../../types/chat';
import { useAttribution } from '../../hooks/useActorAttribution';
import { useAuthStore } from '../../stores/authStore';
import { useChatStore } from '../../stores/chatStore';
import { findSessionById } from '../../utils/findSession';
import { originIcon, originLink, showsOrigin, startedByPerson } from '../../utils/sessionOrigin';
import { Tooltip } from '../ui';

/**
 * The leading icon of a session row: where the session comes from. The
 * origin sentence is its tooltip and its accessible name. A plain web chat
 * has the chat icon and no tooltip.
 */
export function SessionOriginIcon({ session }: { session: Session }) {
  const { icon: Icon, tone } = originIcon(session);
  const glyph = <Icon size={13} className={`shrink-0 ${tone}`} aria-hidden="true" />;
  if (!showsOrigin(session)) return glyph;
  return (
    <Tooltip content={session.origin_label} side="right">
      <span role="img" aria-label={session.origin_label} className="shrink-0 flex">
        {glyph}
      </span>
    </Tooltip>
  );
}

/**
 * One line above the first message of the open session: where it comes
 * from, the person who wrote the first message when that is someone else,
 * and a link to the page of the origin. Nothing for a plain web chat, and
 * nothing for a fork, which has its own line under the header.
 *
 * The row comes from the loaded session lists, else from `unlisted`: the
 * row that the page read by id.
 */
export function SessionOriginLine({ unlisted }: { unlisted?: Session | null }) {
  const listed = useChatStore((s) => findSessionById(
    s.activeSession, s.sessions, s.archivedSessions, s.systemSessions,
  ));
  const session = listed ?? unlisted ?? undefined;
  const viewerActorId = useAuthStore((s) => s.viewer?.id ?? null);
  const creatorId = session && startedByPerson(session) ? session.created_by_actor_id : null;
  const creator = useAttribution(creatorId !== viewerActorId ? creatorId : null);
  if (!session || !showsOrigin(session) || session.origin_detail?.fork_of) return null;

  const { icon: Icon } = originIcon(session);
  const link = originLink(session);
  const byPerson = creatorId && creatorId !== viewerActorId && !creator.system
    ? ` by ${creator.name}` : '';
  return (
    <div className="px-5 pt-4 pb-1" data-testid="session-origin">
      <div className="max-w-[var(--chat-width)] mx-auto flex items-center justify-center gap-1.5 min-w-0 text-xs text-text-faint">
        <Icon size={12} className="shrink-0" aria-hidden="true" />
        <span className="truncate">{session.origin_label}{byPerson}</span>
        {link && (
          <>
            <span aria-hidden="true">·</span>
            <Link
              to={link.to}
              className="shrink-0 text-text-muted hover:text-text underline-offset-2 hover:underline"
            >
              {link.label}
            </Link>
          </>
        )}
      </div>
    </div>
  );
}
