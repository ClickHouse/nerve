import type { SyntheticEvent } from 'react';
import { Link } from 'react-router-dom';
import type { Task, TaskSession, TaskSessionRelation } from '../../api/client';
import { MessageSquare } from '../ui/icons';

/** The label before a session's title in the task detail. */
const RELATION_LABELS: Record<TaskSessionRelation, string> = {
  created: 'Created in',
  implemented: 'Implemented in',
  updated: 'Updated in',
};

/**
 * Keep a click, a pointer press and a key press on the link. The cards
 * that hold it open the task on a click, and the board card starts a drag
 * on a pointer press or on Space and Enter.
 */
const keepEvent = (e: SyntheticEvent) => e.stopPropagation();

/**
 * "from <session title>": a link to the chat that created the task.
 *
 * Nothing renders when no session created the task, or when that session
 * does not exist any more (the server sends no title then). The caller
 * shows the task's source in that case.
 */
export function CreatedInLink({ task, className = '' }: { task: Task; className?: string }) {
  if (!task.created_session_id || task.created_session_title == null) return null;
  return (
    <span className={`min-w-0 truncate text-text-faint ${className}`}>
      from{' '}
      <Link
        to={`/chat/${task.created_session_id}`}
        onClick={keepEvent}
        onPointerDown={keepEvent}
        onKeyDown={keepEvent}
        title={`Open the chat: ${task.created_session_title}`}
        className="text-text-muted hover:text-accent hover:underline"
      >
        {task.created_session_title}
      </Link>
    </span>
  );
}

/**
 * The sessions that created, implemented or updated a task, each a link to
 * its chat. Nothing renders when there are none, for example for a task
 * that a person created and changed in the web UI.
 */
export function TaskSessions({ sessions }: { sessions?: TaskSession[] }) {
  if (!sessions?.length) return null;
  return (
    <section aria-label="Sessions" className="mt-8 pt-5 border-t border-border-subtle">
      <h2 className="text-sm font-semibold text-text mb-3">Sessions</h2>
      <ul className="space-y-2">
        {sessions.map((session) => (
          <li key={session.id} className="flex items-start gap-3 text-sm leading-5">
            <span className="w-24 shrink-0 text-xs leading-5 text-text-faint">
              {RELATION_LABELS[session.relation]}
            </span>
            <Link
              to={`/chat/${session.id}`}
              className="min-w-0 flex items-start gap-1.5 text-accent hover:underline"
            >
              {/* The icon marks the title as a chat link. In the light
                  theme the accent is close to the text colour. */}
              <MessageSquare size={12} aria-hidden="true" className="shrink-0 mt-1" />
              <span className="min-w-0 break-words">{session.title}</span>
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}
