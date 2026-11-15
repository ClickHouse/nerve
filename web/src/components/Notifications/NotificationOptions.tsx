import { Fragment, useId, useState } from 'react';
import { ChevronDown, ChevronUp, Clock } from '../ui/icons';
import { Button } from '../ui';
import { ActorAttribution } from '../Chat/ActorLabel';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { formatMessageTime } from '../../utils/messageTime';
import { notificationOptions } from './askOptions';
import { AnswerIcon, OptionList } from './OptionList';

/** The statuses that end a question without an answer, and how the card says so. */
const CLOSED_TEXT: Record<string, string> = {
  expired: 'Expired without an answer',
  dismissed: 'Dismissed without an answer',
};

/**
 * The answer part of a question or an approval, the same in the chat, on the
 * Notifications page and in a toast: the options while it waits, then one
 * line with the answer and "Show options" to open the list, read-only.
 * `onAnswered` runs when the server took an answer given here.
 */
export function NotificationOptions({ notif, onAnswered }: {
  notif: Notification;
  onAnswered?: () => void;
}) {
  const answerNotification = useNotificationStore(s => s.answerNotification);
  // The answer on its way. The server sends the answer event before it
  // replies, so the row has its new status when this clears.
  const [sending, setSending] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);
  const [open, setOpen] = useState(false);
  const listId = useId();

  if (notif.type === 'notify') return null;
  const options = notificationOptions(notif);
  const isApproval = notif.type === 'approval';

  if (notif.status === 'pending') {
    const choose = async (value: string) => {
      setSending(value);
      setFailed(false);
      const ok = await answerNotification(notif.id, value);
      setSending(null);
      setFailed(!ok);
      if (ok) onAnswered?.();
    };
    return (
      <div className="mt-3 space-y-2">
        {/* A question does not say this: the agent may go on without the answer. */}
        {isApproval && <p className="text-xs text-text-muted">Nothing happens until you choose.</p>}
        <OptionList
          options={options}
          label={notif.title}
          onChoose={choose}
          allowOther={!isApproval}
          disabled={sending !== null}
          chosen={sending}
        />
        {failed && <p role="alert" className="text-xs text-error">Could not send the answer. Try again.</p>}
      </div>
    );
  }

  const answered = notif.status === 'answered';
  const closed = CLOSED_TEXT[notif.status];
  if (!answered && !closed) return null;
  const answer = notif.answer ?? '';
  const chosen = options.find(o => o.value === answer);
  return (
    <div className="mt-3">
      <div className="flex items-start gap-2">
        {answered
          ? <AnswerIcon mark={chosen?.mark} />
          : <Clock size={14} className="shrink-0 mt-0.5 text-text-faint" aria-hidden="true" />}
        <div className="flex-1 min-w-0">
          {answered ? (
            <>
              <p className="text-sm font-medium text-text break-words">
                <span className="sr-only">Answer: </span>
                {chosen?.label ?? answer}
              </p>
              <AnswerMeta notif={notif} />
            </>
          ) : (
            <p className="text-sm text-text-muted">{closed}</p>
          )}
        </div>
        {options.length > 0 && (
          <Button
            variant="ghost"
            size="xs"
            className="shrink-0 -my-0.5"
            aria-expanded={open}
            aria-controls={listId}
            onClick={() => setOpen(o => !o)}
          >
            {open ? <ChevronUp size={12} aria-hidden="true" /> : <ChevronDown size={12} aria-hidden="true" />}
            {open ? 'Hide options' : 'Show options'}
          </Button>
        )}
      </div>
      <div id={listId} className={open ? 'mt-2' : undefined}>
        {open && <OptionList options={options} label={notif.title} chosen={answered ? answer : null} />}
      </div>
    </div>
  );
}

/** Who answered, when, and through which channel. */
function AnswerMeta({ notif }: { notif: Notification }) {
  const parts = [
    notif.answered_by_actor_id
      && <ActorAttribution actorId={notif.answered_by_actor_id} causeId={null} verb="Answered by" />,
    formatMessageTime(notif.answered_at ?? undefined),
    notif.answered_by && `via ${notif.answered_by}`,
  ].filter(Boolean);
  return (
    <p className="text-xs text-text-faint flex flex-wrap items-center gap-x-1.5 min-w-0">
      {parts.map((part, i) => (
        <Fragment key={i}>
          {i > 0 && <span aria-hidden="true">·</span>}
          <span className="min-w-0">{part}</span>
        </Fragment>
      ))}
    </p>
  );
}
