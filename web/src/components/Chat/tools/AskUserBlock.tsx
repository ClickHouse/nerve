import { useEffect, type ReactNode } from 'react';
import { HelpCircle, Loader2 } from '../../ui/icons';
import { Badge, type BadgeTone } from '../../ui';
import type { ToolCallBlockData } from '../../../types/chat';
import { useNotificationStore } from '../../../stores/notificationStore';
import { extractResultText } from '../../../utils/extractResultText';
import { NotificationBody } from '../../Notifications/NotificationParts';
import { NotificationOptions } from '../../Notifications/NotificationOptions';
import { OptionList } from '../../Notifications/OptionList';
import { parseAskOptions, questionIdFromResult, questionOptions } from '../../Notifications/askOptions';

const PRIORITY_TONES: Record<string, BadgeTone> = {
  urgent: 'danger',
  high: 'warning',
};

/**
 * Nerve's `ask_user` call in the chat: the question with its options,
 * answerable here while it waits. It reads the question's notification, so
 * an answer from any place (this card, the Notifications page, a toast, a
 * chat channel) folds it to the answer.
 */
export function AskUserBlock({ block }: { block: ToolCallBlockData }) {
  const id = block.isError ? null : questionIdFromResult(block.result);
  const row = useNotificationStore(s => (id ? s.byId[id] : undefined));
  const fetchNotification = useNotificationStore(s => s.fetchNotification);
  useEffect(() => {
    if (id) fetchNotification(id);
  }, [id, fetchNotification]);

  const title = String(block.input.title || '');
  const body = String(block.input.body || '');
  const priority = String(block.input.priority || 'normal');
  const running = block.status === 'running';
  const waiting = row?.status === 'pending';

  let content: ReactNode = null;
  if (running) {
    content = (
      <p className="mt-2 flex items-center gap-1.5 text-xs text-text-dim">
        <Loader2 size={12} className="animate-spin" aria-hidden="true" /> Sending…
      </p>
    );
  } else if (!id) {
    const text = block.result ? extractResultText(block.result) : '';
    content = <p className="mt-2 text-xs text-error whitespace-pre-wrap">{text || 'The question was not sent.'}</p>;
  } else if (row) {
    content = <NotificationOptions notif={row} />;
  } else if (row === null) {
    // The question could not be read: its options, without a status.
    const options = questionOptions(parseAskOptions(block.input.options));
    if (options.length > 0) content = <div className="mt-3"><OptionList options={options} label={title} /></div>;
  }

  return (
    <div className="my-2 rounded-lg border border-border bg-surface px-3.5 py-3">
      <div className="flex items-start gap-2">
        <HelpCircle size={15} className="mt-0.5 shrink-0 text-hue-blue" aria-hidden="true" />
        <p className="flex-1 min-w-0 text-sm font-medium text-text break-words">{title || 'Question'}</p>
        {waiting && priority in PRIORITY_TONES && (
          <Badge tone={PRIORITY_TONES[priority]} className="shrink-0">{priority}</Badge>
        )}
      </div>
      {body && (waiting || row === null) && (
        <NotificationBody body={body} className="mt-1 text-sm text-text-muted" />
      )}
      {content}
    </div>
  );
}
