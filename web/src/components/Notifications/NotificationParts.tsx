import { useNavigate } from 'react-router-dom';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { Button, type ButtonSize, cx } from '../ui';
import { notificationRefs, type Notification } from '../../stores/notificationStore';

/**
 * A notification body, as markdown. A single line break stays a line break,
 * as in the plain text that agents often send.
 */
export function NotificationBody({ body, className }: { body: string; className?: string }) {
  return (
    <div className={cx('[&_p]:whitespace-pre-wrap [&_li]:whitespace-pre-wrap', className)}>
      <MarkdownContent content={body} />
    </div>
  );
}

/**
 * "Open plan" and "Open task" for a notification that names them. `onOpen`
 * runs before the navigation, for example to close a toast.
 */
export function NotificationLinks({ notif, size = 'md', onOpen }: {
  notif: Notification;
  size?: ButtonSize;
  onOpen?: () => void;
}) {
  const navigate = useNavigate();
  const { planId, taskId } = notificationRefs(notif);
  if (!planId && !taskId) return null;

  const open = (path: string) => {
    onOpen?.();
    navigate(path);
  };

  return (
    <div className="mt-2 flex flex-wrap gap-1.5">
      {planId && (
        <Button
          variant="accentSoft"
          size={size}
          onClick={() => open(`/plans/${encodeURIComponent(planId)}`)}
        >
          Open plan
        </Button>
      )}
      {taskId && (
        <Button
          variant="secondary"
          size={size}
          onClick={() => open(`/tasks/${encodeURIComponent(taskId)}`)}
        >
          Open task
        </Button>
      )}
    </div>
  );
}
