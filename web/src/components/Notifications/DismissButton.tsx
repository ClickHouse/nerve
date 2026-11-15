import { useState } from 'react';
import { Button, cx } from '../ui';
import { EyeOff } from '../ui/icons';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';

/**
 * Dismiss for a pending notification, on the Notifications page and in the
 * chat. A question or an approval then ends without an answer, and the agent
 * that asked a question gets a note. When the server refuses, the row is read
 * again: an answer from a different place replaces the button.
 */
export function DismissButton({ notif, className }: { notif: Notification; className?: string }) {
  const dismissNotification = useNotificationStore(s => s.dismissNotification);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  if (notif.status !== 'pending') return null;

  const dismiss = async () => {
    setBusy(true);
    setFailed(false);
    const ok = await dismissNotification(notif.id);
    setBusy(false);
    setFailed(!ok);
  };

  return (
    <span className={cx('inline-flex flex-wrap items-center gap-2', className)}>
      <Button variant="ghost" size="xs" onClick={dismiss} disabled={busy}>
        <EyeOff size={11} aria-hidden="true" />
        <span>Dismiss</span>
      </Button>
      {failed && <span role="alert" className="text-xs text-error">Could not dismiss. Try again.</span>}
    </span>
  );
}
