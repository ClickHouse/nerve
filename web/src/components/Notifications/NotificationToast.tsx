import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { IconButton } from '../ui';
import { Bell, HelpCircle, ShieldCheck, X } from '../ui/icons';
import { NotificationBody, NotificationLinks } from './NotificationParts';
import { NotificationOptions } from './NotificationOptions';

/** How long a toast that waits for nothing stays open. */
const TOAST_DURATION = 5000;
const MAX_VISIBLE = 3;

export function NotificationToast() {
  const toastQueue = useNotificationStore(s => s.toastQueue);
  if (toastQueue.length === 0) return null;

  // Above the bottom navigation on a phone; bottom right on a desktop.
  return (
    <div className="fixed z-50 inset-x-2 bottom-[calc(env(safe-area-inset-bottom)+4rem)] md:inset-x-auto md:right-4 md:bottom-4 md:w-96 flex flex-col gap-2">
      {toastQueue.slice(0, MAX_VISIBLE).map(notif => (
        <Toast key={notif.id} notif={notif} />
      ))}
    </div>
  );
}

/**
 * One toast. A question or an approval stays open until it is answered here
 * or dismissed. An answer from another place folds it to the answer, and it
 * then closes after the same time as a plain notification.
 */
function Toast({ notif }: { notif: Notification }) {
  const dismissToast = useNotificationStore(s => s.dismissToast);
  const navigate = useNavigate();
  const waitsForAnswer = notif.type !== 'notify' && notif.status === 'pending';

  useEffect(() => {
    if (waitsForAnswer) return;
    const timer = setTimeout(() => dismissToast(notif.id), TOAST_DURATION);
    return () => clearTimeout(timer);
  }, [waitsForAnswer, notif.id, dismissToast]);

  const close = () => dismissToast(notif.id);
  const Icon = notif.type === 'approval' ? ShieldCheck : notif.type === 'question' ? HelpCircle : Bell;
  const iconColor = notif.type === 'approval' ? 'text-hue-violet' : notif.type === 'question' ? 'text-hue-blue' : 'text-accent';

  return (
    <div className="max-h-[60dvh] overflow-y-auto bg-surface-raised border border-border-subtle rounded-lg shadow-xl p-3 animate-slide-in">
      <div className="flex items-start gap-2">
        <Icon size={16} className={`${iconColor} shrink-0 mt-0.5`} />
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-2">
            <p
              className="text-sm font-medium text-text cursor-pointer hover:text-accent"
              onClick={() => {
                navigate('/notifications');
                close();
              }}
            >
              {notif.title}
            </p>
            <IconButton label="Dismiss" size="xs" onClick={close}>
              <X size={14} />
            </IconButton>
          </div>
          {notif.body && (
            <NotificationBody body={notif.body} className="text-xs text-text-muted mt-0.5 line-clamp-3" />
          )}
          <NotificationLinks notif={notif} size="xs" onOpen={close} />
          <NotificationOptions notif={notif} onAnswered={close} />
        </div>
      </div>
    </div>
  );
}
