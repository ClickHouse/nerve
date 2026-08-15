import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { Button, IconButton, type ButtonVariant } from '../ui';
import { Bell, HelpCircle, ShieldCheck, X } from '../ui/icons';

const TOAST_DURATION = 5000;

/**
 * The three canonical approval answers, as button variants rather than the
 * hand-rolled `emerald-400/15` + `red-400/15` class strings they replace —
 * which were stock Tailwind, so they did not follow the light theme.
 * `success` and `danger` are both *tinted*: `--theme-success` is a feedback
 * foreground (pale mint in dark mode) and would be unreadable as a solid fill.
 */
const APPROVAL_QUICK_VARIANTS: Record<string, ButtonVariant> = {
  approve: 'success',
  decline: 'danger',
  snooze_24h: 'secondary',
};

const APPROVAL_QUICK_LABELS: Record<string, string> = {
  approve: '✅ Approve',
  decline: '❌ Decline',
  snooze_24h: '💤 Snooze',
};

function quickLabel(value: string, notif: Notification): string {
  const labels = notif.option_labels;
  if (labels && labels[value]) return labels[value];
  return APPROVAL_QUICK_LABELS[value] || value;
}

export function NotificationToast() {
  const { toastQueue, dismissToast, answerNotification } = useNotificationStore();
  const navigate = useNavigate();

  // Auto-dismiss toasts after duration
  useEffect(() => {
    if (toastQueue.length === 0) return;
    const timer = setTimeout(() => {
      dismissToast(toastQueue[0].id);
    }, TOAST_DURATION);
    return () => clearTimeout(timer);
  }, [toastQueue]);

  if (toastQueue.length === 0) return null;

  // Show max 3 toasts
  const visible = toastQueue.slice(0, 3);

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
      {visible.map((notif) => {
        const isQuestion = notif.type === 'question';
        const isApproval = notif.type === 'approval';
        const options = notif.options ? (typeof notif.options === 'string' ? JSON.parse(notif.options) : notif.options) : null;

        return (
          <div
            key={notif.id}
            className="bg-surface-raised border border-border-subtle rounded-lg shadow-xl p-3 animate-slide-in"
          >
            <div className="flex items-start gap-2">
              {isApproval ? (
                <ShieldCheck size={16} className="text-hue-violet shrink-0 mt-0.5" />
              ) : isQuestion ? (
                <HelpCircle size={16} className="text-hue-blue shrink-0 mt-0.5" />
              ) : (
                <Bell size={16} className="text-accent shrink-0 mt-0.5" />
              )}
              <div className="flex-1 min-w-0">
                <div className="flex items-start justify-between gap-2">
                  <p
                    className="text-sm font-medium text-text cursor-pointer hover:text-accent"
                    onClick={() => {
                      navigate('/notifications');
                      dismissToast(notif.id);
                    }}
                  >
                    {notif.title}
                  </p>
                  <IconButton
                    label="Dismiss"
                    size="xs"
                    onClick={() => dismissToast(notif.id)}
                  >
                    <X size={14} />
                  </IconButton>
                </div>
                {notif.body && (
                  <p className="text-xs text-text-muted mt-0.5 line-clamp-2">{notif.body}</p>
                )}
                {/* Quick answer buttons for questions */}
                {isQuestion && options && notif.status === 'pending' && (
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {/* The pill chip, in its selected treatment: an answer
                        option is a choice among a set, and `pill active` is
                        the accent-on-accent-tint the hand-rolled classes here
                        were already spelling out. */}
                    {options.slice(0, 3).map((opt: string) => (
                      <Button
                        key={opt}
                        variant="pill"
                        size="xs"
                        active
                        onClick={() => {
                          answerNotification(notif.id, opt);
                          dismissToast(notif.id);
                        }}
                      >
                        {opt}
                      </Button>
                    ))}
                  </div>
                )}
                {/* Quick action buttons for approvals */}
                {isApproval && options && notif.status === 'pending' && (
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {options.slice(0, 3).map((value: string) => (
                      <Button
                        key={value}
                        variant={APPROVAL_QUICK_VARIANTS[value] ?? 'secondary'}
                        size="xs"
                        onClick={() => {
                          answerNotification(notif.id, value);
                          dismissToast(notif.id);
                        }}
                      >
                        {quickLabel(value, notif)}
                      </Button>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
