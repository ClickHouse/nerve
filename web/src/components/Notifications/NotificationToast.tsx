import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, HelpCircle, ShieldCheck, X } from 'lucide-react';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';

const TOAST_DURATION = 5000;

const APPROVAL_QUICK_BUTTON_STYLES: Record<string, string> = {
  approve: 'bg-emerald-400/15 text-hue-emerald border-emerald-400/30 hover:bg-emerald-400/25',
  decline: 'bg-red-400/15 text-hue-red border-red-400/30 hover:bg-red-400/25',
  snooze_24h: 'bg-border-subtle/40 text-text-muted border-border-subtle hover:bg-border-subtle/60',
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
                  <button
                    onClick={() => dismissToast(notif.id)}
                    className="text-text-faint hover:text-text-muted shrink-0 cursor-pointer"
                  >
                    <X size={14} />
                  </button>
                </div>
                {notif.body && (
                  <p className="text-xs text-text-muted mt-0.5 line-clamp-2">{notif.body}</p>
                )}
                {/* Quick answer buttons for questions */}
                {isQuestion && options && notif.status === 'pending' && (
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {options.slice(0, 3).map((opt: string) => (
                      <button
                        key={opt}
                        onClick={() => {
                          answerNotification(notif.id, opt);
                          dismissToast(notif.id);
                        }}
                        className="px-2 py-0.5 bg-accent/15 text-accent rounded text-xs border border-accent/30 hover:bg-accent/25 cursor-pointer"
                      >
                        {opt}
                      </button>
                    ))}
                  </div>
                )}
                {/* Quick action buttons for approvals */}
                {isApproval && options && notif.status === 'pending' && (
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {options.slice(0, 3).map((value: string) => {
                      const style = APPROVAL_QUICK_BUTTON_STYLES[value] || 'bg-accent/15 text-accent border-accent/30 hover:bg-accent/25';
                      return (
                        <button
                          key={value}
                          onClick={() => {
                            answerNotification(notif.id, value);
                            dismissToast(notif.id);
                          }}
                          className={`px-2 py-0.5 rounded text-xs border cursor-pointer ${style}`}
                        >
                          {quickLabel(value, notif)}
                        </button>
                      );
                    })}
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
