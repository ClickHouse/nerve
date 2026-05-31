import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, X, CheckCheck, EyeOff, Check, XCircle, Moon } from 'lucide-react';
import { useNotificationStore, type Notification } from '../stores/notificationStore';

const STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-hue-yellow border-yellow-400/20',
  answered: 'bg-emerald-400/10 text-hue-emerald border-emerald-400/20',
  expired: 'bg-border-subtle/50 text-text-muted border-border-subtle',
  dismissed: 'bg-border-subtle/50 text-text-dim border-border-subtle',
};

const PRIORITY_DOTS: Record<string, string> = {
  urgent: 'bg-red-500',
  high: 'bg-orange-400',
  normal: '',
  low: '',
};

const TYPE_BADGE_STYLES: Record<string, string> = {
  question: 'bg-blue-400/10 text-hue-blue border-blue-400/20',
  approval: 'bg-violet-400/10 text-hue-violet border-violet-400/20',
  notify: 'bg-border-subtle/50 text-text-muted border-border-subtle',
};

const STATUS_FILTERS = [
  { label: 'All', value: '' },
  { label: 'Pending', value: 'pending' },
  { label: 'Answered', value: 'answered' },
  { label: 'Expired', value: 'expired' },
];

const TYPE_FILTERS = [
  { label: 'All', value: '' },
  { label: 'Notifications', value: 'notify' },
  { label: 'Questions', value: 'question' },
  { label: 'Approvals', value: 'approval' },
];

// Approval-kind button styling. Keyed by the option ``value`` the
// dispatcher receives, not the human label, so the styling stays
// stable even when labels are renamed.
const APPROVAL_BUTTON_STYLES: Record<string, string> = {
  approve:
    'bg-emerald-400/15 text-hue-emerald border-emerald-400/30 hover:bg-emerald-400/25',
  decline:
    'bg-red-400/15 text-hue-red border-red-400/30 hover:bg-red-400/25',
  snooze_24h:
    'bg-border-subtle/40 text-text-muted border-border-subtle hover:bg-border-subtle/60',
};

const APPROVAL_DEFAULT_BUTTON_STYLE =
  'bg-accent/15 text-accent border-accent/30 hover:bg-accent/25';

const APPROVAL_BUTTON_ICONS: Record<string, typeof Check> = {
  approve: Check,
  decline: XCircle,
  snooze_24h: Moon,
};

const APPROVAL_DEFAULT_LABELS: Record<string, string> = {
  approve: 'Approve',
  decline: 'Decline',
  snooze_24h: 'Snooze 24h',
};

function approvalLabel(value: string, labels: Record<string, string> | null | undefined): string {
  if (labels && labels[value]) return labels[value];
  if (APPROVAL_DEFAULT_LABELS[value]) return APPROVAL_DEFAULT_LABELS[value];
  // Fall back to value with underscores replaced and title cased.
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function parseOptionLabels(notif: Notification): Record<string, string> | null {
  if (notif.option_labels) return notif.option_labels;
  if (!notif.metadata) return null;
  try {
    const meta = typeof notif.metadata === 'string' ? JSON.parse(notif.metadata) : notif.metadata;
    if (meta && typeof meta === 'object' && 'option_labels' in meta) {
      const labels = (meta as Record<string, unknown>).option_labels;
      if (labels && typeof labels === 'object') {
        return labels as Record<string, string>;
      }
    }
  } catch {
    // Malformed JSON: just fall back to defaults.
  }
  return null;
}

function FreeTextInput({ onSubmit }: { onSubmit: (text: string) => void }) {
  const [text, setText] = useState('');
  const [open, setOpen] = useState(false);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="px-3 py-1 text-sm text-text-dim border border-dashed border-border rounded-lg hover:border-border-subtle hover:text-text-muted cursor-pointer"
      >
        Custom answer...
      </button>
    );
  }

  return (
    <div className="flex items-center gap-2 w-full mt-1">
      <input
        type="text"
        autoFocus
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && text.trim()) {
            onSubmit(text.trim());
            setText('');
            setOpen(false);
          }
          if (e.key === 'Escape') setOpen(false);
        }}
        className="flex-1 bg-surface-raised border border-border-subtle rounded-lg px-3 py-1 text-sm text-text outline-none focus:border-accent"
        placeholder="Type your answer..."
      />
      <button
        onClick={() => {
          if (text.trim()) {
            onSubmit(text.trim());
            setText('');
            setOpen(false);
          }
        }}
        className="px-3 py-1 bg-accent/15 text-accent rounded-lg text-sm border border-accent/30 hover:bg-accent/25 cursor-pointer"
      >
        Send
      </button>
      <button
        onClick={() => { setText(''); setOpen(false); }}
        className="text-text-dim hover:text-text-muted cursor-pointer"
      >
        <X size={14} />
      </button>
    </div>
  );
}

function NotificationCard({ notif }: { notif: Notification }) {
  const navigate = useNavigate();
  const { answerNotification, dismissNotification } = useNotificationStore();
  const priorityDot = PRIORITY_DOTS[notif.priority];
  const options = notif.options ? (typeof notif.options === 'string' ? JSON.parse(notif.options) : notif.options) : null;
  const isApproval = notif.type === 'approval';
  const optionLabels = isApproval ? parseOptionLabels(notif) : null;

  return (
    <div className={`p-4 bg-surface border rounded-lg transition-colors ${
      notif.status === 'pending' ? 'border-border-subtle' : 'border-border-subtle'
    }`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            {priorityDot && <span className={`w-2 h-2 rounded-full shrink-0 ${priorityDot}`} />}
            <h3 className="font-medium text-[15px] text-text">{notif.title}</h3>
          </div>
          {notif.body && (
            <p className="text-sm text-text-muted mt-1 whitespace-pre-wrap">{notif.body}</p>
          )}
          {isApproval && notif.target_kind && notif.target_id && (
            <p className="text-[11px] text-text-faint mt-1 font-mono">
              {notif.target_kind}: {notif.target_id}
            </p>
          )}
        </div>
        <div className="flex items-center gap-2 text-[12px] shrink-0">
          <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[notif.status] || STATUS_STYLES.dismissed}`}>
            {notif.status}
          </span>
          <span className={`px-2 py-0.5 rounded-full border ${TYPE_BADGE_STYLES[notif.type] || TYPE_BADGE_STYLES.notify}`}>
            {notif.type}
          </span>
        </div>
      </div>

      {/* Session link + meta */}
      <div className="flex items-center gap-3 mt-2 text-[12px]">
        <button
          onClick={() => navigate(`/chat/${notif.session_id}`)}
          className="text-accent hover:underline cursor-pointer"
        >
          Session: {notif.session_title || notif.session_id}
        </button>
        <span className="text-text-faint">{notif.created_at?.slice(0, 16).replace('T', ' ')}</span>
        {notif.status === 'pending' && notif.type === 'notify' && (
          <button
            onClick={() => dismissNotification(notif.id)}
            className="flex items-center gap-1 px-2 py-0.5 rounded text-text-muted hover:text-text-secondary hover:bg-surface-hover cursor-pointer transition-colors"
          >
            <EyeOff size={11} />
            <span>Dismiss</span>
          </button>
        )}
      </div>

      {/* Answer UI for pending questions */}
      {notif.type === 'question' && notif.status === 'pending' && (
        <div className="mt-3 flex flex-wrap gap-2">
          {options?.map((opt: string) => (
            <button
              key={opt}
              onClick={() => answerNotification(notif.id, opt)}
              className="px-3 py-1.5 bg-accent/15 text-accent rounded-lg text-sm border border-accent/30 hover:bg-accent/25 cursor-pointer transition-colors"
            >
              {opt}
            </button>
          ))}
          <FreeTextInput onSubmit={(text) => answerNotification(notif.id, text)} />
        </div>
      )}

      {/* Action UI for pending approvals */}
      {isApproval && notif.status === 'pending' && (
        <div className="mt-3 flex flex-wrap gap-2">
          {options?.map((value: string) => {
            const Icon = APPROVAL_BUTTON_ICONS[value];
            const buttonStyle = APPROVAL_BUTTON_STYLES[value] || APPROVAL_DEFAULT_BUTTON_STYLE;
            const label = approvalLabel(value, optionLabels);
            return (
              <button
                key={value}
                onClick={() => answerNotification(notif.id, value)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm border cursor-pointer transition-colors ${buttonStyle}`}
              >
                {Icon ? <Icon size={14} /> : null}
                <span>{label}</span>
              </button>
            );
          })}
        </div>
      )}

      {/* Show answer if answered */}
      {notif.status === 'answered' && (
        <div className="mt-2 text-sm text-hue-emerald">
          Answer: {isApproval ? approvalLabel(notif.answer || '', optionLabels) : notif.answer}{' '}
          <span className="text-text-faint">(via {notif.answered_by})</span>
        </div>
      )}
    </div>
  );
}

export function NotificationsPage() {
  const {
    notifications, pendingCount, filter, typeFilter, loading,
    loadNotifications, setFilter, setTypeFilter, dismissAll,
  } = useNotificationStore();

  useEffect(() => { loadNotifications(); }, []);

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-border-subtle px-6 py-3 flex items-center gap-4 bg-bg shrink-0">
        <Bell size={18} className="text-accent" />
        <h1 className="text-lg font-semibold">Notifications</h1>

        {/* Status filters */}
        <div className="flex items-center gap-1 ml-2">
          {STATUS_FILTERS.map(f => (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={`px-3 py-1 text-[12px] rounded-full border cursor-pointer transition-colors
                ${filter === f.value
                  ? 'bg-accent/15 text-accent border-accent/30'
                  : 'text-text-dim border-border hover:border-border hover:text-text-muted'
                }`}
            >
              {f.label}
            </button>
          ))}
        </div>

        {/* Type filters */}
        <div className="flex items-center gap-1 ml-1">
          {TYPE_FILTERS.map(f => (
            <button
              key={f.value}
              onClick={() => setTypeFilter(f.value)}
              className={`px-3 py-1 text-[12px] rounded-full border cursor-pointer transition-colors
                ${typeFilter === f.value
                  ? 'bg-accent/15 text-accent border-accent/30'
                  : 'text-text-dim border-border hover:border-border hover:text-text-muted'
                }`}
            >
              {f.label}
            </button>
          ))}
        </div>

        {/* Dismiss All */}
        {pendingCount > 0 && (
          <button
            onClick={dismissAll}
            className="ml-auto flex items-center gap-1.5 px-3 py-1 text-[12px] rounded-lg border border-border text-text-muted hover:text-text-secondary hover:border-border hover:bg-surface-raised cursor-pointer transition-colors"
          >
            <CheckCheck size={13} />
            Dismiss All
          </button>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="text-text-faint text-center py-10">Loading...</div>
        ) : notifications.length === 0 ? (
          <div className="text-text-faint text-center py-10">
            {filter || typeFilter ? 'No matching notifications' : 'No notifications yet.'}
          </div>
        ) : (
          <div className="max-w-3xl mx-auto space-y-2">
            {notifications.map(notif => (
              <NotificationCard key={notif.id} notif={notif} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
