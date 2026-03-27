import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, X, CheckCheck, EyeOff } from 'lucide-react';
import { useNotificationStore, type Notification } from '../stores/notificationStore';
import { NOTIFICATION_STATUS_STYLES as STATUS_STYLES } from '../constants/statusStyles';

const PRIORITY_DOTS: Record<string, string> = {
  urgent: 'bg-red-500',
  high: 'bg-orange-400',
  normal: '',
  low: '',
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
];

function FreeTextInput({ onSubmit }: { onSubmit: (text: string) => void }) {
  const [text, setText] = useState('');
  const [open, setOpen] = useState(false);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="px-3 py-1 text-sm text-[#666] border border-dashed border-[#444] rounded-lg hover:border-[#666] hover:text-[#888] cursor-pointer"
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
        className="flex-1 bg-[#1a1a1a] border border-[#333] rounded-lg px-3 py-1 text-sm text-[#e0e0e0] outline-none focus:border-[#6366f1]"
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
        className="px-3 py-1 bg-[#6366f1]/15 text-[#6366f1] rounded-lg text-sm border border-[#6366f1]/30 hover:bg-[#6366f1]/25 cursor-pointer"
      >
        Send
      </button>
      <button
        onClick={() => { setText(''); setOpen(false); }}
        className="text-[#666] hover:text-[#999] cursor-pointer"
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

  return (
    <div className={`p-4 bg-[#141414] border rounded-lg transition-colors ${
      notif.status === 'pending' ? 'border-[#333]' : 'border-[#222]'
    }`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            {priorityDot && <span className={`w-2 h-2 rounded-full shrink-0 ${priorityDot}`} />}
            <h3 className="font-medium text-[15px] text-[#e0e0e0]">{notif.title}</h3>
          </div>
          {notif.body && (
            <p className="text-sm text-[#888] mt-1 whitespace-pre-wrap">{notif.body}</p>
          )}
        </div>
        <div className="flex items-center gap-2 text-[12px] shrink-0">
          <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[notif.status] || STATUS_STYLES.dismissed}`}>
            {notif.status}
          </span>
          <span className={`px-2 py-0.5 rounded-full border ${notif.type === 'question' ? 'bg-blue-400/10 text-blue-400 border-blue-400/20' : 'bg-[#333]/50 text-[#888] border-[#333]'}`}>
            {notif.type}
          </span>
        </div>
      </div>

      {/* Session link + meta */}
      <div className="flex items-center gap-3 mt-2 text-[12px]">
        <button
          onClick={() => navigate(`/chat/${notif.session_id}`)}
          className="text-[#6366f1] hover:underline cursor-pointer"
        >
          Session: {notif.session_title || notif.session_id}
        </button>
        <span className="text-[#444]">{notif.created_at?.slice(0, 16).replace('T', ' ')}</span>
        {notif.status === 'pending' && notif.type === 'notify' && (
          <button
            onClick={() => dismissNotification(notif.id)}
            className="flex items-center gap-1 px-2 py-0.5 rounded text-[#777] hover:text-[#bbb] hover:bg-[#1f1f1f] cursor-pointer transition-colors"
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
              className="px-3 py-1.5 bg-[#6366f1]/15 text-[#6366f1] rounded-lg text-sm border border-[#6366f1]/30 hover:bg-[#6366f1]/25 cursor-pointer transition-colors"
            >
              {opt}
            </button>
          ))}
          <FreeTextInput onSubmit={(text) => answerNotification(notif.id, text)} />
        </div>
      )}

      {/* Show answer if answered */}
      {notif.status === 'answered' && (
        <div className="mt-2 text-sm text-emerald-400">
          Answer: {notif.answer} <span className="text-[#555]">(via {notif.answered_by})</span>
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
      <div className="border-b border-[#222] px-6 py-3 flex items-center gap-4 bg-[#0f0f0f] shrink-0">
        <Bell size={18} className="text-[#6366f1]" />
        <h1 className="text-lg font-semibold">Notifications</h1>

        {/* Status filters */}
        <div className="flex items-center gap-1 ml-2">
          {STATUS_FILTERS.map(f => (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={`px-3 py-1 text-[12px] rounded-full border cursor-pointer transition-colors
                ${filter === f.value
                  ? 'bg-[#6366f1]/15 text-[#6366f1] border-[#6366f1]/30'
                  : 'text-[#666] border-[#2a2a2a] hover:border-[#444] hover:text-[#999]'
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
                  ? 'bg-[#6366f1]/15 text-[#6366f1] border-[#6366f1]/30'
                  : 'text-[#666] border-[#2a2a2a] hover:border-[#444] hover:text-[#999]'
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
            className="ml-auto flex items-center gap-1.5 px-3 py-1 text-[12px] rounded-lg border border-[#2a2a2a] text-[#888] hover:text-[#ccc] hover:border-[#444] hover:bg-[#1a1a1a] cursor-pointer transition-colors"
          >
            <CheckCheck size={13} />
            Dismiss All
          </button>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="text-[#444] text-center py-10">Loading...</div>
        ) : notifications.length === 0 ? (
          <div className="text-[#444] text-center py-10">
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
