import { create } from 'zustand';
import { api } from '../api/client';

export interface Notification {
  id: string;
  session_id: string;
  session_title: string | null;
  type: 'notify' | 'question' | 'approval';
  title: string;
  body: string;
  priority: string;
  status: string;
  options: string[] | null;
  answer: string | null;
  answered_by: string | null;
  answered_at: string | null;
  created_at: string;
  expires_at: string | null;
  // Set while a snoozed row waits for the maintenance tick to fan it
  // back out. Present on pending rows only; cleared on re-delivery.
  redeliver_at?: string | null;
  // How many times the row has been re-delivered (snooze round trips).
  redelivery_count?: number;
  target_kind?: string | null;
  target_id?: string | null;
  // Optional label map for approval-kind rows: value -> human label.
  // Sent on the WS notification payload and stored on the row metadata.
  option_labels?: Record<string, string> | null;
  metadata?: string | Record<string, unknown> | null;
}

// A deterministic suppression rule. Matched notify's are persisted as
// status='silenced' and not delivered; priority is never changed.
export interface Silence {
  id: string;
  pattern: string;
  action: string;
  reason: string;
  created_by: string;
  created_at: string;
  expires_at: string | null;
  hit_count: number;
  last_hit_at: string | null;
  override_count: number;
  last_override_at: string | null;
  enabled: number;
}

interface NotificationState {
  notifications: Notification[];
  pendingCount: number;
  filter: string;
  typeFilter: string;
  loading: boolean;
  toastQueue: Notification[];
  silences: Silence[];

  loadNotifications: () => Promise<void>;
  setFilter: (f: string) => void;
  setTypeFilter: (f: string) => void;
  answerNotification: (id: string, answer: string) => Promise<void>;
  dismissNotification: (id: string) => Promise<void>;
  dismissAll: () => Promise<void>;
  handleWSNotification: (data: any) => void;
  handleWSNotificationAnswered: (data: any) => void;
  handleWSNotificationExpired: (data: any) => void;
  dismissToast: (id: string) => void;
  loadSilences: () => Promise<void>;
  addSilence: (pattern: string, reason: string, ttlHours: number) => Promise<void>;
  removeSilence: (id: string) => Promise<void>;
}

export const useNotificationStore = create<NotificationState>((set, get) => ({
  notifications: [],
  pendingCount: 0,
  filter: 'pending',
  typeFilter: '',
  loading: true,
  toastQueue: [],
  silences: [],

  loadNotifications: async () => {
    try {
      const { filter, typeFilter } = get();
      const data = await api.listNotifications(filter || undefined, typeFilter || undefined);
      set({
        notifications: data.notifications,
        pendingCount: data.pending_count,
        loading: false,
      });
    } catch (e) {
      console.error('Failed to load notifications:', e);
      set({ loading: false });
    }
  },

  setFilter: (f: string) => {
    set({ filter: f });
    get().loadNotifications();
  },

  setTypeFilter: (f: string) => {
    set({ typeFilter: f });
    get().loadNotifications();
  },

  answerNotification: async (id: string, answer: string) => {
    try {
      await api.answerNotification(id, answer);
      get().loadNotifications();
    } catch (e) {
      console.error('Failed to answer notification:', e);
    }
  },

  dismissNotification: async (id: string) => {
    try {
      await api.dismissNotification(id);
      get().loadNotifications();
    } catch (e) {
      console.error('Failed to dismiss notification:', e);
    }
  },

  dismissAll: async () => {
    try {
      await api.dismissAllNotifications();
      get().loadNotifications();
    } catch (e) {
      console.error('Failed to dismiss all:', e);
    }
  },

  handleWSNotification: (data: any) => {
    const silenced = data.silenced === true;
    const redelivered = data.redelivered === true;
    const notif: Notification = {
      id: data.notification_id,
      session_id: data.session_id,
      session_title: null,
      type: data.notification_type,
      title: data.title,
      body: data.body,
      priority: data.priority,
      status: silenced ? 'silenced' : 'pending',
      options: data.options,
      answer: null,
      answered_by: null,
      answered_at: null,
      created_at: new Date().toISOString(),
      expires_at: null,
      redeliver_at: null,
      redelivery_count: data.redelivery_count ?? 0,
      target_kind: data.target_kind ?? null,
      target_id: data.target_id ?? null,
      option_labels: data.option_labels ?? null,
      // Carry the silence context inline so the greyed row can render
      // the reason/pattern without a metadata round-trip.
      metadata: silenced
        ? {
            silenced_by: data.silenced_by ?? null,
            silence_reason: data.silence_reason ?? '',
            silence_pattern: data.silence_pattern ?? '',
          }
        : null,
    };

    set(s => {
      // A re-delivered (previously snoozed) row already exists in the
      // list — refresh it in place and move it to the top instead of
      // duplicating. Its pending count was never decremented on snooze,
      // so it doesn't count again.
      const existing = redelivered
        ? s.notifications.find(n => n.id === notif.id)
        : undefined;
      const rest = existing
        ? s.notifications.filter(n => n.id !== notif.id)
        : s.notifications;
      return {
        notifications: [existing ? { ...existing, ...notif } : notif, ...rest],
        // A silenced notification was NOT delivered/escalated — it does not
        // count as pending and never raises a toast/sound. A re-delivered
        // row was already pending (snooze never decremented it), so it
        // must not count twice.
        pendingCount: silenced || redelivered ? s.pendingCount : s.pendingCount + 1,
        toastQueue: silenced ? s.toastQueue : [...s.toastQueue, notif],
      };
    });
  },

  handleWSNotificationAnswered: (data: any) => {
    // A snoozed approval stays pending server-side — mirror that: keep
    // the row actionable, stamp when it will resurface, leave the
    // pending count alone (matches the server's pending_count).
    if (data.approval_status === 'snoozed') {
      set(s => ({
        notifications: s.notifications.map(n =>
          n.id === data.notification_id
            ? { ...n, status: 'pending', redeliver_at: data.snooze_until ?? null }
            : n
        ),
      }));
      return;
    }
    set(s => ({
      notifications: s.notifications.map(n =>
        n.id === data.notification_id
          ? { ...n, status: 'answered', answer: data.answer, answered_by: data.answered_by }
          : n
      ),
      pendingCount: Math.max(0, s.pendingCount - 1),
    }));
  },

  handleWSNotificationExpired: (data: any) => {
    set(s => {
      const wasPending = s.notifications.some(
        n => n.id === data.notification_id && n.status === 'pending'
      );
      return {
        notifications: s.notifications.map(n =>
          n.id === data.notification_id ? { ...n, status: 'expired' } : n
        ),
        pendingCount: wasPending
          ? Math.max(0, s.pendingCount - 1)
          : s.pendingCount,
      };
    });
  },

  dismissToast: (id: string) => {
    set(s => ({ toastQueue: s.toastQueue.filter(n => n.id !== id) }));
  },

  loadSilences: async () => {
    try {
      const data = await api.listSilences();
      set({ silences: data.silences });
    } catch (e) {
      console.error('Failed to load silences:', e);
    }
  },

  addSilence: async (pattern: string, reason: string, ttlHours: number) => {
    try {
      await api.createSilence(pattern, reason, ttlHours);
      get().loadSilences();
    } catch (e) {
      console.error('Failed to create silence:', e);
      throw e;
    }
  },

  removeSilence: async (id: string) => {
    try {
      await api.deleteSilence(id);
      get().loadSilences();
    } catch (e) {
      console.error('Failed to delete silence:', e);
    }
  },
}));
