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
  target_kind?: string | null;
  target_id?: string | null;
  // Optional label map for approval-kind rows: value -> human label.
  // Sent on the WS notification payload and stored on the row metadata.
  option_labels?: Record<string, string> | null;
  metadata?: string | Record<string, unknown> | null;
}

interface NotificationState {
  notifications: Notification[];
  pendingCount: number;
  filter: string;
  typeFilter: string;
  loading: boolean;
  toastQueue: Notification[];

  loadNotifications: () => Promise<void>;
  setFilter: (f: string) => void;
  setTypeFilter: (f: string) => void;
  answerNotification: (id: string, answer: string) => Promise<void>;
  dismissNotification: (id: string) => Promise<void>;
  dismissAll: () => Promise<void>;
  handleWSNotification: (data: any) => void;
  handleWSNotificationAnswered: (data: any) => void;
  dismissToast: (id: string) => void;
}

export const useNotificationStore = create<NotificationState>((set, get) => ({
  notifications: [],
  pendingCount: 0,
  filter: 'pending',
  typeFilter: '',
  loading: true,
  toastQueue: [],

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
    const notif: Notification = {
      id: data.notification_id,
      session_id: data.session_id,
      session_title: null,
      type: data.notification_type,
      title: data.title,
      body: data.body,
      priority: data.priority,
      status: 'pending',
      options: data.options,
      answer: null,
      answered_by: null,
      answered_at: null,
      created_at: new Date().toISOString(),
      expires_at: null,
      target_kind: data.target_kind ?? null,
      target_id: data.target_id ?? null,
      option_labels: data.option_labels ?? null,
    };

    set(s => ({
      notifications: [notif, ...s.notifications],
      pendingCount: s.pendingCount + 1,
      toastQueue: [...s.toastQueue, notif],
    }));
  },

  handleWSNotificationAnswered: (data: any) => {
    set(s => ({
      notifications: s.notifications.map(n =>
        n.id === data.notification_id
          ? { ...n, status: 'answered', answer: data.answer, answered_by: data.answered_by }
          : n
      ),
      pendingCount: Math.max(0, s.pendingCount - 1),
    }));
  },

  dismissToast: (id: string) => {
    set(s => ({ toastQueue: s.toastQueue.filter(n => n.id !== id) }));
  },
}));
