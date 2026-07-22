import { create } from 'zustand';
import { api, setToken, clearToken, getToken } from '../api/client';
import { clearAllDrafts } from './helpers/draftStorage';

interface AuthState {
  authenticated: boolean;
  loading: boolean;
  checking: boolean;
  error: string | null;
  login: (password: string) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
}

export const useAuthStore = create<AuthState>((set) => ({
  authenticated: !!getToken(),
  loading: false,
  checking: !getToken(),
  error: null,

  login: async (password: string) => {
    set({ loading: true, error: null });
    try {
      const { token } = await api.login(password);
      setToken(token);
      set({ authenticated: true, loading: false });
    } catch (e: any) {
      set({ error: e.message || 'Login failed', loading: false });
    }
  },

  logout: () => {
    clearToken();
    // Purge unsent drafts so nothing leaks to the next user on a shared browser.
    clearAllDrafts();
    set({ authenticated: false });
  },

  checkAuth: async () => {
    if (!getToken()) {
      // No token — check if auth is even required
      try {
        const { auth_required } = await api.authStatus();
        if (!auth_required) {
          // No password configured — auto-login
          const { token } = await api.login('');
          setToken(token);
          set({ authenticated: true });
          return;
        }
      } catch {
        // Status check failed — fall through to login page
      }
      set({ authenticated: false, checking: false });
      return;
    }
    try {
      await api.checkAuth();
      set({ authenticated: true, checking: false });
    } catch {
      clearToken();
      set({ authenticated: false, checking: false });
    }
  },
}));
