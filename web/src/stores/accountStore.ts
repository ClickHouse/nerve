import { create } from 'zustand';
import { api, type Account } from '../api/client';
import { useAuthStore } from './authStore';

/**
 * The server's message, out of the error the API layer throws.
 *
 * `request()` raises `Error("409: {\"detail\": \"...\"}")` for anything that is
 * not a 401. The detail is the whole value here — "set a password on the
 * existing account first" is the answer to "why can I not add anybody?", and
 * "409: {...}" is not.
 */
export function errorDetail(e: unknown, fallback = 'Something went wrong'): string {
  const raw = e instanceof Error ? e.message : String(e ?? '');
  const body = raw.slice(raw.indexOf(':') + 1).trim();
  try {
    const parsed = JSON.parse(body);
    if (typeof parsed?.detail === 'string') return parsed.detail;
    // FastAPI's own validation errors are a list of {msg, loc}.
    if (Array.isArray(parsed?.detail) && parsed.detail[0]?.msg) {
      return String(parsed.detail[0].msg);
    }
  } catch {
    // Not JSON — fall through to the raw text.
  }
  return raw || fallback;
}

export interface AccountState {
  accounts: Account[];
  loading: boolean;
  /** The account a disable/enable is in flight for, so only its row is busy. */
  busyId: string | null;
  error: string | null;
  load: () => Promise<void>;
  create: (body: { username: string; password: string; display_name?: string }) => Promise<boolean>;
  update: (id: string, body: { username?: string; display_name?: string }) => Promise<boolean>;
  setEnabled: (id: string, enabled: boolean) => Promise<boolean>;
  changeOwnPassword: (body: { current_password?: string; new_password: string }) => Promise<boolean>;
  clearError: () => void;
}

/**
 * Every mutation reloads the list and re-reads `/api/auth/status`.
 *
 * Not laziness: both of the things this screen does — setting the first
 * password, adding the second account — change how the *login form* behaves,
 * and a stale descriptor would leave a tab auto-logging-in or asking for the
 * wrong fields. The list is small and the call is one request.
 */
async function refreshAll(set: (partial: Partial<AccountState>) => void): Promise<void> {
  const { accounts } = await api.listAccounts();
  set({ accounts, loading: false });
  await useAuthStore.getState().refreshStatus();
}

export const useAccountStore = create<AccountState>((set) => ({
  accounts: [],
  loading: true,
  busyId: null,
  error: null,

  clearError: () => set({ error: null }),

  load: async () => {
    try {
      await refreshAll(set);
      set({ error: null });
    } catch (e) {
      set({ loading: false, error: errorDetail(e, 'Could not load accounts') });
    }
  },

  create: async (body) => {
    set({ error: null });
    try {
      await api.createAccount(body);
      await refreshAll(set);
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not create the account') });
      return false;
    }
  },

  update: async (id, body) => {
    set({ error: null, busyId: id });
    try {
      await api.updateAccount(id, body);
      await refreshAll(set);
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not update the account') });
      return false;
    } finally {
      set({ busyId: null });
    }
  },

  setEnabled: async (id, enabled) => {
    set({ error: null, busyId: id });
    try {
      await api.setAccountEnabled(id, enabled);
      await refreshAll(set);
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not change the account') });
      return false;
    } finally {
      set({ busyId: null });
    }
  },

  changeOwnPassword: async (body) => {
    set({ error: null });
    try {
      await api.changeOwnPassword(body);
      await refreshAll(set);
      return true;
    } catch (e) {
      set({ error: errorDetail(e, 'Could not change the password') });
      return false;
    }
  },
}));

/** The signed-in account's own row, once the list has loaded. */
export function selectSelf(state: AccountState): Account | undefined {
  return state.accounts.find((account) => account.is_self);
}

/** Whether anything blocks adding a person, and what to say about it. */
export function blockedReason(accounts: Account[]): string | null {
  if (accounts.length === 1 && !accounts[0].has_password) {
    return 'Set a password on this account before adding anyone — until then '
      + 'every caller is signed in as it, and a second account could not be '
      + 'told apart from the first.';
  }
  const unnamed = accounts.find((account) => !account.username);
  if (unnamed) {
    return 'Give this account a username before adding anyone — an account '
      + 'without one cannot be signed in to once a second account exists.';
  }
  return null;
}

export { useAccountStore as default };
