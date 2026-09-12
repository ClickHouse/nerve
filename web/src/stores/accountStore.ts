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

/** What is said when the write landed and only the redraw did not. */
const SAVED_BUT_STALE =
  'Saved. The list could not be refreshed — reload to see the current state.';

/** Replace an account in the list, or append it if it is new. */
function upsert(accounts: Account[], account: Account): Account[] {
  const index = accounts.findIndex((existing) => existing.id === account.id);
  if (index === -1) return [...accounts, account];
  const next = accounts.slice();
  next[index] = account;
  return next;
}

/**
 * Re-read the list and `/api/auth/status`. Returns the failure, or `null`.
 *
 * The status half is not laziness: both of the things this screen does —
 * setting the first password, adding the second account — change how the *login
 * form* behaves, and a stale descriptor would leave a tab auto-logging-in or
 * asking for the wrong fields.
 *
 * Deliberately **not** part of any mutation's error path. A refresh that fails
 * after a write that committed is a redraw problem, and reporting it as a
 * failed write is how a caller ends up retrying a create that already happened
 * (username taken) or a password change with a current password that is no
 * longer current.
 */
async function resync(
  set: (partial: Partial<AccountState>) => void,
): Promise<unknown | null> {
  // Both, independently. They answer different questions of different servers'
  // worth of state, and the list failing used to skip the status refresh
  // entirely — so an install that had just set its first password or added its
  // second account could be left with `setupPending` and `loginMode` describing
  // the instance it was five seconds ago, purely because a list request
  // happened to fail.
  const [listOutcome] = await Promise.allSettled([
    api.listAccounts(),
    // refreshStatus swallows its own failures and keeps the last known answer.
    useAuthStore.getState().refreshStatus(),
  ]);
  if (listOutcome.status === 'rejected') {
    set({ loading: false });
    return listOutcome.reason;
  }
  set({ accounts: listOutcome.value.accounts, loading: false });
  return null;
}

export const useAccountStore = create<AccountState>((set) => ({
  accounts: [],
  loading: true,
  busyId: null,
  error: null,

  clearError: () => set({ error: null }),

  load: async () => {
    set({ error: null });
    const failure = await resync(set);
    if (failure) {
      // The one place the caller wants the reason: nothing was written, so
      // there is nothing to be confused about.
      set({ error: errorDetail(failure, 'Could not load accounts') });
    }
  },

  create: async (body) => applyWrite(
    set, () => api.createAccount(body), 'Could not create the account',
  ),

  update: async (id, body) => applyWrite(
    set, () => api.updateAccount(id, body), 'Could not update the account', id,
  ),

  setEnabled: async (id, enabled) => applyWrite(
    set, () => api.setAccountEnabled(id, enabled), 'Could not change the account', id,
  ),

  changeOwnPassword: async (body) => applyWrite(
    set, () => api.changeOwnPassword(body), 'Could not change the password',
  ),
}));

/**
 * Run one mutation, then redraw — and keep the two apart.
 *
 * `true` means **the write committed**, which is the only thing the caller can
 * act on: it is what tells a form to clear itself, and clearing a form whose
 * write landed is the difference between "done" and a retry that cannot
 * succeed. A redraw that fails afterwards leaves the row the server returned in
 * place and says so; `load()` is the independent retry.
 */
async function applyWrite(
  set: (partial: Partial<AccountState> | ((s: AccountState) => Partial<AccountState>)) => void,
  write: () => Promise<Account>,
  fallback: string,
  busyId?: string,
): Promise<boolean> {
  set({ error: null, ...(busyId ? { busyId } : {}) });
  let account: Account;
  try {
    account = await write();
  } catch (e) {
    set({ error: errorDetail(e, fallback), busyId: null });
    return false;
  }
  // Committed. Show what the server returned, whatever happens next.
  set((state) => ({ accounts: upsert(state.accounts, account) }));
  const failure = await resync(set);
  set({ busyId: null, ...(failure ? { error: SAVED_BUT_STALE } : {}) });
  return true;
}

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
