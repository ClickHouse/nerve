import { useEffect, useState, type FormEvent } from 'react';
import { Users, Plus, AlertTriangle } from '../components/ui/icons';
import { Badge, Button, PageHeader, TextField } from '../components/ui';
import {
  useAccountStore, blockedReason, selectSelf, type AccountState,
} from '../stores/accountStore';
import type { Account } from '../api/client';

/**
 * Accounts.
 *
 * Every account may do everything here to every other account — there are no
 * roles (0.4) — so the page is flat: one list, one "add someone" form, and
 * your own password. The two things it refuses are the server's, not the
 * page's: the last enabled account cannot be disabled, and nobody can be added
 * until the existing account has a password and a username. Both come back as
 * a message from the server rather than being pre-judged here, because both
 * are decided inside a transaction and a client-side guess would go stale.
 */
export function AccountsPage() {
  const accounts = useAccountStore((s: AccountState) => s.accounts);
  const loading = useAccountStore((s: AccountState) => s.loading);
  const error = useAccountStore((s: AccountState) => s.error);
  const load = useAccountStore((s: AccountState) => s.load);
  const self = useAccountStore(selectSelf);
  const [adding, setAdding] = useState(false);

  useEffect(() => { void load(); }, [load]);

  const blocked = blockedReason(accounts);

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden">
      <PageHeader
        icon={<Users size={18} className="text-text-muted" />}
        title="Accounts"
        actions={
          <Button
            variant="primary"
            size="sm"
            onClick={() => setAdding((open) => !open)}
            disabled={loading}
          >
            <Plus size={14} className="mr-1" />
            Add account
          </Button>
        }
      />

      <div className="flex-1 overflow-auto p-4 lg:p-6">
        <div className="max-w-3xl mx-auto">
          {error && (
            <p role="alert" className="text-error text-sm mb-4">{error}</p>
          )}

          {blocked && (
            <div
              className="flex gap-2 items-start border border-border rounded-lg
                bg-surface-raised p-3 mb-4"
            >
              <AlertTriangle size={14} className="text-hue-amber mt-0.5 shrink-0" />
              <p className="text-sm text-text-muted">{blocked}</p>
            </div>
          )}

          {adding && (
            <AddAccountForm onDone={() => setAdding(false)} />
          )}

          {loading ? (
            <p className="text-sm text-text-dim">Loading…</p>
          ) : (
            <ul className="flex flex-col gap-2">
              {accounts.map((account) => (
                <li key={account.id}>
                  <AccountRow account={account} />
                </li>
              ))}
            </ul>
          )}

          {self && <OwnPasswordForm self={self} />}
        </div>
      </div>
    </div>
  );
}

function AccountRow({ account }: { account: Account }) {
  const setEnabled = useAccountStore((s: AccountState) => s.setEnabled);
  const update = useAccountStore((s: AccountState) => s.update);
  const busyId = useAccountStore((s: AccountState) => s.busyId);
  const [renaming, setRenaming] = useState(false);
  const [username, setUsername] = useState(account.username ?? '');
  const busy = busyId === account.id;

  const submitUsername = async (e: FormEvent) => {
    e.preventDefault();
    if (await update(account.id, { username })) setRenaming(false);
  };

  return (
    <div className="border border-border rounded-lg bg-surface-raised p-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-medium text-text truncate">
              {account.username ?? <span className="text-text-dim italic">no username</span>}
            </span>
            {account.is_self && <Badge tone="info">you</Badge>}
            {!account.enabled && <Badge tone="warning">disabled</Badge>}
            {!account.has_password && <Badge tone="warning">no password</Badge>}
          </div>
          <p className="text-2xs text-text-dim mt-1">
            {account.display_name || 'No display name'}
            {' · added '}
            {new Date(account.created_at).toLocaleDateString()}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setRenaming((open) => !open)}
            disabled={busy}
          >
            {account.username ? 'Rename' : 'Set username'}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => void setEnabled(account.id, !account.enabled)}
            disabled={busy}
          >
            {account.enabled ? 'Disable' : 'Enable'}
          </Button>
        </div>
      </div>

      {renaming && (
        <form onSubmit={submitUsername} className="flex items-center gap-2 mt-3">
          <TextField
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="username"
            aria-label={`Username for ${account.username ?? 'this account'}`}
            autoFocus
          />
          <Button type="submit" variant="primary" size="sm" disabled={busy}>Save</Button>
        </form>
      )}
    </div>
  );
}

function AddAccountForm({ onDone }: { onDone: () => void }) {
  const create = useAccountStore((s: AccountState) => s.create);
  const [username, setUsername] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [password, setPassword] = useState('');
  const [saving, setSaving] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSaving(true);
    const ok = await create({
      username, password, display_name: displayName || undefined,
    });
    setSaving(false);
    if (ok) {
      setUsername('');
      setDisplayName('');
      setPassword('');
      onDone();
    }
  };

  return (
    <form
      onSubmit={submit}
      aria-label="Add account"
      className="border border-border rounded-lg bg-surface-raised p-3 mb-4 flex flex-col gap-2"
    >
      <TextField
        value={username}
        onChange={(e) => setUsername(e.target.value)}
        placeholder="username"
        aria-label="New account username"
        autoComplete="off"
        autoFocus
      />
      <TextField
        value={displayName}
        onChange={(e) => setDisplayName(e.target.value)}
        placeholder="Display name (optional)"
        aria-label="New account display name"
        autoComplete="off"
      />
      <TextField
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        placeholder="Password"
        aria-label="New account password"
        autoComplete="new-password"
      />
      <p className="text-2xs text-text-dim">
        They will be able to do everything you can, including disabling your
        account. There are no roles.
      </p>
      <div className="flex items-center gap-2">
        <Button type="submit" variant="primary" size="sm" disabled={saving}>
          {saving ? '…' : 'Create'}
        </Button>
        <Button variant="ghost" size="sm" onClick={onDone}>Cancel</Button>
      </div>
    </form>
  );
}

function OwnPasswordForm({ self }: { self: Account }) {
  const changeOwnPassword = useAccountStore((s: AccountState) => s.changeOwnPassword);
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [saving, setSaving] = useState(false);
  const [done, setDone] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setDone(false);
    const ok = await changeOwnPassword({
      // Only the account that has no password yet may set one without proving
      // the old one — which is also the state this whole screen exists to end.
      current_password: self.has_password ? current : undefined,
      new_password: next,
    });
    setSaving(false);
    if (ok) {
      setCurrent('');
      setNext('');
      setDone(true);
    }
  };

  return (
    <form
      onSubmit={submit}
      aria-label="Your password"
      className="border border-border rounded-lg bg-surface-raised p-3 mt-6 flex flex-col gap-2"
    >
      <h2 className="text-sm font-medium text-text">
        {self.has_password ? 'Change your password' : 'Set a password'}
      </h2>
      {self.has_password && (
        <TextField
          type="password"
          value={current}
          onChange={(e) => setCurrent(e.target.value)}
          placeholder="Current password"
          aria-label="Current password"
          autoComplete="current-password"
        />
      )}
      <TextField
        type="password"
        value={next}
        onChange={(e) => setNext(e.target.value)}
        placeholder="New password"
        aria-label="New password"
        autoComplete="new-password"
      />
      <div className="flex items-center gap-2">
        <Button type="submit" variant="primary" size="sm" disabled={saving || !next}>
          {saving ? '…' : 'Save password'}
        </Button>
        {done && <span className="text-2xs text-text-dim">Saved.</span>}
      </div>
    </form>
  );
}
