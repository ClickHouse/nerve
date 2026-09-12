import { Link } from 'react-router-dom';
import { ShieldQuestion } from '../components/ui/icons';
import { Button } from '../components/ui';

/**
 * Placeholder for the first-run setup wizard.
 *
 * The instance reached here because `/api/auth/status` says `setup_pending`:
 * its one account has no password, which is what a headless install lands as
 * when `NERVE_PASSWORD` was never set. Anyone who can reach the gateway is
 * currently signed in as the owner. Naming the account does not change that,
 * so naming it does not clear this page either.
 *
 * There is deliberately no gate on this page and nothing here is required: an
 * abandoned setup must leave a working instance, so the app is one link away.
 * A later release replaces this with the guided checklist; until then the two
 * things that actually close the hole — a password and a username — are on the
 * accounts screen, which this points at.
 */
export function SetupPage() {
  return (
    <div className="flex-1 h-full overflow-auto flex items-start justify-center p-6">
      <div className="max-w-xl w-full mt-10">
        <div className="flex items-center gap-3 mb-4">
          <ShieldQuestion size={22} className="text-hue-amber" />
          <h1 className="text-xl font-semibold text-text">Setup is not finished</h1>
        </div>

        <p className="text-sm text-text-muted mb-4">
          This instance has no password. Everyone who can reach it is signed in
          as the owner, and no activity can be told apart. That is fine for a
          machine only you can reach, and a real exposure on anything else.
        </p>

        <p className="text-sm text-text-muted mb-6">
          What closes it is a <strong className="text-text">password</strong>,
          on the accounts screen. A{' '}
          <strong className="text-text">username</strong> is worth setting at
          the same time — both have to be in place before a second person can
          be added — but it is the password that secures the instance, and
          setting a username on its own changes nothing about who gets in.
        </p>

        <div className="flex flex-wrap items-center gap-3">
          <Link to="/accounts">
            <Button variant="primary" size="md">Go to accounts</Button>
          </Link>
          <Link to="/chat">
            <Button variant="ghost" size="md">Skip for now</Button>
          </Link>
        </div>

        <p className="text-2xs text-text-dim mt-8">
          Background: <code>docs/accounts.md</code> in the Nerve repository
          covers the local identity model, passwordless installs and what
          changes when a second account is created.
        </p>
      </div>
    </div>
  );
}
