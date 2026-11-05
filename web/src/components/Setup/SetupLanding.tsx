import { useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { setupNotStarted, useSetupStore } from '../../stores/setupStore';

/**
 * Lands an instance that has never been set up on the setup page.
 *
 * An instance that has never chosen a mode — the slim `nerve init` leaves it
 * that way, in both auth modes — has nothing to offer but the wizard, so the
 * first read of the checklist in a sign-in sends the person to `/setup`
 * rather than to a chat page with a banner. Once: after that they may go
 * anywhere (the banner keeps counting the steps left), and a link straight
 * into the wizard is left where it points. A partly set-up instance (Mode
 * chosen, steps left) is the banner's case, and an unknown checklist (an
 * older backend) changes nothing.
 *
 * It mounts with the live app, after the auth check of whichever mode this
 * is — behind a front gateway, after the gateway session has been verified —
 * so the landing never runs ahead of the sign-in. Renders nothing.
 */
export function SetupLanding() {
  const known = useSetupStore((s) => s.state !== null);
  const notStarted = useSetupStore(setupNotStarted);
  const navigate = useNavigate();
  const { pathname } = useLocation();
  // Decided the first time the checklist is known, and not again: the
  // wizard re-reads it on every step change, and the answer must not pull
  // a person who has since left back in.
  const decided = useRef(false);

  useEffect(() => {
    if (decided.current || !known) return;
    decided.current = true;
    // `replace`: the page this arrived on was never seen, so Back should not
    // return to it.
    if (notStarted && !pathname.startsWith('/setup')) navigate('/setup', { replace: true });
  }, [known, notStarted, pathname, navigate]);

  return null;
}
