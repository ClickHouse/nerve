import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useModalSurface } from '../../hooks/useModalSurface';
import { useSetupStore } from '../../stores/setupStore';
import { copyToClipboard } from '../../utils/clipboard';
import { Button } from '../ui';
import { Copy, Loader2 } from '../ui/icons';
import { UNSUPPORTED_WITHOUT_COMMAND, UNSUPPORTED_WITH_COMMAND } from './copy';

/** After this long a person may put the overlay away and carry on. */
export const DISMISSIBLE_AFTER_MS = 10_000;

/**
 * "Restarting Nerve…" over the page. Never a trap:
 *
 * - `restarting`: a spinner and the elapsed time; after ten seconds a
 *   secondary **Not now**. Before that, Escape does nothing and the dialog
 *   says so.
 * - `unsupported`: somebody with access to the server must act; the exact
 *   command with Copy when the server knows one (systemd), otherwise the
 *   sentence names the supervisor's operator and promises no command; and
 *   **Not now**. The wait still runs.
 * - `timed out`: **Retry waiting** and **Close**.
 *
 * Closing hides the overlay and returns to the review step; the generation
 * wait keeps running in the store, so the banner and the review update on
 * their own when Nerve is back.
 */
export function RestartOverlay() {
  const navigate = useNavigate();
  const reconnecting = useSetupStore((s) => s.reconnecting);
  const reconnectFailed = useSetupStore((s) => s.reconnectFailed);
  const elapsedMs = useSetupStore((s) => s.reconnectElapsedMs);
  const outcome = useSetupStore((s) => s.restartOutcome);
  const dismissed = useSetupStore((s) => s.overlayDismissed);
  const dismiss = useSetupStore((s) => s.dismissRestartOverlay);
  const retryWait = useSetupStore((s) => s.retryWait);
  const [copied, setCopied] = useState(false);

  const unsupported = outcome?.mode === 'unsupported';
  const open = (reconnecting || reconnectFailed || unsupported) && !dismissed;
  const phase: 'timed_out' | 'unsupported' | 'restarting' =
    reconnectFailed ? 'timed_out' : unsupported ? 'unsupported' : 'restarting';
  const canDismiss = phase !== 'restarting' || elapsedMs >= DISMISSIBLE_AFTER_MS;

  const close = () => {
    if (!canDismiss) return;
    dismiss();
    navigate('/setup/review');
  };

  const { dialogProps } = useModalSurface<HTMLDivElement>(open, close);
  if (!open) return null;

  const seconds = Math.round(elapsedMs / 1000);
  const timeoutMinutes = outcome && outcome.timeout_hint_seconds > 0
    ? Math.round(outcome.timeout_hint_seconds / 60)
    : 2;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-bg/80 backdrop-blur-sm p-4">
      <div
        {...dialogProps}
        aria-labelledby="restart-overlay-title"
        aria-describedby="restart-overlay-text"
        className="bg-surface-raised p-6 rounded-lg border border-border-subtle w-full max-w-md shadow-xl outline-none flex flex-col gap-4"
      >
        {phase === 'restarting' && (
          <>
            <div className="flex items-center gap-3">
              <Loader2 size={20} className="animate-spin text-accent shrink-0" />
              <h1 id="restart-overlay-title" className="text-lg font-semibold text-text">
                Restarting Nerve… ({seconds} s)
              </h1>
            </div>
            <p id="restart-overlay-text" className="text-sm text-text-muted" role="status">
              {outcome?.message ?? 'This page reconnects on its own; you stay signed in.'}
              {!canDismiss && ' This usually takes a few seconds; the dialog can be put away after ten.'}
              {canDismiss && ' You can keep using the app; the banner updates when Nerve is back.'}
            </p>
            {canDismiss && (
              <div className="flex justify-end">
                <Button variant="ghost" size="md" onClick={close}>Not now</Button>
              </div>
            )}
          </>
        )}

        {phase === 'unsupported' && (
          <>
            <h1 id="restart-overlay-title" className="text-lg font-semibold text-text">
              A restart must be done on the server
            </h1>
            <p id="restart-overlay-text" className="text-sm text-text-muted">
              {outcome?.command ? UNSUPPORTED_WITH_COMMAND : UNSUPPORTED_WITHOUT_COMMAND}
              {reconnecting && <> (waiting, {seconds} s of up to {timeoutMinutes} min)</>}.
              {outcome?.message && <> <span className="block mt-1">{outcome.message}</span></>}
            </p>
            {outcome?.command && (
              <div className="flex items-center gap-2">
                <code className="flex-1 font-mono text-xs rounded border border-border bg-code-bg px-2 py-1.5 overflow-x-auto">
                  {outcome.command}
                </code>
                <Button
                  variant="secondary"
                  size="sm"
                  className="min-h-[44px] md:min-h-0"
                  onClick={() => void copyToClipboard(outcome.command ?? '').then((ok) => setCopied(ok))}
                >
                  <Copy size={14} />{copied ? 'Copied' : 'Copy'}
                </Button>
              </div>
            )}
            <div className="flex justify-end">
              <Button variant="ghost" size="md" onClick={close}>Not now</Button>
            </div>
          </>
        )}

        {phase === 'timed_out' && (
          <>
            <h1 id="restart-overlay-title" className="text-lg font-semibold text-text">
              Nerve did not come back
            </h1>
            <p id="restart-overlay-text" className="text-sm text-text-muted">
              Nerve did not come back within {timeoutMinutes} {timeoutMinutes === 1 ? 'minute' : 'minutes'}.
              On the server, check <code className="font-mono">nerve logs</code> or run{' '}
              <code className="font-mono">nerve start</code>.
            </p>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" size="md" onClick={close}>Close</Button>
              <Button variant="primary" size="md" onClick={() => void retryWait()}>Retry waiting</Button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
