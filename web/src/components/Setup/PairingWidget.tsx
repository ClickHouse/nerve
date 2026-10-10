import { useEffect, useState } from 'react';
import { api, type PairingCode, type PairingStatus } from '../../api/client';
import { errorDetail } from '../../stores/accountStore';
import { copyToClipboard } from '../../utils/clipboard';
import { Button } from '../ui';
import { Check, Copy, ExternalLink, Loader2 } from '../ui/icons';

export const PAIRING_POLL_MS = 2000;

/**
 * In-wizard Telegram pairing.
 *
 * **Get pairing code** issues a single-use code (the only response that ever
 * carries it); the person opens the bot and sends `/pair <code>`; this polls
 * the pure status read every two seconds until the allowlist count grows
 * past what it was at issue, the code expires, or the widget unmounts.
 *
 * When the server cannot name the bot (`link` is null) it degrades to
 * "open your bot in Telegram and send `/pair <code>`" with the code and
 * Copy, and no link button.
 */
export function PairingWidget({ onPaired }: { onPaired?: () => void }) {
  const [status, setStatus] = useState<PairingStatus | null>(null);
  const [code, setCode] = useState<PairingCode | null>(null);
  const [paired, setPaired] = useState(false);
  const [expired, setExpired] = useState(false);
  const [issuing, setIssuing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    let alive = true;
    api.setupPairing().then((s) => { if (alive) setStatus(s); }).catch(() => {});
    return () => { alive = false; };
  }, []);

  // Poll while a code is out. Reads only the count; the code never travels.
  useEffect(() => {
    if (!code || paired) return;
    const issuedCount = code.allowed_users_count;
    const expiresAt = Date.parse(code.expires_at);
    const timer = setInterval(() => {
      if (Number.isFinite(expiresAt) && Date.now() > expiresAt) {
        setExpired(true);
        setCode(null);
        return;
      }
      api.setupPairing().then((s) => {
        setStatus(s);
        if (s.allowed_users_count > issuedCount) {
          setPaired(true);
          onPaired?.();
        }
      }).catch(() => {});
    }, PAIRING_POLL_MS);
    return () => clearInterval(timer);
  }, [code, paired, onPaired]);

  const issue = async () => {
    setIssuing(true);
    setError(null);
    setExpired(false);
    setCopied(false);
    try {
      const issued = await api.setupPairingStart();
      setCode(issued);
      setPaired(false);
    } catch (e) {
      setError(errorDetail(e, 'Could not issue a pairing code'));
    } finally {
      setIssuing(false);
    }
  };

  const link = code?.link ?? status?.link ?? null;
  const botName = code?.bot_username ?? status?.bot_username ?? null;

  if (paired) {
    return (
      <div role="status" className="flex items-start gap-2 rounded-lg border border-success-border bg-success-bg p-3 text-sm text-success">
        <Check size={14} className="mt-0.5 shrink-0" />
        <p>Paired. Nerve will answer you in Telegram{botName ? <> as <strong>@{botName}</strong></> : null}.</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-3">
      <div>
        <h3 className="text-sm font-medium text-text">Pair your Telegram account</h3>
        <p className="text-xs text-text-muted mt-0.5">
          Only paired people can talk to the bot. The code is single-use and expires in an hour.
        </p>
      </div>

      {status && !status.available && !code && (
        <p className="text-sm text-text-muted" role="status">{status.reason ?? 'Pairing is not available right now.'}</p>
      )}

      {!code ? (
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="primary"
            size="sm"
            onClick={() => void issue()}
            disabled={issuing || (status !== null && !status.available)}
            className="min-h-[44px] md:min-h-0"
          >
            {issuing ? <Loader2 size={14} className="animate-spin" /> : null}
            {expired ? 'Get a new pairing code' : 'Get pairing code'}
          </Button>
          {expired && <span className="text-xs text-text-muted" role="status">The code expired.</span>}
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          <p className="text-sm text-text-muted">
            {link
              ? <>Open the bot and send <code className="font-mono">/pair {code.code}</code>:</>
              : <>Open your bot in Telegram and send <code className="font-mono">/pair {code.code}</code>.</>}
          </p>
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-lg tracking-widest text-text" aria-label="Pairing code">{code.code}</span>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => void copyToClipboard(`/pair ${code.code}`).then((ok) => setCopied(ok))}
              className="min-h-[44px] md:min-h-0"
            >
              <Copy size={14} />{copied ? 'Copied' : 'Copy'}
            </Button>
            {link && (
              <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-accent hover:underline underline-offset-2 min-h-[44px] px-2"
              >
                <ExternalLink size={14} />Open {botName ? `@${botName}` : 'the bot'} in Telegram
              </a>
            )}
          </div>
          <p role="status" className="flex items-center gap-2 text-xs text-text-dim">
            <Loader2 size={12} className="animate-spin" />Waiting for your message…
          </p>
        </div>
      )}

      {error && <p role="alert" className="text-sm text-error">{error}</p>}
    </div>
  );
}
