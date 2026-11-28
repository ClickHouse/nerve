import { useEffect, useId, useRef, useState } from 'react';
import { api, apiErrorDetail } from '../../api/client';
import { useHostedStore } from '../../api/hosted';
import { useArtifactStore } from '../../stores/artifactStore';
import type { PublishedArtifact } from '../../types/artifacts';
import { Button, Modal, TextField } from '../ui';
import { Check, Copy, ExternalLink, Loader2, Share } from '../ui/icons';
import { copyToClipboard } from '../../utils/clipboard';
import { httpLink } from '../../utils/httpLink';
import { defaultTitle } from './title';

/** The most characters of a title (the platform refuses more). */
export const MAX_TITLE_CHARS = 200;

/** Who sees a new artifact, before and after the publish, and how a member shares it. */
const BEFORE_TEXT = 'Only members of this agent will see it. You can share it with everyone in your tenant later.';
const AFTER_TEXT = 'Only members of this agent can see it. To share it with everyone in your tenant, open it and select Share.';

/** How long the Copy link button says "Copied". */
const COPIED_MS = 1500;

/** The sentence of a failed publish: the message of the runtime, else a general one. */
function failureText(e: unknown): string {
  const message = apiErrorDetail(e)?.message;
  return typeof message === 'string' && message ? message : 'The publish did not end. Try again.';
}

/**
 * The Publish button of the Preview tab: hosted mode with `artifacts.publish`
 * only. It publishes the bundle of `path` for the members of this agent
 * through the runtime (`POST /api/artifacts/publish`), which runs the code of
 * the tool `publish_artifact` with the person as the actor. After a publish,
 * the next one from this tab is a new version of the same artifact.
 */
export function PublishButton({ path, disabled }: { path: string; disabled: boolean }) {
  const enabled = useArtifactStore(s => s.publish);
  const hosted = useHostedStore(s => s.mode === 'external');
  const [open, setOpen] = useState(false);
  const [title, setTitle] = useState(() => defaultTitle(path));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [published, setPublished] = useState<PublishedArtifact | null>(null);
  const [done, setDone] = useState(false);
  const [copied, setCopied] = useState<boolean | null>(null);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const fieldId = useId();

  useEffect(() => () => {
    if (copiedTimer.current) clearTimeout(copiedTimer.current);
  }, []);

  if (!enabled || !hosted) return null;

  const start = () => {
    setError(null);
    setDone(false);
    setCopied(null);
    setOpen(true);
  };

  const close = () => {
    if (!busy) setOpen(false);
  };

  const publish = async () => {
    const wanted = title.trim();
    if (!wanted || busy) return;
    setBusy(true);
    setError(null);
    try {
      const result = await api.publishArtifact(path, wanted, published?.artifact_id);
      setPublished(result);
      setDone(true);
    } catch (e) {
      setError(failureText(e));
    } finally {
      setBusy(false);
    }
  };

  const link = done && published ? httpLink(published.viewer_url) : null;

  const copyLink = async () => {
    if (!link) return;
    const ok = await copyToClipboard(link);
    setCopied(ok);
    if (copiedTimer.current) clearTimeout(copiedTimer.current);
    if (ok) copiedTimer.current = setTimeout(() => setCopied(null), COPIED_MS);
  };

  return (
    <>
      <Button variant="primary" size="sm" onClick={start} disabled={disabled}>
        <Share size={12} aria-hidden="true" />
        Publish
      </Button>
      <Modal
        open={open}
        onClose={close}
        title={done ? 'Published' : 'Publish'}
        closeOnBackdrop={!busy}
        footer={done ? (
          <>
            <Button variant="secondary" size="sm" onClick={close}>Close</Button>
            {link && (
              <Button variant="secondary" size="sm" onClick={() => void copyLink()}>
                {copied ? <Check size={12} aria-hidden="true" /> : <Copy size={12} aria-hidden="true" />}
                {copied ? 'Copied' : 'Copy link'}
              </Button>
            )}
            {link && (
              <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm bg-accent text-on-accent hover:opacity-90"
              >
                <ExternalLink size={12} aria-hidden="true" /> Open
              </a>
            )}
          </>
        ) : (
          <>
            <Button variant="secondary" size="sm" onClick={close} disabled={busy}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={() => void publish()} disabled={busy || !title.trim()}>
              {busy && <Loader2 size={12} className="motion-safe:animate-spin" aria-hidden="true" />}
              {busy ? 'Publishing...' : 'Publish'}
            </Button>
          </>
        )}
      >
        {done && published ? (
          <div className="p-5 space-y-2 text-sm">
            <p>{published.title} is published as version {published.version}. {AFTER_TEXT}</p>
            {published.changed_after && (
              <p>The files changed during the publish. This version does not have the changes. Publish again to add them.</p>
            )}
            <p className="font-mono text-xs text-text-secondary break-all">{published.viewer_url}</p>
            {copied === false && (
              <p role="alert" className="text-error">Could not copy the link. Select it and copy it.</p>
            )}
          </div>
        ) : (
          <form
            className="p-5 space-y-3 text-sm"
            onSubmit={(event) => {
              event.preventDefault();
              void publish();
            }}
          >
            <p className="font-mono text-xs text-text-secondary break-all">{path}</p>
            <div className="space-y-1">
              <label htmlFor={fieldId} className="block text-text-muted">Title</label>
              <TextField
                id={fieldId}
                value={title}
                maxLength={MAX_TITLE_CHARS}
                onChange={(event) => setTitle(event.target.value)}
                disabled={busy}
              />
            </div>
            <p className="text-text-muted">
              {published
                ? `This publishes a new version of ${published.title} at the same link.`
                : BEFORE_TEXT}
            </p>
            {error && <p role="alert" className="text-error">{error}</p>}
          </form>
        )}
      </Modal>
    </>
  );
}
