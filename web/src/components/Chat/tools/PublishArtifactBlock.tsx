import { ExternalLink, Loader2, Share } from '../../ui/icons';
import { useHostedStore } from '../../../api/hosted';
import { useArtifactStore } from '../../../stores/artifactStore';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractResultText } from '../../../utils/extractResultText';
import { readPublished, viewerFrameUrl, viewerLink } from './publishedArtifact';

/** The height of the viewer in the chat, in CSS pixels. The viewer fills its frame. */
export const VIEWER_FRAME_HEIGHT = 560;

/**
 * What the frame of the viewer may do. The viewer is a page of the platform
 * with its own origin: it keeps that origin (`allow-same-origin`) for its
 * sign-in cookie and its requests, runs its scripts, opens a tab for Open, a
 * download and the sign-in (popups that leave this sandbox), and its content
 * frame may print (`allow-modals`). It may not move this page
 * (no `allow-top-navigation`).
 */
export const VIEWER_SANDBOX = 'allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-modals allow-forms';
/** Full screen for Present, and the clipboard for Copy link. */
export const VIEWER_ALLOW = 'fullscreen; clipboard-write';

const CARD_CLASS = 'my-2 flex items-center gap-2 max-w-full px-3 py-2 rounded-lg border border-border bg-surface text-sm';

/**
 * A `publish_artifact` call. In hosted mode, when the viewer address of the
 * result has exactly the origin of the configured viewer, the viewer in a
 * frame (`embed=1`). In every other case a card with the title and the
 * address as text.
 *
 * The chat does not listen to the frame: a message that the viewer or the
 * content inside it posts to this page reaches no handler of this card.
 */
export function PublishArtifactBlock({ block }: { block: ToolCallBlockData }) {
  const viewerOrigin = useArtifactStore(s => s.viewerOrigin);
  const hosted = useHostedStore(s => s.mode === 'external');
  const title = typeof block.input?.title === 'string' && block.input.title.trim() ? block.input.title.trim() : 'artifact';

  if (block.status === 'running') {
    return (
      <div className="my-1.5 inline-flex items-center gap-2 px-3 py-2 text-sm text-text-muted">
        <Loader2 size={14} className="motion-safe:animate-spin" aria-hidden="true" /> Publishing {title}...
      </div>
    );
  }

  const text = block.result ? extractResultText(block.result) : '';
  const published = block.isError ? null : readPublished(text);
  if (!published) {
    const reason = text.replace(/^Not published:\s*/, '').trim();
    return (
      <div role="status" className="my-1.5 flex items-start gap-2 px-3 py-2 text-sm text-error">
        <Share size={14} className="shrink-0 mt-0.5" aria-hidden="true" />
        <span className="min-w-0 break-words">Could not publish {title}{reason && `: ${reason}`}</span>
      </div>
    );
  }

  const frame = hosted ? viewerFrameUrl(published, viewerOrigin) : null;
  if (frame) {
    return (
      <figure className="my-2 max-w-full rounded-lg border border-border overflow-hidden bg-surface">
        <iframe
          src={frame}
          title={`Artifact: ${title}`}
          sandbox={VIEWER_SANDBOX}
          allow={VIEWER_ALLOW}
          referrerPolicy="no-referrer"
          loading="lazy"
          height={VIEWER_FRAME_HEIGHT}
          className="block w-full border-0"
          style={{ height: VIEWER_FRAME_HEIGHT }}
        />
      </figure>
    );
  }

  const link = viewerLink(published);
  return (
    <div className={CARD_CLASS} role="group" aria-label={`Published artifact: ${title}`}>
      <Share size={14} className="shrink-0 text-text-muted" aria-hidden="true" />
      <span className="font-medium text-text truncate">{title}</span>
      {published.version !== null && <span className="text-text-dim shrink-0">version {published.version}</span>}
      {link ? (
        <a
          href={link}
          target="_blank"
          rel="noopener noreferrer"
          className="ml-auto inline-flex items-center gap-1 min-w-0 text-text-secondary hover:text-text"
        >
          <span className="truncate font-mono text-xs">{published.address}</span>
          <ExternalLink size={12} className="shrink-0" aria-hidden="true" />
        </a>
      ) : (
        <span className="ml-auto truncate font-mono text-xs text-text-secondary">{published.address}</span>
      )}
    </div>
  );
}
