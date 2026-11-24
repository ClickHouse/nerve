import { useState } from 'react';
import { Download, ImageOff, Loader2 } from '../../ui/icons';
import { authUrl } from '../../../api/hosted';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractResultText } from '../../../utils/extractResultText';
import { MarkdownContent } from '../MarkdownContent';

/** The handler names its copy of a local image on this line of the result. */
const WEB_URL = /^web_url: (\/api\/files\/uploads\/[A-Za-z0-9]+)$/m;
/** A refused call: the handler's errors start with this. */
const ERROR_RESULT = /^Error:\s*/;
/** A local image from before the copy existed still shows from its path while the file is there. */
const IMAGE_FILE = /\.(png|jpe?g|gif|webp|avif|svg)$/i;

interface Source {
  /** What the <img> and the viewer show. */
  src: string;
  /** The same file to save. */
  download: string;
  name: string;
}

function sourceOf(block: ToolCallBlockData, result: string): Source | null {
  const filePath = typeof block.input?.file_path === 'string' ? block.input.file_path : '';
  const url = typeof block.input?.url === 'string' ? block.input.url : '';
  const copy = WEB_URL.exec(result)?.[1];
  if (copy) {
    const name = filePath.split('/').pop() || 'image';
    return { src: authUrl(copy), download: authUrl(`${copy}?download=1`), name };
  }
  if (/^https?:\/\//i.test(url)) {
    return { src: url, download: url, name: url.split(/[?#]/)[0].split('/').pop() || 'image' };
  }
  if (filePath && IMAGE_FILE.test(filePath)) {
    const path = authUrl(`/api/files/download?path=${encodeURIComponent(filePath)}`);
    return { src: path, download: path, name: filePath.split('/').pop() || 'image' };
  }
  return null;
}

/**
 * A `send_image` call: the image in the chat, its caption under it. The
 * image is a link marked for the app's image viewer (`data-lightbox`); with
 * no viewer mounted a click opens it in a tab.
 */
export function SendImageBlock({ block }: { block: ToolCallBlockData }) {
  const [broken, setBroken] = useState(false);
  const caption = typeof block.input?.caption === 'string' ? block.input.caption : '';

  if (block.status === 'running') {
    return (
      <div className="my-1.5 inline-flex items-center gap-2 px-3 py-2 text-sm text-text-muted">
        <Loader2 size={14} className="animate-spin" /> Sending image…
      </div>
    );
  }

  const result = block.result ? extractResultText(block.result) : '';
  const source = block.isError || ERROR_RESULT.test(result) ? null : sourceOf(block, result);
  if (!source) {
    const reason = result.replace(ERROR_RESULT, '');
    return (
      <div role="status" className="my-1.5 flex items-start gap-2 px-3 py-2 text-sm text-error">
        <ImageOff size={14} className="shrink-0 mt-0.5" aria-hidden="true" />
        <span className="min-w-0 break-words">Could not send the image{reason && `: ${reason}`}</span>
      </div>
    );
  }

  return (
    <figure className="my-2 max-w-full">
      {broken ? (
        <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-border text-sm text-text-muted">
          <ImageOff size={14} aria-hidden="true" /> {source.name}: the image is no longer available
        </div>
      ) : (
        <a
          href={source.src}
          target="_blank"
          rel="noopener noreferrer"
          data-lightbox={source.src}
          data-lightbox-title={source.name}
          data-lightbox-download={source.download}
          title="Open at full size"
          className="block w-fit max-w-full rounded-lg overflow-hidden border border-border hover:border-accent/50 transition-colors cursor-zoom-in"
        >
          <img
            src={source.src}
            alt={caption || source.name}
            loading="lazy"
            referrerPolicy="no-referrer"
            onError={() => setBroken(true)}
            className="block max-w-full max-h-96 object-contain"
          />
        </a>
      )}
      <figcaption className="mt-1 min-w-0 text-xs text-text-muted">
        {caption && <div className="text-sm text-text-secondary"><MarkdownContent content={caption} /></div>}
        <div className="flex items-center gap-1 min-w-0">
          <span className="truncate">{source.name}</span>
          {!broken && (
            <a
              href={source.download}
              download={source.name}
              aria-label={`Download ${source.name}`}
              className="shrink-0 p-1 rounded hover:text-text hover:bg-surface"
            >
              <Download size={12} aria-hidden="true" />
            </a>
          )}
        </div>
      </figcaption>
    </figure>
  );
}
