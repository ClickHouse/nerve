import { useState } from 'react';
import { Download, FileText, Loader2 } from '../../ui/icons';
import { Modal } from '../../ui';
import { authUrl } from '../../../api/hosted';
import type { ToolCallBlockData } from '../../../types/chat';
import { extractResultText } from '../../../utils/extractResultText';

/** The file names that show as an image. An `<img>` element runs no script, also for an SVG file. */
const IMAGE_FILE = /\.(png|jpe?g|gif|webp|avif|svg)$/i;
/** The handler gives a refused or missing file as a normal result whose text starts with this. */
const ERROR_RESULT = /^Error:\s*/;

const CARD_CLASS = 'inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-surface '
  + 'hover:bg-surface-hover transition-colors text-sm text-text-secondary';

/** The reason that a call failed, or null when the file was sent. */
function failure(block: ToolCallBlockData): string | null {
  // The text of the MCP content blocks. A result in plain text stays as it is.
  const result = block.result ? extractResultText(block.result) : '';
  if (block.isError) return result;
  return ERROR_RESULT.test(result) ? result.replace(ERROR_RESULT, '') : null;
}

/**
 * A `send_file` call. An image file shows in the chat, and a click opens it
 * at full size; another file shows as a download card.
 */
export function SendFileBlock({ block }: { block: ToolCallBlockData }) {
  const [open, setOpen] = useState(false);
  // The file can be gone (moved, or deleted) when an old chat is read again.
  const [broken, setBroken] = useState(false);
  const filePath = typeof block.input?.file_path === 'string' ? block.input.file_path : '';
  const filename = filePath.split('/').pop() || 'file';
  const fileUrl = authUrl(`/api/files/download?path=${encodeURIComponent(filePath)}`);

  if (block.status === 'running') {
    return (
      <div className="my-1.5 inline-flex items-center gap-2 px-3 py-2 text-sm text-text-muted">
        <Loader2 size={14} className="animate-spin" /> Sending file…
      </div>
    );
  }

  const reason = failure(block);
  if (reason !== null) {
    return (
      <div role="status" className="my-1.5 flex items-start gap-2 px-3 py-2 text-sm text-error">
        <FileText size={14} className="shrink-0 mt-0.5" aria-hidden="true" />
        <span className="min-w-0 break-words">Could not send {filename}{reason && `: ${reason}`}</span>
      </div>
    );
  }

  const card = (
    <a href={fileUrl} download={filename} className={CARD_CLASS}>
      <FileText size={14} aria-hidden="true" />
      <span className="truncate max-w-[200px]">{filename}</span>
      <Download size={12} className="text-text-muted" aria-hidden="true" />
    </a>
  );

  if (!IMAGE_FILE.test(filename) || broken) return <div className="my-2">{card}</div>;

  return (
    <figure className="my-2 max-w-full">
      <button
        type="button"
        onClick={() => setOpen(true)}
        title="Open at full size"
        className="block w-fit max-w-full rounded-lg overflow-hidden border border-border hover:border-accent/50 transition-colors cursor-zoom-in focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
      >
        <img
          src={fileUrl}
          alt={filename}
          loading="lazy"
          onError={() => setBroken(true)}
          className="block max-w-full max-h-80 object-contain"
        />
      </button>
      <figcaption className="mt-1 flex items-center gap-1 min-w-0 text-xs text-text-muted">
        <span className="truncate">{filename}</span>
        <a
          href={fileUrl}
          download={filename}
          aria-label={`Download ${filename}`}
          className="shrink-0 p-1 rounded hover:text-text hover:bg-surface"
        >
          <Download size={12} aria-hidden="true" />
        </a>
      </figcaption>
      <Modal open={open} onClose={() => setOpen(false)} title={filename} size="wide" footer={card}>
        <img src={fileUrl} alt={filename} className="block mx-auto max-w-full max-h-[75vh] object-contain" />
      </Modal>
    </figure>
  );
}
