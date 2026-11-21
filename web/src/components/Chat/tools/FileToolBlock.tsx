import { useEffect, useMemo, useState } from 'react';
import { ChevronRight, ChevronDown, FileText, FilePlus, Loader2 } from '../../ui/icons';
import type { ToolCallBlockData } from '../../../types/chat';
import { imageBlob, parseResultMedia, type ResultImage } from '../../../utils/toolResultMedia';

interface Thumb {
  url: string;
  title: string;
}

/**
 * Object URLs for the images a tool result carries, revoked when the block
 * goes. A blob URL rather than a `data:` one, because browsers refuse to open
 * a `data:` URL in a new tab (Cmd-click, the viewer's "Open original").
 */
function useImageUrls(images: ResultImage[], name: string): Thumb[] {
  const thumbs = useMemo(() => {
    if (!images.length || typeof URL.createObjectURL !== 'function') return [];
    const made: Thumb[] = [];
    images.forEach((image, i) => {
      const blob = imageBlob(image);
      if (!blob) return;
      made.push({
        url: URL.createObjectURL(blob),
        title: images.length > 1 ? `${name} (${i + 1})` : name,
      });
    });
    return made;
  }, [images, name]);
  useEffect(() => () => thumbs.forEach(t => URL.revokeObjectURL(t.url)), [thumbs]);
  return thumbs;
}

export function FileToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';
  const filePath = String(block.input.file_path || block.input.path || '');
  const isWrite = block.tool === 'Write';
  const Icon = isWrite ? FilePlus : FileText;
  const name = filePath.split('/').pop() || 'Image';

  // A Read of an image or PDF: its content blocks, shown as pictures instead of base64.
  const media = useMemo(
    () => (block.isError ? null : parseResultMedia(block.result, filePath)),
    [block.result, block.isError, filePath],
  );
  const images = useMemo(() => media?.images ?? [], [media]);
  const thumbs = useImageUrls(images, name);

  // For Read results, show line count
  const lineCount = block.result && !media ? block.result.split('\n').length : null;
  let mediaLabel: string | null = null;
  if (media) {
    const parts: string[] = [];
    if (media.images.length) parts.push(media.images.length > 1 ? `${media.images.length} images` : 'image');
    if (media.documents) parts.push(media.documents > 1 ? `${media.documents} documents` : 'document');
    mediaLabel = parts.join(', ');
  }

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => { if (!media) setExpanded(!expanded); }}
        className={`flex items-center gap-2 w-full px-3 py-2 text-left transition-colors ${media ? 'cursor-default' : 'cursor-pointer hover:bg-surface-raised'}`}
      >
        {isRunning
          ? <Loader2 size={14} className="text-accent animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-error' : 'text-hue-blue'}`} />
        }
        <span className="text-sm leading-tight font-mono font-medium text-text-secondary">{block.tool}</span>
        <span className="text-xs text-text-dim truncate font-mono">{filePath}</span>
        {lineCount && !isWrite && (
          <span className="text-2xs text-text-faint shrink-0">{lineCount} lines</span>
        )}
        {mediaLabel && (
          <span className="text-2xs text-text-faint shrink-0">{mediaLabel}</span>
        )}
        {!media && (
          <div className="ml-auto shrink-0">
            {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
          </div>
        )}
      </button>

      {thumbs.length > 0 && (
        // The viewer (ImageViewerHost) opens these links and steps through the group.
        <div className="border-t border-border px-3 py-2 flex flex-wrap gap-2" data-lightbox-group="">
          {thumbs.map(t => (
            <a
              key={t.url}
              href={t.url}
              target="_blank"
              rel="noopener noreferrer"
              data-lightbox={t.url}
              data-lightbox-title={t.title}
              data-lightbox-download={t.url}
              className="inline-block rounded-md overflow-hidden border border-border hover:border-accent/50 transition-colors"
            >
              <img src={t.url} alt={t.title} className="max-w-[240px] max-h-40 object-contain bg-bg" />
            </a>
          ))}
        </div>
      )}

      {expanded && !media && (
        <div className="border-t border-border">
          {block.result !== undefined && (
            <pre className={`px-3 py-2 text-xs font-mono whitespace-pre-wrap max-h-80 overflow-y-auto bg-bg ${block.isError ? 'text-error' : 'text-text-muted'}`}>
              {block.result}
            </pre>
          )}

          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-xs text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" /> {isWrite ? 'Writing...' : 'Reading...'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
