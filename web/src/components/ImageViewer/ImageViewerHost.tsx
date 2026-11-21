import { useCallback, useEffect, useState } from 'react';
import { Lightbox } from '../Chat/rich/Lightbox';
import { Download, ExternalLink } from '../ui/icons';

export interface ViewerImage {
  src: string;
  title: string;
  download: string;
}

/** The images a click opens: every viewer link in the clicked one's group, and which one it was. */
// eslint-disable-next-line react-refresh/only-export-components -- tested on its own.
export function collectGroup(link: HTMLAnchorElement): { images: ViewerImage[]; index: number } {
  const group = link.closest('[data-lightbox-group], [data-role]');
  const links = group
    ? Array.from(group.querySelectorAll<HTMLAnchorElement>('a[data-lightbox]'))
    : [link];
  const images = links.map(a => ({
    src: a.dataset.lightbox || a.href,
    title: a.dataset.lightboxTitle || a.querySelector('img')?.alt || 'Image',
    download: a.dataset.lightboxDownload || a.href,
  }));
  return { images, index: Math.max(0, links.indexOf(link)) };
}

function isPlainClick(e: MouseEvent): boolean {
  return e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;
}

const actionClass = 'inline-flex items-center justify-center h-8 w-8 rounded text-text-muted hover:text-text hover:bg-surface-hover transition-colors';

/**
 * The app's image viewer: one listener for clicks on `a[data-lightbox]`
 * anywhere (chat images, the send_image card, the Files page), which opens the
 * picture in the zoomable Lightbox. Mounted once, in App.
 */
export function ImageViewerHost() {
  const [state, setState] = useState<{ images: ViewerImage[]; index: number } | null>(null);
  // The picture on screen: the last one that loaded. While the next one
  // loads, the dialog keeps showing this one rather than closing.
  const [shown, setShown] = useState<(ViewerImage & { width: number; height: number }) | null>(null);

  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (e.defaultPrevented || !isPlainClick(e)) return;
      const link = (e.target as Element | null)?.closest?.('a[data-lightbox]');
      if (!(link instanceof HTMLAnchorElement)) return;
      e.preventDefault();
      setState(collectGroup(link));
    };
    document.addEventListener('click', onClick);
    return () => document.removeEventListener('click', onClick);
  }, []);

  const current = state ? state.images[state.index] : null;

  // The Lightbox needs the picture's size at 1:1, which only loading it tells.
  useEffect(() => {
    if (!current) return;
    let cancelled = false;
    const img = new Image();
    img.onload = () => {
      if (!cancelled) setShown({ ...current, width: img.naturalWidth || 1, height: img.naturalHeight || 1 });
    };
    img.onerror = () => {
      if (cancelled) return;
      // Not an image after all: show the file the way the link would have.
      window.open(current.src, '_blank', 'noopener,noreferrer');
      setState(null);
      setShown(null);
    };
    img.src = current.src;
    return () => {
      cancelled = true;
    };
  }, [current]);

  const close = useCallback(() => {
    setState(null);
    setShown(null);
  }, []);
  const step = useCallback((by: number) => {
    setState(s => s && { ...s, index: (s.index + by + s.images.length) % s.images.length });
  }, []);

  if (!state || !shown) return null;
  return (
    <Lightbox
      open
      onClose={close}
      title={shown.title}
      src={shown.src}
      size={{ width: shown.width, height: shown.height }}
      initialView="fit-down"
      nav={{ index: state.index, count: state.images.length, onPrev: () => step(-1), onNext: () => step(1) }}
      actions={(
        <>
          <a href={shown.src} target="_blank" rel="noopener noreferrer" title="Open original" aria-label="Open original" className={actionClass}>
            <ExternalLink size={16} />
          </a>
          <a href={shown.download} download={shown.title} title="Download" aria-label="Download" className={actionClass}>
            <Download size={16} />
          </a>
        </>
      )}
    />
  );
}
