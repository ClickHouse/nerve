import { useCallback, useEffect, useRef, useState } from 'react';
import { Lightbox } from '../Chat/rich/Lightbox';
import { Download, ExternalLink } from '../ui/icons';
import { holdBlobUrls, releaseBlobUrls } from '../../utils/blobUrls';

export interface ViewerImage {
  src: string;
  title: string;
  download: string;
  /** The link that opens it, which gets focus when the viewer closes on it. */
  link: HTMLAnchorElement;
}

const GROUP = '[data-lightbox-group]';

/**
 * The images a click opens, in the order of the page, and which one it was:
 * every viewer link in the outermost group of the clicked one (the chat's
 * message list, the Files page), or else in its message.
 */
// eslint-disable-next-line react-refresh/only-export-components -- tested on its own.
export function collectGroup(link: HTMLAnchorElement): { images: ViewerImage[]; index: number } {
  let group = link.closest(GROUP);
  for (let up = group?.parentElement?.closest(GROUP); up; up = up.parentElement?.closest(GROUP)) group = up;
  group ??= link.closest('[data-role]');
  const links = group
    ? Array.from(group.querySelectorAll<HTMLAnchorElement>('a[data-lightbox]'))
    : [link];
  const images = links.map(a => ({
    src: a.dataset.lightbox || a.href,
    title: a.dataset.lightboxTitle || a.querySelector('img')?.alt || 'Image',
    download: a.dataset.lightboxDownload || a.href,
    link: a,
  }));
  return { images, index: Math.max(0, links.indexOf(link)) };
}

function isPlainClick(e: MouseEvent): boolean {
  return e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;
}

const actionClass = 'inline-flex items-center justify-center h-8 w-8 rounded text-text-muted hover:text-text hover:bg-surface-hover transition-colors';

/**
 * The app's image viewer: one listener for clicks on `a[data-lightbox]`
 * anywhere (chat images, the send_file card, the Files page), which opens the
 * picture in the zoomable Lightbox. Mounted once, in App. It steps through
 * the set and stops at its first and last image. On close, focus goes to
 * the link of the image that showed last.
 */
export function ImageViewerHost() {
  const [state, setState] = useState<{ images: ViewerImage[]; index: number } | null>(null);
  // The picture on screen, and its place in the set: the last one that
  // loaded. While the next one loads, the dialog keeps showing this one
  // rather than closing.
  const [shown, setShown] = useState<(ViewerImage & { index: number; width: number; height: number }) | null>(null);
  const returnRef = useRef<HTMLElement | null>(null);

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
  const index = state?.index ?? 0;
  const images = state?.images;

  // A card can go while the viewer is open (a tool burst folds): the blob
  // URLs of the set stay valid until the viewer closes.
  useEffect(() => {
    if (!images) return;
    const urls = images.flatMap(i => [i.src, i.download]);
    holdBlobUrls(urls);
    return () => releaseBlobUrls(urls);
  }, [images]);

  // The Lightbox needs the picture's size at 1:1, which only loading it tells.
  useEffect(() => {
    if (!current) return;
    let cancelled = false;
    const img = new Image();
    img.onload = () => {
      // An SVG file with no size of its own can have none: it gets the size
      // that CSS gives such an image, 300 × 150, as it has in Chromium.
      if (!cancelled) setShown({ ...current, index, width: img.naturalWidth || 300, height: img.naturalHeight || 150 });
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
  }, [current, index]);

  // Load the neighbours, so that the next step shows its picture at once.
  useEffect(() => {
    if (!state) return;
    for (const near of [state.images[state.index - 1], state.images[state.index + 1]]) {
      if (near) new Image().src = near.src;
    }
  }, [state]);

  // After the Modal's own cleanup, which gives focus back to the link that opened it.
  useEffect(() => {
    if (state) return;
    const link = returnRef.current;
    returnRef.current = null;
    if (link?.isConnected) link.focus();
  }, [state]);

  const close = useCallback(() => {
    returnRef.current = shown?.link ?? null;
    setState(null);
    setShown(null);
  }, [shown]);
  // A step goes from the picture that shows, as the title, the count and
  // the arrows do: a second key press while the next one loads skips none.
  const shownIndex = shown?.index;
  const step = useCallback((by: number) => {
    setState(s => {
      if (!s || shownIndex === undefined) return s;
      const to = Math.min(Math.max(shownIndex + by, 0), s.images.length - 1);
      return to === s.index ? s : { ...s, index: to };
    });
  }, [shownIndex]);

  if (!state || !shown) return null;
  return (
    <Lightbox
      open
      onClose={close}
      title={shown.title}
      src={shown.src}
      size={{ width: shown.width, height: shown.height }}
      initialView="fit-down"
      nav={{ index: shown.index, count: state.images.length, onPrev: () => step(-1), onNext: () => step(1) }}
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
