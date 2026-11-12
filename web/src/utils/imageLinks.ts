import { authUrl } from '../api/hosted';

/** Raster images a browser shows in place; the server serves these inline. SVG is not one. */
const RASTER_EXTENSIONS = /\.(png|jpe?g|gif|webp|avif|bmp)$/i;

export function isRasterImagePath(path: string): boolean {
  return RASTER_EXTENSIONS.test(path);
}

function withParam(url: string, param: string): string {
  return `${url}${url.includes('?') ? '&' : '?'}${param}`;
}

/**
 * Props for a link around an image that opens it in the image viewer.
 *
 * A plain click opens the viewer (ImageViewerHost listens for clicks on
 * `a[data-lightbox]`); Cmd/Ctrl/middle click still opens `href` in a tab,
 * where the server sends the image inline. Links of one message, or of one
 * `[data-lightbox-group]`, are stepped through together.
 *
 * @param url The image, without the token: `/api/files/uploads/<id>`.
 * @param downloadUrl The same file as an attachment; defaults to `url` with `download=1`.
 */
export function imageLinkProps(url: string, title: string, downloadUrl?: string) {
  const src = authUrl(url);
  return {
    href: src,
    target: '_blank',
    rel: 'noopener noreferrer',
    'data-lightbox': src,
    'data-lightbox-title': title,
    'data-lightbox-download': authUrl(downloadUrl ?? withParam(url, 'download=1')),
  };
}
