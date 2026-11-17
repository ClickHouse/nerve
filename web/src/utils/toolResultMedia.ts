/**
 * The pictures and documents inside a tool result, such as Read on an image.
 *
 * The engine stores a non-text tool result as its JSON content blocks, so a
 * Read of a PNG arrives as `[{"type":"image","source":{"type":"base64",
 * "media_type":"image/png","data":"iVBOR..."}}]`. The bytes are the ones the
 * model saw, so the chat can show them even when the file was outside the
 * workspace or is gone.
 */

export interface ResultImage {
  mediaType: string;
  data: string;
}

export interface ResultMedia {
  images: ResultImage[];
  /** PDFs and other `document` blocks: counted, not shown. */
  documents: number;
}

const MEDIA_TYPES: Record<string, string> = {
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  avif: 'image/avif',
  bmp: 'image/bmp',
};

function guessMediaType(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase() ?? '';
  return MEDIA_TYPES[ext] ?? 'image/png';
}

function blocksOf(parsed: unknown): unknown[] | null {
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === 'object' && Array.isArray((parsed as { content?: unknown }).content)) {
    return (parsed as { content: unknown[] }).content;
  }
  return null;
}

/**
 * The media in `result`, or null when it holds none (plain text, an error,
 * JSON of another shape). `path` names the file, for a missing media type.
 */
export function parseResultMedia(result: string | undefined, path = ''): ResultMedia | null {
  if (!result || result[0] !== '[' && result[0] !== '{') return null;
  // Cheap check before parsing a large text result.
  if (!result.includes('"base64"')) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(result);
  } catch {
    return null;
  }
  const blocks = blocksOf(parsed);
  if (!blocks) return null;
  const images: ResultImage[] = [];
  let documents = 0;
  for (const block of blocks) {
    if (!block || typeof block !== 'object') continue;
    const { type, source } = block as { type?: unknown; source?: Record<string, unknown> };
    if (type === 'document') {
      documents += 1;
      continue;
    }
    if (type !== 'image' || !source || source.type !== 'base64' || typeof source.data !== 'string') continue;
    const mediaType = typeof source.media_type === 'string' && source.media_type.startsWith('image/')
      ? source.media_type
      : guessMediaType(path);
    images.push({ mediaType, data: source.data });
  }
  return images.length || documents ? { images, documents } : null;
}

/** A blob for base64 image data; null when the data does not decode. */
export function imageBlob(image: ResultImage): Blob | null {
  try {
    const bin = atob(image.data);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return new Blob([bytes], { type: image.mediaType });
  } catch {
    return null;
  }
}
