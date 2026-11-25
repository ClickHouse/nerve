import type { PreviewBundle } from '../../types/preview';

/** How often the Preview tab reads the description of its bundle while it is visible. */
export const POLL_MS = 2_000;

/** True when a `file_changed` event for `path` (an absolute path) can change `bundle`. */
export function fileEventNamesBundle(bundle: PreviewBundle, path: string): boolean {
  const prefix = `${bundle.root_path.replace(/\/+$/, '')}/`;
  if (!path.startsWith(prefix)) return false;
  const inner = path.slice(prefix.length);
  // A new file of a folder is a change of the bundle too.
  return bundle.kind === 'folder' || bundle.files.some(f => f.path === inner);
}

/** "4 s ago", "3 min ago", "2 h ago", "5 d ago". */
export function agoText(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1000));
  if (seconds < 60) return `${seconds} s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h ago`;
  return `${Math.floor(hours / 24)} d ago`;
}
