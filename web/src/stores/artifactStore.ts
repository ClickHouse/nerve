import { create } from 'zustand';
import type { ViewerArtifacts } from '../types/artifacts';

/**
 * Published artifacts in hosted mode: whether the Preview tab has the
 * Publish button, and the origin of the artifact viewer, whose addresses the
 * chat shows in a frame.
 *
 * From the `artifacts` member of `GET /api/auth/me` (`applyViewerArtifacts`),
 * which only hosted mode has. Without it there is no Publish button, and the
 * chat shows each published artifact as a link.
 */
interface ArtifactState {
  publish: boolean;
  /** An origin (`https://host[:port]`), or null. */
  viewerOrigin: string | null;
}

export const useArtifactStore = create<ArtifactState>(() => ({ publish: false, viewerOrigin: null }));

/** The origin of `value` when it is exactly an http(s) origin, else null. */
function originOf(value: unknown): string | null {
  if (typeof value !== 'string' || !value) return null;
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:' && url.protocol !== 'http:') return null;
    const bare = url.pathname === '/' && !url.search && !url.hash && !url.username && !url.password;
    return bare && !value.includes('?') && !value.includes('#') ? url.origin : null;
  } catch {
    return null;
  }
}

/** Take the `artifacts` member of `GET /api/auth/me` (absent: all off). */
export function applyViewerArtifacts(artifacts: ViewerArtifacts | null | undefined): void {
  const known = !!artifacts && typeof artifacts === 'object';
  useArtifactStore.setState({
    publish: known && artifacts.publish === true,
    viewerOrigin: known ? originOf(artifacts.viewer_origin) : null,
  });
}

/** Forget the settings, for tests. */
export function resetArtifactConfig(): void {
  useArtifactStore.setState({ publish: false, viewerOrigin: null });
}
