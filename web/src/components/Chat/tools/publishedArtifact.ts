/**
 * What the chat reads of a `publish_artifact` result, and where it may show
 * the viewer.
 *
 * The tool result is text (`nerve/artifacts/publish.py`, `result_text`): a
 * line `Viewer address: <address>` and the version in the first line. The
 * chat shows the viewer in a frame only for an address whose origin is
 * exactly the configured viewer origin; every other address is a link.
 */

import { toolBaseName, toolServer } from '../../../utils/toolNames';

/**
 * True for Nerve's own `publish_artifact` (`mcp__nerve__publish_artifact`,
 * or the bare name), not for a tool of that name of another MCP server.
 */
export function isPublishArtifactTool(tool: string): boolean {
  const server = toolServer(tool);
  return toolBaseName(tool) === 'publish_artifact' && (server === null || server === 'nerve');
}

const ADDRESS_LINE = /^Viewer address: (\S+)$/m;
const VERSION = /^Published .* as version (\d+)\.$/m;

/** The query parameter of the viewer for its layout in a frame of a chat. */
export const EMBED_PARAM = 'embed';
/** The query parameter of the viewer for the version to show. */
export const VERSION_PARAM = 'v';

export interface PublishedResult {
  address: string;
  version: number | null;
}

/** The viewer address and the version of a result text, or null. */
export function readPublished(text: string): PublishedResult | null {
  const address = ADDRESS_LINE.exec(text)?.[1];
  if (!address) return null;
  const version = Number(VERSION.exec(text)?.[1]);
  return { address, version: Number.isSafeInteger(version) && version > 0 ? version : null };
}

/** `value` as an http(s) URL without credentials, or null. */
function httpUrl(value: string): URL | null {
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:' && url.protocol !== 'http:') return null;
    if (url.username || url.password) return null;
    return url;
  } catch {
    return null;
  }
}

/** The address of `published` as a link for people, or null when it is not an http(s) address. */
export function viewerLink(published: PublishedResult): string | null {
  return httpUrl(published.address)?.href ?? null;
}

/**
 * The address of the frame of the viewer: the address of `published` with
 * `embed=1` and the version, when its origin is exactly `viewerOrigin`
 * (compared as origins, not as text). Null in every other case.
 */
export function viewerFrameUrl(published: PublishedResult, viewerOrigin: string | null): string | null {
  if (!viewerOrigin) return null;
  const url = httpUrl(published.address);
  const allowed = httpUrl(viewerOrigin);
  if (!url || !allowed || url.origin !== allowed.origin) return null;
  url.searchParams.set(EMBED_PARAM, '1');
  if (published.version !== null) url.searchParams.set(VERSION_PARAM, String(published.version));
  url.hash = '';
  return url.href;
}
