/**
 * An address the server reported for people to open (Loom's dashboard), or
 * null when it is not an absolute http(s) URL: a link built from a response
 * must never carry a `javascript:` or relative value.
 */
export function httpLink(value: string | null | undefined): string | null {
  if (!value) return null;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : null;
  } catch {
    return null;
  }
}
