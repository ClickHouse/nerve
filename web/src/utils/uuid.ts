/**
 * UUID generation that works in non-secure contexts.
 *
 * `crypto.randomUUID()` is only exposed in *secure contexts* — HTTPS or
 * `http://localhost`. When the app is accessed over plain HTTP via a LAN
 * hostname or IP (e.g. `http://192.168.x.x:8900` or a `.local` mDNS name),
 * `crypto.randomUUID` is `undefined` and calling it throws
 * `TypeError: crypto.randomUUID is not a function`.
 *
 * `crypto.getRandomValues()`, however, IS available in non-secure contexts,
 * so we derive an RFC 4122 version-4 UUID from it as a fallback. As a last
 * resort (no Web Crypto at all) we fall back to `Math.random()` — these ids
 * are only used as client-side keys, never for anything security-sensitive.
 */
export function randomUUID(): string {
  // Fast path: native, available in secure contexts.
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }

  // Fallback: build a v4 UUID from crypto.getRandomValues (non-secure-context safe).
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10
    const hex: string[] = [];
    for (let i = 0; i < 256; i++) hex.push((i + 0x100).toString(16).slice(1));
    return (
      hex[bytes[0]] + hex[bytes[1]] + hex[bytes[2]] + hex[bytes[3]] + '-' +
      hex[bytes[4]] + hex[bytes[5]] + '-' +
      hex[bytes[6]] + hex[bytes[7]] + '-' +
      hex[bytes[8]] + hex[bytes[9]] + '-' +
      hex[bytes[10]] + hex[bytes[11]] + hex[bytes[12]] + hex[bytes[13]] + hex[bytes[14]] + hex[bytes[15]]
    );
  }

  // Last resort: no Web Crypto at all. Not cryptographically strong, but these
  // ids are only client-side keys, never security-sensitive.
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
