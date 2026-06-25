/**
 * Copy text to clipboard with a fallback for non-secure contexts.
 *
 * `navigator.clipboard` is only exposed in *secure contexts* — HTTPS or
 * `http://localhost`. When the app is accessed over plain HTTP via a LAN
 * hostname or IP, or behind a proxy without a trusted cert, the entire
 * `clipboard` object is `undefined` and any call throws synchronously.
 *
 * Falls back to the deprecated `document.execCommand('copy')` via a hidden
 * off-screen `<textarea>` — still works in every browser we care about.
 *
 * Mirrors the same non-secure-context fallback pattern already used in
 * `utils/uuid.ts` for `crypto.randomUUID`.
 *
 * @returns true on success, false on failure.
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to legacy path.
    }
  }

  if (typeof document === 'undefined') return false;

  // Legacy fallback: off-screen textarea + execCommand('copy').
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.setAttribute('readonly', '');
  ta.style.position = 'fixed';
  ta.style.top = '-1000px';
  ta.style.left = '-1000px';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  try {
    ta.select();
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    document.body.removeChild(ta);
  }
}
