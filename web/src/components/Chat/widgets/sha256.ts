/**
 * SHA-256 of a text, as 64 hex digits, computed here and at once.
 *
 * A widget's saved data is found by the hash of its source (`storage.ts`).
 * Two sources with the same hash would share their data, so the hash has to
 * be one for which nobody can make a second text on purpose; the short hash
 * that names a widget's height (`heights.ts`) is not. The browser's own
 * SHA-256 answers later, not now, and only on a page served securely.
 *
 * The text is hashed as its UTF-16 code units, low byte first: every text
 * has its own bytes that way, also one with half a surrogate pair in it.
 */

/** The first 32 bits of the fractional parts of the cube roots of the first 64 primes. */
const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const rotate = (word: number, by: number): number => (word >>> by) | (word << (32 - by));

export function sha256(text: string): string {
  const length = text.length * 2;
  // The text, one bit set after it, zeros, and its length in bits in the last eight bytes: whole blocks of 64.
  const bytes = new Uint8Array(Math.ceil((length + 9) / 64) * 64);
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i);
    bytes[2 * i] = unit & 0xff;
    bytes[2 * i + 1] = unit >>> 8;
  }
  bytes[length] = 0x80;
  const view = new DataView(bytes.buffer);
  view.setUint32(bytes.length - 8, Math.floor(length / 0x20000000));
  view.setUint32(bytes.length - 4, (length << 3) >>> 0);

  const hash = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ]);
  const words = new Uint32Array(64);
  for (let block = 0; block < bytes.length; block += 64) {
    for (let i = 0; i < 16; i++) words[i] = view.getUint32(block + 4 * i);
    for (let i = 16; i < 64; i++) {
      const early = words[i - 15];
      const late = words[i - 2];
      words[i] = words[i - 16] + (rotate(early, 7) ^ rotate(early, 18) ^ (early >>> 3))
        + words[i - 7] + (rotate(late, 17) ^ rotate(late, 19) ^ (late >>> 10));
    }
    let [a, b, c, d, e, f, g, h] = hash;
    for (let i = 0; i < 64; i++) {
      const one = (h + (rotate(e, 6) ^ rotate(e, 11) ^ rotate(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + words[i]) >>> 0;
      const two = ((rotate(a, 2) ^ rotate(a, 13) ^ rotate(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + one) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (one + two) >>> 0;
    }
    hash[0] += a;
    hash[1] += b;
    hash[2] += c;
    hash[3] += d;
    hash[4] += e;
    hash[5] += f;
    hash[6] += g;
    hash[7] += h;
  }
  return Array.from(hash, word => word.toString(16).padStart(8, '0')).join('');
}
