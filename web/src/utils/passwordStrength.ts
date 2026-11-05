/**
 * A rough strength estimate: length first, then variety. Not a security
 * boundary — the server accepts what it accepts — but a nudge away from
 * "password1" on an instance that is about to be reachable from a LAN.
 */
export function passwordStrength(password: string): { score: 0 | 1 | 2 | 3 | 4; label: string } {
  if (!password) return { score: 0, label: 'Empty' };
  let score = 0;
  if (password.length >= 8) score += 1;
  if (password.length >= 12) score += 1;
  const classes = [/[a-z]/, /[A-Z]/, /\d/, /[^A-Za-z0-9]/].filter((re) => re.test(password)).length;
  if (classes >= 2) score += 1;
  if (classes >= 3 && password.length >= 10) score += 1;
  if (/\s/.test(password) && password.length >= 16) score = Math.max(score, 3);  // passphrases
  const clamped = Math.min(4, score) as 0 | 1 | 2 | 3 | 4;
  const labels = ['Too short', 'Weak', 'Fair', 'Good', 'Strong'];
  return { score: clamped, label: labels[clamped] };
}
