import { passwordStrength } from '../../utils/passwordStrength';

/** A `<meter>` with a spoken label, so the strength is never colour-only. */
export function PasswordStrength({ password }: { password: string }) {
  if (!password) return null;
  const { score, label } = passwordStrength(password);
  return (
    <div className="flex items-center gap-2">
      <meter
        min={0}
        max={4}
        low={2}
        high={3}
        optimum={4}
        value={score}
        aria-label="Password strength"
        aria-valuetext={label}
        className="h-2 w-32"
      />
      <span className="text-xs text-text-muted">{label}</span>
    </div>
  );
}
