import type { ReactNode } from 'react';
import { AlertTriangle, CircleHelp, Lock } from '../ui/icons';

/**
 * A short, non-blocking note: a caution, a lockdown reason, a tip.
 * `role="status"` so it is announced when it appears; never a trap.
 */
export function Notice({ tone = 'info', icon, children, className }: {
  tone?: 'info' | 'warning' | 'error' | 'locked';
  icon?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  const glyph = icon ?? (
    tone === 'warning' ? <AlertTriangle size={14} />
      : tone === 'locked' ? <Lock size={14} />
        : tone === 'error' ? <AlertTriangle size={14} />
          : <CircleHelp size={14} />
  );
  const colour = tone === 'warning' ? 'text-hue-amber'
    : tone === 'error' ? 'text-error'
      : 'text-text-muted';
  const frame = tone === 'error'
    ? 'border-error-border bg-error-bg'
    : 'border-border bg-surface-raised';
  return (
    <div role="status" className={`flex gap-2 items-start rounded-lg border p-3 ${frame} ${className ?? ''}`}>
      <span className={`mt-0.5 shrink-0 ${colour}`}>{glyph}</span>
      <div className={`text-sm min-w-0 ${tone === 'error' ? 'text-error' : 'text-text-muted'}`}>{children}</div>
    </div>
  );
}
