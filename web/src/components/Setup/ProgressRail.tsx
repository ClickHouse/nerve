import { Link } from 'react-router-dom';
import type { SetupState } from '../../api/client';
import { Badge } from '../ui';
import { ListChecks } from '../ui/icons';
import { REVIEW, stepTitle, stopPath, waitsForMode, type WizardStop } from './navigation';
import { StatusBadge } from './StatusBadge';

/**
 * The progress rail: an ordered list of the steps with their status, the
 * current one marked `aria-current="step"`. Items are ordinary links and
 * ordinary tab stops; a blocked step shows its reason as helper text —
 * except the four that merely wait for Mode, whose badge already says so.
 */
export function ProgressRail({ state, current, onNavigate }: {
  state: SetupState;
  current: WizardStop | null;
  /** Called after a link is followed (closes the phone drawer). */
  onNavigate?: () => void;
}) {
  return (
    <nav aria-label="Setup steps" className="flex flex-col gap-1">
      <ol className="flex flex-col gap-0.5">
        {state.steps.map((step) => {
          const active = current === step.id;
          return (
            <li key={step.id}>
              <Link
                to={stopPath(step.id)}
                onClick={onNavigate}
                aria-current={active ? 'step' : undefined}
                className={`flex flex-col gap-1 rounded-lg px-3 py-2 min-h-[44px] justify-center transition-colors
                  ${active ? 'bg-accent/10 text-text' : 'text-text-muted hover:bg-surface-hover hover:text-text'}`}
              >
                <span className="flex items-center justify-between gap-2">
                  <span className="text-sm">{stepTitle(step)}</span>
                  <StatusBadge step={step} />
                </span>
                {step.status === 'blocked' && step.blocked_reason && !waitsForMode(step) && (
                  <span className="text-2xs text-text-dim leading-snug">{step.blocked_reason}</span>
                )}
              </Link>
            </li>
          );
        })}
        <li>
          <Link
            to={stopPath(REVIEW)}
            onClick={onNavigate}
            aria-current={current === REVIEW ? 'step' : undefined}
            className={`flex items-center justify-between gap-2 rounded-lg px-3 py-2 min-h-[44px] transition-colors
              ${current === REVIEW ? 'bg-accent/10 text-text' : 'text-text-muted hover:bg-surface-hover hover:text-text'}`}
          >
            <span className="text-sm flex items-center gap-2"><ListChecks size={14} />Review</span>
            {state.finished && <Badge tone="success">Done</Badge>}
          </Link>
        </li>
      </ol>
    </nav>
  );
}
