import type { FocusEventHandler } from 'react';
import { Button } from '../../ui';
import { Square } from '../../ui/icons';

/**
 * The per-agent Stop: quiet at rest (■), it turns into
 * a solid "Stop?" when armed and stops on the second press. One element in
 * both states, so keyboard focus stays on it between the two presses.
 */
export function TwoStepStopButton({ label, armed, onPress, onDisarm, tabIndex, onFocus }: {
  /** The agent's label, for the accessible name ("Stop build-api"). */
  label: string;
  armed: boolean;
  onPress: () => void;
  onDisarm: () => void;
  tabIndex?: number;
  onFocus?: FocusEventHandler<HTMLButtonElement>;
}) {
  return (
    <Button
      variant={armed ? 'dangerSolid' : 'dangerGhost'}
      size="xs"
      aria-label={armed ? `Confirm: stop ${label}` : `Stop ${label}`}
      title={armed ? `Click again to stop ${label}` : `Stop ${label}`}
      onClick={onPress}
      onBlur={armed ? onDisarm : undefined}
      onFocus={onFocus}
      tabIndex={tabIndex}
      className={armed ? 'py-0.5' : 'p-1'}
    >
      {armed ? 'Stop?' : <Square size={12} aria-hidden="true" />}
    </Button>
  );
}
