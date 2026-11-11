import { modelLabel } from '../../utils/modelNames';

/**
 * The composer's Daybreak switch, for Codex chats.
 *
 * Daybreak is not a model. With the switch on, each turn of the chat asks
 * Codex for the Daybreak cyber access program of the signed-in account. The
 * composer renders the switch only when that account has Daybreak on at
 * least one model (or the chat still has it on). On a model without Daybreak
 * the switch stays in the row, greyed out, and its tooltip names the models
 * that offer it. A chat that has it on can always turn it off.
 */
export function DaybreakToggle({ enabled, offered, models, disabled, onToggle }: {
  /** The switch of this chat. */
  enabled: boolean;
  /** The chat's model offers Daybreak. */
  offered: boolean;
  /** The models that offer Daybreak, for the tooltip of a model that does not. */
  models: string[];
  disabled?: boolean;
  onToggle: (enabled: boolean) => void;
}) {
  const canToggle = offered || enabled;
  const offering = models.map(modelLabel).join(', ');
  const title = offered
    ? 'Daybreak: turns in this chat run with the Daybreak cyber access program of your Codex account'
    : enabled
      ? `Daybreak is on, but this model does not offer it. Turn it off, or choose a model that offers it${offering ? `: ${offering}` : ''}`
      : `Daybreak is not available for this model. Models that offer it: ${offering}`;
  return (
    <button
      type="button"
      onClick={() => { if (canToggle) onToggle(!enabled); }}
      disabled={disabled}
      // `aria-disabled` rather than `disabled` on a model without Daybreak, so
      // that the button still gets the pointer and focus that show the tooltip.
      aria-disabled={canToggle ? undefined : 'true'}
      aria-pressed={enabled}
      title={title}
      className={`h-8 px-2.5 rounded-lg flex items-center text-xs font-medium shrink-0 transition-all ring-1 ring-inset disabled:opacity-30 disabled:cursor-not-allowed ${
        enabled
          ? 'cursor-pointer text-hue-amber bg-hue-amber/10 hover:bg-hue-amber/15 ring-hue-amber/25'
          : canToggle
            ? 'cursor-pointer text-text-muted ring-border hover:text-text-secondary hover:bg-surface-hover'
            : 'text-text-muted ring-border opacity-50 cursor-not-allowed'
      }`}
    >
      Daybreak
    </button>
  );
}
