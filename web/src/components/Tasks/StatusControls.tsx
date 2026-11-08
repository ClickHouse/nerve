import {
  STATUS_TINT_CLASSES, useTaskStatusStore, statusBadgeStyle, statusTintStyle,
} from '../../stores/taskStatusStore';
import { Badge, Select } from '../ui';

/** Colored pill showing a task's status label (falls back to the raw name). */
export function StatusBadge({ status, className = '' }: {
  status: string;
  className?: string;
}) {
  const meta = useTaskStatusStore((s) => s.statuses.find((x) => x.name === status));
  return (
    // The colour is a user-configured hex rather than a design token, so there
    // is no utility class to name it and it has to be an inline style — which
    // also puts it above `tone`'s classes without depending on their order.
    <Badge size="sm" pill outline style={statusBadgeStyle(meta?.color)} className={className}>
      {meta?.label || status}
    </Badge>
  );
}

/**
 * Dropdown of configured statuses, driven by the task-status store.
 *
 * `tinted` gives the background and the border the colour of the current
 * status, for a place where the select is the only display of the status.
 * The options keep the normal colours: a desktop browser draws the open
 * list with the colours of the select.
 */
export function StatusSelect({ value, onChange, className = '', tinted = false }: {
  value: string;
  onChange: (status: string) => void;
  className?: string;
  tinted?: boolean;
}) {
  const statuses = useTaskStatusStore((s) => s.statuses);
  const color = statuses.find((s) => s.name === value)?.color;
  return (
    <Select
      fieldSize="sm"
      aria-label="Status"
      value={value}
      style={tinted ? statusTintStyle(color) : undefined}
      onChange={(e) => onChange(e.target.value)}
      // Keep the current value selectable even if it's somehow not in the
      // configured set (defensive — deletion is blocked while a status is in
      // use). `Select` renders an unmatched `value` as its own option rather
      // than letting the control snap to — and then submit — option one.
      options={statuses.map((s) => ({ value: s.name, label: s.label }))}
      className={
        tinted
          ? `font-medium ${STATUS_TINT_CLASSES} [&_option]:bg-surface-raised ${className}`
          : className
      }
    />
  );
}
