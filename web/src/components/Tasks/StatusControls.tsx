import { useTaskStatusStore, statusBadgeStyle } from '../../stores/taskStatusStore';
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

/** Dropdown of configured statuses, driven by the task-status store. */
export function StatusSelect({ value, onChange, className = '' }: {
  value: string;
  onChange: (status: string) => void;
  className?: string;
}) {
  const statuses = useTaskStatusStore((s) => s.statuses);
  return (
    <Select
      fieldSize="sm"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      // Keep the current value selectable even if it's somehow not in the
      // configured set (defensive — deletion is blocked while a status is in
      // use). `Select` renders an unmatched `value` as its own option rather
      // than letting the control snap to — and then submit — option one.
      options={statuses.map((s) => ({ value: s.name, label: s.label }))}
      className={className}
    />
  );
}
