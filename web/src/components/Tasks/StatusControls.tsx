import { useTaskStatusStore, statusBadgeStyle } from '../../stores/taskStatusStore';

/** Colored pill showing a task's status label (falls back to the raw name). */
export function StatusBadge({ status, className = '' }: {
  status: string;
  className?: string;
}) {
  const meta = useTaskStatusStore((s) => s.statuses.find((x) => x.name === status));
  return (
    <span
      className={`px-2 py-0.5 rounded-full border ${className}`}
      style={statusBadgeStyle(meta?.color)}
    >
      {meta?.label || status}
    </span>
  );
}

/** Dropdown of configured statuses, driven by the task-status store. */
export function StatusSelect({ value, onChange, className = '' }: {
  value: string;
  onChange: (status: string) => void;
  className?: string;
}) {
  const statuses = useTaskStatusStore((s) => s.statuses);
  // Keep the current value selectable even if it's somehow not in the
  // configured set (defensive — deletion is blocked while a status is in use).
  const hasValue = statuses.some((s) => s.name === value);
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={className}
    >
      {!hasValue && value && <option value={value}>{value}</option>}
      {statuses.map((s) => (
        <option key={s.name} value={s.name}>{s.label}</option>
      ))}
    </select>
  );
}
