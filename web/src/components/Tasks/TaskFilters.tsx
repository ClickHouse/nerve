import { useTaskStatusStore } from '../../stores/taskStatusStore';
import { Button } from '../ui';

export function TaskFilters({ active, onChange }: {
  active: string;
  onChange: (filter: string) => void;
}) {
  const statuses = useTaskStatusStore((s) => s.statuses);
  const filters = [
    { value: '', label: 'Active' },
    ...statuses.map((s) => ({ value: s.name, label: s.label })),
  ];

  return (
    <div className="flex gap-1">
      {filters.map(f => (
        <Button
          key={f.value || 'active'}
          variant="pill"
          size="sm"
          active={active === f.value}
          aria-pressed={active === f.value}
          onClick={() => onChange(f.value)}
        >
          {f.label}
        </Button>
      ))}
    </div>
  );
}
