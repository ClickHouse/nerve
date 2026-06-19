import { useTaskStatusStore } from '../../stores/taskStatusStore';

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
        <button
          key={f.value || 'active'}
          onClick={() => onChange(f.value)}
          className={`px-3 py-1.5 text-[13px] rounded-md cursor-pointer transition-colors
            ${active === f.value
              ? 'bg-accent/15 text-accent font-medium'
              : 'text-text-dim hover:text-text-muted hover:bg-surface-raised'
            }`}
        >
          {f.label}
        </button>
      ))}
    </div>
  );
}
