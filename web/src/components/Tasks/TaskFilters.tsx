const FILTERS = [
  { value: '', label: 'Active' },
  { value: 'pending', label: 'Pending' },
  { value: 'in_progress', label: 'In Progress' },
  { value: 'done', label: 'Done' },
  { value: 'deferred', label: 'Deferred' },
];

export function TaskFilters({ active, onChange }: {
  active: string;
  onChange: (filter: string) => void;
}) {
  return (
    <div className="flex gap-1">
      {FILTERS.map(f => (
        <button
          key={f.value}
          onClick={() => onChange(f.value)}
          className={`px-3 py-1.5 text-[13px] rounded-md cursor-pointer transition-colors
            ${active === f.value
              ? 'bg-[#6366f1]/15 text-[#6366f1] font-medium'
              : 'text-text-dim hover:text-text-muted hover:bg-surface-raised'
            }`}
        >
          {f.label}
        </button>
      ))}
    </div>
  );
}
