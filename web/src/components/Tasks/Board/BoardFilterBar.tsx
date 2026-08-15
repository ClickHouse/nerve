import { useTaskStore } from '../../../stores/taskStore';
import { Button } from '../../ui';
import { Tag, X } from '../../ui/icons';

/** How many tag facets to offer before it stops being a filter bar. */
const MAX_FACETS = 12;

export function BoardFilterBar() {
  const availableTags = useTaskStore((s) => s.availableTags);
  const tagFilter = useTaskStore((s) => s.tagFilter);
  const setTagFilter = useTaskStore((s) => s.setTagFilter);

  if (availableTags.length === 0) return null;

  // Ranked by count server-side; an active filter is pinned so it can
  // always be switched off even if it falls outside the top slice — or has
  // left the list altogether, which happens as soon as its last open task is
  // retagged or finished, since the facets exclude done. Synthesising a
  // zero-count chip keeps the "Clear" affordance reachable; the alternative
  // is a filter the user can see the effect of but not turn off.
  const shown = availableTags.slice(0, MAX_FACETS);
  const facets =
    tagFilter && !shown.some((t) => t.name === tagFilter)
      ? [...shown, availableTags.find((t) => t.name === tagFilter) ?? { name: tagFilter, count: 0 }]
      : shown;

  return (
    <div className="flex items-center gap-1.5 px-4 py-2 border-b border-border-subtle overflow-x-auto shrink-0">
      <Tag size={12} className="text-text-faint shrink-0" />
      {facets.map((tag) => {
        const active = tag.name === tagFilter;
        return (
          <Button
            key={tag.name}
            variant="pill"
            size="xs"
            active={active}
            aria-pressed={active}
            onClick={() => setTagFilter(active ? '' : tag.name)}
          >
            {tag.name}
            <span className="tabular-nums opacity-60">{tag.count}</span>
          </Button>
        );
      })}
      {tagFilter && (
        <Button variant="ghost" size="xs" onClick={() => setTagFilter('')}>
          <X size={11} /> Clear
        </Button>
      )}
    </div>
  );
}
