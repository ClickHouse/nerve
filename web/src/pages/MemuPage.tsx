import { useEffect, useState, useMemo } from 'react';
import {
  Database, Search, Plus, X, Pencil, Trash2, ChevronDown, ChevronRight,
  FileText, Clock, Circle, History,
} from '../components/ui/icons';
import { Badge, Button, IconButton, Select, TextArea, TextField } from '../components/ui';
import { useMemoryStore, type Category, type MemoryItem, type Resource, type TabView } from '../stores/memoryStore';
import { PageHeader } from '../components/ui/PageHeader';
import { PaneToggle } from '../components/ui/PaneToggle';
import { Drawer } from '../components/ui/Drawer';
import { useIsMobile } from '../hooks/useMediaQuery';

/**
 * Memory-type identity colours. Data-driven: the map is read at runtime to
 * colour a dot, a badge and a badge tint, so these stay CSS colour *values*
 * rather than becoming utility classes. They are `--theme-hue-*` now, so the
 * identity colour follows the light/dark theme like the rest of the page —
 * the raw hexes here were pinned dark-theme values.
 */
const TYPE_COLORS: Record<string, string> = {
  profile: 'var(--theme-accent)',
  event: 'var(--theme-hue-amber)',
  knowledge: 'var(--theme-hue-green)',
  behavior: 'var(--theme-hue-red)',
  skill: 'var(--theme-hue-blue)',
  tool: 'var(--theme-hue-purple)',
};

/**
 * A 15% wash of an identity colour, matching the 15% alpha `Badge`'s tones use.
 *
 * `color-mix` rather than the `color + '20'` string concat this replaces: that
 * only ever worked for a literal hex, and produced the invalid `var(--theme-
 * accent)20` for the one entry that was already a token — so `profile` badges
 * have been rendering with no tint at all.
 */
const tint = (color: string) => `color-mix(in oklab, ${color} 15%, transparent)`;

const FACT_TYPES = ['profile', 'knowledge', 'behavior', 'skill', 'tool'];

const TABS: { key: TabView; label: string }[] = [
  { key: 'facts', label: 'Facts' },
  { key: 'timeline', label: 'Timeline' },
  { key: 'sources', label: 'Sources' },
  { key: 'log', label: 'Log' },
];

function formatPath(url: string): string {
  const parts = url.split('/');
  return parts[parts.length - 1] || url;
}

function formatDateGroup(iso: string): string {
  // Date-only strings (YYYY-MM-DD) are parsed as UTC by JS; force local interpretation
  const d = new Date(iso.length === 10 ? iso + 'T00:00:00' : iso);
  return d.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });
}

/** The memory-type chip, coloured from `TYPE_COLORS`. */
function TypeBadge({ type, className }: { type: string; className?: string }) {
  const color = TYPE_COLORS[type] || 'var(--theme-text-muted)';
  return (
    <Badge className={className} style={{ backgroundColor: tint(color), color }}>
      {type}
    </Badge>
  );
}

// --- Inline Edit Form ---

function EditForm({ item, onSave, onCancel }: {
  item: MemoryItem;
  onSave: (data: { content: string; memory_type: string; categories?: string[] }) => void;
  onCancel: () => void;
}) {
  const { categories, categoryItems } = useMemoryStore();
  const [content, setContent] = useState(item.summary);
  const [memType, setMemType] = useState(item.memory_type);
  const [saving, setSaving] = useState(false);

  const [selectedCatIds, setSelectedCatIds] = useState<Set<string>>(() => {
    const ids = new Set<string>();
    for (const [catId, itemIds] of Object.entries(categoryItems)) {
      if (itemIds.includes(item.id)) ids.add(catId);
    }
    return ids;
  });

  const toggleCat = (catId: string) => {
    setSelectedCatIds(prev => {
      const next = new Set(prev);
      if (next.has(catId)) next.delete(catId);
      else next.add(catId);
      return next;
    });
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const catNames = categories.filter(c => selectedCatIds.has(c.id)).map(c => c.name);
      await onSave({ content, memory_type: memType, categories: catNames });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="border border-accent/30 rounded-lg p-3 bg-accent/5 space-y-2">
      <TextArea
        value={content}
        onChange={e => setContent(e.target.value)}
        rows={3}
      />
      <div className="flex items-center gap-2">
        <Select
          fieldSize="sm"
          value={memType}
          onChange={e => setMemType(e.target.value)}
          options={Object.keys(TYPE_COLORS).map(t => ({ value: t, label: t }))}
        />
        <div className="flex-1" />
        <Button variant="ghost" onClick={onCancel}>Cancel</Button>
        <Button variant="primary" onClick={handleSave} disabled={saving || !content.trim()}>
          {saving ? 'Saving...' : 'Save'}
        </Button>
      </div>
      {categories.length > 0 && (
        <div className="flex flex-wrap gap-1 pt-1 border-t border-border-subtle/50">
          <span className="text-2xs text-text-dim self-center mr-1">Categories:</span>
          {categories.map(cat => (
            <Button
              key={cat.id}
              variant="pill"
              size="xs"
              active={selectedCatIds.has(cat.id)}
              onClick={() => toggleCat(cat.id)}
            >
              {cat.name.replace(/_/g, ' ')}
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}

// --- Delete Confirmation ---

function DeleteConfirm({ item, onConfirm, onCancel }: { item: MemoryItem; onConfirm: () => void; onCancel: () => void }) {
  const [deleting, setDeleting] = useState(false);
  const handleDelete = async () => { setDeleting(true); try { await onConfirm(); } finally { setDeleting(false); } };
  return (
    <div className="border border-error-border rounded-lg p-3 bg-error-bg">
      <div className="text-xs text-text-secondary mb-2">Delete this memory item?</div>
      <div className="text-xs text-text-muted mb-3 line-clamp-2">{item.summary}</div>
      <div className="flex items-center gap-2 justify-end">
        <Button variant="ghost" onClick={onCancel}>Cancel</Button>
        <Button variant="dangerSolid" onClick={handleDelete} disabled={deleting}>
          {deleting ? 'Deleting...' : 'Delete'}
        </Button>
      </div>
    </div>
  );
}

// --- Memory Item Row ---

function ItemRow({ item, isEditing, isDeleting, onEdit, onDelete, onSave, onCancelEdit, onConfirmDelete, onCancelDelete }: {
  item: MemoryItem; isEditing: boolean; isDeleting: boolean;
  onEdit: () => void; onDelete: () => void;
  onSave: (data: { content: string; memory_type: string; categories?: string[] }) => void;
  onCancelEdit: () => void; onConfirmDelete: () => void; onCancelDelete: () => void;
}) {
  if (isEditing) return <EditForm item={item} onSave={onSave} onCancel={onCancelEdit} />;
  if (isDeleting) return <DeleteConfirm item={item} onConfirm={onConfirmDelete} onCancel={onCancelDelete} />;
  return (
    <div className="group flex items-start gap-2 px-3 py-2 rounded hover:bg-surface-raised transition-colors">
      <TypeBadge type={item.memory_type} />
      <span className="text-xs text-text-secondary flex-1">{item.summary}</span>
      <div className="shrink-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <IconButton size="xs" label="Edit" onClick={onEdit}><Pencil size={12} /></IconButton>
        <IconButton size="xs" variant="dangerGhost" label="Delete" onClick={onDelete}><Trash2 size={12} /></IconButton>
      </div>
    </div>
  );
}

// --- Editable Category Summary ---

function CategorySummaryEditor({ category }: { category: Category }) {
  const { editingCategoryId, setEditingCategoryId, updateCategory } = useMemoryStore();
  const isEditing = editingCategoryId === category.id;
  const [value, setValue] = useState(category.summary || '');
  const [saving, setSaving] = useState(false);

  useEffect(() => { if (isEditing) setValue(category.summary || ''); }, [isEditing, category.summary]);

  if (!isEditing) {
    return (
      <div
        className="px-3 py-2 bg-bg-sunken text-xs text-text-muted whitespace-pre-wrap border-b border-border-subtle group/summary cursor-pointer hover:bg-surface-hover transition-colors"
        onClick={() => setEditingCategoryId(category.id)}
        title="Click to edit summary"
      >
        {category.summary || <span className="text-text-faint italic">No summary — click to add</span>}
        <Pencil size={10} className="inline ml-1 opacity-0 group-hover/summary:opacity-50" />
      </div>
    );
  }

  const handleSave = async () => {
    setSaving(true);
    try { await updateCategory(category.id, { summary: value }); } finally { setSaving(false); }
  };

  return (
    <div className="px-3 py-2 bg-bg-sunken border-b border-border-subtle">
      <TextArea fieldSize="sm" value={value} onChange={e => setValue(e.target.value)} rows={3} autoFocus />
      <div className="flex justify-end gap-2 mt-1">
        <Button variant="ghost" size="xs" onClick={() => setEditingCategoryId(null)}>Cancel</Button>
        <Button variant="primary" size="xs" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving...' : 'Save'}
        </Button>
      </div>
    </div>
  );
}

// --- Create Category Form ---

function CreateCategoryForm({ onClose }: { onClose: () => void }) {
  const createCategory = useMemoryStore(s => s.createCategory);
  const [name, setName] = useState('');
  const [desc, setDesc] = useState('');
  const [creating, setCreating] = useState(false);

  const handleCreate = async () => {
    if (!name.trim()) return;
    setCreating(true);
    try { await createCategory(name.trim(), desc.trim()); onClose(); } catch (e) { console.error('Failed to create category:', e); }
    setCreating(false);
  };

  return (
    <div className="border border-accent/30 rounded-lg p-3 bg-accent/5 mx-3 mb-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-medium">New Category</span>
        <IconButton size="xs" label="Close" onClick={onClose}><X size={12} /></IconButton>
      </div>
      <TextField fieldSize="sm" placeholder="Name (e.g. travel_plans)" value={name} onChange={e => setName(e.target.value)} className="mb-2" />
      <TextField fieldSize="sm" placeholder="Description" value={desc} onChange={e => setDesc(e.target.value)} className="mb-2" />
      <Button variant="primary" size="xs" onClick={handleCreate} disabled={!name.trim() || creating}>
        {creating ? 'Creating...' : 'Create'}
      </Button>
    </div>
  );
}

// --- Heatmap ---

function Heatmap({ items, onDayClick, selectedDate }: { items: MemoryItem[]; onDayClick: (d: string) => void; selectedDate: string | null }) {
  const dayCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const item of items) {
      const ds = item.happened_at || item.created_at;
      if (!ds) continue;
      const day = ds.substring(0, 10);
      counts[day] = (counts[day] || 0) + 1;
    }
    return counts;
  }, [items]);

  const weeks = useMemo(() => {
    const today = new Date();
    const totalDays = 26 * 7;
    const start = new Date(today);
    start.setDate(start.getDate() - totalDays + 1);
    start.setDate(start.getDate() - start.getDay());

    const result: { dateStr: string; count: number }[][] = [];
    let week: typeof result[0] = [];
    const d = new Date(start);

    while (d <= today) {
      const ds = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
      week.push({ dateStr: ds, count: dayCounts[ds] || 0 });
      if (week.length === 7) { result.push(week); week = []; }
      d.setDate(d.getDate() + 1);
    }
    if (week.length > 0) result.push(week);
    return result;
  }, [dayCounts]);

  // Data-driven, so these stay colour values rather than utilities. The three
  // steps are one hue at three alphas, keyed off item count; `color-mix` lets
  // that hue be the theme token instead of the old pinned `#f59e0b`.
  const getColor = (count: number, dateStr: string) => {
    if (selectedDate === dateStr) return 'var(--theme-accent)';
    if (count === 0) return 'var(--theme-surface)';
    if (count <= 2) return 'color-mix(in oklab, var(--theme-hue-amber) 20%, transparent)';
    if (count <= 5) return 'color-mix(in oklab, var(--theme-hue-amber) 53%, transparent)';
    return 'var(--theme-hue-amber)';
  };

  return (
    <div className="px-3 py-2 border-b border-border-subtle overflow-x-auto">
      <div className="text-2xs text-text-faint mb-1">Memory activity — last 6 months</div>
      <div style={{ display: 'flex', gap: 2 }}>
        {weeks.map((week, wi) => (
          <div key={wi} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {week.map(({ dateStr, count }) => (
              <div
                key={dateStr}
                onClick={() => onDayClick(dateStr)}
                title={`${dateStr}: ${count} item${count !== 1 ? 's' : ''}`}
                style={{
                  width: 11, height: 11,
                  backgroundColor: getColor(count, dateStr),
                  borderRadius: 2,
                  cursor: 'pointer',
                }}
              />
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Facts Tab ---

function FactsView() {
  const { items, categories, categoryItems, selectedCategory, searchQuery, editingItemId, deletingItemId,
    setEditingItemId, setDeletingItemId, updateItem, deleteItem } = useMemoryStore();
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const facts = useMemo(() => {
    let filtered = items.filter(i => FACT_TYPES.includes(i.memory_type));
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      filtered = filtered.filter(i => i.summary.toLowerCase().includes(q));
    }
    return filtered;
  }, [items, searchQuery]);

  const factIds = useMemo(() => new Set(facts.map(i => i.id)), [facts]);

  const grouped = useMemo(() => {
    if (selectedCategory) {
      const cat = categories.find(c => c.id === selectedCategory);
      if (!cat) return [];
      const ids = categoryItems[cat.id] || [];
      const catFacts = ids.map(id => facts.find(f => f.id === id)).filter(Boolean) as MemoryItem[];
      return [{ category: cat, items: catFacts }];
    }
    return categories
      .map(cat => {
        const ids = categoryItems[cat.id] || [];
        const catFacts = ids.filter(id => factIds.has(id)).map(id => facts.find(f => f.id === id)).filter(Boolean) as MemoryItem[];
        return { category: cat, items: catFacts };
      })
      .filter(g => g.items.length > 0);
  }, [categories, categoryItems, facts, factIds, selectedCategory]);

  const categorizedIds = useMemo(() => {
    const s = new Set<string>();
    for (const ids of Object.values(categoryItems)) for (const id of ids) s.add(id);
    return s;
  }, [categoryItems]);

  const uncategorized = useMemo(() => facts.filter(f => !categorizedIds.has(f.id)), [facts, categorizedIds]);

  const toggle = (catId: string) => setCollapsed(prev => ({ ...prev, [catId]: !prev[catId] }));
  const handleSave = async (id: string, data: { content: string; memory_type: string; categories?: string[] }) => { await updateItem(id, data); };

  return (
    <div className="space-y-1 p-3">
      {grouped.map(({ category, items: catItems }) => (
        <div key={category.id} className="border border-border-subtle rounded-lg overflow-hidden">
          <Button variant="subtle" size="md" fullWidth onClick={() => toggle(category.id)} className="justify-start rounded-none">
            {collapsed[category.id] ? <ChevronRight size={13} className="text-text-faint" /> : <ChevronDown size={13} className="text-text-faint" />}
            <span className="font-medium">{category.name.replace(/_/g, ' ')}</span>
            <span className="text-xs text-text-faint">{catItems.length}</span>
          </Button>
          {!collapsed[category.id] && (
            <div className="border-t border-border-subtle">
              <CategorySummaryEditor category={category} />
              <div className="divide-y divide-border-subtle">
                {catItems.map(item => (
                  <ItemRow key={item.id} item={item} isEditing={editingItemId === item.id} isDeleting={deletingItemId === item.id}
                    onEdit={() => setEditingItemId(item.id)} onDelete={() => setDeletingItemId(item.id)}
                    onSave={(data) => handleSave(item.id, data)} onCancelEdit={() => setEditingItemId(null)}
                    onConfirmDelete={() => deleteItem(item.id)} onCancelDelete={() => setDeletingItemId(null)} />
                ))}
              </div>
            </div>
          )}
        </div>
      ))}

      {uncategorized.length > 0 && (
        <div className="border border-border-subtle rounded-lg overflow-hidden">
          <Button variant="subtle" size="md" fullWidth onClick={() => toggle('__uncategorized')} className="justify-start rounded-none">
            {collapsed['__uncategorized'] ? <ChevronRight size={13} className="text-text-faint" /> : <ChevronDown size={13} className="text-text-faint" />}
            <span className="font-medium text-text-muted">uncategorized</span>
            <span className="text-xs text-text-faint">{uncategorized.length}</span>
          </Button>
          {!collapsed['__uncategorized'] && (
            <div className="border-t border-border-subtle divide-y divide-border-subtle">
              {uncategorized.map(item => (
                <ItemRow key={item.id} item={item} isEditing={editingItemId === item.id} isDeleting={deletingItemId === item.id}
                  onEdit={() => setEditingItemId(item.id)} onDelete={() => setDeletingItemId(item.id)}
                  onSave={(data) => handleSave(item.id, data)} onCancelEdit={() => setEditingItemId(null)}
                  onConfirmDelete={() => deleteItem(item.id)} onCancelDelete={() => setDeletingItemId(null)} />
              ))}
            </div>
          )}
        </div>
      )}

      {grouped.length === 0 && uncategorized.length === 0 && (
        <div className="text-center text-text-faint text-sm py-12">{searchQuery ? 'No facts match your search' : 'No facts found'}</div>
      )}
    </div>
  );
}

// --- Timeline Tab ---

function TimelineView() {
  const { items, categories, categoryItems, searchQuery, editingItemId, deletingItemId,
    setEditingItemId, setDeletingItemId, updateItem, deleteItem } = useMemoryStore();
  const [filterDate, setFilterDate] = useState<string | null>(null);

  const eventDate = (item: MemoryItem) => item.happened_at ?? item.created_at;

  const allEvents = useMemo(() => items.filter(i => i.memory_type === 'event'), [items]);

  const events = useMemo(() => {
    let filtered = allEvents;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      filtered = filtered.filter(i => i.summary.toLowerCase().includes(q));
    }
    if (filterDate) {
      filtered = filtered.filter(i => (eventDate(i))?.substring(0, 10) === filterDate);
    }
    return filtered.sort((a, b) => new Date(eventDate(b)).getTime() - new Date(eventDate(a)).getTime());
  }, [allEvents, searchQuery, filterDate]);

  const itemCategoryMap = useMemo(() => {
    const map: Record<string, string[]> = {};
    for (const [catId, ids] of Object.entries(categoryItems)) {
      for (const id of ids) { if (!map[id]) map[id] = []; map[id].push(catId); }
    }
    return map;
  }, [categoryItems]);

  const categoryMap = useMemo(() => new Map(categories.map(c => [c.id, c])), [categories]);

  const grouped = useMemo(() => {
    const groups: { date: string; dateKey: string; items: MemoryItem[] }[] = [];
    let currentDate = '';
    for (const event of events) {
      const dateKey = eventDate(event)?.substring(0, 10) || '';
      if (dateKey !== currentDate) {
        currentDate = dateKey;
        groups.push({ date: eventDate(event), dateKey, items: [] });
      }
      groups[groups.length - 1].items.push(event);
    }
    return groups;
  }, [events]);

  const handleSave = async (id: string, data: { content: string; memory_type: string; categories?: string[] }) => { await updateItem(id, data); };

  const handleDayClick = (dateStr: string) => {
    setFilterDate(prev => prev === dateStr ? null : dateStr);
    setTimeout(() => {
      const el = document.getElementById(`timeline-date-${dateStr}`);
      el?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 50);
  };

  return (
    <div className="flex flex-col h-full">
      <Heatmap items={allEvents} onDayClick={handleDayClick} selectedDate={filterDate} />
      {filterDate && (
        <div className="px-3 py-1.5 bg-warning-bg border-b border-border-subtle flex items-center gap-2">
          <span className="text-xs text-warning">Showing: {filterDate}</span>
          <IconButton size="xs" label="Clear date filter" className="text-warning" onClick={() => setFilterDate(null)}><X size={12} /></IconButton>
        </div>
      )}
      <div className="flex-1 overflow-y-auto p-3">
        {grouped.length === 0 && (
          <div className="text-center text-text-faint text-sm py-12">{searchQuery ? 'No events match your search' : 'No events found'}</div>
        )}
        {grouped.map(group => (
          <div key={group.dateKey} id={`timeline-date-${group.dateKey}`} className="mb-4">
            <div className="flex items-center gap-2 mb-2 px-2">
              <Clock size={12} className="text-warning" />
              <span className="text-xs text-warning font-medium">{formatDateGroup(group.date)}</span>
            </div>
            <div className="border-l-2 border-border-subtle ml-3 pl-4 space-y-1">
              {group.items.map(item => {
                if (editingItemId === item.id) return <EditForm key={item.id} item={item} onSave={(data) => handleSave(item.id, data)} onCancel={() => setEditingItemId(null)} />;
                if (deletingItemId === item.id) return <DeleteConfirm key={item.id} item={item} onConfirm={() => deleteItem(item.id)} onCancel={() => setDeletingItemId(null)} />;
                const catIds = itemCategoryMap[item.id] || [];
                return (
                  <div key={item.id} className="group flex items-start gap-2 px-2 py-1.5 rounded hover:bg-surface-raised transition-colors">
                    <div className="w-2 h-2 rounded-full bg-warning mt-1.5 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="text-xs text-text-secondary">{item.summary}</div>
                      {catIds.length > 0 && (
                        <div className="flex gap-1 mt-1">
                          {catIds.map(cid => { const cat = categoryMap.get(cid); return cat ? <Badge key={cid}>{cat.name}</Badge> : null; })}
                        </div>
                      )}
                    </div>
                    <div className="shrink-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <IconButton size="xs" label="Edit" onClick={() => setEditingItemId(item.id)}><Pencil size={12} /></IconButton>
                      <IconButton size="xs" variant="dangerGhost" label="Delete" onClick={() => setDeletingItemId(item.id)}><Trash2 size={12} /></IconButton>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Sources Tab (grouped by day, expandable) ---

function SourcesView() {
  const { items, resources } = useMemoryStore();
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const toggleExpand = (id: string) => setExpanded(prev => { const n = new Set(prev); if (n.has(id)) n.delete(id); else n.add(id); return n; });

  const resourceItems = useMemo(() => {
    const map: Record<string, MemoryItem[]> = {};
    for (const item of items) { if (item.resource_id) { if (!map[item.resource_id]) map[item.resource_id] = []; map[item.resource_id].push(item); } }
    return map;
  }, [items]);

  const grouped = useMemo(() => {
    const groups: { date: string; resources: Resource[] }[] = [];
    const sorted = [...resources].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    let currentDate = '';
    for (const res of sorted) {
      const dateKey = res.created_at?.substring(0, 10) || 'unknown';
      if (dateKey !== currentDate) { currentDate = dateKey; groups.push({ date: dateKey, resources: [] }); }
      groups[groups.length - 1].resources.push(res);
    }
    return groups;
  }, [resources]);

  return (
    <div className="p-3 space-y-4">
      {grouped.map(group => (
        <div key={group.date}>
          <div className="flex items-center gap-2 mb-2 px-1">
            <Clock size={12} className="text-info" />
            <span className="text-xs text-info font-medium">{formatDateGroup(group.date)}</span>
            <span className="text-2xs text-text-faint">{group.resources.length} sources</span>
          </div>
          <div className="space-y-2 ml-3">
            {group.resources.map(res => {
              const resItems = resourceItems[res.id] || [];
              const isExpanded = expanded.has(res.id);
              return (
                <div key={res.id} className="border border-border-subtle rounded-lg overflow-hidden">
                  <Button variant="subtle" size="md" fullWidth onClick={() => toggleExpand(res.id)} className="justify-start rounded-none text-left">
                    {isExpanded ? <ChevronDown size={13} className="text-text-faint" /> : <ChevronRight size={13} className="text-text-faint" />}
                    <FileText size={13} className="text-text-dim" />
                    <span className="text-xs font-medium flex-1 truncate">{formatPath(res.url)}</span>
                    <Badge tone="info">{res.modality}</Badge>
                    <span className="text-xs text-text-faint">{resItems.length} items</span>
                  </Button>
                  {isExpanded && resItems.length > 0 && (
                    <div className="border-t border-border-subtle divide-y divide-border-subtle">
                      {resItems.map(item => (
                        <div key={item.id} className="flex items-start gap-2 px-3 py-2">
                          <TypeBadge type={item.memory_type} className="mt-0.5" />
                          <span className="text-xs text-text-muted">{item.summary}</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {isExpanded && resItems.length === 0 && (
                    <div className="border-t border-border-subtle px-3 py-2 text-xs text-text-faint">No items extracted from this source</div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}
      {resources.length === 0 && <div className="text-center text-text-faint text-sm py-12">No sources found</div>}
    </div>
  );
}

// --- Audit Log Tab ---

/** Audit-action identity colours. Same contract as `TYPE_COLORS` above. */
const ACTION_COLORS: Record<string, string> = {
  item_created: 'var(--theme-hue-green)',
  item_updated: 'var(--theme-accent)',
  item_deleted: 'var(--theme-hue-red)',
  category_created: 'var(--theme-hue-purple)',
  category_updated: 'var(--theme-hue-purple)',
  conversation_indexed: 'var(--theme-hue-amber)',
  file_indexed: 'var(--theme-hue-blue)',
};

function LogView() {
  const { auditLogs, auditLoading, auditFilter, loadAuditLogs, setAuditFilter } = useMemoryStore();
  useEffect(() => { loadAuditLogs(); }, []);

  if (auditLoading) return <div className="text-center text-text-faint text-sm py-12">Loading audit log...</div>;

  return (
    <div className="p-3 space-y-1">
      <div className="flex gap-2 mb-3">
        <Select
          fieldSize="sm"
          value={auditFilter.action}
          onChange={e => setAuditFilter({ action: e.target.value })}
          emptyLabel="All actions"
          options={Object.keys(ACTION_COLORS).map(a => ({ value: a, label: a.replace(/_/g, ' ') }))}
        />
        <Select
          fieldSize="sm"
          value={auditFilter.target_type}
          onChange={e => setAuditFilter({ target_type: e.target.value })}
          emptyLabel="All types"
          options={[
            { value: 'item', label: 'item' },
            { value: 'category', label: 'category' },
            { value: 'resource', label: 'resource' },
          ]}
        />
      </div>

      {auditLogs.map(entry => {
        const color = ACTION_COLORS[entry.action] || 'var(--theme-text-muted)';
        return (
          <div key={entry.id} className="flex items-start gap-2 px-3 py-2 rounded hover:bg-surface-raised transition-colors border border-border-subtle">
            <Badge className="mt-0.5" style={{ backgroundColor: tint(color), color }}>
              {entry.action.replace(/_/g, ' ')}
            </Badge>
            <div className="flex-1 min-w-0">
              <div className="text-xs text-text-secondary">
                {entry.target_type}{entry.target_id ? `: ${entry.target_id.length > 20 ? entry.target_id.substring(0, 20) + '...' : entry.target_id}` : ''}
              </div>
              {entry.source && <span className="text-2xs text-text-dim">via {entry.source}</span>}
            </div>
            <span className="text-2xs text-text-faint shrink-0">{new Date(entry.timestamp).toLocaleString()}</span>
          </div>
        );
      })}

      {auditLogs.length === 0 && <div className="text-center text-text-faint text-sm py-12">No audit log entries</div>}
    </div>
  );
}

// --- Sidebar ---

function Sidebar({ inDrawer = false, onSelect }: {
  inDrawer?: boolean;
  /**
   * Fired whenever a tap changes which facts are listed. Drawer mode closes
   * on it, so the newly filtered list is actually revealed; creating a
   * category happens inside this pane and deliberately does not fire it.
   */
  onSelect?: () => void;
}) {
  const { items, categories, categoryItems, selectedCategory, setSelectedCategory } = useMemoryStore();
  const [showCreateCat, setShowCreateCat] = useState(false);

  const stats = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const item of items) counts[item.memory_type] = (counts[item.memory_type] || 0) + 1;
    return counts;
  }, [items]);

  const catCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const [catId, ids] of Object.entries(categoryItems)) counts[catId] = ids.length;
    return counts;
  }, [categoryItems]);

  return (
    <div className={inDrawer
      // The drawer supplies the width and the edge; the fixed column and its
      // own border would fight them.
      ? 'w-full flex-1 min-h-0 flex flex-col overflow-hidden'
      : 'w-52 shrink-0 border-r border-border-subtle flex flex-col overflow-hidden'}>
      <div className="p-3 border-b border-border-subtle">
        <div className="text-xs text-text-dim uppercase tracking-wider mb-2">Types</div>
        <div className="space-y-1">
          {Object.entries(TYPE_COLORS).map(([type, color]) => (
            <div key={type} className="flex items-center gap-2">
              {/* `fill` opts the glyph into Click UI's recolour rule and
                  `color` supplies the value, so the dot is filled with the
                  type's identity colour. */}
              <Circle size={8} fill={color} color={color} />
              <span className="text-xs text-text-muted flex-1">{type}</span>
              <span className="text-xs text-text-dim">{stats[type] || 0}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-3">
        <div className="text-xs text-text-dim uppercase tracking-wider mb-2">Categories</div>
        {selectedCategory && (
          <Button variant="accent" size="xs" fullWidth onClick={() => { setSelectedCategory(null); onSelect?.(); }} className="justify-start mb-1">
            <X size={10} /> Clear filter
          </Button>
        )}
        <div className="space-y-0.5">
          {categories.map(cat => {
            const isActive = selectedCategory === cat.id;
            return (
              <Button
                key={cat.id}
                variant="subtle"
                size="xs"
                fullWidth
                active={isActive}
                onClick={() => { setSelectedCategory(isActive ? null : cat.id); onSelect?.(); }}
                className="justify-start px-2 py-1.5 gap-2"
              >
                <span className="flex-1 truncate text-left">{cat.name.replace(/_/g, ' ')}</span>
                <span className="text-2xs text-text-dim">{catCounts[cat.id] || 0}</span>
              </Button>
            );
          })}
        </div>
      </div>
      {showCreateCat ? (
        <CreateCategoryForm onClose={() => setShowCreateCat(false)} />
      ) : (
        <div className="p-3 border-t border-border-subtle">
          <Button variant="ghost" fullWidth onClick={() => setShowCreateCat(true)} className="rounded border border-dashed border-border-subtle hover:border-border">
            <Plus size={12} /> Category
          </Button>
        </div>
      )}
    </div>
  );
}

// --- Main Page ---

export function MemuPage() {
  const { loading, available, items, categories, resources, activeTab, searchQuery,
    load, setActiveTab, setSearchQuery } = useMemoryStore();

  useEffect(() => { load(); }, [load]);

  // Types/categories collapse into a left drawer on a phone — three panes
  // sharing 412px left every one of them unreadable.
  //
  // Declared above the loading/unavailable early returns: hooks after a
  // conditional return change the hook order between renders, which is what
  // the rules of hooks forbid (React throws once `loading` flips to false).
  const isMobile = useIsMobile();
  const [paneOpen, setPaneOpen] = useState(false);

  // Choosing a category shuts the drawer, driven by `Sidebar`'s `onSelect`.
  // A watcher over `selectedCategory` would miss re-tapping the category that
  // is already active — which clears the filter, so the list underneath does
  // change — and would leave the drawer covering the result either way.
  //
  // All that is left here is retiring the drawer when the layout leaves the
  // phone breakpoint, so a later resize back down doesn't arrive with an
  // overlay already open. Adjusted during render rather than in an effect:
  // an effect paints the stale state for a frame first.
  const [lastIsMobile, setLastIsMobile] = useState(isMobile);
  if (lastIsMobile !== isMobile) {
    setLastIsMobile(isMobile);
    setPaneOpen(false);
  }

  if (loading) return <div className="flex-1 flex items-center justify-center text-text-faint">Loading...</div>;

  if (!available) {
    return (
      <div className="flex-1 flex items-center justify-center text-text-faint">
        <div className="text-center">
          <Database size={32} className="mx-auto mb-3 text-text-faint" />
          <div>memU not available</div>
          <div className="text-xs text-text-faint mt-1">Semantic memory service is not initialized</div>
        </div>
      </div>
    );
  }

  const showSearch = activeTab === 'facts' || activeTab === 'timeline';

  return (
    <div className="h-full flex flex-col">
      <PageHeader
        leading={isMobile
          ? <PaneToggle open={paneOpen} onToggle={() => setPaneOpen(o => !o)} label="types and categories" />
          : undefined}
        title="Semantic Memory"
        filters={TABS.map(tab => (
          <Button key={tab.key} variant="pill" active={activeTab === tab.key} onClick={() => setActiveTab(tab.key)}>
            {tab.key === 'log' && <History size={11} />}{tab.label}
          </Button>
        ))}
        actions={
          // The counts are context, not a control: below `lg` the tabs and
          // the pane toggle are the better use of the row.
          <span className="hidden lg:inline text-xs text-text-faint whitespace-nowrap">
            {items.length} items · {categories.length} categories · {resources.length} sources
          </span>
        }
      />

      <div className="flex-1 flex overflow-hidden">
        {isMobile ? (
          <Drawer open={paneOpen} onClose={() => setPaneOpen(false)} side="left" label="Types and categories">
            <Sidebar inDrawer onSelect={() => setPaneOpen(false)} />
          </Drawer>
        ) : (
          <Sidebar />
        )}
        <div className="flex-1 flex flex-col overflow-hidden">
          {showSearch && (
            <div className="px-3 py-2 border-b border-border-subtle shrink-0">
              <div className="relative">
                <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-faint" />
                <TextField
                  fieldSize="sm"
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  placeholder={`Search ${activeTab === 'facts' ? 'facts' : 'events'}...`}
                  className="pl-8 pr-8 py-1.5"
                />
                {searchQuery && (
                  <IconButton
                    size="xs"
                    label="Clear search"
                    onClick={() => setSearchQuery('')}
                    className="absolute right-1 top-1/2 -translate-y-1/2"
                  >
                    <X size={12} />
                  </IconButton>
                )}
              </div>
            </div>
          )}
          <div className="flex-1 overflow-y-auto">
            {activeTab === 'facts' && <FactsView />}
            {activeTab === 'timeline' && <TimelineView />}
            {activeTab === 'sources' && <SourcesView />}
            {activeTab === 'log' && <LogView />}
          </div>
        </div>
      </div>
    </div>
  );
}
