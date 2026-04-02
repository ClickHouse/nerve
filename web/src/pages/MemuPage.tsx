import { useEffect, useState, useMemo } from 'react';
import {
  Database, Search, Plus, X, Pencil, Trash2, ChevronDown, ChevronRight,
  FileText, Clock, Circle, History,
} from 'lucide-react';
import { useMemoryStore, type Category, type MemoryItem, type Resource, type TabView } from '../stores/memoryStore';

const TYPE_COLORS: Record<string, string> = {
  profile: '#6366f1',
  event: '#f59e0b',
  knowledge: '#22c55e',
  behavior: '#ef4444',
  skill: '#3b82f6',
  tool: '#a855f7',
};

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
    <div className="border border-[#6366f1]/30 rounded-lg p-3 bg-[#6366f1]/5 space-y-2">
      <textarea
        value={content}
        onChange={e => setContent(e.target.value)}
        rows={3}
        className="w-full bg-surface-raised border border-border-subtle rounded px-3 py-2 text-[13px] text-[#ddd] outline-none focus:border-[#6366f1]/50 resize-none"
      />
      <div className="flex items-center gap-2">
        <select
          value={memType}
          onChange={e => setMemType(e.target.value)}
          className="bg-surface-raised border border-border-subtle rounded px-2 py-1 text-[12px] text-[#ddd] outline-none focus:border-[#6366f1]/50"
        >
          {Object.keys(TYPE_COLORS).map(t => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <div className="flex-1" />
        <button onClick={onCancel} className="px-3 py-1 text-xs text-text-muted hover:text-text-secondary cursor-pointer transition-colors">Cancel</button>
        <button onClick={handleSave} disabled={saving || !content.trim()} className="px-3 py-1 bg-[#6366f1] text-white text-xs rounded cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[#5558e6] transition-colors">
          {saving ? 'Saving...' : 'Save'}
        </button>
      </div>
      {categories.length > 0 && (
        <div className="flex flex-wrap gap-1 pt-1 border-t border-border-subtle/50">
          <span className="text-[10px] text-text-dim self-center mr-1">Categories:</span>
          {categories.map(cat => {
            const selected = selectedCatIds.has(cat.id);
            return (
              <button key={cat.id} onClick={() => toggleCat(cat.id)} className={`text-[10px] px-1.5 py-0.5 rounded cursor-pointer transition-colors ${selected ? 'bg-[#6366f1]/25 text-[#6366f1] border border-[#6366f1]/40' : 'bg-surface-raised text-text-dim border border-border-subtle hover:text-text-muted hover:border-border'}`}>
                {cat.name.replace(/_/g, ' ')}
              </button>
            );
          })}
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
    <div className="border border-red-500/30 rounded-lg p-3 bg-red-500/5">
      <div className="text-[12px] text-text-secondary mb-2">Delete this memory item?</div>
      <div className="text-[11px] text-text-muted mb-3 line-clamp-2">{item.summary}</div>
      <div className="flex items-center gap-2 justify-end">
        <button onClick={onCancel} className="px-3 py-1 text-xs text-text-muted hover:text-text-secondary cursor-pointer transition-colors">Cancel</button>
        <button onClick={handleDelete} disabled={deleting} className="px-3 py-1 bg-red-600 text-white text-xs rounded cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed hover:bg-red-700 transition-colors">
          {deleting ? 'Deleting...' : 'Delete'}
        </button>
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
      <span className="text-[10px] px-1.5 py-0.5 rounded mt-0.5 shrink-0" style={{ backgroundColor: (TYPE_COLORS[item.memory_type] || '#666') + '20', color: TYPE_COLORS[item.memory_type] || '#666' }}>
        {item.memory_type}
      </span>
      <span className="text-[12px] text-text-secondary flex-1">{item.summary}</span>
      <div className="shrink-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <button onClick={onEdit} className="p-1 rounded hover:bg-surface-raised text-text-dim hover:text-text-muted cursor-pointer transition-colors" title="Edit"><Pencil size={12} /></button>
        <button onClick={onDelete} className="p-1 rounded hover:bg-surface-raised text-text-dim hover:text-red-400 cursor-pointer transition-colors" title="Delete"><Trash2 size={12} /></button>
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
        className="px-3 py-2 bg-bg-sunken text-[11px] text-text-muted whitespace-pre-wrap border-b border-border-subtle group/summary cursor-pointer hover:bg-[#111] transition-colors"
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
      <textarea value={value} onChange={e => setValue(e.target.value)} rows={3} autoFocus className="w-full bg-surface-raised border border-border-subtle rounded px-2 py-1.5 text-[11px] text-[#ddd] outline-none focus:border-[#6366f1]/50 resize-none" />
      <div className="flex justify-end gap-2 mt-1">
        <button onClick={() => setEditingCategoryId(null)} className="px-2 py-0.5 text-[10px] text-text-muted hover:text-text-secondary cursor-pointer">Cancel</button>
        <button onClick={handleSave} disabled={saving} className="px-2 py-0.5 bg-[#6366f1] text-white text-[10px] rounded cursor-pointer disabled:opacity-40 hover:bg-[#5558e6] transition-colors">
          {saving ? 'Saving...' : 'Save'}
        </button>
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
    <div className="border border-[#6366f1]/30 rounded-lg p-3 bg-[#6366f1]/5 mx-3 mb-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[12px] font-medium">New Category</span>
        <button onClick={onClose} className="text-text-faint hover:text-text-muted cursor-pointer"><X size={12} /></button>
      </div>
      <input type="text" placeholder="Name (e.g. travel_plans)" value={name} onChange={e => setName(e.target.value)} className="w-full bg-surface-raised border border-border-subtle rounded px-2 py-1 text-[12px] text-[#ddd] mb-2 outline-none focus:border-[#6366f1]/50" />
      <input type="text" placeholder="Description" value={desc} onChange={e => setDesc(e.target.value)} className="w-full bg-surface-raised border border-border-subtle rounded px-2 py-1 text-[12px] text-[#ddd] mb-2 outline-none focus:border-[#6366f1]/50" />
      <button onClick={handleCreate} disabled={!name.trim() || creating} className="px-3 py-1 bg-[#6366f1] text-white text-[11px] rounded cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[#5558e6] transition-colors">
        {creating ? 'Creating...' : 'Create'}
      </button>
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

  const getColor = (count: number, dateStr: string) => {
    if (selectedDate === dateStr) return '#6366f1';
    if (count === 0) return '#161616';
    if (count <= 2) return '#f59e0b33';
    if (count <= 5) return '#f59e0b88';
    return '#f59e0b';
  };

  return (
    <div className="px-3 py-2 border-b border-border-subtle overflow-x-auto">
      <div className="text-[10px] text-text-faint mb-1">Memory activity — last 6 months</div>
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
          <button onClick={() => toggle(category.id)} className="w-full px-3 py-2.5 flex items-center gap-2 hover:bg-surface-raised cursor-pointer transition-colors">
            {collapsed[category.id] ? <ChevronRight size={13} className="text-text-faint" /> : <ChevronDown size={13} className="text-text-faint" />}
            <span className="font-medium text-[13px]">{category.name.replace(/_/g, ' ')}</span>
            <span className="text-[11px] text-text-faint">{catItems.length}</span>
          </button>
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
          <button onClick={() => toggle('__uncategorized')} className="w-full px-3 py-2.5 flex items-center gap-2 hover:bg-surface-raised cursor-pointer transition-colors">
            {collapsed['__uncategorized'] ? <ChevronRight size={13} className="text-text-faint" /> : <ChevronDown size={13} className="text-text-faint" />}
            <span className="font-medium text-[13px] text-text-muted">uncategorized</span>
            <span className="text-[11px] text-text-faint">{uncategorized.length}</span>
          </button>
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
        <div className="text-center text-text-faint text-[13px] py-12">{searchQuery ? 'No facts match your search' : 'No facts found'}</div>
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
        <div className="px-3 py-1.5 bg-[#f59e0b]/10 border-b border-border-subtle flex items-center gap-2">
          <span className="text-[11px] text-[#f59e0b]">Showing: {filterDate}</span>
          <button onClick={() => setFilterDate(null)} className="text-[#f59e0b] hover:text-[#fbbf24] cursor-pointer"><X size={12} /></button>
        </div>
      )}
      <div className="flex-1 overflow-y-auto p-3">
        {grouped.length === 0 && (
          <div className="text-center text-text-faint text-[13px] py-12">{searchQuery ? 'No events match your search' : 'No events found'}</div>
        )}
        {grouped.map(group => (
          <div key={group.dateKey} id={`timeline-date-${group.dateKey}`} className="mb-4">
            <div className="flex items-center gap-2 mb-2 px-2">
              <Clock size={12} className="text-[#f59e0b]" />
              <span className="text-[12px] text-[#f59e0b] font-medium">{formatDateGroup(group.date)}</span>
            </div>
            <div className="border-l-2 border-border-subtle ml-3 pl-4 space-y-1">
              {group.items.map(item => {
                if (editingItemId === item.id) return <EditForm key={item.id} item={item} onSave={(data) => handleSave(item.id, data)} onCancel={() => setEditingItemId(null)} />;
                if (deletingItemId === item.id) return <DeleteConfirm key={item.id} item={item} onConfirm={() => deleteItem(item.id)} onCancel={() => setDeletingItemId(null)} />;
                const catIds = itemCategoryMap[item.id] || [];
                return (
                  <div key={item.id} className="group flex items-start gap-2 px-2 py-1.5 rounded hover:bg-surface-raised transition-colors">
                    <div className="w-2 h-2 rounded-full bg-[#f59e0b] mt-1.5 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="text-[12px] text-text-secondary">{item.summary}</div>
                      {catIds.length > 0 && (
                        <div className="flex gap-1 mt-1">
                          {catIds.map(cid => { const cat = categoryMap.get(cid); return cat ? <span key={cid} className="text-[10px] px-1.5 py-0.5 rounded bg-border-subtle text-text-muted">{cat.name}</span> : null; })}
                        </div>
                      )}
                    </div>
                    <div className="shrink-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button onClick={() => setEditingItemId(item.id)} className="p-1 rounded hover:bg-surface-raised text-text-dim hover:text-text-muted cursor-pointer transition-colors" title="Edit"><Pencil size={12} /></button>
                      <button onClick={() => setDeletingItemId(item.id)} className="p-1 rounded hover:bg-surface-raised text-text-dim hover:text-red-400 cursor-pointer transition-colors" title="Delete"><Trash2 size={12} /></button>
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
            <Clock size={12} className="text-[#3b82f6]" />
            <span className="text-[12px] text-[#3b82f6] font-medium">{formatDateGroup(group.date)}</span>
            <span className="text-[10px] text-text-faint">{group.resources.length} sources</span>
          </div>
          <div className="space-y-2 ml-3">
            {group.resources.map(res => {
              const resItems = resourceItems[res.id] || [];
              const isExpanded = expanded.has(res.id);
              return (
                <div key={res.id} className="border border-border-subtle rounded-lg overflow-hidden">
                  <button onClick={() => toggleExpand(res.id)} className="w-full p-3 flex items-center gap-2 hover:bg-surface-raised cursor-pointer transition-colors text-left">
                    {isExpanded ? <ChevronDown size={13} className="text-text-faint" /> : <ChevronRight size={13} className="text-text-faint" />}
                    <FileText size={13} className="text-text-dim" />
                    <span className="text-[12px] font-medium flex-1 truncate">{formatPath(res.url)}</span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded" style={{ backgroundColor: '#3b82f620', color: '#3b82f6' }}>{res.modality}</span>
                    <span className="text-[11px] text-text-faint">{resItems.length} items</span>
                  </button>
                  {isExpanded && resItems.length > 0 && (
                    <div className="border-t border-border-subtle divide-y divide-border-subtle">
                      {resItems.map(item => (
                        <div key={item.id} className="flex items-start gap-2 px-3 py-2">
                          <span className="text-[10px] px-1.5 py-0.5 rounded mt-0.5 shrink-0" style={{ backgroundColor: (TYPE_COLORS[item.memory_type] || '#666') + '20', color: TYPE_COLORS[item.memory_type] || '#666' }}>{item.memory_type}</span>
                          <span className="text-[11px] text-text-muted">{item.summary}</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {isExpanded && resItems.length === 0 && (
                    <div className="border-t border-border-subtle px-3 py-2 text-[11px] text-text-faint">No items extracted from this source</div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}
      {resources.length === 0 && <div className="text-center text-text-faint text-[13px] py-12">No sources found</div>}
    </div>
  );
}

// --- Audit Log Tab ---

const ACTION_COLORS: Record<string, string> = {
  item_created: '#22c55e',
  item_updated: '#6366f1',
  item_deleted: '#ef4444',
  category_created: '#a855f7',
  category_updated: '#a855f7',
  conversation_indexed: '#f59e0b',
  file_indexed: '#3b82f6',
};

function LogView() {
  const { auditLogs, auditLoading, auditFilter, loadAuditLogs, setAuditFilter } = useMemoryStore();
  useEffect(() => { loadAuditLogs(); }, []);

  if (auditLoading) return <div className="text-center text-text-faint text-[13px] py-12">Loading audit log...</div>;

  return (
    <div className="p-3 space-y-1">
      <div className="flex gap-2 mb-3">
        <select value={auditFilter.action} onChange={e => setAuditFilter({ action: e.target.value })} className="bg-surface-raised border border-border-subtle rounded px-2 py-1 text-[12px] text-[#ddd] outline-none">
          <option value="">All actions</option>
          {Object.keys(ACTION_COLORS).map(a => <option key={a} value={a}>{a.replace(/_/g, ' ')}</option>)}
        </select>
        <select value={auditFilter.target_type} onChange={e => setAuditFilter({ target_type: e.target.value })} className="bg-surface-raised border border-border-subtle rounded px-2 py-1 text-[12px] text-[#ddd] outline-none">
          <option value="">All types</option>
          <option value="item">item</option>
          <option value="category">category</option>
          <option value="resource">resource</option>
        </select>
      </div>

      {auditLogs.map(entry => (
        <div key={entry.id} className="flex items-start gap-2 px-3 py-2 rounded hover:bg-surface-raised transition-colors border border-border-subtle">
          <span className="text-[10px] px-1.5 py-0.5 rounded mt-0.5 shrink-0" style={{ backgroundColor: (ACTION_COLORS[entry.action] || '#666') + '20', color: ACTION_COLORS[entry.action] || '#666' }}>
            {entry.action.replace(/_/g, ' ')}
          </span>
          <div className="flex-1 min-w-0">
            <div className="text-[12px] text-text-secondary">
              {entry.target_type}{entry.target_id ? `: ${entry.target_id.length > 20 ? entry.target_id.substring(0, 20) + '...' : entry.target_id}` : ''}
            </div>
            {entry.source && <span className="text-[10px] text-text-dim">via {entry.source}</span>}
          </div>
          <span className="text-[10px] text-text-faint shrink-0">{new Date(entry.timestamp).toLocaleString()}</span>
        </div>
      ))}

      {auditLogs.length === 0 && <div className="text-center text-text-faint text-[13px] py-12">No audit log entries</div>}
    </div>
  );
}

// --- Sidebar ---

function Sidebar() {
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
    <div className="w-52 shrink-0 border-r border-border-subtle flex flex-col overflow-hidden">
      <div className="p-3 border-b border-border-subtle">
        <div className="text-[11px] text-text-dim uppercase tracking-wider mb-2">Types</div>
        <div className="space-y-1">
          {Object.entries(TYPE_COLORS).map(([type, color]) => (
            <div key={type} className="flex items-center gap-2">
              <Circle size={8} fill={color} color={color} />
              <span className="text-[12px] text-text-muted flex-1">{type}</span>
              <span className="text-[11px] text-text-dim">{stats[type] || 0}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-3">
        <div className="text-[11px] text-text-dim uppercase tracking-wider mb-2">Categories</div>
        {selectedCategory && (
          <button onClick={() => setSelectedCategory(null)} className="w-full text-left px-2 py-1 mb-1 text-[11px] text-[#6366f1] hover:bg-surface-raised rounded cursor-pointer transition-colors flex items-center gap-1">
            <X size={10} /> Clear filter
          </button>
        )}
        <div className="space-y-0.5">
          {categories.map(cat => {
            const isActive = selectedCategory === cat.id;
            return (
              <button key={cat.id} onClick={() => setSelectedCategory(isActive ? null : cat.id)} className={`w-full text-left px-2 py-1.5 rounded text-[12px] cursor-pointer transition-colors flex items-center gap-2 ${isActive ? 'bg-[#6366f1]/15 text-[#6366f1]' : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'}`}>
                <span className="flex-1 truncate">{cat.name.replace(/_/g, ' ')}</span>
                <span className="text-[10px] text-text-dim">{catCounts[cat.id] || 0}</span>
              </button>
            );
          })}
        </div>
      </div>
      {showCreateCat ? (
        <CreateCategoryForm onClose={() => setShowCreateCat(false)} />
      ) : (
        <div className="p-3 border-t border-border-subtle">
          <button onClick={() => setShowCreateCat(true)} className="w-full flex items-center justify-center gap-1.5 px-3 py-1.5 rounded border border-dashed border-border-subtle text-[12px] text-text-dim hover:text-text-muted hover:border-border cursor-pointer transition-colors">
            <Plus size={12} /> Category
          </button>
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
      <div className="border-b border-border-subtle px-5 py-2.5 flex items-center justify-between bg-bg shrink-0">
        <div className="flex items-center gap-3">
          <span className="font-medium text-[15px]">Semantic Memory</span>
          <span className="text-xs text-text-faint">{items.length} items · {categories.length} categories · {resources.length} sources</span>
        </div>
        <div className="flex gap-1 items-center">
          {TABS.map(tab => (
            <button key={tab.key} onClick={() => setActiveTab(tab.key)} className={`px-3 py-1 rounded text-xs cursor-pointer transition-colors ${activeTab === tab.key ? 'bg-[#6366f1]/20 text-[#6366f1]' : 'text-text-dim hover:text-text-muted'}`}>
              {tab.key === 'log' && <History size={11} className="inline mr-1 -mt-0.5" />}{tab.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 flex overflow-hidden">
        <Sidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          {showSearch && (
            <div className="px-3 py-2 border-b border-border-subtle shrink-0">
              <div className="relative">
                <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-faint" />
                <input type="text" value={searchQuery} onChange={e => setSearchQuery(e.target.value)} placeholder={`Search ${activeTab === 'facts' ? 'facts' : 'events'}...`} className="w-full bg-surface-raised border border-border-subtle rounded pl-8 pr-3 py-1.5 text-[12px] text-[#ddd] outline-none focus:border-border-subtle placeholder:text-text-faint" />
                {searchQuery && (
                  <button onClick={() => setSearchQuery('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-text-faint hover:text-text-muted cursor-pointer"><X size={12} /></button>
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
