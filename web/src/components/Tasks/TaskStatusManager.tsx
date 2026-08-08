import { useEffect, useState } from 'react';
import { X, Plus, Trash2, Pencil, Check, Lock } from 'lucide-react';
import {
  useTaskStatusStore,
  statusBadgeStyle,
  type TaskStatusDef,
} from '../../stores/taskStatusStore';
import { Modal } from '../ui/Modal';

const PALETTE = [
  '#ef4444', '#f97316', '#f59e0b', '#eab308', '#84cc16', '#22c55e',
  '#10b981', '#14b8a6', '#06b6d4', '#3b82f6', '#6366f1', '#8b5cf6',
  '#a855f7', '#d946ef', '#ec4899', '#f43f5e',
];

const randomColor = () => PALETTE[Math.floor(Math.random() * PALETTE.length)];
const NAME_RE = /^[a-z0-9][a-z0-9_]*$/;

/** Pull the server's `detail` out of a thrown request Error ("409: {json}"). */
function parseErr(e: unknown): string {
  const msg = String((e as Error)?.message ?? e);
  const m = msg.match(/^\d+:\s*([\s\S]*)$/);
  if (m) {
    try {
      const j = JSON.parse(m[1]);
      if (j?.detail) return j.detail;
    } catch { /* not JSON */ }
    return m[1];
  }
  return msg;
}

export function TaskStatusManager({ onClose }: { onClose: () => void }) {
  const { statuses, load, create, update, remove } = useTaskStatusStore();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  // Inline edit state (existing rows)
  const [editing, setEditing] = useState<string | null>(null);
  const [editLabel, setEditLabel] = useState('');
  const [editDesc, setEditDesc] = useState('');

  // Add form
  const [showAdd, setShowAdd] = useState(false);
  const [name, setName] = useState('');
  const [label, setLabel] = useState('');
  const [color, setColor] = useState(randomColor());
  const [description, setDescription] = useState('');

  useEffect(() => { load(true); }, []);

  const beginEdit = (s: TaskStatusDef) => {
    setEditing(s.name);
    setEditLabel(s.label);
    setEditDesc(s.description);
    setError('');
  };

  const saveEdit = async (s: TaskStatusDef) => {
    setBusy(true); setError('');
    try {
      await update(s.name, { label: editLabel.trim() || s.name, description: editDesc.trim() });
      setEditing(null);
    } catch (e) { setError(parseErr(e)); }
    finally { setBusy(false); }
  };

  const changeColor = async (s: TaskStatusDef, c: string) => {
    setError('');
    try { await update(s.name, { color: c }); }
    catch (e) { setError(parseErr(e)); }
  };

  const handleDelete = async (s: TaskStatusDef) => {
    setError('');
    try { await remove(s.name); }
    catch (e) { setError(parseErr(e)); }
  };

  const resetAdd = () => {
    setName(''); setLabel(''); setColor(randomColor());
    setDescription(''); setShowAdd(false); setError('');
  };

  const handleCreate = async () => {
    const n = name.trim().toLowerCase();
    if (!NAME_RE.test(n)) {
      setError('Name must be lowercase letters, digits, and underscores (e.g. in_review).');
      return;
    }
    setBusy(true); setError('');
    try {
      await create({
        name: n,
        label: label.trim() || undefined,
        color,
        description: description.trim() || undefined,
      });
      resetAdd();
    } catch (e) { setError(parseErr(e)); }
    finally { setBusy(false); }
  };

  // The add form is pinned below the scrolling status list, so it stays
  // reachable however many statuses are configured. It's a form rather
  // than a button row, hence the footer layout override.
  const addStatusFooter = showAdd ? (
    <div className="space-y-3">
      <div className="flex gap-2">
        <label
          className="relative w-9 h-9 rounded-lg border border-border shrink-0 cursor-pointer"
          style={{ backgroundColor: color }}
          title="Pick a color"
        >
          <input
            type="color"
            value={color}
            onChange={e => setColor(e.target.value)}
            className="absolute inset-0 opacity-0 cursor-pointer"
          />
        </label>
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="name (e.g. in_review)"
          autoFocus
          className="flex-1 px-3 py-2 bg-surface border border-border-subtle rounded-lg text-[13px] text-text outline-none focus:border-accent/50"
        />
        <input
          value={label}
          onChange={e => setLabel(e.target.value)}
          placeholder="Label (optional)"
          className="flex-1 px-3 py-2 bg-surface border border-border-subtle rounded-lg text-[13px] text-text outline-none focus:border-accent/50"
        />
      </div>
      <textarea
        value={description}
        onChange={e => setDescription(e.target.value)}
        rows={2}
        placeholder="Description (optional)"
        className="w-full px-3 py-2 bg-surface border border-border-subtle rounded-lg text-[13px] text-text outline-none focus:border-accent/50 resize-none"
      />
      <div className="flex justify-end gap-2">
        <button
          onClick={resetAdd}
          className="px-3 py-1.5 text-[13px] text-text-muted hover:text-text cursor-pointer"
        >
          Cancel
        </button>
        <button
          onClick={handleCreate}
          disabled={busy || !name.trim()}
          className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] bg-accent hover:bg-accent-hover text-white rounded-lg cursor-pointer disabled:opacity-50"
        >
          <Plus size={14} /> Add status
        </button>
      </div>
    </div>
  ) : (
    <button
      onClick={() => { setColor(randomColor()); setShowAdd(true); }}
      className="flex items-center gap-1.5 px-3 py-1.5 text-[13px] text-accent hover:bg-accent/10 rounded-lg cursor-pointer"
    >
      <Plus size={14} /> New status
    </button>
  );

  return (
    <Modal
      open
      onClose={onClose}
      title="Task Statuses"
      size="xl"
      footer={addStatusFooter}
      footerClassName="p-4"
    >
      <div className="p-5 space-y-2">
        {error && (
          <div className="px-3 py-2 mb-1 text-[12px] text-hue-red bg-red-400/10 border border-red-400/20 rounded-lg">
            {error}
          </div>
        )}

          {statuses.map(s => (
            <div key={s.name} className="border border-border-subtle rounded-lg p-3">
              <div className="flex items-center gap-3">
                {/* Color swatch — click to recolor */}
                <label
                  className="relative w-6 h-6 rounded-full border border-border shrink-0 cursor-pointer"
                  style={{ backgroundColor: s.color }}
                  title="Change color"
                >
                  <input
                    type="color"
                    value={s.color}
                    onChange={e => changeColor(s, e.target.value)}
                    className="absolute inset-0 opacity-0 cursor-pointer"
                  />
                </label>

                <div className="min-w-0 flex-1">
                  {editing === s.name ? (
                    <input
                      value={editLabel}
                      onChange={e => setEditLabel(e.target.value)}
                      className="w-full px-2 py-1 text-[13px] bg-surface border border-border-subtle rounded text-text outline-none focus:border-accent/50"
                      placeholder="Label"
                    />
                  ) : (
                    <div className="flex items-center gap-2">
                      <span style={statusBadgeStyle(s.color)} className="px-2 py-0.5 rounded-full border text-[12px]">
                        {s.label}
                      </span>
                      <code className="text-[11px] text-text-faint">{s.name}</code>
                      {!!s.is_system && (
                        <span className="flex items-center gap-1 text-[10px] text-text-faint" title="Protected — cannot be deleted">
                          <Lock size={10} /> protected
                        </span>
                      )}
                    </div>
                  )}
                  {editing !== s.name && s.description && (
                    <div className="text-[12px] text-text-dim mt-1">{s.description}</div>
                  )}
                </div>

                <div className="flex items-center gap-1 shrink-0">
                  {editing === s.name ? (
                    <>
                      <button
                        onClick={() => saveEdit(s)}
                        disabled={busy}
                        className="p-1.5 text-hue-green hover:bg-surface-hover rounded cursor-pointer disabled:opacity-50"
                        title="Save"
                      >
                        <Check size={14} />
                      </button>
                      <button
                        onClick={() => setEditing(null)}
                        className="p-1.5 text-text-faint hover:bg-surface-hover rounded cursor-pointer"
                        title="Cancel"
                      >
                        <X size={14} />
                      </button>
                    </>
                  ) : (
                    <>
                      <button
                        onClick={() => beginEdit(s)}
                        className="p-1.5 text-text-faint hover:text-text-muted hover:bg-surface-hover rounded cursor-pointer"
                        title="Edit label & description"
                      >
                        <Pencil size={14} />
                      </button>
                      <button
                        onClick={() => handleDelete(s)}
                        disabled={!!s.is_system}
                        className="p-1.5 text-text-faint hover:text-hue-red hover:bg-surface-hover rounded cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed"
                        title={s.is_system ? 'Protected status' : 'Delete'}
                      >
                        <Trash2 size={14} />
                      </button>
                    </>
                  )}
                </div>
              </div>

              {editing === s.name && (
                <textarea
                  value={editDesc}
                  onChange={e => setEditDesc(e.target.value)}
                  rows={2}
                  placeholder="Description (optional)"
                  className="mt-2 w-full px-2 py-1 text-[12px] bg-surface border border-border-subtle rounded text-text outline-none focus:border-accent/50 resize-none"
                />
              )}
            </div>
          ))}
      </div>
    </Modal>
  );
}
