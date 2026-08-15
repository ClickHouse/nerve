import { useEffect, useState } from 'react';
import {
  useTaskStatusStore,
  statusBadgeStyle,
  type TaskStatusDef,
} from '../../stores/taskStatusStore';
import { Badge, Button, IconButton, Modal, TextArea, TextField } from '../ui';
import { X, Plus, Trash2, Pencil, Check, Lock } from '../ui/icons';

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
          {/* Stays a bare `<input>`: it is invisible, stretched over the
              swatch, and the swatch itself is the control. `TextField`'s field
              chrome would be painted onto something with `opacity-0`. */}
          <input
            type="color"
            value={color}
            onChange={e => setColor(e.target.value)}
            className="absolute inset-0 opacity-0 cursor-pointer"
          />
        </label>
        <TextField
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="name (e.g. in_review)"
          autoFocus
          fullWidth={false}
          className="flex-1"
        />
        <TextField
          value={label}
          onChange={e => setLabel(e.target.value)}
          placeholder="Label (optional)"
          fullWidth={false}
          className="flex-1"
        />
      </div>
      <TextArea
        value={description}
        onChange={e => setDescription(e.target.value)}
        rows={2}
        placeholder="Description (optional)"
      />
      <div className="flex justify-end gap-2">
        <Button variant="ghost" onClick={resetAdd}>
          Cancel
        </Button>
        <Button
          variant="primary"
          onClick={handleCreate}
          disabled={busy || !name.trim()}
        >
          <Plus size={14} /> Add status
        </Button>
      </div>
    </div>
  ) : (
    // `accent` rather than a ghost carrying an accent class: a call site cannot
    // recolour a variant (Tailwind orders colour utilities alphabetically), and
    // this control is an accent-text affordance, not a selected one.
    <Button
      variant="accent"
      onClick={() => { setColor(randomColor()); setShowAdd(true); }}
    >
      <Plus size={14} /> New status
    </Button>
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
          <div className="px-3 py-2 mb-1 text-xs text-error bg-error-bg border border-error-border rounded-lg">
            {error}
          </div>
        )}

          {statuses.map(s => (
            <div key={s.name} className="border border-border-subtle rounded-lg p-3">
              <div className="flex items-center gap-3">
                {/* Color swatch — click to recolor. Bare `<input>` for the
                    same reason as the one in the add form above. */}
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
                    <TextField
                      fieldSize="sm"
                      value={editLabel}
                      onChange={e => setEditLabel(e.target.value)}
                      placeholder="Label"
                    />
                  ) : (
                    <div className="flex items-center gap-2">
                      {/* User-configured hex, not a theme token — inline, as
                          `Badge` leaves `style` open for. */}
                      <Badge size="sm" pill outline style={statusBadgeStyle(s.color)}>
                        {s.label}
                      </Badge>
                      <code className="text-xs text-text-faint">{s.name}</code>
                      {!!s.is_system && (
                        <Badge size="xs" title="Protected — cannot be deleted">
                          <Lock size={10} />
                          protected
                        </Badge>
                      )}
                    </div>
                  )}
                  {editing !== s.name && s.description && (
                    <div className="text-xs text-text-dim mt-1">{s.description}</div>
                  )}
                </div>

                <div className="flex items-center gap-1 shrink-0">
                  {editing === s.name ? (
                    <>
                      {/* `IconButton` has no success tone and a green class on
                          the button would lose to the variant's own colour, so
                          the hue goes on the icon — the house convention for a
                          tinted control. */}
                      <IconButton
                        label="Save"
                        size="xs"
                        onClick={() => saveEdit(s)}
                        disabled={busy}
                      >
                        <Check size={14} className="text-hue-green" />
                      </IconButton>
                      <IconButton
                        label="Cancel"
                        size="xs"
                        onClick={() => setEditing(null)}
                      >
                        <X size={14} />
                      </IconButton>
                    </>
                  ) : (
                    <>
                      <IconButton
                        label="Edit label & description"
                        size="xs"
                        onClick={() => beginEdit(s)}
                      >
                        <Pencil size={14} />
                      </IconButton>
                      <IconButton
                        label={s.is_system ? 'Protected status' : 'Delete'}
                        size="xs"
                        variant="dangerGhost"
                        onClick={() => handleDelete(s)}
                        disabled={!!s.is_system}
                      >
                        <Trash2 size={14} />
                      </IconButton>
                    </>
                  )}
                </div>
              </div>

              {editing === s.name && (
                <TextArea
                  fieldSize="sm"
                  value={editDesc}
                  onChange={e => setEditDesc(e.target.value)}
                  rows={2}
                  placeholder="Description (optional)"
                  className="mt-2"
                />
              )}
            </div>
          ))}
      </div>
    </Modal>
  );
}
