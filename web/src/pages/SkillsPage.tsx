import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Plus, RefreshCw, Zap, CheckCircle, XCircle, Clock } from '../components/ui/icons';
import { useSkillsStore, type Skill } from '../stores/skillsStore';
import { Badge, Button, Modal, TextArea, TextField } from '../components/ui';

function SkillCard({ skill }: { skill: Skill }) {
  const navigate = useNavigate();
  const { toggleSkill } = useSkillsStore();
  const successRate = skill.total_invocations > 0
    ? Math.round((skill.success_count / skill.total_invocations) * 100)
    : null;

  return (
    <div
      className="bg-surface-raised border border-border rounded-lg p-4 hover:border-border cursor-pointer transition-colors"
      onClick={() => navigate(`/skills/${encodeURIComponent(skill.id)}`)}
    >
      <div className="flex items-start justify-between mb-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium text-text truncate">{skill.name}</h3>
            <Badge>v{skill.version}</Badge>
          </div>
          <p className="text-xs text-text-muted mt-1 line-clamp-2">{skill.description}</p>
        </div>
        {/* A switch, not an icon button: the track *is* the control, so there
            is no glyph for `IconButton` to take as children.

            `bg-success-solid`, not `bg-success`: the white knob has to stay
            legible on the track, and the bare token is a feedback *foreground*
            — pale mint in dark mode, where the knob would vanish. */}
        <button
          type="button"
          onClick={(e) => { e.stopPropagation(); toggleSkill(skill.id, !skill.enabled); }}
          className={`ml-3 w-8 h-4 rounded-full transition-colors flex items-center shrink-0 cursor-pointer ${
            skill.enabled ? 'bg-success-solid justify-end' : 'bg-border-subtle justify-start'
          }`}
          title={skill.enabled ? 'Enabled' : 'Disabled'}
          aria-pressed={skill.enabled}
        >
          <div className="w-3 h-3 rounded-full bg-white mx-0.5" />
        </button>
      </div>

      <div className="flex items-center gap-4 mt-3 text-2xs text-text-dim">
        <div className="flex items-center gap-1">
          <Zap size={10} />
          <span>{skill.total_invocations} uses</span>
        </div>
        {successRate !== null && (
          <div className="flex items-center gap-1">
            {successRate >= 90 ? <CheckCircle size={10} className="text-hue-emerald" /> : <XCircle size={10} className="text-hue-amber" />}
            <span>{successRate}%</span>
          </div>
        )}
        {skill.last_used && (
          <div className="flex items-center gap-1">
            <Clock size={10} />
            <span>{new Date(skill.last_used).toLocaleDateString()}</span>
          </div>
        )}
        {!skill.enabled && (
          <span className="text-hue-amber/70">disabled</span>
        )}
      </div>
    </div>
  );
}

function CreateSkillDialog() {
  const { createSkill, actionLoading, setShowCreateDialog } = useSkillsStore();
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [content, setContent] = useState('');

  const handleCreate = async () => {
    if (!name.trim() || !description.trim()) return;
    const id = await createSkill({ name: name.trim(), description: description.trim(), content: content.trim() });
    if (id) {
      navigate(`/skills/${encodeURIComponent(id)}`);
    }
  };

  return (
    <Modal
      open
      onClose={() => setShowCreateDialog(false)}
      title="New Skill"
      size="lg"
      // Three fields of typing behind a backdrop click is too much to lose.
      closeOnBackdrop={false}
      footer={
        <>
          <Button variant="ghost" onClick={() => setShowCreateDialog(false)}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={handleCreate}
            disabled={!name.trim() || !description.trim() || actionLoading}
          >
            {actionLoading ? 'Creating...' : 'Create Skill'}
          </Button>
        </>
      }
    >
      <div className="p-4 space-y-3">
        <div>
          <label className="text-xs text-text-muted block mb-1">Name</label>
          <TextField
            value={name} onChange={e => setName(e.target.value)}
            placeholder="e.g. code-review"
            autoFocus
          />
        </div>
        <div>
          <label className="text-xs text-text-muted block mb-1">Description</label>
          <TextArea
            value={description} onChange={e => setDescription(e.target.value)}
            resizable
            className="min-h-[60px]"
            placeholder='This skill should be used when the user asks to "review code"...'
          />
        </div>
        <div>
          <label className="text-xs text-text-muted block mb-1">Instructions (optional)</label>
          <TextArea
            value={content} onChange={e => setContent(e.target.value)}
            resizable
            className="min-h-[120px] font-mono text-xs"
            placeholder="Markdown instructions for the agent..."
          />
        </div>
      </div>
    </Modal>
  );
}

export function SkillsPage() {
  const { skills, loading, showCreateDialog, loadSkills, syncSkills, setShowCreateDialog, actionLoading } = useSkillsStore();

  useEffect(() => { loadSkills(); }, []);

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border shrink-0">
        <div className="flex items-center gap-2">
          <h1 className="text-sm font-medium text-text">Skills</h1>
          <Badge>{skills.length}</Badge>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="subtle"
            size="xs"
            onClick={syncSkills}
            disabled={actionLoading}
            title="Re-scan filesystem for new skills"
          >
            <RefreshCw size={12} className={actionLoading ? 'animate-spin' : ''} />
            Sync
          </Button>
          <Button variant="primary" size="xs" onClick={() => setShowCreateDialog(true)}>
            <Plus size={12} />
            New Skill
          </Button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        {loading ? (
          <div className="text-center text-text-dim text-sm py-12">Loading skills...</div>
        ) : skills.length === 0 ? (
          <div className="text-center py-12">
            <Zap size={32} className="mx-auto text-text-faint mb-3" />
            <p className="text-sm text-text-dim mb-1">No skills yet</p>
            <p className="text-xs text-text-faint">
              Create skills to extend the agent with specialized workflows.
              <br />
              Skills are stored as SKILL.md files in <code className="text-accent">workspace/skills/</code>
            </p>
          </div>
        ) : (
          <div className="grid gap-3 grid-cols-1 md:grid-cols-2 xl:grid-cols-3">
            {skills.map(skill => (
              <SkillCard key={skill.id} skill={skill} />
            ))}
          </div>
        )}
      </div>

      {showCreateDialog && <CreateSkillDialog />}
    </div>
  );
}
