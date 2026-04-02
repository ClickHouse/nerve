import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Plus, RefreshCw, Zap, CheckCircle, XCircle, Clock } from 'lucide-react';
import { useSkillsStore, type Skill } from '../stores/skillsStore';

function SkillCard({ skill }: { skill: Skill }) {
  const navigate = useNavigate();
  const { toggleSkill } = useSkillsStore();
  const successRate = skill.total_invocations > 0
    ? Math.round((skill.success_count / skill.total_invocations) * 100)
    : null;

  return (
    <div
      className="bg-surface-raised border border-border rounded-lg p-4 hover:border-[#3a3a3a] cursor-pointer transition-colors"
      onClick={() => navigate(`/skills/${encodeURIComponent(skill.id)}`)}
    >
      <div className="flex items-start justify-between mb-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium text-text truncate">{skill.name}</h3>
            <span className="text-[10px] text-text-dim bg-surface-raised px-1.5 py-0.5 rounded">
              v{skill.version}
            </span>
          </div>
          <p className="text-xs text-text-muted mt-1 line-clamp-2">{skill.description}</p>
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); toggleSkill(skill.id, !skill.enabled); }}
          className={`ml-3 w-8 h-4 rounded-full transition-colors flex items-center shrink-0 cursor-pointer ${
            skill.enabled ? 'bg-emerald-600 justify-end' : 'bg-border-subtle justify-start'
          }`}
          title={skill.enabled ? 'Enabled' : 'Disabled'}
        >
          <div className="w-3 h-3 rounded-full bg-white mx-0.5" />
        </button>
      </div>

      <div className="flex items-center gap-4 mt-3 text-[10px] text-text-dim">
        <div className="flex items-center gap-1">
          <Zap size={10} />
          <span>{skill.total_invocations} uses</span>
        </div>
        {successRate !== null && (
          <div className="flex items-center gap-1">
            {successRate >= 90 ? <CheckCircle size={10} className="text-emerald-500" /> : <XCircle size={10} className="text-amber-500" />}
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
          <span className="text-amber-500/70">disabled</span>
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
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50" onClick={() => setShowCreateDialog(false)}>
      <div className="bg-surface-raised border border-border rounded-lg w-[500px] max-h-[80vh] overflow-auto" onClick={e => e.stopPropagation()}>
        <div className="p-4 border-b border-border">
          <h2 className="text-sm font-medium text-text">New Skill</h2>
        </div>
        <div className="p-4 space-y-3">
          <div>
            <label className="text-xs text-text-muted block mb-1">Name</label>
            <input
              value={name} onChange={e => setName(e.target.value)}
              className="w-full bg-bg border border-border rounded px-3 py-1.5 text-sm text-text outline-none focus:border-[#6366f1]"
              placeholder="e.g. code-review"
              autoFocus
            />
          </div>
          <div>
            <label className="text-xs text-text-muted block mb-1">Description</label>
            <textarea
              value={description} onChange={e => setDescription(e.target.value)}
              className="w-full bg-bg border border-border rounded px-3 py-1.5 text-sm text-text outline-none focus:border-[#6366f1] min-h-[60px] resize-y"
              placeholder='This skill should be used when the user asks to "review code"...'
            />
          </div>
          <div>
            <label className="text-xs text-text-muted block mb-1">Instructions (optional)</label>
            <textarea
              value={content} onChange={e => setContent(e.target.value)}
              className="w-full bg-bg border border-border rounded px-3 py-1.5 text-sm text-text outline-none focus:border-[#6366f1] min-h-[120px] resize-y font-mono text-xs"
              placeholder="Markdown instructions for the agent..."
            />
          </div>
        </div>
        <div className="p-4 border-t border-border flex justify-end gap-2">
          <button onClick={() => setShowCreateDialog(false)} className="px-3 py-1.5 text-xs text-text-muted hover:text-text-secondary cursor-pointer">
            Cancel
          </button>
          <button
            onClick={handleCreate}
            disabled={!name.trim() || !description.trim() || actionLoading}
            className="px-3 py-1.5 text-xs bg-[#6366f1] text-white rounded hover:bg-[#5558e6] disabled:opacity-50 cursor-pointer disabled:cursor-default"
          >
            {actionLoading ? 'Creating...' : 'Create Skill'}
          </button>
        </div>
      </div>
    </div>
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
          <span className="text-[10px] text-text-dim bg-surface-raised px-1.5 py-0.5 rounded">
            {skills.length}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={syncSkills}
            disabled={actionLoading}
            className="flex items-center gap-1 px-2 py-1 text-xs text-text-muted hover:text-text-secondary hover:bg-surface-hover rounded cursor-pointer disabled:opacity-50"
            title="Re-scan filesystem for new skills"
          >
            <RefreshCw size={12} className={actionLoading ? 'animate-spin' : ''} />
            Sync
          </button>
          <button
            onClick={() => setShowCreateDialog(true)}
            className="flex items-center gap-1 px-2 py-1 text-xs bg-[#6366f1] text-white rounded hover:bg-[#5558e6] cursor-pointer"
          >
            <Plus size={12} />
            New Skill
          </button>
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
              Skills are stored as SKILL.md files in <code className="text-[#6366f1]">workspace/skills/</code>
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
