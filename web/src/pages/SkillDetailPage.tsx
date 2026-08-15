import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Save, Trash2, Zap, CheckCircle, XCircle, Clock, FileText } from 'lucide-react';
import { useSkillsStore } from '../stores/skillsStore';
import { Modal } from '../components/ui/Modal';

function UsageBar({ total, success }: { total: number; success: number }) {
  if (total === 0) return null;
  const pct = Math.round((success / total) * 100);
  return (
    <div className="w-full bg-surface-raised rounded-full h-1.5">
      <div
        className={`h-1.5 rounded-full ${pct >= 90 ? 'bg-emerald-500' : pct >= 70 ? 'bg-amber-500' : 'bg-red-500'}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

export function SkillDetailPage() {
  const { skillId } = useParams<{ skillId: string }>();
  const navigate = useNavigate();
  const { selectedSkill, detailLoading, actionLoading, loadSkill, updateSkill, deleteSkill, toggleSkill, clearSelectedSkill } = useSkillsStore();
  const [editContent, setEditContent] = useState('');
  const [hasChanges, setHasChanges] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  useEffect(() => {
    if (skillId) loadSkill(decodeURIComponent(skillId));
    return () => clearSelectedSkill();
  }, [skillId]);

  useEffect(() => {
    if (selectedSkill) {
      setEditContent(selectedSkill.raw);
      setHasChanges(false);
    }
  }, [selectedSkill]);

  const handleSave = async () => {
    if (!selectedSkill || !hasChanges) return;
    await updateSkill(selectedSkill.id, editContent);
    setHasChanges(false);
  };

  const handleDelete = async () => {
    if (!selectedSkill) return;
    await deleteSkill(selectedSkill.id);
    navigate('/skills');
  };

  if (detailLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-sm text-text-dim">Loading skill...</div>
      </div>
    );
  }

  if (!selectedSkill) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-sm text-text-dim">Skill not found</div>
      </div>
    );
  }

  const stats = selectedSkill.stats;
  const successRate = stats.total_invocations > 0
    ? Math.round((stats.success_count / stats.total_invocations) * 100)
    : null;

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border shrink-0">
        <div className="flex items-center gap-3">
          <button onClick={() => navigate('/skills')} className="text-text-dim hover:text-text-secondary cursor-pointer">
            <ArrowLeft size={16} />
          </button>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-sm font-medium text-text">{selectedSkill.name}</h1>
              <span className="text-[10px] text-text-dim bg-surface-raised px-1.5 py-0.5 rounded">v{selectedSkill.version}</span>
            </div>
            <p className="text-[10px] text-text-dim mt-0.5">{selectedSkill.id}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => toggleSkill(selectedSkill.id, !selectedSkill.enabled)}
            className={`px-2 py-1 text-xs rounded cursor-pointer ${
              selectedSkill.enabled
                ? 'bg-emerald-500/15 text-emerald-600 hover:bg-emerald-500/25'
                : 'bg-surface-raised text-text-dim hover:bg-surface-raised'
            }`}
          >
            {selectedSkill.enabled ? 'Enabled' : 'Disabled'}
          </button>
          {hasChanges && (
            <button
              onClick={handleSave}
              disabled={actionLoading}
              className="flex items-center gap-1 px-2 py-1 text-xs bg-accent text-on-accent rounded hover:bg-accent-hover cursor-pointer disabled:opacity-50"
            >
              <Save size={12} />
              Save
            </button>
          )}
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="flex items-center gap-1 px-2 py-1 text-xs text-hue-red hover:bg-red-900/20 rounded cursor-pointer"
          >
            <Trash2 size={12} />
          </button>
        </div>
      </div>

      {/* Body */}
      <div className="flex-1 flex overflow-hidden">
        {/* Editor */}
        <div className="flex-1 flex flex-col overflow-hidden border-r border-border">
          <div className="px-4 py-2 border-b border-border flex items-center gap-2">
            <FileText size={12} className="text-text-dim" />
            <span className="text-xs text-text-muted">SKILL.md</span>
            {hasChanges && <span className="text-[10px] text-hue-amber">unsaved</span>}
          </div>
          <textarea
            value={editContent}
            onChange={e => { setEditContent(e.target.value); setHasChanges(true); }}
            className="flex-1 bg-bg text-text-secondary text-xs font-mono p-4 resize-none outline-none leading-relaxed"
            spellCheck={false}
            onKeyDown={e => {
              if ((e.metaKey || e.ctrlKey) && e.key === 's') {
                e.preventDefault();
                handleSave();
              }
            }}
          />
        </div>

        {/* Side Panel */}
        <div className="w-[300px] shrink-0 overflow-y-auto bg-surface">
          {/* Stats */}
          <div className="p-4 border-b border-border">
            <h3 className="text-xs font-medium text-text-muted mb-3">Usage Statistics</h3>
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-1 text-xs text-text-muted">
                  <Zap size={10} />
                  <span>Invocations</span>
                </div>
                <span className="text-xs text-text font-mono">{stats.total_invocations}</span>
              </div>
              {successRate !== null && (
                <>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-1 text-xs text-text-muted">
                      <CheckCircle size={10} />
                      <span>Success Rate</span>
                    </div>
                    <span className={`text-xs font-mono ${successRate >= 90 ? 'text-hue-emerald' : 'text-hue-amber'}`}>
                      {successRate}%
                    </span>
                  </div>
                  <UsageBar total={stats.total_invocations} success={stats.success_count} />
                </>
              )}
              {stats.avg_duration_ms != null && (
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-1 text-xs text-text-muted">
                    <Clock size={10} />
                    <span>Avg Duration</span>
                  </div>
                  <span className="text-xs text-text font-mono">{stats.avg_duration_ms}ms</span>
                </div>
              )}
              {stats.last_used && (
                <div className="flex items-center justify-between">
                  <span className="text-xs text-text-muted">Last Used</span>
                  <span className="text-xs text-text">{new Date(stats.last_used).toLocaleString()}</span>
                </div>
              )}
            </div>
          </div>

          {/* Metadata */}
          <div className="p-4 border-b border-border">
            <h3 className="text-xs font-medium text-text-muted mb-3">Metadata</h3>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-text-dim">User Invocable</span>
                <span className={selectedSkill.user_invocable ? 'text-hue-emerald' : 'text-text-dim'}>
                  {selectedSkill.user_invocable ? 'Yes' : 'No'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-dim">Model Invocable</span>
                <span className={selectedSkill.model_invocable ? 'text-hue-emerald' : 'text-text-dim'}>
                  {selectedSkill.model_invocable ? 'Yes' : 'No'}
                </span>
              </div>
              {selectedSkill.allowed_tools && (
                <div>
                  <span className="text-text-dim">Allowed Tools</span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {selectedSkill.allowed_tools.map(t => (
                      <span key={t} className="text-[10px] bg-surface-raised text-text-muted px-1.5 py-0.5 rounded">{t}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* References */}
          {selectedSkill.references.length > 0 && (
            <div className="p-4 border-b border-border">
              <h3 className="text-xs font-medium text-text-muted mb-2">References</h3>
              <div className="space-y-1">
                {selectedSkill.references.map(ref => (
                  <div key={ref} className="text-xs text-accent font-mono truncate">{ref}</div>
                ))}
              </div>
            </div>
          )}

          {/* Recent Usage */}
          {selectedSkill.recent_usage.length > 0 && (
            <div className="p-4">
              <h3 className="text-xs font-medium text-text-muted mb-2">Recent Usage</h3>
              <div className="space-y-2">
                {selectedSkill.recent_usage.map(u => (
                  <div key={u.id} className="flex items-center justify-between text-[10px]">
                    <div className="flex items-center gap-1.5">
                      {u.success ? (
                        <CheckCircle size={10} className="text-hue-emerald" />
                      ) : (
                        <XCircle size={10} className="text-hue-red" />
                      )}
                      <span className="text-text-muted">{u.invoked_by}</span>
                    </div>
                    <div className="flex items-center gap-2 text-text-dim">
                      {u.duration_ms != null && <span>{u.duration_ms}ms</span>}
                      <span>{new Date(u.created_at).toLocaleString()}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Delete Confirmation */}
      <Modal
        open={showDeleteConfirm}
        onClose={() => setShowDeleteConfirm(false)}
        title="Delete Skill"
        size="sm"
        footer={
          <>
            <button onClick={() => setShowDeleteConfirm(false)} className="px-3 py-1.5 text-xs text-text-muted hover:text-text-secondary cursor-pointer">
              Cancel
            </button>
            <button
              onClick={handleDelete}
              disabled={actionLoading}
              className="px-3 py-1.5 text-xs bg-red-600 text-white rounded hover:bg-red-700 cursor-pointer disabled:opacity-50"
            >
              {actionLoading ? 'Deleting...' : 'Delete'}
            </button>
          </>
        }
      >
        <p className="px-5 py-4 text-xs text-text-muted">
          This will permanently delete <strong>{selectedSkill.name}</strong> and all its files. This cannot be undone.
        </p>
      </Modal>
    </div>
  );
}
