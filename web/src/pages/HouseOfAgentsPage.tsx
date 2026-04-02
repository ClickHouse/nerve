import { useEffect, useState } from 'react';
import { Users, Download, Check, Loader2, FileText, Trash2, Plus, Save } from 'lucide-react';
import { useHoaStore } from '../stores/hoaStore';

export function HouseOfAgentsPage() {
  const { status, pipelines, selectedPipeline, loading, installing, loadStatus, loadPipelines, loadPipeline, savePipeline, deletePipeline, installBinary, clearSelectedPipeline } = useHoaStore();
  const [editorContent, setEditorContent] = useState('');
  const [newPipelineId, setNewPipelineId] = useState('');
  const [showNewForm, setShowNewForm] = useState(false);

  useEffect(() => {
    loadStatus();
    loadPipelines();
  }, []);

  useEffect(() => {
    if (selectedPipeline) {
      setEditorContent(selectedPipeline.content);
    }
  }, [selectedPipeline]);

  const handleSave = async () => {
    if (selectedPipeline) {
      await savePipeline(selectedPipeline.id, editorContent);
      await loadPipeline(selectedPipeline.id);
    }
  };

  const handleCreate = async () => {
    const id = newPipelineId.trim().replace(/\s+/g, '-').toLowerCase();
    if (!id) return;
    await savePipeline(id, `# ${id} pipeline\n# Describe your pipeline here\n`);
    setNewPipelineId('');
    setShowNewForm(false);
    await loadPipeline(id);
  };

  if (!status?.enabled) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center space-y-3">
          <Users size={48} className="text-text-faint mx-auto" />
          <h2 className="text-lg text-text-muted">houseofagents is not enabled</h2>
          <p className="text-[13px] text-text-faint max-w-md">
            Enable multi-agent execution by adding <code className="bg-surface-raised px-1.5 py-0.5 rounded text-amber-400">houseofagents.enabled: true</code> to your config.yaml
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-border-subtle px-6 py-4 bg-bg shrink-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Users size={20} className="text-amber-400" />
            <h1 className="text-lg font-semibold text-text">House of Agents</h1>
            {status.available ? (
              <span className="px-2 py-0.5 text-[11px] rounded-full bg-emerald-400/10 text-emerald-400 border border-emerald-400/20">
                <Check size={10} className="inline mr-1" />
                {status.version || 'Installed'}
              </span>
            ) : (
              <button
                onClick={installBinary}
                disabled={installing}
                className="flex items-center gap-1.5 px-3 py-1 text-[12px] bg-amber-600/80 hover:bg-amber-500/80 disabled:opacity-50 text-white rounded-lg cursor-pointer"
              >
                {installing ? <Loader2 size={12} className="animate-spin" /> : <Download size={12} />}
                {installing ? 'Installing...' : 'Install Binary'}
              </button>
            )}
          </div>
          <div className="text-[12px] text-text-faint">
            Default: <span className="text-text-muted">{status.default_mode}</span>
            {' · '}
            Agents: <span className="text-text-muted">{status.default_agents.join(', ')}</span>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Pipeline list */}
        <div className="w-64 border-r border-border-subtle overflow-y-auto bg-bg-sunken">
          <div className="p-3 border-b border-border-subtle flex items-center justify-between">
            <span className="text-[12px] text-text-faint font-medium uppercase tracking-wider">Pipelines</span>
            <button
              onClick={() => setShowNewForm(!showNewForm)}
              className="p-1 text-text-faint hover:text-text-muted hover:bg-surface-hover rounded cursor-pointer"
            >
              <Plus size={14} />
            </button>
          </div>

          {showNewForm && (
            <div className="p-2 border-b border-border-subtle">
              <input
                value={newPipelineId}
                onChange={e => setNewPipelineId(e.target.value)}
                placeholder="pipeline-name"
                className="w-full px-2 py-1 text-[12px] bg-surface-raised border border-border-subtle rounded text-text-secondary placeholder-[#555] focus:outline-none focus:border-amber-400/50"
                onKeyDown={e => e.key === 'Enter' && handleCreate()}
                autoFocus
              />
            </div>
          )}

          {loading ? (
            <div className="p-4 text-center text-text-faint">Loading...</div>
          ) : pipelines.length === 0 ? (
            <div className="p-4 text-center text-text-faint text-[12px]">No pipelines</div>
          ) : (
            pipelines.map(p => (
              <button
                key={p.id}
                onClick={() => loadPipeline(p.id)}
                className={`w-full text-left px-3 py-2 text-[13px] border-b border-border-subtle cursor-pointer transition-colors ${
                  selectedPipeline?.id === p.id
                    ? 'bg-amber-400/5 text-amber-300 border-l-2 border-l-amber-400'
                    : 'text-text-muted hover:bg-surface border-l-2 border-l-transparent'
                }`}
              >
                <div className="font-medium">{p.name}</div>
                {p.description && (
                  <div className="text-[11px] text-text-faint truncate mt-0.5">{p.description}</div>
                )}
              </button>
            ))
          )}
        </div>

        {/* Editor */}
        <div className="flex-1 flex flex-col">
          {selectedPipeline ? (
            <>
              <div className="border-b border-border-subtle px-4 py-2 flex items-center justify-between bg-bg">
                <div className="flex items-center gap-2">
                  <FileText size={14} className="text-amber-400" />
                  <span className="text-[13px] text-text-secondary font-mono">{selectedPipeline.id}.toml</span>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={handleSave}
                    className="flex items-center gap-1 px-3 py-1 text-[12px] bg-amber-600/80 hover:bg-amber-500/80 text-white rounded cursor-pointer"
                  >
                    <Save size={12} /> Save
                  </button>
                  <button
                    onClick={() => { deletePipeline(selectedPipeline.id); clearSelectedPipeline(); }}
                    className="flex items-center gap-1 px-3 py-1 text-[12px] bg-red-600/40 hover:bg-red-500/40 text-red-300 rounded cursor-pointer"
                  >
                    <Trash2 size={12} /> Delete
                  </button>
                </div>
              </div>
              <textarea
                value={editorContent}
                onChange={e => setEditorContent(e.target.value)}
                className="flex-1 p-4 text-[13px] font-mono bg-bg-sunken text-text-secondary focus:outline-none resize-none"
                spellCheck={false}
              />
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-text-faint">
              <div className="text-center space-y-2">
                <FileText size={32} className="mx-auto" />
                <p className="text-[13px]">Select a pipeline to edit</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
