import { useEffect, useState, useCallback, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Edit3, Eye, Save, Calendar, ExternalLink } from 'lucide-react';
import { useTaskStore } from '../stores/taskStore';
import { MarkdownContent } from '../components/Chat/MarkdownContent';
import { TASK_STATUS_STYLES as STATUS_STYLES } from '../constants/statusStyles';

export function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();
  const {
    selectedTask, detailLoading, saving,
    loadTask, saveTaskContent, updateStatus, clearSelectedTask,
  } = useTaskStore();

  const [mode, setMode] = useState<'edit' | 'preview'>('preview');
  const [localContent, setLocalContent] = useState('');
  const [dirty, setDirty] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (taskId) loadTask(taskId);
    return () => clearSelectedTask();
  }, [taskId]);

  // Sync local content when task loads
  useEffect(() => {
    if (selectedTask?.content != null) {
      setLocalContent(selectedTask.content);
      setDirty(false);
    }
  }, [selectedTask?.content]);

  const handleContentChange = useCallback((value: string) => {
    setLocalContent(value);
    setDirty(true);
  }, []);

  const handleSave = useCallback(async () => {
    if (taskId && dirty) {
      await saveTaskContent(taskId, localContent);
      setDirty(false);
    }
  }, [taskId, dirty, localContent, saveTaskContent]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      handleSave();
    }
  }, [handleSave]);

  if (detailLoading) {
    return (
      <div className="h-full flex items-center justify-center text-[#444]">
        Loading...
      </div>
    );
  }

  if (!selectedTask) {
    return (
      <div className="h-full flex flex-col items-center justify-center gap-3 text-[#444]">
        <span>Task not found</span>
        <button
          onClick={() => navigate('/tasks')}
          className="text-[13px] text-[#6366f1] hover:underline cursor-pointer"
        >
          Back to tasks
        </button>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-[#222] px-6 py-3 bg-[#0f0f0f] shrink-0">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-3 min-w-0">
            <button
              onClick={() => navigate('/tasks')}
              className="p-1.5 text-[#666] hover:text-[#aaa] hover:bg-[#1a1a1a] rounded cursor-pointer shrink-0"
            >
              <ArrowLeft size={18} />
            </button>
            <h1 className="text-lg font-semibold text-[#e0e0e0] truncate">{selectedTask.title}</h1>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {/* Edit / Preview toggle */}
            <div className="flex bg-[#1a1a1a] rounded-md border border-[#2a2a2a]">
              <button
                onClick={() => setMode('edit')}
                className={`px-2.5 py-1.5 text-[12px] rounded-l-md cursor-pointer transition-colors
                  ${mode === 'edit' ? 'bg-[#252525] text-[#e0e0e0]' : 'text-[#666] hover:text-[#aaa]'}`}
              >
                <Edit3 size={14} />
              </button>
              <button
                onClick={() => setMode('preview')}
                className={`px-2.5 py-1.5 text-[12px] rounded-r-md cursor-pointer transition-colors
                  ${mode === 'preview' ? 'bg-[#252525] text-[#e0e0e0]' : 'text-[#666] hover:text-[#aaa]'}`}
              >
                <Eye size={14} />
              </button>
            </div>

            {/* Save button */}
            {dirty && (
              <button
                onClick={handleSave}
                disabled={saving}
                className="flex items-center gap-1.5 px-3 py-1.5 text-[12px] bg-[#6366f1] hover:bg-[#818cf8] text-white rounded-md cursor-pointer disabled:opacity-50"
              >
                <Save size={12} />
                {saving ? 'Saving...' : 'Save'}
              </button>
            )}
          </div>
        </div>

        {/* Meta row */}
        <div className="flex items-center gap-3 ml-9 text-[12px]">
          <span className={`px-2 py-0.5 rounded-full border ${STATUS_STYLES[selectedTask.status] || STATUS_STYLES.deferred}`}>
            {selectedTask.status}
          </span>
          <select
            value={selectedTask.status}
            onChange={(e) => updateStatus(selectedTask.id, e.target.value)}
            className="text-[12px] px-2 py-1 bg-[#1a1a1a] border border-[#2a2a2a] rounded text-[#aaa] outline-none cursor-pointer"
          >
            <option value="pending">Pending</option>
            <option value="in_progress">In Progress</option>
            <option value="done">Done</option>
            <option value="deferred">Deferred</option>
          </select>
          {selectedTask.deadline && (
            <span className="flex items-center gap-1 text-[#666]">
              <Calendar size={11} /> {selectedTask.deadline}
            </span>
          )}
          {selectedTask.source && (
            <span className="text-[#555]">from {selectedTask.source}</span>
          )}
          {selectedTask.source_url && (
            <a
              href={selectedTask.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1 text-[#6366f1] hover:underline"
            >
              <ExternalLink size={11} /> source
            </a>
          )}
        </div>
      </div>

      {/* Content area */}
      {mode === 'edit' ? (
        <textarea
          ref={textareaRef}
          value={localContent}
          onChange={e => handleContentChange(e.target.value)}
          onKeyDown={handleKeyDown}
          className="flex-1 p-6 bg-[#0a0a0a] text-[14px] text-[#e0e0e0] font-mono leading-relaxed outline-none resize-none"
          spellCheck={false}
          placeholder="Task content..."
        />
      ) : (
        <div className="flex-1 overflow-y-auto p-6">
          <div className="max-w-3xl mx-auto">
            {localContent ? (
              <MarkdownContent content={localContent} />
            ) : (
              <span className="text-[#444] italic">No content</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
