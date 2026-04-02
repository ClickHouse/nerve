import { useState, useEffect } from 'react';
import { Loader2, Terminal, Bot, Check, AlertTriangle } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import type { Session } from '../../types/chat';

/** Format elapsed seconds as "Xs" / "Xm Ys". */
function formatElapsed(startedAt: number): string {
  const sec = Math.floor((Date.now() - startedAt) / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${sec % 60}s`;
}

/** Icon for task tool type. */
function toolIcon(tool: string) {
  if (tool === 'Task') return Bot;
  return Terminal;
}

const STATUS_COLORS = {
  running: 'text-emerald-400',
  done: 'text-text-faint',
  timeout: 'text-amber-400',
} as const;

export function BackgroundJobs({ sessions, activeSession, onSelect }: {
  sessions: Session[];
  activeSession: string;
  onSelect: (id: string) => void;
}) {
  const [hovering, setHovering] = useState(false);
  const [, setTick] = useState(0);
  const backgroundTasks = useChatStore(s => s.backgroundTasks);

  // Running sessions (other than active)
  const runningSessions = sessions.filter(s => s.is_running && s.id !== activeSession);
  // Active background tasks in current session
  const runningTasks = backgroundTasks.filter(t => t.status === 'running');

  // Tick every second to update elapsed timers while hovering
  useEffect(() => {
    if (!hovering || runningTasks.length === 0) return;
    const id = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(id);
  }, [hovering, runningTasks.length]);

  const totalRunning = runningTasks.length + runningSessions.length;
  if (totalRunning === 0 && backgroundTasks.length === 0) return null;

  return (
    <div
      className="relative"
      onMouseEnter={() => setHovering(true)}
      onMouseLeave={() => setHovering(false)}
    >
      {/* Badge */}
      <div className={`flex items-center gap-1.5 px-2 py-1 rounded text-[12px] cursor-default ${
        runningTasks.length > 0
          ? 'text-emerald-400 bg-emerald-400/10'
          : 'text-text-faint bg-surface-raised'
      }`}>
        {runningTasks.length > 0 ? (
          <span className="relative flex h-2 w-2 shrink-0">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
          </span>
        ) : (
          <Check size={11} className="shrink-0" />
        )}
        <span className="tabular-nums">
          {runningTasks.length > 0
            ? `${runningTasks.length} bg task${runningTasks.length > 1 ? 's' : ''}`
            : `${backgroundTasks.length} done`
          }
        </span>
        {runningSessions.length > 0 && (
          <span className="text-text-faint">
            + {runningSessions.length} session{runningSessions.length > 1 ? 's' : ''}
          </span>
        )}
      </div>

      {/* Dropdown */}
      {hovering && (
        <div className="absolute right-0 top-full mt-1.5 z-50 bg-surface-raised border border-border-subtle rounded-lg shadow-xl min-w-[280px] max-w-[380px] py-1">
          {/* Background tasks (current session) */}
          {backgroundTasks.length > 0 && (
            <>
              <div className="px-3 py-1.5 text-[10px] text-text-faint uppercase tracking-wider">
                Background Tasks
              </div>
              {backgroundTasks.map(task => {
                const Icon = toolIcon(task.tool);
                const statusColor = STATUS_COLORS[task.status];
                return (
                  <div
                    key={task.task_id}
                    className="flex items-center gap-2 px-3 py-1.5 text-[12px]"
                  >
                    {task.status === 'running' ? (
                      <Loader2 size={12} className="shrink-0 text-emerald-400 animate-spin" />
                    ) : task.status === 'done' ? (
                      <Check size={12} className="shrink-0 text-emerald-500" />
                    ) : (
                      <AlertTriangle size={12} className="shrink-0 text-amber-400" />
                    )}
                    <span className={`flex-1 min-w-0 truncate ${statusColor}`}>
                      {task.label}
                    </span>
                    <span className="shrink-0 text-[10px] text-text-faint flex items-center gap-1">
                      <Icon size={10} />
                      {task.status === 'running' ? formatElapsed(task.startedAt) : task.status}
                    </span>
                  </div>
                );
              })}
            </>
          )}

          {/* Running sessions (other sessions) */}
          {runningSessions.length > 0 && (
            <>
              {backgroundTasks.length > 0 && <div className="border-t border-border my-1" />}
              <div className="px-3 py-1.5 text-[10px] text-text-faint uppercase tracking-wider">
                Other Running Sessions
              </div>
              {runningSessions.map(s => (
                <button
                  key={s.id}
                  onClick={() => { setHovering(false); onSelect(s.id); }}
                  className="w-full flex items-center gap-2 px-3 py-1.5 text-left text-[12px] text-text-muted hover:bg-border-subtle hover:text-text transition-colors cursor-pointer"
                >
                  <Loader2 size={12} className="shrink-0 text-emerald-400 animate-spin" />
                  <span className="flex-1 min-w-0 truncate">{s.title || s.id}</span>
                  <span className="shrink-0 text-[10px] text-text-faint">
                    {s.source || 'web'}
                  </span>
                </button>
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}
