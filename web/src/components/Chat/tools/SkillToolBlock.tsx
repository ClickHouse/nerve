import { useState } from 'react';
import { ChevronRight, ChevronDown, Sparkles, BookOpen, Play, List, Plus, Pencil, Loader2 } from 'lucide-react';
import type { ToolCallBlockData } from '../../../types/chat';

/** Extract readable text from MCP content blocks. */
function extractText(result: string): string {
  try {
    const parsed = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter((b: any) => b.type === 'text')
        .map((b: any) => b.text)
        .join('\n');
    }
  } catch { /* not JSON */ }
  return result;
}

/** Parse skill list from result text. */
function parseSkillList(text: string): Array<{ name: string; id: string; description: string }> {
  const skills: Array<{ name: string; id: string; description: string }> = [];
  for (const line of text.split('\n')) {
    const match = line.match(/^-\s+\*\*(.+?)\*\*\s+\(`(.+?)`\):\s*(.+)/);
    if (match) {
      skills.push({ name: match[1], id: match[2], description: match[3].trim() });
    }
  }
  return skills;
}

export function SkillToolBlock({ block }: { block: ToolCallBlockData }) {
  const [expanded, setExpanded] = useState(false);
  const isRunning = block.status === 'running';

  const toolName = block.tool.split('__').pop() || block.tool;
  const isList = toolName === 'skill_list';
  const isGet = toolName === 'skill_get';
  const isRef = toolName === 'skill_read_reference';
  const isRun = toolName === 'skill_run_script';
  const isCreate = toolName === 'skill_create';
  const isUpdate = toolName === 'skill_update';

  let label: string;
  let Icon = Sparkles;
  if (isList) { label = 'List Skills'; Icon = List; }
  else if (isGet) { label = 'Load Skill'; Icon = BookOpen; }
  else if (isRef) { label = 'Read Reference'; Icon = BookOpen; }
  else if (isRun) { label = 'Run Script'; Icon = Play; }
  else if (isCreate) { label = 'Create Skill'; Icon = Plus; }
  else if (isUpdate) { label = 'Update Skill'; Icon = Pencil; }
  else { label = 'Skill'; }

  const skillName = String(block.input.name || '');
  const refPath = String(block.input.path || '');
  const skillDescription = String(block.input.description || '');

  const resultText = block.result ? extractText(block.result) : '';
  const skillList = isList ? parseSkillList(resultText) : [];

  // For skill_get, extract the skill title from the result
  let loadedSkillTitle = '';
  if (isGet && resultText) {
    const titleMatch = resultText.match(/^#\s+Skill:\s+(.+?)(?:\s+\(v[\d.]+\))?$/m);
    if (titleMatch) loadedSkillTitle = titleMatch[1];
  }

  // Count lines for loaded skill content
  const contentLines = resultText ? resultText.split('\n').length : 0;

  return (
    <div className="my-1.5 border border-border rounded-lg bg-surface overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer hover:bg-surface-raised transition-colors"
      >
        {isRunning
          ? <Loader2 size={14} className="text-[#6366f1] animate-spin shrink-0" />
          : <Icon size={14} className={`shrink-0 ${block.isError ? 'text-red-400' : 'text-purple-400'}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary">{label}</span>

        {/* Skill name badge */}
        {skillName && (
          <span className="text-[11px] font-mono bg-purple-500/10 text-purple-300 px-1.5 py-0.5 rounded truncate max-w-[200px]">
            {skillName}
          </span>
        )}

        {/* Reference path */}
        {isRef && refPath && (
          <span className="text-[11px] text-text-dim font-mono truncate">{refPath}</span>
        )}

        {/* List count */}
        {isList && skillList.length > 0 && (
          <span className="text-[10px] text-text-faint shrink-0">{skillList.length} skills</span>
        )}

        {/* Loaded content hint */}
        {isGet && !isRunning && resultText && !block.isError && (
          <span className="text-[10px] text-text-faint shrink-0">{contentLines} lines</span>
        )}

        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-text-faint" /> : <ChevronRight size={14} className="text-text-faint" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border">
          {/* Skill list */}
          {skillList.length > 0 && (
            <div className="px-3 py-2 space-y-1.5 max-h-60 overflow-y-auto">
              {skillList.map((s) => (
                <div key={s.id} className="flex items-start gap-2 text-[12px]">
                  <Sparkles size={11} className="text-purple-400 mt-0.5 shrink-0" />
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5">
                      <span className="text-text-secondary font-medium">{s.name}</span>
                      <span className="text-[10px] text-text-faint font-mono">{s.id}</span>
                    </div>
                    <p className="text-[11px] text-text-muted truncate">{s.description}</p>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Loaded skill content */}
          {isGet && resultText && !block.isError && !skillList.length && (
            <div className="max-h-80 overflow-y-auto">
              {loadedSkillTitle && (
                <div className="px-3 py-1.5 border-b border-border-subtle flex items-center gap-2">
                  <Sparkles size={11} className="text-purple-400" />
                  <span className="text-[12px] text-text-secondary font-medium">{loadedSkillTitle}</span>
                </div>
              )}
              <pre className="px-3 py-2 text-[11px] font-mono whitespace-pre-wrap text-text-muted leading-relaxed">
                {resultText}
              </pre>
            </div>
          )}

          {/* Script output */}
          {isRun && resultText && !block.isError && (
            <div className="px-3 py-2">
              <div className="text-[10px] uppercase tracking-wider text-text-faint mb-1">Output</div>
              <pre className="text-[12px] font-mono whitespace-pre-wrap max-h-60 overflow-y-auto bg-bg-sunken rounded p-2 border border-border-subtle text-text-muted">
                {resultText}
              </pre>
            </div>
          )}

          {/* Reference content */}
          {isRef && resultText && !block.isError && (
            <div className="max-h-80 overflow-y-auto">
              {refPath && (
                <div className="px-3 py-1.5 border-b border-border-subtle">
                  <span className="text-[11px] text-text-dim font-mono">{skillName}/{refPath}</span>
                </div>
              )}
              <pre className="px-3 py-2 text-[11px] font-mono whitespace-pre-wrap text-text-muted leading-relaxed">
                {resultText}
              </pre>
            </div>
          )}

          {/* Create skill */}
          {isCreate && (
            <div className="px-3 py-2">
              {skillName && (
                <div className="flex items-center gap-2 mb-1.5">
                  <Plus size={11} className="text-purple-400" />
                  <span className="text-[12px] text-text-secondary font-medium">{skillName}</span>
                </div>
              )}
              {skillDescription && (
                <p className="text-[11px] text-text-muted mb-1.5 pl-5">{skillDescription.slice(0, 300)}</p>
              )}
              {String(block.input.content || '') && (
                <pre className="text-[11px] font-mono text-text-dim whitespace-pre-wrap max-h-40 overflow-y-auto bg-bg-sunken rounded p-2 border border-border-subtle mt-1">
                  {String(block.input.content).slice(0, 500)}{String(block.input.content).length > 500 ? '...' : ''}
                </pre>
              )}
              {resultText && !block.isError && (
                <div className="mt-2 text-[11px] text-emerald-400/70 flex items-center gap-1">
                  <Sparkles size={10} /> {resultText}
                </div>
              )}
            </div>
          )}

          {/* Update skill */}
          {isUpdate && (
            <div className="px-3 py-2">
              {String(block.input.content || '') && (
                <pre className="text-[11px] font-mono text-text-muted whitespace-pre-wrap max-h-60 overflow-y-auto bg-bg-sunken rounded p-2 border border-border-subtle">
                  {String(block.input.content).slice(0, 800)}{String(block.input.content).length > 800 ? '\n...' : ''}
                </pre>
              )}
              {resultText && !block.isError && (
                <div className="mt-2 text-[11px] text-emerald-400/70 flex items-center gap-1">
                  <Sparkles size={10} /> {resultText}
                </div>
              )}
            </div>
          )}

          {/* Fallback for unknown skill tools */}
          {!isList && !isGet && !isRun && !isRef && !isCreate && !isUpdate && resultText && !block.isError && (
            <pre className="px-3 py-2 text-[12px] whitespace-pre-wrap max-h-60 overflow-y-auto text-text-muted">
              {resultText}
            </pre>
          )}

          {/* Error */}
          {block.isError && resultText && (
            <pre className="px-3 py-2 text-[12px] text-red-400 whitespace-pre-wrap border-t border-border-subtle">
              {resultText}
            </pre>
          )}

          {/* Running */}
          {isRunning && block.result === undefined && (
            <div className="px-3 py-3 text-[12px] text-text-dim flex items-center gap-2">
              <Loader2 size={12} className="animate-spin" />
              {isGet ? 'Loading skill...' : isRun ? 'Running script...' : 'Working...'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
