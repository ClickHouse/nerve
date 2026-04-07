import { useRef, useEffect, useState, useCallback } from 'react';
import { X, Lightbulb, Bot, Search, Wrench, Files, Loader2, Check, Ban } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { MarkdownContent } from './MarkdownContent';
import { SelectionToolbar } from './SelectionToolbar';
import { BlockRenderer } from './BlockRenderer';
import { FileChangesPanel } from './FileChangesPanel';
import type { PanelTab } from '../../types/chat';

const TAB_ICONS: Record<string, typeof Bot> = {
  Plan: Lightbulb,
  Explore: Search,
  'general-purpose': Wrench,
  files: Files,
};

const TAB_COLORS: Record<string, string> = {
  Plan: 'text-amber-400',
  Explore: 'text-cyan-400',
  'general-purpose': 'text-link',
  files: 'text-teal-400',
};

function formatElapsed(startedAt: number, completedAt?: number): string {
  const ms = (completedAt || Date.now()) - startedAt;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${seconds % 60}s`;
}

// ------------------------------------------------------------------ //
//  ElapsedTimer — ticks every second for running tabs                  //
// ------------------------------------------------------------------ //

function ElapsedTimer({ startedAt }: { startedAt: number }) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(interval);
  }, []);
  return <>{formatElapsed(startedAt)}</>;
}

// ------------------------------------------------------------------ //
//  TabBar — shown when multiple tabs exist                             //
// ------------------------------------------------------------------ //

function TabBar({ panels, activeId, onFocus, onClose }: {
  panels: PanelTab[];
  activeId: string | null;
  onFocus: (id: string) => void;
  onClose: (id: string) => void;
}) {
  return (
    <div className="flex items-center border-b border-border-subtle bg-bg-sunken overflow-x-auto shrink-0">
      {panels.map(tab => {
        const Icon = TAB_ICONS[tab.subagentType] || Bot;
        const color = TAB_COLORS[tab.subagentType] || 'text-text-muted';
        const isActive = tab.id === activeId;
        return (
          <button
            key={tab.id}
            onClick={() => onFocus(tab.id)}
            className={`group flex items-center gap-1.5 px-3 py-2 text-[12px] border-r border-surface-raised shrink-0 transition-colors cursor-pointer ${
              isActive ? 'bg-bg-sunken text-text-secondary' : 'text-text-dim hover:text-text-muted hover:bg-surface-hover'
            }`}
          >
            {tab.status === 'running'
              ? <Loader2 size={11} className={`animate-spin ${color}`} />
              : <Icon size={11} className={tab.isError ? 'text-red-400' : color} />
            }
            <span className="truncate max-w-[100px]">{tab.label}</span>
            {tab.status !== 'running' && tab.completedAt && (
              <span className="text-[10px] text-text-faint">{formatElapsed(tab.startedAt, tab.completedAt)}</span>
            )}
            <span
              onClick={(e) => { e.stopPropagation(); onClose(tab.id); }}
              className="ml-1 text-text-faint hover:text-text-muted opacity-0 group-hover:opacity-100 transition-opacity"
            >
              <X size={10} />
            </span>
          </button>
        );
      })}
    </div>
  );
}

// ------------------------------------------------------------------ //
//  TabHeader — icon + label + description + close                      //
// ------------------------------------------------------------------ //

function TabHeader({ tab, onClose }: { tab: PanelTab; onClose: () => void }) {
  const Icon = TAB_ICONS[tab.subagentType] || Bot;
  const color = TAB_COLORS[tab.subagentType] || 'text-text-muted';

  return (
    <div className="flex items-center justify-between px-4 py-2.5 border-b border-border-subtle bg-bg shrink-0">
      <div className="flex items-center gap-2 min-w-0">
        {tab.status === 'running'
          ? <Loader2 size={14} className={`animate-spin shrink-0 ${color}`} />
          : <Icon size={14} className={`shrink-0 ${tab.isError ? 'text-red-400' : color}`} />
        }
        <span className="text-[13px] font-medium text-text-secondary">{tab.label}</span>
        {tab.description && (
          <span className="text-[11px] text-text-faint truncate">{tab.description}</span>
        )}
        {tab.model && (
          <span className="text-[10px] text-text-faint shrink-0">{tab.model}</span>
        )}
        {tab.status === 'running' && (
          <span className="text-[10px] text-text-faint shrink-0">
            <ElapsedTimer startedAt={tab.startedAt} />
          </span>
        )}
      </div>
      <button
        onClick={onClose}
        className="w-6 h-6 flex items-center justify-center text-text-faint hover:text-text-muted rounded cursor-pointer transition-colors shrink-0"
      >
        <X size={14} />
      </button>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  TabContent — live blocks + markdown body                            //
// ------------------------------------------------------------------ //

function TabContent({ tab, containerRef }: { tab: PanelTab; containerRef: React.RefObject<HTMLDivElement | null> }) {
  const cursorColor = tab.type === 'plan' ? 'bg-amber-400' : 'bg-link';
  const hasBlocks = tab.blocks.length > 0;
  const endRef = useRef<HTMLDivElement>(null);
  const isNearBottom = useRef(true);

  const handleScroll = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    isNearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100;
  }, [containerRef]);

  // Auto-scroll when new blocks/content arrive, only if user hasn't scrolled up
  useEffect(() => {
    if (isNearBottom.current) {
      endRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [tab.blocks.length, tab.content, tab.streaming]);

  return (
    <div
      ref={containerRef}
      className="flex-1 overflow-y-auto px-5 py-4 relative"
      data-role="plan"
      onScroll={handleScroll}
    >
      <SelectionToolbar containerRef={containerRef} />

      {/* Live blocks — shared renderer with auto-grouping */}
      {hasBlocks && (
        <div className="mb-3">
          <BlockRenderer
            blocks={tab.blocks}
            streaming={tab.streaming}
            cursorColor={cursorColor}
            textClassName="text-[13px] my-1"
          />
        </div>
      )}

      {/* Separator between blocks and final content */}
      {hasBlocks && tab.content && (
        <div className="border-t border-border-subtle my-3" />
      )}

      {tab.content ? (
        <div className="text-[13px]">
          <MarkdownContent content={tab.content} />
          {tab.streaming && (
            <span className={`streaming-cursor inline-block w-1.5 h-4 ${cursorColor} ml-0.5 align-text-bottom`} />
          )}
        </div>
      ) : tab.streaming && !hasBlocks ? (
        <div className="flex items-center gap-2 text-[13px] text-text-dim pt-4">
          <Loader2 size={14} className="animate-spin" />
          {tab.type === 'plan' ? 'Planning...' : `${tab.label} working...`}
        </div>
      ) : !hasBlocks ? (
        <div className="text-[13px] text-text-faint">No content</div>
      ) : null}

      <div ref={endRef} />
    </div>
  );
}

// ------------------------------------------------------------------ //
//  PlanActions — approve/decline footer for plan tabs                  //
// ------------------------------------------------------------------ //

function PlanActions({ tab }: { tab: PanelTab }) {
  const pendingInteraction = useChatStore(s => s.pendingInteraction);
  const answerInteraction = useChatStore(s => s.answerInteraction);
  const denyInteraction = useChatStore(s => s.denyInteraction);
  const sendMessage = useChatStore(s => s.sendMessage);
  const isStreaming = useChatStore(s => s.isStreaming);
  const closePanelTab = useChatStore(s => s.closePanelTab);

  const isPlanExit = pendingInteraction?.interactionType === 'plan_exit';
  const canApprove = tab.content && !tab.streaming && (!isStreaming || isPlanExit);

  if (!canApprove) return null;

  const handleApprove = () => {
    if (isPlanExit) {
      answerInteraction(null);
    } else {
      sendMessage('Looks good — implement it.');
    }
    closePanelTab(tab.id);
  };

  const handleDecline = () => {
    if (isPlanExit) {
      denyInteraction('User declined the plan.');
    }
    closePanelTab(tab.id);
  };

  return (
    <div className="flex items-center justify-end gap-2 px-4 py-2.5 border-t border-border-subtle bg-bg shrink-0">
      {isPlanExit && (
        <button
          onClick={handleDecline}
          className="flex items-center gap-1.5 px-3 py-1 bg-surface-raised hover:bg-surface-hover text-text-muted text-[12px] font-medium rounded-md cursor-pointer transition-colors"
        >
          <Ban size={12} />
          Decline
        </button>
      )}
      <button
        onClick={handleApprove}
        className="flex items-center gap-1.5 px-3 py-1 bg-emerald-600 hover:bg-emerald-500 text-white text-[12px] font-medium rounded-md cursor-pointer transition-colors"
      >
        <Check size={12} />
        Approve
      </button>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  SidePanel — main exported component                                 //
// ------------------------------------------------------------------ //

export function SidePanel() {
  const panels = useChatStore(s => s.panels);
  const activePanelId = useChatStore(s => s.activePanelId);
  const panelVisible = useChatStore(s => s.panelVisible);
  const panelWidth = useChatStore(s => s.panelWidth);
  const togglePanel = useChatStore(s => s.togglePanel);
  const focusPanelTab = useChatStore(s => s.focusPanelTab);
  const closePanelTab = useChatStore(s => s.closePanelTab);
  const setPanelWidth = useChatStore(s => s.setPanelWidth);

  const activeTab = panels.find(p => p.id === activePanelId) || panels[0] || null;
  const containerRef = useRef<HTMLDivElement>(null);

  // Drag-to-resize (disable transition during drag for responsiveness)
  const [isDragging, setIsDragging] = useState(false);
  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = panelWidth;
    const parent = (e.target as HTMLElement).closest('.flex-1');
    const parentWidth = parent?.getBoundingClientRect().width || window.innerWidth;

    const handleMove = (moveEvent: MouseEvent) => {
      const delta = startX - moveEvent.clientX; // drag left = wider panel
      const newPct = startWidth + (delta / parentWidth) * 100;
      setPanelWidth(newPct);
    };
    const handleUp = () => {
      setIsDragging(false);
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [panelWidth, setPanelWidth]);

  // Scroll to top when active tab changes
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = 0;
    }
  }, [activePanelId]);

  if (!activeTab) return null;

  const isOpen = panelVisible;
  const showTabs = panels.length > 1;

  return (
    <div
      className={`side-panel flex flex-col bg-bg-sunken shrink-0 relative overflow-hidden ${
        isOpen ? 'border-l border-border-subtle' : 'border-l-0'
      } ${isDragging ? '' : 'transition-[width] duration-200'}`}
      style={{ width: isOpen ? `${panelWidth}%` : '0px' }}
    >
      {/* Resize handle */}
      <div
        onMouseDown={handleResizeStart}
        className="absolute left-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-accent/30 z-10"
      />

      {/* Tab bar (when multiple tabs) */}
      {showTabs && (
        <TabBar
          panels={panels}
          activeId={activePanelId}
          onFocus={focusPanelTab}
          onClose={closePanelTab}
        />
      )}

      {/* Header for active tab */}
      <TabHeader tab={activeTab} onClose={togglePanel} />

      {/* Content area */}
      {activeTab.type === 'files'
        ? <FileChangesPanel />
        : <TabContent tab={activeTab} containerRef={containerRef} />
      }

      {/* Footer actions (approve/decline for plans) */}
      {activeTab.type === 'plan' && <PlanActions tab={activeTab} />}
    </div>
  );
}
