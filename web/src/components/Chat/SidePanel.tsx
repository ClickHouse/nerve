import { useRef, useEffect, useState, useCallback } from 'react';
import { X, Loader2, Check, Ban } from '../ui/icons';
import { Button, IconButton } from '../ui';
import { useChatStore } from '../../stores/chatStore';
import { useAgentStore } from '../../stores/agentStore';
import { isLive } from '../../stores/helpers/agentSelectors';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { MarkdownContent } from './MarkdownContent';
import { SelectionToolbar } from './SelectionToolbar';
import { BlockRenderer } from './BlockRenderer';
import { FileChangesPanel } from './FileChangesPanel';
import { WorkflowPanel } from './WorkflowPanel';
import { AgentsPanel } from './agents/AgentsPanel';
import { AgentTranscript } from './agents/AgentTranscript';
import { identityVisual, statusVisual } from './agents/agentVisuals';
import type { PanelTab } from '../../types/chat';

/** Tabs that draw their own header (the roster and an agent's transcript). */
const OWN_HEADER = new Set<PanelTab['type']>(['agents', 'agent']);

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
  const agents = useAgentStore(s => s.agents);
  const liveAgents = Object.values(agents).filter(a => !a.ambient && isLive(a)).length;
  return (
    <div className="flex items-center border-b border-border-subtle bg-bg-sunken overflow-x-auto shrink-0">
      {panels.map(tab => {
        const isActive = tab.id === activeId;
        // An agent tab follows its record's label (it may open before the record loads).
        const label = (tab.type === 'agent' && tab.agentId && agents[tab.agentId]?.label) || tab.label;
        return (
          // The tab and its close button are siblings: a button inside a
          // button is invalid, and the close control has to be reachable
          // (and visible) without hovering.
          // The underline sits on the wrapper so it spans the close button too.
          <div
            key={tab.id}
            className={`group flex items-center shrink-0 border-r border-r-surface-raised border-b-2 hover:bg-surface-hover ${
              isActive ? 'border-b-accent' : 'border-b-transparent'
            }`}
          >
            <Button
              variant="tab"
              size="sm"
              active={isActive}
              // `aria-pressed`, not `aria-selected`: these look like tabs but the
              // strip is not a `tablist`, the panes are not `tabpanel`s, and there
              // is no roving tab stop or arrow-key movement. `aria-selected` is
              // only defined on `option`/`tab`/`row`/`gridcell`, so on a bare
              // button it is a false promise *and* invalid ARIA. Same reasoning as
              // the answer pills in InteractiveQuestionCard.
              aria-pressed={isActive}
              onClick={() => onFocus(tab.id)}
              className="gap-1.5 pl-3 pr-1 py-2 border-b-0"
            >
              <TabIcon tab={tab} />
              <span className="truncate max-w-[100px]">{label}</span>
              {tab.type === 'agents' && liveAgents > 0 && (
                <span className="text-2xs tabular-nums text-text-faint">{liveAgents}</span>
              )}
              {!OWN_HEADER.has(tab.type) && tab.status !== 'running' && tab.completedAt && (
                <span className="text-2xs text-text-faint">{formatElapsed(tab.startedAt, tab.completedAt)}</span>
              )}
            </Button>
            <IconButton
              size="xs"
              label={`Close ${label}`}
              onClick={() => onClose(tab.id)}
              className="mr-1.5 text-text-faint"
            >
              <X size={10} />
            </IconButton>
          </div>
        );
      })}
    </div>
  );
}

/** A tab's icon: an agent's live status, a running spinner, or the tab's identity. */
function TabIcon({ tab }: { tab: PanelTab }) {
  const agent = useAgentStore(s => (tab.type === 'agent' && tab.agentId ? s.agents[tab.agentId] : undefined));
  if (tab.type === 'agent' && agent) {
    const visual = statusVisual(agent.status, agent.attention);
    return <visual.Icon size={11} className={visual.className} aria-hidden="true" />;
  }
  const { Icon, className } = identityVisual(tab.type === 'agents' ? 'agents' : tab.subagentType);
  if (tab.status === 'running' && !OWN_HEADER.has(tab.type)) {
    return <Loader2 size={11} className={`motion-safe:animate-spin ${className}`} aria-hidden="true" />;
  }
  return <Icon size={11} className={tab.isError ? 'text-hue-red' : className} aria-hidden="true" />;
}

// ------------------------------------------------------------------ //
//  TabHeader — icon + label + description + close                      //
// ------------------------------------------------------------------ //

function TabHeader({ tab, onClose }: { tab: PanelTab; onClose: () => void }) {
  const { Icon, className: color } = identityVisual(tab.subagentType);

  return (
    <div className="flex items-center justify-between px-4 py-2.5 border-b border-border-subtle bg-bg shrink-0">
      <div className="flex items-center gap-2 min-w-0">
        {tab.status === 'running'
          ? <Loader2 size={14} className={`animate-spin shrink-0 ${color}`} />
          : <Icon size={14} className={`shrink-0 ${tab.isError ? 'text-hue-red' : color}`} />
        }
        <span className="text-sm font-medium text-text-secondary">{tab.label}</span>
        {tab.description && (
          <span className="text-xs text-text-faint truncate">{tab.description}</span>
        )}
        {tab.model && (
          <span className="text-2xs text-text-faint shrink-0">{tab.model}</span>
        )}
        {tab.status === 'running' && (
          <span className="text-2xs text-text-faint shrink-0">
            <ElapsedTimer startedAt={tab.startedAt} />
          </span>
        )}
      </div>
      <IconButton label="Close the panel" size="xs" onClick={onClose}>
        <X size={14} />
      </IconButton>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  TabContent — live blocks + markdown body                            //
// ------------------------------------------------------------------ //

function TabContent({ tab, containerRef }: { tab: PanelTab; containerRef: React.RefObject<HTMLDivElement | null> }) {
  const cursorColor = tab.type === 'plan' ? 'bg-hue-amber' : 'bg-link';
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
            textClassName="text-sm my-1"
          />
        </div>
      )}

      {/* Separator between blocks and final content */}
      {hasBlocks && tab.content && (
        <div className="border-t border-border-subtle my-3" />
      )}

      {tab.content ? (
        <div className="text-sm">
          <MarkdownContent content={tab.content} />
          {tab.streaming && (
            <span className={`streaming-cursor inline-block w-1.5 h-4 ${cursorColor} ml-0.5 align-text-bottom`} />
          )}
        </div>
      ) : tab.streaming && !hasBlocks ? (
        <div className="flex items-center gap-2 text-sm text-text-dim pt-4">
          <Loader2 size={14} className="animate-spin" />
          {tab.type === 'plan' ? 'Planning...' : `${tab.label} working...`}
        </div>
      ) : !hasBlocks ? (
        <div className="text-sm text-text-faint">No content</div>
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
        <Button size="sm" onClick={handleDecline} className="py-1 rounded-md font-medium">
          <Ban size={12} />
          Decline
        </Button>
      )}
      <Button variant="success" size="sm" onClick={handleApprove} className="py-1">
        <Check size={12} />
        Approve
      </Button>
    </div>
  );
}

// ------------------------------------------------------------------ //
//  PanelBody — the content of the active tab                           //
// ------------------------------------------------------------------ //

function PanelBody({ tab, containerRef }: { tab: PanelTab; containerRef: React.RefObject<HTMLDivElement | null> }) {
  switch (tab.type) {
    case 'files':
      return <FileChangesPanel />;
    case 'workflow':
      return <WorkflowPanel tab={tab} />;
    case 'agents':
      return <AgentsPanel />;
    case 'agent':
      return tab.agentId ? <AgentTranscript key={tab.agentId} agentId={tab.agentId} /> : null;
    default:
      return <TabContent tab={tab} containerRef={containerRef} />;
  }
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
  const mobile = useIsMobile();

  const activeTab = panels.find(p => p.id === activePanelId) || panels[0] || null;
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  // Hiding the panel with focus inside it (Mod+Shift+E, Mod+\, a header's ×)
  // would strand focus in content nobody can see: hand it to the composer.
  useEffect(() => {
    if (panelVisible) return;
    if (panelRef.current?.contains(document.activeElement)) {
      document.getElementById('nerve-chat-input')?.focus();
    }
  }, [panelVisible]);

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

  // A phone has no room to split the viewport: at 412px a 45% panel leaves the
  // transcript an unreadable sliver. Cover it instead, and dismiss the same way
  // as on desktop — via the tab header's close button.
  if (mobile) {
    return (
      // absolute, not fixed: this covers the chat column (ChatPage's root is
      // the positioned ancestor), so the bottom nav stays visible and usable
      // underneath it. Fixed inset-0 covered the nav too, leaving the close
      // button as the only way out of the panel.
      // overflow-hidden matters as much as the positioning: without it the
      // `flex-1 overflow-y-auto` content region grows past the panel (flex
      // items default to min-height:auto) and spills over the bottom nav
      // instead of scrolling inside.
      // It stops being a modal here — the nav outside it is meant to stay
      // reachable — so it is a named region rather than a dialog, and what it
      // covers is made inert by ChatPage instead of trapped by this panel.
      <div
        ref={panelRef}
        role="region"
        aria-label={activeTab.label || 'Panel'}
        className={`side-panel absolute inset-0 z-30 flex flex-col overflow-hidden bg-bg-sunken ${isOpen ? '' : 'hidden'}`}
      >
        {showTabs && (
          <TabBar
            panels={panels}
            activeId={activePanelId}
            onFocus={focusPanelTab}
            onClose={closePanelTab}
          />
        )}
        {!OWN_HEADER.has(activeTab.type) && <TabHeader tab={activeTab} onClose={togglePanel} />}
        <PanelBody tab={activeTab} containerRef={containerRef} />
        {activeTab.type === 'plan' && <PlanActions tab={activeTab} />}
      </div>
    );
  }

  return (
    <div
      ref={panelRef}
      // Collapsed to no width rather than unmounted (for the slide): keep Tab
      // out of it while it is hidden.
      inert={isOpen ? undefined : true}
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

      {/* Header for active tab (the roster and transcripts draw their own) */}
      {!OWN_HEADER.has(activeTab.type) && <TabHeader tab={activeTab} onClose={togglePanel} />}

      {/* Content area */}
      <PanelBody tab={activeTab} containerRef={containerRef} />

      {/* Footer actions (approve/decline for plans) */}
      {activeTab.type === 'plan' && <PlanActions tab={activeTab} />}
    </div>
  );
}
