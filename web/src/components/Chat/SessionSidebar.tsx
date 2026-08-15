import { useState, useMemo, useRef, useEffect, useCallback, useLayoutEffect } from 'react';
import { Link } from 'react-router-dom';
import { Plus, X, MessageSquare, ChevronRight, ChevronDown, Bot, Loader2, Search, Hammer, MoreHorizontal, Star, StarFilled, Pencil, Trash2, Archive, Repeat } from '../ui/icons';
import { Button, IconButton, TextField } from '../ui';
import type { Session, AgentStatus } from '../../types/chat';
import { groupByDate, parseTimestamp } from '../../utils/dateGroups';
import { useChatStore } from '../../stores/chatStore';
import { useModalSurface } from '../../hooks/useModalSurface';
import { safeAreaInsets } from '../../utils/safeArea';

/** Strip leading '#' and 'Implement: ' prefixes from generated titles. */
function cleanTitle(session: Session): string {
  const raw = session.title || session.id;
  return raw.replace(/^#+\s*/, '').replace(/^Implement:\s*/i, '');
}

/** Check if this session is an async plan implementation. */
function isImplementSession(session: Session): boolean {
  return /^(#+\s*)?Implement:\s/i.test(session.title || '');
}

/** Format a date string as a short relative/absolute label. */
function formatShortDate(dateStr: string): string {
  const date = new Date(dateStr.includes('T') ? dateStr : dateStr.replace(' ', 'T') + 'Z');
  const now = new Date();
  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterdayStart = new Date(todayStart.getTime() - 86400000);

  if (date >= todayStart) {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
  if (date >= yesterdayStart) {
    return 'Yesterday';
  }
  return date.toLocaleDateString([], { month: 'short', day: 'numeric' });
}

// Which session groups (Running / Starred / date buckets) the user has
// collapsed, persisted across reloads. Keyed by the group's visible label,
// mirroring the quota-safe write-through pattern in helpers/draftStorage.ts —
// if localStorage is full or disabled the collapse state stays in memory only.
const COLLAPSED_GROUPS_KEY = 'nerve_sidebar_collapsed_groups';

function loadCollapsedGroups(): Set<string> {
  try {
    const raw = localStorage.getItem(COLLAPSED_GROUPS_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(arr) ? arr.filter((x: unknown): x is string => typeof x === 'string') : []);
  } catch {
    return new Set();
  }
}

function saveCollapsedGroups(groups: Set<string>): void {
  try {
    localStorage.setItem(COLLAPSED_GROUPS_KEY, JSON.stringify([...groups]));
  } catch { /* quota exceeded / disabled — keep the in-memory state only */ }
}

export function SessionSidebar({ sessions, activeSession, agentStatus, onCreate, onDelete, collapsed, mobile = false, onRequestClose }: {
  sessions: Session[];
  activeSession: string;
  agentStatus: AgentStatus;
  onCreate: () => void;
  onDelete: (id: string) => void;
  collapsed?: boolean;
  /** Render as an off-canvas drawer instead of an inline column. */
  mobile?: boolean;
  /** Drawer mode only — tapping the scrim asks the parent to close. */
  onRequestClose?: () => void;
}) {
  const [systemExpanded, setSystemExpanded] = useState(false);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(loadCollapsedGroups);
  const [localQuery, setLocalQuery] = useState('');
  const [searchHovered, setSearchHovered] = useState(false);
  const [searchFocused, setSearchFocused] = useState(false);
  const [searchMounted, setSearchMounted] = useState(false);
  const [searchVisible, setSearchVisible] = useState(false);
  // Programmatic mount trigger — set true when something (e.g. Cmd+K) wants
  // the search input visible without a mouse hover or focus event.
  const [searchPinned, setSearchPinned] = useState(false);
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const { searchResults, searchLoading, searchSessions, clearSearch, renameSession, toggleStar, archiveSession, virtualSession, discardVirtualSession, sidebarWidth, setSidebarWidth } = useChatStore();
  const searchFocusNonce = useChatStore(s => s.searchFocusNonce);

  // In drawer mode the list is a modal overlay: it needs focus, Tab
  // containment, Escape, and focus restoration. Declared before the search
  // effects below so that when Cmd+K opens the drawer and asks for search
  // focus in the same tick, the search input wins the race.
  const drawerOpen = mobile && !collapsed;
  const { dialogProps } = useModalSurface<HTMLDivElement>(drawerOpen, onRequestClose);

  // Opening a conversation should reveal it, so every row dismisses the
  // drawer. Leaving this to the parent's `activeSession` watcher isn't enough:
  // re-tapping the conversation that is already open never changes the route,
  // and the drawer would stay parked over the transcript.
  const handleSelect = mobile ? onRequestClose : undefined;

  // Drag-to-resize the session list. It is left-anchored against the nav rail,
  // so the width tracks the cursor 1:1. The width transition is disabled while
  // dragging so it stays responsive.
  const [isDragging, setIsDragging] = useState(false);
  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = sidebarWidth;
    const prevCursor = document.body.style.cursor;
    const prevSelect = document.body.style.userSelect;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    const handleMove = (ev: MouseEvent) => setSidebarWidth(startWidth + (ev.clientX - startX));
    const handleUp = () => {
      setIsDragging(false);
      document.body.style.cursor = prevCursor;
      document.body.style.userSelect = prevSelect;
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [sidebarWidth, setSidebarWidth]);

  const isSearching = localQuery.trim().length > 0;
  const shouldShowSearch = searchHovered || searchFocused || isSearching || searchPinned;

  // Mount/unmount the search input with a fade transition (200ms).
  useEffect(() => {
    if (shouldShowSearch) {
      if (closeTimerRef.current) {
        clearTimeout(closeTimerRef.current);
        closeTimerRef.current = null;
      }
      setSearchMounted(true);
    } else if (searchMounted) {
      setSearchVisible(false);
      closeTimerRef.current = setTimeout(() => {
        setSearchMounted(false);
        closeTimerRef.current = null;
      }, 200);
    }
  }, [shouldShowSearch, searchMounted]);

  // After mount, flip to visible on next frame so the CSS transition runs.
  useEffect(() => {
    if (searchMounted && !searchVisible) {
      const id = requestAnimationFrame(() => setSearchVisible(true));
      return () => cancelAnimationFrame(id);
    }
  }, [searchMounted, searchVisible]);

  // External "focus the search" request (e.g. Cmd+K). Pin the input so it
  // mounts; the focus effect below takes over once it's in the DOM.
  useEffect(() => {
    if (searchFocusNonce > 0) setSearchPinned(true);
  }, [searchFocusNonce]);

  // Once the pinned input is in the DOM, focus + select it. We focus as soon
  // as it's mounted (not after the fade-in) so the browser's focus event
  // races less; the CSS transition still runs for the visual fade.
  useEffect(() => {
    if (searchPinned && searchMounted && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [searchPinned, searchMounted]);

  // Release the pin only after onFocus has confirmed the focus landed —
  // dropping it earlier risks a brief render with pinned=false AND
  // focused=false, which collapses shouldShowSearch and fades the input out.
  useEffect(() => {
    if (searchPinned && searchFocused) setSearchPinned(false);
  }, [searchPinned, searchFocused]);

  // Clean up pending close timer on unmount.
  useEffect(() => {
    return () => {
      if (closeTimerRef.current) clearTimeout(closeTimerRef.current);
    };
  }, []);

  // Debounced search
  const handleSearchChange = useCallback((value: string) => {
    setLocalQuery(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (!value.trim()) {
      clearSearch();
      return;
    }
    debounceRef.current = setTimeout(() => {
      searchSessions(value);
    }, 300);
  }, [searchSessions, clearSearch]);

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  // Escape key clears search
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isSearching) {
        setLocalQuery('');
        clearSearch();
        inputRef.current?.blur();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isSearching, clearSearch]);

  const { conversations, systemSessions } = useMemo(() => {
    // External = Codex/Claude-Code/Cursor satellite sessions (MCP server +
    // Codex thread sync). Live alongside web/telegram conversations.
    const convos = sessions.filter(
      s => s.source === 'web' || s.source === 'telegram' || s.source === 'api' || s.source === 'external',
    );
    const system = sessions.filter(s => s.source === 'cron' || s.source === 'hook');
    return { conversations: convos, systemSessions: system };
  }, [sessions]);

  const activeIsRunning = agentStatus.state !== 'idle';

  // Split running and starred conversations into their pinned groups at the
  // top. Within every group the order is purely updated_at — which means
  // "last message activity" (opening/starring a chat doesn't bump it), so
  // browsing never reshuffles the list. Sort explicitly rather than trusting
  // API array order to keep that invariant regardless of fetch shape.
  const { pinnedRunning, pinnedStarred, restConversations } = useMemo(() => {
    const running: Session[] = [];
    const starred: Session[] = [];
    const rest: Session[] = [];
    for (const s of conversations) {
      const isRunning = s.id === activeSession ? activeIsRunning : !!s.is_running;
      if (isRunning) running.push(s);
      else if (s.starred) starred.push(s);
      else rest.push(s);
    }
    const byUpdatedDesc = (a: Session, b: Session) =>
      parseTimestamp(b.updated_at).getTime() - parseTimestamp(a.updated_at).getTime();
    starred.sort(byUpdatedDesc);
    rest.sort(byUpdatedDesc);
    return { pinnedRunning: running, pinnedStarred: starred, restConversations: rest };
  }, [conversations, activeSession, activeIsRunning]);

  const groupedConversations = useMemo(() => groupByDate(restConversations), [restConversations]);

  // Collapse/expand a session group (Running / Starred / a date bucket),
  // persisting the new set so it survives a reload.
  const toggleGroup = useCallback((label: string) => {
    setCollapsedGroups(prev => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      saveCollapsedGroups(next);
      return next;
    });
  }, []);

  // Never leave the active session hidden inside a collapsed group: when the
  // active session changes, expand whichever group holds it — once, so a later
  // manual collapse of that same group still sticks.
  const autoExpandedForRef = useRef<string | null>(null);
  useEffect(() => {
    if (!activeSession || autoExpandedForRef.current === activeSession) return;
    let label: string | null = null;
    if (pinnedRunning.some(s => s.id === activeSession)) label = 'Running';
    else if (pinnedStarred.some(s => s.id === activeSession)) label = 'Starred';
    else label = groupedConversations.find(g => g.items.some(s => s.id === activeSession))?.group ?? null;
    if (!label) return; // not located yet (sessions still loading) — retry on the next update
    const found = label;
    autoExpandedForRef.current = activeSession;
    setCollapsedGroups(prev => {
      if (!prev.has(found)) return prev;
      const next = new Set(prev);
      next.delete(found);
      saveCollapsedGroups(next);
      return next;
    });
  }, [activeSession, pinnedRunning, pinnedStarred, groupedConversations]);

  // Count running system sessions for the badge
  const runningSystemCount = useMemo(
    () => systemSessions.filter(s => s.is_running).length,
    [systemSessions],
  );

  // Auto-expand system section when something starts running
  useLayoutEffect(() => {
    if (runningSystemCount > 0 && !systemExpanded) {
      setSystemExpanded(true);
    }
  }, [runningSystemCount]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <>
      {/* Drawer scrim. Only in mobile mode, and only while open — a phone has
          no room for a persistent column, so the list sits above the
          transcript and the scrim is what dismisses it. */}
      {drawerOpen && (
        <div
          onClick={onRequestClose}
          className="fixed inset-0 z-40 bg-black/60 transition-opacity duration-200"
          aria-hidden="true"
        />
      )}
    <div
      {...(mobile ? dialogProps : {})}
      aria-label={mobile ? 'Conversations' : undefined}
      className={mobile
        ? `bg-surface border-r border-border-subtle flex flex-col overflow-hidden fixed inset-y-0 left-0 z-50 w-[85vw] max-w-[320px] transition-transform duration-200 outline-none ${collapsed ? '-translate-x-full' : 'translate-x-0'}`
        : `bg-surface border-r border-border-subtle flex flex-col shrink-0 overflow-hidden relative ${collapsed ? 'border-r-0' : ''} ${isDragging ? '' : 'transition-all duration-200'}`}
      // Fixed in drawer mode, so the shell's safe-area padding does not reach
      // it: without this its first controls sit under the status bar.
      style={mobile ? safeAreaInsets('left') : { width: collapsed ? 0 : sidebarWidth }}
      // Keep the closed drawer out of the tab order: it stays mounted so the
      // slide transition has something to animate, but it is off-canvas.
      inert={mobile && collapsed ? true : undefined}
    >
      {/* Drag-to-resize handle on the right edge (hidden when collapsed).
          Pointer-driven and mouse-only, so it has no place in drawer mode. */}
      {!collapsed && !mobile && (
        <div
          onMouseDown={handleResizeStart}
          className="group/resize absolute top-0 right-0 bottom-0 z-20 w-2 cursor-col-resize"
          title="Drag to resize the session list"
        >
          <div className={`absolute inset-y-0 right-0 w-px transition-colors ${isDragging ? 'bg-accent' : 'bg-transparent group-hover/resize:bg-accent/50'}`} />
        </div>
      )}

      {/* Search + New chat */}
      <div className="px-2 py-1.5 border-b border-border-subtle">
        <div className="relative h-7">
          {/* Search pill (always visible, hover-zone trigger) */}
          <Button
            variant="pill"
            size="xs"
            onMouseEnter={() => setSearchHovered(true)}
            onMouseLeave={() => setSearchHovered(false)}
            className="absolute left-0 top-1/2 -translate-y-1/2 h-6 pl-1.5 pr-2.5 z-10 border-border-subtle hover:bg-surface-hover"
          >
            <Search size={11} className="pointer-events-none" />
            <span>Search sessions</span>
          </Button>

          {/* New chat pill (hidden under input when open) */}
          <Button
            variant="pill"
            size="xs"
            onClick={() => { onCreate(); handleSelect?.(); }}
            title="New chat"
            className="absolute right-0 top-1/2 -translate-y-1/2 h-6 pl-1.5 pr-2.5 border-border-subtle hover:bg-surface-hover"
          >
            <Plus size={11} />
            <span>New chat</span>
          </Button>

          {searchMounted && (
            <>
              <TextField
                id="nerve-sidebar-search"
                fieldSize="sm"
                ref={inputRef}
                value={localQuery}
                onChange={e => handleSearchChange(e.target.value)}
                onFocus={() => setSearchFocused(true)}
                onBlur={() => setSearchFocused(false)}
                onMouseEnter={() => setSearchHovered(true)}
                onMouseLeave={() => setSearchHovered(false)}
                placeholder="Search sessions..."
                className={`absolute inset-0 h-full pl-7 pr-7 z-20 transition-all duration-200 ease-out ${
                  searchVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'
                }`}
              />
              {isSearching && (
                <IconButton
                  label="Clear the search"
                  size="xs"
                  onClick={() => { setLocalQuery(''); clearSearch(); }}
                  className={`absolute right-1.5 top-1/2 -translate-y-1/2 z-30 transition-opacity duration-200 ${
                    searchVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'
                  }`}
                >
                  <X size={12} />
                </IconButton>
              )}
            </>
          )}
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto">
        {/* Search results mode */}
        {isSearching ? (
          <div>
            {searchLoading && !searchResults && (
              <div className="flex items-center gap-2 px-3 py-3 text-xs text-text-faint">
                <Loader2 size={11} className="animate-spin" />
                Searching...
              </div>
            )}
            {searchResults && (
              <>
                <div className="px-3 py-1.5 text-2xs text-text-faint">
                  {searchResults.length} result{searchResults.length !== 1 ? 's' : ''}
                  {searchLoading && <Loader2 size={9} className="inline ml-1.5 animate-spin" />}
                </div>
                {searchResults.length === 0 ? (
                  <div className="px-3 py-2 text-xs text-text-faint">No matching sessions</div>
                ) : (
                  searchResults.map((s) => (
                    <SessionItem
                      key={s.id}
                      session={s}
                      isActive={s.id === activeSession}
                      isRunning={s.id === activeSession ? activeIsRunning : !!s.is_running}
                      onDelete={onDelete}
                      onRename={renameSession}
                      onToggleStar={toggleStar}
                      onArchive={archiveSession}
                      onSelect={handleSelect}
                      showDate
                    />
                  ))
                )}
              </>
            )}
          </div>
        ) : (
          <>
            {/* Virtual "new chat" — pinned at the very top until the first
                message materializes it server-side. */}
            {virtualSession && (
              <Link
                to={`/chat/${virtualSession.id}`}
                onClick={handleSelect}
                className={`group flex items-center gap-2 px-3 py-1.5 mx-1 mt-1 rounded-md cursor-pointer text-xs transition-colors no-underline
                  ${virtualSession.id === activeSession
                    ? 'bg-accent/10 text-text'
                    : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
                  }`}
              >
                <MessageSquare size={13} className="shrink-0 opacity-50" />
                <div className="flex-1 min-w-0">
                  <div className="truncate text-xs leading-tight italic">New chat</div>
                </div>
                {virtualSession.id === activeSession && activeIsRunning && (
                  <Loader2 size={12} className="shrink-0 text-accent animate-spin" />
                )}
                <IconButton
                  label="Discard new chat"
                  size="xs"
                  onClick={(e) => { e.preventDefault(); e.stopPropagation(); discardVirtualSession(); }}
                  className="opacity-0 group-hover:opacity-100 transition-opacity"
                >
                  <X size={13} />
                </IconButton>
              </Link>
            )}

            {/* Pinned running sessions */}
            {pinnedRunning.length > 0 && (
              <div>
                <GroupHeader
                  label="Running"
                  count={pinnedRunning.length}
                  collapsed={collapsedGroups.has('Running')}
                  tone="text-success/70"
                  onToggle={() => toggleGroup('Running')}
                />
                {!collapsedGroups.has('Running') && pinnedRunning.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                    onArchive={archiveSession}
                    onSelect={handleSelect}
                  />
                ))}
              </div>
            )}

            {/* Pinned starred sessions (ordered by last message activity —
                stable while browsing, since opening a chat doesn't bump it) */}
            {pinnedStarred.length > 0 && (
              <div>
                <GroupHeader
                  label="Starred"
                  count={pinnedStarred.length}
                  collapsed={collapsedGroups.has('Starred')}
                  tone="text-hue-yellow/70"
                  onToggle={() => toggleGroup('Starred')}
                />
                {!collapsedGroups.has('Starred') && pinnedStarred.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning={false}
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                    onArchive={archiveSession}
                    onSelect={handleSelect}
                  />
                ))}
              </div>
            )}

            {/* Normal date-grouped view */}
            {groupedConversations.length === 0 && pinnedRunning.length === 0 && pinnedStarred.length === 0 && !virtualSession && (
              <div className="px-3 py-2 text-xs text-text-faint">No conversations yet</div>
            )}

            {groupedConversations.map(({ group, items }) => (
              <div key={group}>
                <GroupHeader
                  label={group}
                  count={items.length}
                  collapsed={collapsedGroups.has(group)}
                  onToggle={() => toggleGroup(group)}
                />
                {!collapsedGroups.has(group) && items.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning={s.id === activeSession ? activeIsRunning : !!s.is_running}
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                    onArchive={archiveSession}
                    onSelect={handleSelect}
                  />
                ))}
              </div>
            ))}

            {/* System sessions */}
            {systemSessions.length > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <Button
                  variant="subtle"
                  size="sm"
                  fullWidth
                  onClick={() => setSystemExpanded(!systemExpanded)}
                  aria-expanded={systemExpanded}
                  className="justify-start gap-1.5 px-3 py-1.5 rounded-none text-left"
                >
                  {systemExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Bot size={10} className="text-text-faint" />
                  <span className="text-2xs uppercase tracking-wider text-text-faint font-medium">
                    System ({systemSessions.length})
                  </span>
                  {runningSystemCount > 0 && (
                    <span className="ml-auto flex items-center gap-1 text-2xs text-hue-emerald">
                      <span className="relative flex h-1.5 w-1.5">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75" />
                        <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-current" />
                      </span>
                      {runningSystemCount}
                    </span>
                  )}
                </Button>

                {systemExpanded && systemSessions.map((s) => (
                  <Link
                    key={s.id}
                    to={`/chat/${s.id}`}
                    onClick={handleSelect}
                    className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-xs leading-tight transition-colors no-underline
                      ${s.id === activeSession
                        ? 'bg-accent/10 text-text-muted'
                        : 'text-text-faint hover:bg-surface-raised hover:text-text-muted'
                      }`}
                  >
                    <Bot size={11} className="shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="truncate">{cleanTitle(s)}</div>
                    </div>
                    <StatusIndicator
                      session={s}
                      isActive={s.id === activeSession}
                      isRunning={s.id === activeSession ? activeIsRunning : !!s.is_running}
                    />
                  </Link>
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
    </>
  );
}


/** Collapsable session-group header: chevron + label, with a hidden-count hint when collapsed. */
function GroupHeader({ label, count, collapsed, tone, onToggle }: {
  label: string;
  count: number;
  collapsed: boolean;
  tone?: string;
  onToggle: () => void;
}) {
  return (
    <Button
      variant="subtle"
      size="xs"
      fullWidth
      onClick={onToggle}
      aria-expanded={!collapsed}
      className="justify-start gap-1 px-3 pt-2 pb-0.5 rounded-none text-left"
    >
      {collapsed
        ? <ChevronRight size={10} className="shrink-0 text-text-faint" />
        : <ChevronDown size={10} className="shrink-0 text-text-faint" />
      }
      <span className={`text-2xs font-medium ${tone ?? 'text-text-faint'}`}>{label}</span>
      {collapsed && count > 0 && (
        <span className="text-2xs text-text-faint/60 tabular-nums">{count}</span>
      )}
    </Button>
  );
}


/** Sidebar icon tint for review-loop observer sessions, by loop status. */
function reviewLoopIconTone(status: string): string {
  switch (status) {
    case 'passed': return 'text-success/80';
    case 'awaiting_user': return 'text-warning/90';
    case 'failed':
    case 'killed': return 'text-error/70';
    default: return 'text-success/80';  // pending / implementing / verifying
  }
}


/** Pulsing dot for running sessions, solid dot for other notable states. */
function StatusIndicator({ session, isActive, isRunning }: {
  session: Session;
  isActive: boolean;
  isRunning: boolean;
}) {
  // Waiting for user input (AskUserQuestion / plan mode): pulsing blue dot.
  // Takes priority over the running spinner/dot — the session is paused, not
  // working, and needs the user's attention.
  if (session.awaiting_input) {
    return (
      <span className="relative flex h-2 w-2 shrink-0" title="Waiting for your input">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-hue-blue opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-hue-blue" />
      </span>
    );
  }

  // Review loop parked on a decision: pulsing orange dot — same "needs you"
  // urgency class as awaiting_input.
  if (session.review_loop?.status === 'awaiting_user') {
    return (
      <span className="relative flex h-2 w-2 shrink-0" title="Review loop needs your decision">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-hue-orange opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-hue-orange" />
      </span>
    );
  }

  // Review loop leg working: the observer session itself is idle (legs run
  // in their own workflow sessions), so surface the loop's activity here.
  const loopLive = session.review_loop
    && ['pending', 'implementing', 'verifying'].includes(session.review_loop.status);
  if (loopLive && !isRunning) {
    return (
      <span className="relative flex h-2 w-2 shrink-0" title={`Review loop ${session.review_loop!.status}`}>
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-hue-emerald opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-hue-emerald" />
      </span>
    );
  }

  // Active + running: spinner
  if (isActive && isRunning) {
    return <Loader2 size={12} className="shrink-0 text-accent animate-spin" />;
  }

  // Non-active but running: pulsing green dot
  if (isRunning) {
    return (
      <span className="relative flex h-2 w-2 shrink-0">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-hue-emerald opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-hue-emerald" />
      </span>
    );
  }

  // Error state: solid red
  if (session.status === 'error') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-hue-red" />;
  }

  // Stopped: solid yellow
  if (session.status === 'stopped') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-hue-yellow" />;
  }

  // Idle / created / active-but-not-running: no indicator (reduces noise)
  return null;
}


function SessionItem({ session, isActive, isRunning, onDelete, onRename, onToggleStar, onArchive, onSelect, showDate }: {
  session: Session;
  isActive: boolean;
  isRunning: boolean;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => Promise<void>;
  onToggleStar: (id: string) => Promise<void>;
  onArchive: (id: string) => Promise<void>;
  /** Fired when the row itself is opened (not its menu) — drawer mode uses it to close. */
  onSelect?: () => void;
  showDate?: boolean;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState('');
  const menuRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  // Unsent draft for this chat (hidden on the active one — its text is in the box).
  const hasDraft = useChatStore(s => !!(s.drafts[session.id] || '').trim());

  // Close menu on outside click
  useEffect(() => {
    if (!menuOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [menuOpen]);

  // Focus input when renaming
  useEffect(() => {
    if (renaming) inputRef.current?.focus();
  }, [renaming]);

  const handleRenameSubmit = () => {
    const trimmed = renameValue.trim();
    if (trimmed && trimmed !== cleanTitle(session)) {
      onRename(session.id, trimmed);
    }
    setRenaming(false);
  };

  if (renaming) {
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md bg-surface-raised">
        <MessageSquare size={13} className="shrink-0 opacity-50" />
        <TextField
          bare
          fullWidth={false}
          ref={inputRef}
          value={renameValue}
          onChange={e => setRenameValue(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') handleRenameSubmit();
            if (e.key === 'Escape') setRenaming(false);
          }}
          onBlur={handleRenameSubmit}
          aria-label="Rename this session"
          className="flex-1 min-w-0 text-xs border-b border-text-faint"
        />
      </div>
    );
  }

  return (
    <Link
      to={`/chat/${session.id}`}
      onClick={onSelect}
      className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-xs transition-colors no-underline
        ${isActive
          ? 'bg-accent/10 text-text'
          : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
        }`}
    >
      {session.review_loop
        ? <Repeat size={13} className={`shrink-0 ${reviewLoopIconTone(session.review_loop.status)}`} />
        : isImplementSession(session)
        ? <Hammer size={13} className="shrink-0 text-hue-cyan/70" />
        : <MessageSquare size={13} className="shrink-0 opacity-50" />
      }
      <div className="flex-1 min-w-0">
        <div className="truncate text-xs leading-tight">{cleanTitle(session)}</div>
      </div>

      {/* Unsent draft marker */}
      {hasDraft && !isActive && (
        <span title="Unsent draft" className="shrink-0 flex items-center">
          <Pencil size={11} className="text-text-faint" />
        </span>
      )}

      {/* Status indicator (always visible) */}
      <StatusIndicator session={session} isActive={isActive} isRunning={isRunning} />

      {/* Date label in search results */}
      {showDate && !isRunning && (
        <span className="shrink-0 text-2xs text-text-faint tabular-nums">
          {formatShortDate(session.updated_at)}
        </span>
      )}

      {/* Menu trigger: starred → show star, on hover → three dots; unstarred → three dots on hover */}
      <div className="relative shrink-0" ref={menuRef}>
        <button
          type="button"
          onClick={(e) => { e.preventDefault(); e.stopPropagation(); setMenuOpen(!menuOpen); }}
          title="Session actions"
          aria-label={`Actions for ${cleanTitle(session)}`}
          aria-expanded={menuOpen}
          aria-haspopup="menu"
          className={`p-0.5 cursor-pointer transition-opacity ${
            session.starred
              ? 'text-hue-yellow opacity-100 [&>*:first-child]:block [&>*:last-child]:hidden hover:[&>*:first-child]:hidden hover:[&>*:last-child]:block hover:text-text-muted'
              : 'text-border-subtle opacity-0 group-hover:opacity-100 hover:text-text-muted'
          }`}
        >
          {session.starred ? (
            <>
              <StarFilled size={13} className="text-hue-yellow" />
              <MoreHorizontal size={14} />
            </>
          ) : (
            <MoreHorizontal size={14} />
          )}
        </button>

        {menuOpen && (
          <div className="absolute right-0 top-full mt-1 z-50 bg-surface-raised border border-border-subtle rounded-lg shadow-xl py-1 min-w-[140px]">
            <Button
              variant="subtle"
              size="sm"
              fullWidth
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                onToggleStar(session.id);
                setMenuOpen(false);
              }}
              className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              {session.starred
                ? <StarFilled size={14} className="text-hue-yellow" />
                : <Star size={14} />}
              {session.starred ? 'Unstar' : 'Star'}
            </Button>
            <Button
              variant="subtle"
              size="sm"
              fullWidth
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setRenameValue(cleanTitle(session));
                setRenaming(true);
                setMenuOpen(false);
              }}
              className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              <Pencil size={14} />
              Rename
            </Button>
            <Button
              variant="subtle"
              size="sm"
              fullWidth
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setMenuOpen(false);
                onArchive(session.id);
              }}
              className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              <Archive size={14} />
              Archive
            </Button>
            <div className="border-t border-border my-1" />
            <Button
              variant="dangerGhost"
              size="sm"
              fullWidth
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setMenuOpen(false);
                onDelete(session.id);
              }}
              className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              <Trash2 size={14} />
              Delete
            </Button>
          </div>
        )}
      </div>
    </Link>
  );
}
