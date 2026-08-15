import { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { Plus, X, MessageSquare, ChevronRight, ChevronDown, Bot, Loader2, Search, Hammer, MoreHorizontal, Star, Pencil, Trash2, Archive, ArchiveRestore, Repeat, Unlink } from '../ui/icons';
import { Button, IconButton, TextField } from '../ui';
import type { Session, AgentStatus } from '../../types/chat';
import { groupByDate, parseTimestamp, loadCollapsedGroups, saveCollapsedGroups, loadExpandedParents, saveExpandedParents } from '../../utils/dateGroups';
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

// Collapsed-group persistence (Running / Starred / date buckets, keyed by the
// group's visible label) lives in utils/dateGroups next to the bucket
// taxonomy and its default-collapsed set.

/** Order by last message activity, newest first (opening/starring doesn't bump it). */
const byUpdatedDesc = (a: Session, b: Session) =>
  parseTimestamp(b.updated_at).getTime() - parseTimestamp(a.updated_at).getTime();

/**
 * Parked: no turn in flight, but the session is not done either — a wake-up is
 * scheduled, or a background task is still running inside its CLI. Such a
 * session counts as running (it stays in the pinned "Running" group and never
 * falls back to idle); only its dot differs, so "working now" still reads
 * apart from "will pick itself back up".
 */
const isParked = (s: Session) => !!s.pending_wakeup_at || !!s.has_background_tasks;

/** Drag-to-nest wiring shared by every draggable session row. */
type RowDnd = {
  draggingId: string | null;
  dragOverId: string | null;
  onDragStart: (id: string) => void;
  onDragEnd: () => void;
  onDragOver: (id: string) => void;
  onDragLeave: (id: string) => void;
  onDrop: (id: string) => void;
};

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
  // Archived group: collapsed by default and NOT persisted (mirrors System), so every reload starts collapsed and fetches nothing until expanded.
  const [archivedExpanded, setArchivedExpanded] = useState(false);
  // Collapsed-group persistence (Running / Starred / date buckets, keyed by visible label) lives in utils/dateGroups.
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(loadCollapsedGroups);
  // Per-session expand state for parent→children nesting (persisted, keyed by
  // parent id). Drag state for the nest gesture: draggingId = the row being
  // dragged, dragOverId = the row it's hovering (the drop target highlight),
  // rootDropActive = the "remove from parent" strip is hovered.
  const [expandedParents, setExpandedParents] = useState<Set<string>>(loadExpandedParents);
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const [dragOverId, setDragOverId] = useState<string | null>(null);
  const [rootDropActive, setRootDropActive] = useState(false);
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

  const { searchResults, searchLoading, searchSessions, clearSearch, renameSession, toggleStar, archiveSession, setSessionParent, virtualSession, discardVirtualSession, sidebarWidth, setSidebarWidth, sessionsHasMore, loadMoreSessions, archivedSessions, archivedCount, archivedLoading, archivedHasMore, loadArchivedSessions, clearArchivedSessions, unarchiveSession, starArchivedSession, systemSessions, systemCount, systemLoading, systemHasMore, loadSystemSessions, clearSystemSessions } = useChatStore();
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

  // Main feed = whatever the server sent (already excludes archived + system sources); no client-side source whitelist, so an unknown source lands in the feed rather than nowhere.
  const conversations = sessions;

  const activeIsRunning = agentStatus.state !== 'idle';

  // Split running and starred conversations into their pinned groups at the
  // top. Within every group the order is purely updated_at — which means
  // "last message activity" (opening/starring a chat doesn't bump it), so
  // browsing never reshuffles the list. Sort explicitly rather than trusting
  // API array order to keep that invariant regardless of fetch shape.
  // Nest children under their parent: a session whose parent_session_id resolves
  // to another session in the feed renders only beneath that parent (recursively,
  // to whatever depth the data carries — no client-side cap). A session whose
  // parent isn't in the feed (not returned under the page limit, archived, or
  // deleted) stays top-level, so the client draws exactly the hierarchy the
  // server sent and nothing more.
  const { childrenByParent, topLevel } = useMemo(() => {
    const byId = new Map(conversations.map(s => [s.id, s]));
    const kids = new Map<string, Session[]>();
    const top: Session[] = [];
    for (const s of conversations) {
      const pid = s.parent_session_id;
      if (pid && pid !== s.id && byId.has(pid)) {
        const arr = kids.get(pid);
        if (arr) arr.push(s); else kids.set(pid, [s]);
      } else {
        top.push(s);
      }
    }
    for (const arr of kids.values()) arr.sort(byUpdatedDesc);
    return { childrenByParent: kids, topLevel: top };
  }, [conversations]);

  // Split TOP-LEVEL conversations into pinned Running / Starred / rest. Children
  // never appear here — they ride under their parent wherever it renders,
  // including inside the pinned Starred group.
  const { pinnedRunning, pinnedStarred, restConversations } = useMemo(() => {
    const running: Session[] = [];
    const starred: Session[] = [];
    const rest: Session[] = [];
    for (const s of topLevel) {
      const isRunning = s.id === activeSession ? activeIsRunning : !!s.is_running;
      // Parked sessions count as running: work is still pending, it just
      // isn't executing this second.
      if (isRunning || isParked(s)) running.push(s);
      else if (s.starred) starred.push(s);
      else rest.push(s);
    }
    starred.sort(byUpdatedDesc);
    rest.sort(byUpdatedDesc);
    return { pinnedRunning: running, pinnedStarred: starred, restConversations: rest };
  }, [topLevel, activeSession, activeIsRunning]);

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

  // Expand/collapse one parent's children, persisting so it survives reload.
  const toggleExpandParent = useCallback((id: string) => {
    setExpandedParents(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      saveExpandedParents(next);
      return next;
    });
  }, []);

  const handleRemoveParent = useCallback((id: string) => {
    setSessionParent(id, null);
  }, [setSessionParent]);

  // Drop a dragged row ONTO a target row → nest it under the target and reveal
  // it. No-op on self; the server rejects self/cycle links and the store's
  // optimistic update reverts on rejection.
  const handleRowDrop = useCallback((targetId: string) => {
    const src = draggingId;
    setDragOverId(null);
    setDraggingId(null);
    setRootDropActive(false);
    if (!src || src === targetId) return;
    setSessionParent(src, targetId);
    setExpandedParents(prev => {
      if (prev.has(targetId)) return prev;
      const next = new Set(prev);
      next.add(targetId);
      saveExpandedParents(next);
      return next;
    });
  }, [draggingId, setSessionParent]);

  // Drop onto the "remove from parent" strip → clear the parent (→ top-level).
  const handleUnparentDrop = useCallback(() => {
    const src = draggingId;
    setRootDropActive(false);
    setDragOverId(null);
    setDraggingId(null);
    if (src) setSessionParent(src, null);
  }, [draggingId, setSessionParent]);

  const dnd: RowDnd = {
    draggingId,
    dragOverId,
    onDragStart: (id) => setDraggingId(id),
    onDragEnd: () => { setDraggingId(null); setDragOverId(null); setRootDropActive(false); },
    onDragOver: (id) => setDragOverId(id),
    onDragLeave: (id) => setDragOverId(cur => (cur === id ? null : cur)),
    onDrop: handleRowDrop,
  };

  // The un-parent drop strip shows only while dragging a row that HAS a parent.
  const draggedHasParent = !!(draggingId && conversations.find(s => s.id === draggingId)?.parent_session_id);

  // One top-level feed row + its nested subtree (Running / Starred / date
  // buckets all render through this so nesting + drag behave identically).
  const renderTree = (s: Session) => (
    <SessionTree
      key={s.id}
      session={s}
      depth={0}
      childrenByParent={childrenByParent}
      expandedParents={expandedParents}
      onToggleExpand={toggleExpandParent}
      activeSession={activeSession}
      activeIsRunning={activeIsRunning}
      dnd={dnd}
      onDelete={onDelete}
      onRename={renameSession}
      onToggleStar={toggleStar}
      onArchive={archiveSession}
      onRemoveParent={handleRemoveParent}
      onSelect={handleSelect}
    />
  );

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

  // Never leave the active session hidden inside a collapsed parent: expand its
  // whole ancestor chain when it (or the feed) changes. The seen-set guards any
  // pre-existing cycle so the walk always ends.
  useEffect(() => {
    if (!activeSession) return;
    const byId = new Map(conversations.map(s => [s.id, s]));
    const toOpen: string[] = [];
    const seen = new Set<string>();
    let cur = byId.get(activeSession)?.parent_session_id;
    while (cur && byId.has(cur) && !seen.has(cur)) {
      seen.add(cur);
      toOpen.push(cur);
      cur = byId.get(cur)?.parent_session_id;
    }
    if (toOpen.length === 0) return;
    setExpandedParents(prev => {
      if (toOpen.every(id => prev.has(id))) return prev;
      const next = new Set(prev);
      toOpen.forEach(id => next.add(id));
      saveExpandedParents(next);
      return next;
    });
  }, [activeSession, conversations]);

  // (System sessions load lazily now — no running-count badge / auto-expand.)

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

      {/* overflow-x-hidden: this list only ever scrolls vertically. Rows
          truncate, so anything sticking out sideways is a layout slip, not
          content the user needs to reach by scrolling. */}
      <div className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden">
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
                className={`group flex items-center gap-2 px-3 py-1.5 mx-1 mt-1 rounded-md cursor-pointer text-sm transition-colors no-underline
                  ${virtualSession.id === activeSession
                    ? 'bg-accent/10 text-text'
                    : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
                  }`}
              >
                <MessageSquare size={13} className="shrink-0 opacity-50" />
                <div className="flex-1 min-w-0">
                  <div className="truncate text-sm leading-tight italic">New chat</div>
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
                {!collapsedGroups.has('Running') && pinnedRunning.map(renderTree)}
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
                {!collapsedGroups.has('Starred') && pinnedStarred.map(renderTree)}
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
                {!collapsedGroups.has(group) && items.map(renderTree)}
              </div>
            ))}

            {/* Feed page window exhausted — never truncate silently. */}
            {sessionsHasMore && <MoreRow onClick={loadMoreSessions} />}

            {/* Un-parent target: drop a nested row here to make it top-level.
                Shown only while dragging a row that has a parent — a drop lands
                here (not on a row) purely by event bubbling, no position math. */}
            {draggingId && draggedHasParent && (
              <div
                onDragOver={(e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; setRootDropActive(true); }}
                onDragLeave={() => setRootDropActive(false)}
                onDrop={(e) => { e.preventDefault(); e.stopPropagation(); handleUnparentDrop(); }}
                className={`mx-1 my-1 px-3 py-2 rounded-md border border-dashed text-[11px] text-center transition-colors ${
                  rootDropActive
                    ? 'border-accent text-accent bg-accent/10'
                    : 'border-border-subtle text-text-faint'
                }`}
              >
                Drop here to remove from parent
              </div>
            )}

            {/* System sessions (cron/hook) — lazy: nothing fetched until expanded, dropped on collapse, so the next expand repeats the identical request. */}
            {systemCount > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <Button
                  variant="subtle"
                  size="sm"
                  fullWidth
                  onClick={() => {
                    const next = !systemExpanded;
                    setSystemExpanded(next);
                    if (next) loadSystemSessions();
                    else clearSystemSessions();
                  }}
                  aria-expanded={systemExpanded}
                  className="justify-start gap-1.5 px-3 py-1.5 rounded-none text-left"
                >
                  {systemExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Bot size={10} className="text-text-faint" />
                  <span className="text-2xs uppercase tracking-wider text-text-faint font-medium">
                    System ({systemCount})
                  </span>
                </Button>

                {systemExpanded && (
                  <>
                    {systemLoading && systemSessions === null && (
                      <div className="flex items-center gap-2 px-3 py-2 text-xs text-text-faint">
                        <Loader2 size={11} className="animate-spin" />
                        Loading...
                      </div>
                    )}
                    {systemSessions !== null && systemSessions.length === 0 && (
                      <div className="px-3 py-2 text-xs text-text-faint">No system sessions</div>
                    )}
                    {systemSessions !== null && systemSessions.map((s) => (
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
                    {systemHasMore && <MoreRow onClick={() => loadSystemSessions(true)} />}
                  </>
                )}
              </div>
            )}

            {/* Archived sessions — lazy, mirror of System: fetched on expand, dropped on collapse. Rendered last, collapsed by default. */}
            {archivedCount > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <Button
                  variant="subtle"
                  size="sm"
                  fullWidth
                  onClick={() => {
                    const next = !archivedExpanded;
                    setArchivedExpanded(next);
                    if (next) loadArchivedSessions();
                    else clearArchivedSessions();
                  }}
                  aria-expanded={archivedExpanded}
                  className="justify-start gap-1.5 px-3 py-1.5 rounded-none text-left"
                >
                  {archivedExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Archive size={10} className="text-text-faint" />
                  <span className="text-2xs uppercase tracking-wider text-text-faint font-medium">
                    Archived ({archivedCount})
                  </span>
                </Button>

                {archivedExpanded && (
                  <>
                    {archivedLoading && archivedSessions === null && (
                      <div className="flex items-center gap-2 px-3 py-2 text-xs text-text-faint">
                        <Loader2 size={11} className="animate-spin" />
                        Loading...
                      </div>
                    )}
                    {archivedSessions !== null && archivedSessions.length === 0 && (
                      <div className="px-3 py-2 text-xs text-text-faint">No archived sessions</div>
                    )}
                    {archivedSessions !== null && archivedSessions.map((s) => (
                      <SessionItem
                        key={s.id}
                        session={s}
                        isActive={s.id === activeSession}
                        isRunning={false}
                        onDelete={onDelete}
                        onRename={renameSession}
                        onToggleStar={toggleStar}
                        onArchive={archiveSession}
                        onUnarchive={unarchiveSession}
                        onStarArchived={starArchivedSession}
                        onSelect={handleSelect}
                        archived
                        showDate
                      />
                    ))}
                    {archivedHasMore && <MoreRow onClick={() => loadArchivedSessions(true)} />}
                  </>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
    </>
  );
}


/** '...' row: pulls the next page of a list that the page window cut short. */
function MoreRow({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      title="Load more"
      // Width has to leave room for its own margins: plain `w-full` + `mx-1`
      // is 100% + 8px, which overflows the list and puts a horizontal
      // scrollbar under the whole panel (mx-1 = 0.25rem a side).
      className="w-[calc(100%-0.5rem)] px-3 py-1 mx-1 text-left text-[12px] leading-none tracking-widest text-text-faint hover:text-text-muted hover:bg-surface-raised rounded-md cursor-pointer transition-colors"
    >
      ...
    </button>
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


/** Tooltip for the parked dot: what exactly the session is waiting on. */
function parkedTitle(session: Session): string {
  const parts: string[] = [];
  if (session.has_background_tasks) parts.push('Background job running');
  if (session.pending_wakeup_at) {
    const at = parseTimestamp(session.pending_wakeup_at);
    parts.push(`Scheduled wake-up at ${at.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`);
  }
  return parts.join(' · ') || 'Waiting on pending work';
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

  // Error state: solid red. Ranked above "parked" on purpose — a failed turn
  // must not be masked by the background work it left behind.
  if (session.status === 'error') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-hue-red" />;
  }

  // Parked: no turn in flight, but a wake-up is scheduled or a background job
  // is still running. Same pulsing dot as running (the session isn't done),
  // in violet — the one hue no other indicator uses — so "working right now"
  // stays distinguishable from "will pick itself back up".
  if (isParked(session)) {
    return (
      <span className="relative flex h-2 w-2 shrink-0" title={parkedTitle(session)}>
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-violet-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-violet-500" />
      </span>
    );
  }

  // Stopped: solid yellow
  if (session.status === 'stopped') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-hue-yellow" />;
  }

  // Idle / created / active-but-not-running: no indicator (reduces noise)
  return null;
}


/**
 * One feed row plus its nested subtree: renders the row via SessionItem, then —
 * when it has children and is expanded — recurses for each child at depth+1.
 * Depth is unbounded; the client draws whatever hierarchy the server returned.
 */
function SessionTree({
  session, depth, childrenByParent, expandedParents, onToggleExpand,
  activeSession, activeIsRunning, dnd,
  onDelete, onRename, onToggleStar, onArchive, onRemoveParent, onSelect,
}: {
  session: Session;
  depth: number;
  childrenByParent: Map<string, Session[]>;
  expandedParents: Set<string>;
  onToggleExpand: (id: string) => void;
  activeSession: string;
  activeIsRunning: boolean;
  dnd: RowDnd;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => Promise<void>;
  onToggleStar: (id: string) => Promise<void>;
  onArchive: (id: string) => Promise<void>;
  onRemoveParent: (id: string) => void;
  onSelect?: () => void;
}) {
  const kids = childrenByParent.get(session.id);
  const hasChildren = !!kids && kids.length > 0;
  const expanded = expandedParents.has(session.id);
  return (
    <>
      <SessionItem
        session={session}
        showUnread
        isActive={session.id === activeSession}
        isRunning={session.id === activeSession ? activeIsRunning : !!session.is_running}
        depth={depth}
        hasChildren={hasChildren}
        childCount={kids ? kids.length : 0}
        expanded={expanded}
        onToggleExpand={onToggleExpand}
        onDelete={onDelete}
        onRename={onRename}
        onToggleStar={onToggleStar}
        onArchive={onArchive}
        onRemoveParent={onRemoveParent}
        onSelect={onSelect}
        draggable
        dnd={dnd}
      />
      {hasChildren && expanded && kids!.map(k => (
        <SessionTree
          key={k.id}
          session={k}
          depth={depth + 1}
          childrenByParent={childrenByParent}
          expandedParents={expandedParents}
          onToggleExpand={onToggleExpand}
          activeSession={activeSession}
          activeIsRunning={activeIsRunning}
          dnd={dnd}
          onDelete={onDelete}
          onRename={onRename}
          onToggleStar={onToggleStar}
          onArchive={onArchive}
          onRemoveParent={onRemoveParent}
          onSelect={onSelect}
        />
      ))}
    </>
  );
}


function SessionItem({ session, isActive, isRunning, onDelete, onRename, onToggleStar, onArchive, onUnarchive, onStarArchived, archived, onSelect, showDate, showUnread = false,
  depth = 0, hasChildren = false, childCount = 0, expanded = false, onToggleExpand, onRemoveParent, draggable = false, dnd }: {
  session: Session;
  isActive: boolean;
  isRunning: boolean;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => Promise<void>;
  onToggleStar: (id: string) => Promise<void>;
  onArchive: (id: string) => Promise<void>;
  onUnarchive?: (id: string) => Promise<void>;
  onStarArchived?: (id: string) => Promise<void>;
  archived?: boolean;
  /** Fired when the row itself is opened (not its menu) — drawer mode uses it to close. */
  onSelect?: () => void;
  showDate?: boolean;
  /** Feed-only: light up an "unread" marker when updated since last opened. */
  showUnread?: boolean;
  /** Nesting: depth (0 = top level) indents the row; a parent shows a
      chevron in place of its icon that toggles its children. */
  depth?: number;
  hasChildren?: boolean;
  /** Direct-child count, shown as a badge when the parent is collapsed. */
  childCount?: number;
  expanded?: boolean;
  onToggleExpand?: (id: string) => void;
  onRemoveParent?: (id: string) => void;
  /** Drag-to-nest: only feed rows are draggable; search/archived rows aren't. */
  draggable?: boolean;
  dnd?: RowDnd;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState('');
  const menuRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  // Unsent draft for this chat (hidden on the active one — its text is in the box).
  const hasDraft = useChatStore(s => !!(s.drafts[session.id] || '').trim());
  // Unread = updated since you last opened it (client-only, see readStorage).
  // Never for the open session or a running/parked one; feed-scoped via
  // showUnread.
  const lastSeen = useChatStore(s => s.reads[session.id]);
  const readsBaseline = useChatStore(s => s.readsBaseline);
  const isUnread = showUnread && !isActive && !isRunning && !isParked(session)
    && parseTimestamp(session.updated_at).getTime() > Math.max(lastSeen ?? 0, readsBaseline);

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
          className="flex-1 min-w-0 text-sm border-b border-text-faint"
        />
      </div>
    );
  }

  const isDragSource = dnd?.draggingId === session.id;
  const isDropTarget = !!dnd && dnd.dragOverId === session.id
    && !!dnd.draggingId && dnd.draggingId !== session.id;

  return (
    <Link
      to={`/chat/${session.id}`}
      onClick={onSelect}
      draggable={draggable || undefined}
      onDragStart={draggable && dnd ? (e) => {
        // Override the browser's default <a href> drag with our session id.
        e.dataTransfer.setData('text/plain', session.id);
        e.dataTransfer.effectAllowed = 'move';
        dnd.onDragStart(session.id);
      } : undefined}
      onDragEnd={draggable && dnd ? () => dnd.onDragEnd() : undefined}
      onDragOver={dnd ? (e) => {
        if (!dnd.draggingId || dnd.draggingId === session.id) return;
        e.preventDefault();            // allow the drop
        e.dataTransfer.dropEffect = 'move';
        dnd.onDragOver(session.id);
      } : undefined}
      onDragLeave={dnd ? () => dnd.onDragLeave(session.id) : undefined}
      onDrop={dnd ? (e) => { e.preventDefault(); e.stopPropagation(); dnd.onDrop(session.id); } : undefined}
      style={depth ? { paddingLeft: 12 + depth * 16 } : undefined}
      className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-sm transition-colors no-underline
        ${isActive
          ? 'bg-accent/10 text-text'
          : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
        }${isDropTarget ? ' ring-2 ring-inset ring-accent bg-accent/10' : ''}${isDragSource ? ' opacity-50' : ''}`}
    >
      {hasChildren ? (
        <button
          type="button"
          onClick={(e) => { e.preventDefault(); e.stopPropagation(); onToggleExpand?.(session.id); }}
          className="shrink-0 -ml-0.5 p-0.5 rounded text-text-faint hover:text-text-muted hover:bg-surface-hover cursor-pointer"
          title={expanded ? 'Collapse children' : 'Expand children'}
        >
          {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        </button>
      ) : session.review_loop ? (
        <Repeat size={13} className={`shrink-0 ${reviewLoopIconTone(session.review_loop.status)}`} />
      ) : isImplementSession(session) ? (
        <Hammer size={13} className="shrink-0 text-hue-violet/60" />
      ) : (
        <MessageSquare size={13} className="shrink-0 opacity-50" />
      )}
      <div className="flex-1 min-w-0">
        <div className={`truncate text-sm leading-tight${isUnread ? ' font-semibold text-text' : ''}`}>{cleanTitle(session)}</div>
      </div>

      {/* Collapsed parent: badge the hidden direct-child count (mirrors GroupHeader). */}
      {hasChildren && !expanded && childCount > 0 && (
        <span
          className="shrink-0 text-[10px] text-text-faint/60 tabular-nums"
          title={`${childCount} nested session${childCount !== 1 ? 's' : ''}`}
        >
          {childCount}
        </span>
      )}

      {/* Unread marker: updated since you last opened it (client-only). */}
      {isUnread && (
        <span title="Unread — updated since you last opened it" className="shrink-0 flex items-center">
          <span className="h-2 w-2 rounded-full bg-accent" />
        </span>
      )}

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
              <Star size={13} className="fill-hue-yellow" />
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
                if (archived) onStarArchived?.(session.id);
                else onToggleStar(session.id);
                setMenuOpen(false);
              }}
              className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
            >
              <Star size={14} className={session.starred ? 'text-hue-yellow fill-hue-yellow' : ''} />
              {archived ? 'Star' : session.starred ? 'Unstar' : 'Star'}
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
            {archived ? (
              <Button
                variant="subtle"
                size="sm"
                fullWidth
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setMenuOpen(false);
                  onUnarchive?.(session.id);
                }}
                className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
              >
                <ArchiveRestore size={14} />
                Unarchive
              </Button>
            ) : (
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
            )}
            {!archived && session.parent_session_id && onRemoveParent && (
              <Button
                variant="subtle"
                size="sm"
                fullWidth
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setMenuOpen(false);
                  onRemoveParent(session.id);
                }}
                className="justify-start gap-2.5 px-3 py-1.5 rounded-none text-left"
              >
                <Unlink size={14} />
                Remove from parent
              </Button>
            )}
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
