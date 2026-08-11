import { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { Plus, X, MessageSquare, ChevronRight, ChevronDown, Bot, Loader2, Search, Hammer, MoreHorizontal, Star, Pencil, Trash2, Archive, ArchiveRestore, Repeat } from 'lucide-react';
import type { Session, AgentStatus } from '../../types/chat';
import { groupByDate, parseTimestamp, loadCollapsedGroups, saveCollapsedGroups } from '../../utils/dateGroups';
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
  // Archived group: collapsed by default and NOT persisted (mirrors System),
  // so every reload starts collapsed and fetches nothing until expanded.
  const [archivedExpanded, setArchivedExpanded] = useState(false);
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

  const { searchResults, searchLoading, searchSessions, clearSearch, renameSession, toggleStar, archiveSession, virtualSession, discardVirtualSession, sidebarWidth, setSidebarWidth, sessionsHasMore, loadMoreSessions, archivedSessions, archivedCount, archivedLoading, archivedHasMore, loadArchivedSessions, clearArchivedSessions, unarchiveSession, starArchivedSession, systemSessions, systemCount, systemLoading, systemHasMore, loadSystemSessions, clearSystemSessions } = useChatStore();
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

  // Main feed = whatever the server sent. It already excludes archived rows
  // and system (cron/hook) sources, which load lazily into their own groups.
  // No client-side source whitelist: a source the UI hasn't heard of yet
  // (workflow, a new channel) belongs in the feed rather than nowhere.
  const conversations = sessions;

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
          <button
            type="button"
            onMouseEnter={() => setSearchHovered(true)}
            onMouseLeave={() => setSearchHovered(false)}
            className="absolute left-0 top-1/2 -translate-y-1/2 h-6 pl-1.5 pr-2.5 rounded-full border border-border-subtle flex items-center gap-1 text-[11px] text-text-faint hover:text-text-muted hover:bg-surface-hover cursor-pointer z-10"
          >
            <Search size={11} className="pointer-events-none" />
            <span>Search sessions</span>
          </button>

          {/* New chat pill (hidden under input when open) */}
          <button
            onClick={() => { onCreate(); handleSelect?.(); }}
            title="New chat"
            className="absolute right-0 top-1/2 -translate-y-1/2 h-6 pl-1.5 pr-2.5 rounded-full border border-border-subtle flex items-center gap-1 text-[11px] text-text-faint hover:text-text-muted hover:bg-surface-hover cursor-pointer"
          >
            <Plus size={11} />
            <span>New chat</span>
          </button>

          {searchMounted && (
            <>
              <input
                id="nerve-sidebar-search"
                ref={inputRef}
                type="text"
                value={localQuery}
                onChange={e => handleSearchChange(e.target.value)}
                onFocus={() => setSearchFocused(true)}
                onBlur={() => setSearchFocused(false)}
                onMouseEnter={() => setSearchHovered(true)}
                onMouseLeave={() => setSearchHovered(false)}
                placeholder="Search sessions..."
                className={`absolute inset-0 w-full h-full bg-surface-raised border border-border rounded-md text-[12px] text-text-secondary placeholder-text-faint pl-7 pr-7 outline-none focus:border-text-faint transition-all duration-200 ease-out z-20 ${
                  searchVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'
                }`}
              />
              {isSearching && (
                <button
                  onClick={() => { setLocalQuery(''); clearSearch(); }}
                  className={`absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 text-text-faint hover:text-text-muted cursor-pointer transition-opacity duration-200 z-30 ${
                    searchVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'
                  }`}
                >
                  <X size={12} />
                </button>
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
              <div className="flex items-center gap-2 px-3 py-3 text-[11px] text-text-faint">
                <Loader2 size={11} className="animate-spin" />
                Searching...
              </div>
            )}
            {searchResults && (
              <>
                <div className="px-3 py-1.5 text-[10px] text-text-faint">
                  {searchResults.length} result{searchResults.length !== 1 ? 's' : ''}
                  {searchLoading && <Loader2 size={9} className="inline ml-1.5 animate-spin" />}
                </div>
                {searchResults.length === 0 ? (
                  <div className="px-3 py-2 text-[11px] text-text-faint">No matching sessions</div>
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
                  <div className="truncate text-[13px] italic">New chat</div>
                </div>
                {virtualSession.id === activeSession && activeIsRunning && (
                  <Loader2 size={12} className="shrink-0 text-accent animate-spin" />
                )}
                <button
                  onClick={(e) => { e.preventDefault(); e.stopPropagation(); discardVirtualSession(); }}
                  className="p-0.5 text-text-faint hover:text-text-muted opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer shrink-0"
                  title="Discard new chat"
                >
                  <X size={13} />
                </button>
              </Link>
            )}

            {/* Pinned running sessions */}
            {pinnedRunning.length > 0 && (
              <div>
                <GroupHeader
                  label="Running"
                  count={pinnedRunning.length}
                  collapsed={collapsedGroups.has('Running')}
                  tone="text-emerald-600/70"
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
                  tone="text-yellow-600/70"
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
              <div className="px-3 py-2 text-[11px] text-text-faint">No conversations yet</div>
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

            {/* Feed page window exhausted — never truncate silently. */}
            {sessionsHasMore && <MoreRow onClick={loadMoreSessions} />}

            {/* System sessions (cron/hook) — lazy: nothing is fetched until the
                group is expanded, and collapsing drops the rows again, so the
                next expand repeats the identical request. */}
            {systemCount > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <button
                  onClick={() => {
                    const next = !systemExpanded;
                    setSystemExpanded(next);
                    if (next) loadSystemSessions();
                    else clearSystemSessions();
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 w-full text-left cursor-pointer hover:bg-surface-raised transition-colors"
                >
                  {systemExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Bot size={10} className="text-text-faint" />
                  <span className="text-[10px] uppercase tracking-wider text-text-faint font-medium">
                    System ({systemCount})
                  </span>
                </button>

                {systemExpanded && (
                  <>
                    {systemLoading && systemSessions === null && (
                      <div className="flex items-center gap-2 px-3 py-2 text-[11px] text-text-faint">
                        <Loader2 size={11} className="animate-spin" />
                        Loading...
                      </div>
                    )}
                    {systemSessions !== null && systemSessions.length === 0 && (
                      <div className="px-3 py-2 text-[11px] text-text-faint">No system sessions</div>
                    )}
                    {systemSessions !== null && systemSessions.map((s) => (
                      <Link
                        key={s.id}
                        to={`/chat/${s.id}`}
                        onClick={handleSelect}
                        className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-[12px] transition-colors no-underline
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

            {/* Archived sessions — lazy, mirror of System: fetched on expand,
                dropped on collapse. Rendered last, collapsed by default. */}
            {archivedCount > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <button
                  onClick={() => {
                    const next = !archivedExpanded;
                    setArchivedExpanded(next);
                    if (next) loadArchivedSessions();
                    else clearArchivedSessions();
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 w-full text-left cursor-pointer hover:bg-surface-raised transition-colors"
                >
                  {archivedExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Archive size={10} className="text-text-faint" />
                  <span className="text-[10px] uppercase tracking-wider text-text-faint font-medium">
                    Archived ({archivedCount})
                  </span>
                </button>

                {archivedExpanded && (
                  <>
                    {archivedLoading && archivedSessions === null && (
                      <div className="flex items-center gap-2 px-3 py-2 text-[11px] text-text-faint">
                        <Loader2 size={11} className="animate-spin" />
                        Loading...
                      </div>
                    )}
                    {archivedSessions !== null && archivedSessions.length === 0 && (
                      <div className="px-3 py-2 text-[11px] text-text-faint">No archived sessions</div>
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
      className="w-full px-3 py-1 mx-1 text-left text-[12px] leading-none tracking-widest text-text-faint hover:text-text-muted hover:bg-surface-raised rounded-md cursor-pointer transition-colors"
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
    <button
      onClick={onToggle}
      className="flex items-center gap-1 px-3 pt-2 pb-0.5 w-full text-left cursor-pointer hover:bg-surface-raised/60 transition-colors"
    >
      {collapsed
        ? <ChevronRight size={10} className="shrink-0 text-text-faint" />
        : <ChevronDown size={10} className="shrink-0 text-text-faint" />
      }
      <span className={`text-[10px] font-medium ${tone ?? 'text-text-faint'}`}>{label}</span>
      {collapsed && count > 0 && (
        <span className="text-[10px] text-text-faint/60 tabular-nums">{count}</span>
      )}
    </button>
  );
}


/** Sidebar icon tint for review-loop observer sessions, by loop status. */
function reviewLoopIconTone(status: string): string {
  switch (status) {
    case 'passed': return 'text-emerald-500/80';
    case 'awaiting_user': return 'text-orange-400/90';
    case 'failed':
    case 'killed': return 'text-red-400/70';
    default: return 'text-emerald-400/80';  // pending / implementing / verifying
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
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500" />
      </span>
    );
  }

  // Review loop parked on a decision: pulsing orange dot — same "needs you"
  // urgency class as awaiting_input.
  if (session.review_loop?.status === 'awaiting_user') {
    return (
      <span className="relative flex h-2 w-2 shrink-0" title="Review loop needs your decision">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-orange-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-orange-500" />
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
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
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
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
      </span>
    );
  }

  // Error state: solid red
  if (session.status === 'error') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-red-500" />;
  }

  // Stopped: solid yellow
  if (session.status === 'stopped') {
    return <span className="inline-flex rounded-full h-1.5 w-1.5 shrink-0 bg-yellow-500" />;
  }

  // Idle / created / active-but-not-running: no indicator (reduces noise)
  return null;
}


function SessionItem({ session, isActive, isRunning, onDelete, onRename, onToggleStar, onArchive, onUnarchive, onStarArchived, archived, onSelect, showDate }: {
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
        <input
          ref={inputRef}
          value={renameValue}
          onChange={e => setRenameValue(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') handleRenameSubmit();
            if (e.key === 'Escape') setRenaming(false);
          }}
          onBlur={handleRenameSubmit}
          className="flex-1 min-w-0 bg-transparent text-[13px] text-text outline-none border-b border-text-faint"
        />
      </div>
    );
  }

  return (
    <Link
      to={`/chat/${session.id}`}
      onClick={onSelect}
      className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-sm transition-colors no-underline
        ${isActive
          ? 'bg-accent/10 text-text'
          : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
        }`}
    >
      {session.review_loop
        ? <Repeat size={13} className={`shrink-0 ${reviewLoopIconTone(session.review_loop.status)}`} />
        : isImplementSession(session)
        ? <Hammer size={13} className="shrink-0 text-hue-violet/60" />
        : <MessageSquare size={13} className="shrink-0 opacity-50" />
      }
      <div className="flex-1 min-w-0">
        <div className="truncate text-[13px]">{cleanTitle(session)}</div>
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
        <span className="shrink-0 text-[10px] text-text-faint tabular-nums">
          {formatShortDate(session.updated_at)}
        </span>
      )}

      {/* Menu trigger: starred → show star, on hover → three dots; unstarred → three dots on hover */}
      <div className="relative shrink-0" ref={menuRef}>
        <button
          onClick={(e) => { e.preventDefault(); e.stopPropagation(); setMenuOpen(!menuOpen); }}
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
            <button
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                if (archived) onStarArchived?.(session.id);
                else onToggleStar(session.id);
                setMenuOpen(false);
              }}
              className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-text-secondary hover:bg-border-subtle cursor-pointer transition-colors"
            >
              <Star size={14} className={session.starred ? 'text-hue-yellow fill-hue-yellow' : ''} />
              {archived ? 'Star' : session.starred ? 'Unstar' : 'Star'}
            </button>
            <button
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setRenameValue(cleanTitle(session));
                setRenaming(true);
                setMenuOpen(false);
              }}
              className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-text-secondary hover:bg-border-subtle cursor-pointer transition-colors"
            >
              <Pencil size={14} />
              Rename
            </button>
            {archived ? (
              <button
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setMenuOpen(false);
                  onUnarchive?.(session.id);
                }}
                className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-text-secondary hover:bg-border-subtle cursor-pointer transition-colors"
              >
                <ArchiveRestore size={14} />
                Unarchive
              </button>
            ) : (
              <button
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setMenuOpen(false);
                  onArchive(session.id);
                }}
                className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-text-secondary hover:bg-border-subtle cursor-pointer transition-colors"
              >
                <Archive size={14} />
                Archive
              </button>
            )}
            <div className="border-t border-border my-1" />
            <button
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setMenuOpen(false);
                onDelete(session.id);
              }}
              className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-hue-red hover:bg-border-subtle cursor-pointer transition-colors"
            >
              <Trash2 size={14} />
              Delete
            </button>
          </div>
        )}
      </div>
    </Link>
  );
}
