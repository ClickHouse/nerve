import { useState, useMemo, useRef, useEffect, useCallback, useLayoutEffect } from 'react';
import { Plus, X, MessageSquare, ChevronRight, ChevronDown, Bot, Loader2, Search, Hammer, MoreHorizontal, Star, Pencil, Trash2 } from 'lucide-react';
import type { Session, AgentStatus } from '../../types/chat';
import { groupByDate } from '../../utils/dateGroups';
import { useChatStore } from '../../stores/chatStore';

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

export function SessionSidebar({ sessions, activeSession, agentStatus, onSelect, onCreate, onDelete, collapsed }: {
  sessions: Session[];
  activeSession: string;
  agentStatus: AgentStatus;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onDelete: (id: string) => void;
  collapsed?: boolean;
}) {
  const [systemExpanded, setSystemExpanded] = useState(false);
  const [localQuery, setLocalQuery] = useState('');
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const { searchResults, searchLoading, searchSessions, clearSearch, renameSession, toggleStar } = useChatStore();

  const isSearching = localQuery.trim().length > 0;

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
    const convos = sessions.filter(s => s.source === 'web' || s.source === 'telegram' || s.source === 'api');
    const system = sessions.filter(s => s.source === 'cron' || s.source === 'hook');
    return { conversations: convos, systemSessions: system };
  }, [sessions]);

  const activeIsRunning = agentStatus.state !== 'idle';

  // Split running and starred conversations to pin them at the top
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
    return { pinnedRunning: running, pinnedStarred: starred, restConversations: rest };
  }, [conversations, activeSession, activeIsRunning]);

  const groupedConversations = useMemo(() => groupByDate(restConversations), [restConversations]);

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
    <div className={`bg-surface border-r border-border-subtle flex flex-col shrink-0 transition-all duration-200 overflow-hidden ${collapsed ? 'w-0 border-r-0' : 'w-60'}`}>
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2.5 border-b border-border-subtle">
        <span className="text-[10px] uppercase tracking-wider text-text-faint font-medium">Conversations</span>
        <button
          onClick={onCreate}
          className="w-5 h-5 rounded flex items-center justify-center text-text-faint hover:text-text-muted hover:bg-surface-hover cursor-pointer"
          title="New session"
        >
          <Plus size={12} />
        </button>
      </div>

      {/* Search */}
      <div className="px-2 py-1.5">
        <div className="relative">
          <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-text-faint" />
          <input
            ref={inputRef}
            type="text"
            value={localQuery}
            onChange={e => handleSearchChange(e.target.value)}
            placeholder="Search sessions..."
            className="w-full bg-surface-raised border border-border rounded-md text-[12px] text-text-secondary placeholder-text-faint pl-7 pr-7 py-1.5 outline-none focus:border-text-faint transition-colors"
          />
          {isSearching && (
            <button
              onClick={() => { setLocalQuery(''); clearSearch(); }}
              className="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 text-text-faint hover:text-text-muted cursor-pointer"
            >
              <X size={12} />
            </button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
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
                      onSelect={onSelect}
                      onDelete={onDelete}
                      onRename={renameSession}
                      onToggleStar={toggleStar}
                      showDate
                    />
                  ))
                )}
              </>
            )}
          </div>
        ) : (
          <>
            {/* Pinned running sessions */}
            {pinnedRunning.length > 0 && (
              <div>
                <div className="px-3 pt-2 pb-0.5">
                  <span className="text-[10px] text-emerald-600/70 font-medium">Running</span>
                </div>
                {pinnedRunning.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning
                    onSelect={onSelect}
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                  />
                ))}
              </div>
            )}

            {/* Pinned starred sessions */}
            {pinnedStarred.length > 0 && (
              <div>
                <div className="px-3 pt-2 pb-0.5">
                  <span className="text-[10px] text-yellow-600/70 font-medium">Starred</span>
                </div>
                {pinnedStarred.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning={false}
                    onSelect={onSelect}
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                  />
                ))}
              </div>
            )}

            {/* Normal date-grouped view */}
            {groupedConversations.length === 0 && pinnedRunning.length === 0 && pinnedStarred.length === 0 && (
              <div className="px-3 py-2 text-[11px] text-text-faint">No conversations yet</div>
            )}

            {groupedConversations.map(({ group, items }) => (
              <div key={group}>
                <div className="px-3 pt-2.5 pb-0.5">
                  <span className="text-[10px] text-text-faint font-medium">{group}</span>
                </div>
                {items.map((s) => (
                  <SessionItem
                    key={s.id}
                    session={s}
                    isActive={s.id === activeSession}
                    isRunning={s.id === activeSession ? activeIsRunning : !!s.is_running}
                    onSelect={onSelect}
                    onDelete={onDelete}
                    onRename={renameSession}
                    onToggleStar={toggleStar}
                  />
                ))}
              </div>
            ))}

            {/* System sessions */}
            {systemSessions.length > 0 && (
              <div className="mt-2 border-t border-border-subtle pt-1">
                <button
                  onClick={() => setSystemExpanded(!systemExpanded)}
                  className="flex items-center gap-1.5 px-3 py-1.5 w-full text-left cursor-pointer hover:bg-surface-raised transition-colors"
                >
                  {systemExpanded
                    ? <ChevronDown size={10} className="text-text-faint" />
                    : <ChevronRight size={10} className="text-text-faint" />
                  }
                  <Bot size={10} className="text-text-faint" />
                  <span className="text-[10px] uppercase tracking-wider text-text-faint font-medium">
                    System ({systemSessions.length})
                  </span>
                  {runningSystemCount > 0 && (
                    <span className="ml-auto flex items-center gap-1 text-[10px] text-emerald-500">
                      <span className="relative flex h-1.5 w-1.5">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                        <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-emerald-500" />
                      </span>
                      {runningSystemCount}
                    </span>
                  )}
                </button>

                {systemExpanded && systemSessions.map((s) => (
                  <div
                    key={s.id}
                    onClick={() => onSelect(s.id)}
                    className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-[12px] transition-colors
                      ${s.id === activeSession
                        ? 'bg-[#1f1f2f] text-text-muted'
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
                  </div>
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}


/** Pulsing dot for running sessions, solid dot for other notable states. */
function StatusIndicator({ session, isActive, isRunning }: {
  session: Session;
  isActive: boolean;
  isRunning: boolean;
}) {
  // Active + running: spinner
  if (isActive && isRunning) {
    return <Loader2 size={12} className="shrink-0 text-[#6366f1] animate-spin" />;
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


function SessionItem({ session, isActive, isRunning, onSelect, onDelete, onRename, onToggleStar, showDate }: {
  session: Session;
  isActive: boolean;
  isRunning: boolean;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => Promise<void>;
  onToggleStar: (id: string) => Promise<void>;
  showDate?: boolean;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState('');
  const menuRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

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
    <div
      onClick={() => onSelect(session.id)}
      className={`group flex items-center gap-2 px-3 py-1.5 mx-1 rounded-md cursor-pointer text-sm transition-colors
        ${isActive
          ? 'bg-[#1f1f2f] text-text'
          : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'
        }`}
    >
      {isImplementSession(session)
        ? <Hammer size={13} className="shrink-0 text-violet-400/60" />
        : <MessageSquare size={13} className="shrink-0 opacity-50" />
      }
      <div className="flex-1 min-w-0">
        <div className="truncate text-[13px]">{cleanTitle(session)}</div>
      </div>

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
          onClick={(e) => { e.stopPropagation(); setMenuOpen(!menuOpen); }}
          className={`p-0.5 cursor-pointer transition-opacity ${
            session.starred
              ? 'text-yellow-500 opacity-100 [&>*:first-child]:block [&>*:last-child]:hidden hover:[&>*:first-child]:hidden hover:[&>*:last-child]:block hover:text-text-muted'
              : 'text-border-subtle opacity-0 group-hover:opacity-100 hover:text-text-muted'
          }`}
        >
          {session.starred ? (
            <>
              <Star size={13} className="fill-yellow-500" />
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
                e.stopPropagation();
                onToggleStar(session.id);
                setMenuOpen(false);
              }}
              className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-text-secondary hover:bg-border-subtle cursor-pointer transition-colors"
            >
              <Star size={14} className={session.starred ? 'text-yellow-500 fill-yellow-500' : ''} />
              {session.starred ? 'Unstar' : 'Star'}
            </button>
            <button
              onClick={(e) => {
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
            <div className="border-t border-border my-1" />
            <button
              onClick={(e) => {
                e.stopPropagation();
                setMenuOpen(false);
                onDelete(session.id);
              }}
              className="flex items-center gap-2.5 w-full px-3 py-1.5 text-[13px] text-red-400 hover:bg-border-subtle cursor-pointer transition-colors"
            >
              <Trash2 size={14} />
              Delete
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
