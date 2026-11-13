import { useState, useRef, useEffect, useLayoutEffect, useCallback, type KeyboardEvent, type ClipboardEvent, type DragEvent, type MouseEvent as ReactMouseEvent } from 'react';
import { ArrowUp, X, Plus, Trash2, Sparkles, HelpCircle, StickyNote, Paperclip, FileText, Loader2, Repeat, MoreHorizontal, Clock, ChevronRight } from '../ui/icons';
import { Button, IconButton, TextField } from '../ui';
import { useChatStore, EMPTY_REVIEW_LOOP, registerComposerFlush, daybreakPrograms, offersDaybreak } from '../../stores/chatStore';
import type { QuoteAction, QuoteEntry } from '../../stores/chatStore';
import { useAgentStore } from '../../stores/agentStore';
import { useIsTouch } from '../../hooks/useMediaQuery';
import { dockLine } from '../../stores/helpers/agentSelectors';
import { api } from '../../api/client';
import { randomUUID } from '../../utils/uuid';
import { findSessionById } from '../../utils/findSession';
import { FOCUS_COMPOSER_COMBO, formatCombo } from '../../utils/keyboard';
import { PromptRewriteCard } from './PromptRewriteCard';
import { DaybreakToggle } from './DaybreakToggle';
import { ModelSelector } from './ModelSelector';
import { ReviewLoopPanel } from './ReviewLoopPanel';
import { QueuedMessages } from './agents/QueuedMessages';
import { StopSplitButton } from './agents/StopSplitButton';

const ACTION_CONFIG: Record<QuoteAction, { icon: typeof Plus; label: string; color: string; placeholder: string }> = {
  add:      { icon: Plus,       label: 'Add',     color: 'var(--theme-accent)',     placeholder: 'Instructions...' },
  remove:   { icon: Trash2,     label: 'Remove',  color: 'var(--theme-hue-red)',    placeholder: 'Instructions...' },
  improve:  { icon: Sparkles,   label: 'Improve', color: 'var(--theme-hue-purple)', placeholder: 'Instructions...' },
  question: { icon: HelpCircle, label: 'Ask',     color: 'var(--theme-hue-amber)',  placeholder: 'What do you want to know?' },
  note:     { icon: StickyNote, label: 'Note',    color: 'var(--theme-text-muted)', placeholder: 'Your note...' },
};

// Actions that auto-focus the instruction input (need user input)
const FOCUS_ACTIONS = new Set<QuoteAction>(['add', 'question', 'note']);

const FOCUS_HINT = `${formatCombo(FOCUS_COMPOSER_COMBO)} to focus`;

// The composer's controls sit on its own surface (`bg-surface-raised`), where
// the ghost hover colour is that same surface and would not show. The hover
// goes one step lighter, and the icon is `text-muted` so it stays visible at
// rest.
const ROW_ICON = 'rounded-lg text-text-muted hover:text-text hover:bg-surface-hover';

// "Run later" submenu — exactly these four, in this order. The three timed
// delays schedule a one-shot wakeup; "none" ("Just accept it") saves the
// prompt for the user to run manually.
const RUN_LATER_OPTIONS: Array<{ delay: string; label: string }> = [
  { delay: '30m', label: 'Run in 30 mins' },
  { delay: '1h', label: 'Run in 1 hour' },
  { delay: '24h', label: 'Run in 24 hours' },
  { delay: 'none', label: 'Just accept it' },
];

// Prompt rewrite — refine the first message of a new chat before sending.
const REWRITE_PREF_KEY = 'nerve_prompt_rewrite';
const REWRITE_MIN_CHARS = 20;    // shorter prompts are sent as-is
const REWRITE_MAX_CHARS = 6000;  // matches the backend cap

type RewriteFlowState =
  | { status: 'idle' }
  | { status: 'loading'; original: string }
  | { status: 'ready'; original: string; rewritten: string; model: string }
  | { status: 'error'; original: string; message: string };

interface AttachmentFile {
  id: string;
  file: File;
  preview?: string;
  uploading: boolean;
  uploadedId?: string;
  uploadedMeta?: { filename: string; media_type: string; file_type: string };
  error?: string;
}

export function ChatInput({ onSend, onStop, isStreaming, disabled, seed = null }: {
  onSend: (message: string, fileIds?: string[], imageBlocks?: Array<{ url: string; filename: string; media_type: string }>) => void;
  /** Stop the turn; the store's stop is async (it answers with what happened). */
  onStop: () => void | Promise<void>;
  /** A turn is running: Send becomes Queue next to the split Stop. Typing and
   *  sending stay possible — a message sent now runs after the turn. */
  isStreaming: boolean;
  disabled?: boolean;
  /**
   * Text to put in the composer once, for the first session that has no
   * draft of its own — the setup wizard's suggested first message. Never
   * overwrites a draft somebody typed.
   */
  seed?: string | null;
}) {
  const [input, setInput] = useState('');
  const seedRef = useRef<string | null>(seed);
  const [attachments, setAttachments] = useState<AttachmentFile[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const [textFocused, setTextFocused] = useState(false);
  // "Run later" kebab menu (mirrors the SessionSidebar three-dots menu).
  const [menuOpen, setMenuOpen] = useState(false);
  const [runLaterOpen, setRunLaterOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const lastInstructionRef = useRef<HTMLInputElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const dragCountRef = useRef(0);
  // Shell-style prompt history (ArrowUp/ArrowDown recall previously-sent
  // prompts of this chat). -1 = not navigating (live draft); 0 = most recent.
  const historyIndexRef = useRef(-1);
  const historyStashRef = useRef('');

  const quotes = useChatStore(s => s.quotes);
  const removeQuote = useChatStore(s => s.removeQuote);
  const updateQuoteInstruction = useChatStore(s => s.updateQuoteInstruction);
  const clearQuotes = useChatStore(s => s.clearQuotes);
  const setDraft = useChatStore(s => s.setDraft);
  const activeSession = useChatStore(s => s.activeSession);
  const ensureRealSession = useChatStore(s => s.ensureRealSession);
  const runLater = useChatStore(s => s.runLater);
  const isNewChat = useChatStore(s => s.messages.length === 0);

  // Persist the composer draft, but NOT on every keystroke. Writing to the
  // global store per character re-renders every store subscriber (the message
  // list included), which stalls typing on long chats. Debounce the write and
  // flush it on blur / send so no draft is lost.
  const draftTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const cancelDraftFlush = useCallback(() => {
    if (draftTimerRef.current !== null) {
      clearTimeout(draftTimerRef.current);
      draftTimerRef.current = null;
    }
  }, []);
  const scheduleDraft = useCallback((sessionId: string, text: string) => {
    cancelDraftFlush();
    draftTimerRef.current = setTimeout(() => {
      draftTimerRef.current = null;
      setDraft(sessionId, text);
    }, 400);
  }, [cancelDraftFlush, setDraft]);
  // Clear any pending draft write when the composer unmounts.
  useEffect(() => cancelDraftFlush, [cancelDraftFlush]);
  // Re-entry leaves the page at once, so it writes the text without the delay.
  useEffect(() => registerComposerFlush(() => {
    cancelDraftFlush();
    setDraft(activeSession, input);
  }), [activeSession, input, cancelDraftFlush, setDraft]);

  // Backend selector renders only while the chat is virtual (unsent):
  // the choice binds at server-side session creation and is sticky.
  const isVirtualChat = useChatStore(
    s => s.virtualSession !== null && s.virtualSession.id === s.activeSession,
  );
  const newChatBackend = useChatStore(s => s.newChatBackend);
  const newChatReviewLoop = useChatStore(s => s.newChatReviewLoop);
  const setNewChatReviewLoop = useChatStore(s => s.setNewChatReviewLoop);
  const backendDefault = useChatStore(s => s.backendDefault);
  const chosenBackend = newChatBackend ?? backendDefault;
  const sessions = useChatStore(s => s.sessions);
  const archivedSessions = useChatStore(s => s.archivedSessions);
  const systemSessions = useChatStore(s => s.systemSessions);
  // The active session row may live in the feed or a lazy archived/system group.
  const activeSessionRow = findSessionById(activeSession, sessions, archivedSessions, systemSessions);
  const activeBackend = isVirtualChat
    ? (chosenBackend ?? 'claude')
    : (activeSessionRow?.backend ?? 'claude');

  // ── Model picker (per-chat) ──
  // A virtual chat's pick lives in newChatModels until the session is
  // created; a real session's model IS its row (sessions[].model), so the
  // picker always shows — and only ever changes — the current chat.
  const availableModels = useChatStore(s => s.availableModels);
  const newChatModels = useChatStore(s => s.newChatModels);
  const modelDefaults = useChatStore(s => s.modelDefaults);
  const setNewChatModel = useChatStore(s => s.setNewChatModel);
  const setSessionModel = useChatStore(s => s.setSessionModel);
  const loadModels = useChatStore(s => s.loadModels);
  const backendOptions = useChatStore(s => s.backendOptions);
  const setNewChatBackend = useChatStore(s => s.setNewChatBackend);
  const modelsDefault = modelDefaults[activeBackend] ?? null;
  const currentModel = isVirtualChat
    ? (newChatModels[activeBackend] ?? modelsDefault)
    : (activeSessionRow?.model ?? modelsDefault);
  // What the model menu can switch to. A new chat can take a model of any
  // available backend, and the backend comes with it. A chat that exists
  // keeps its backend.
  const pickableModels = availableModels.filter(m => m.backend === activeBackend
    || (isVirtualChat && backendOptions.some(o => o.id === m.backend && o.available !== false)));

  // ── Daybreak switch (per-chat, Codex only) ──
  // Like the model: a virtual chat's switch lives in newChatDaybreak until
  // the session is created; a real session's switch IS its row.
  const newChatDaybreak = useChatStore(s => s.newChatDaybreak);
  const setNewChatDaybreak = useChatStore(s => s.setNewChatDaybreak);
  const setSessionDaybreak = useChatStore(s => s.setSessionDaybreak);
  const daybreakModels = Object.keys(daybreakPrograms(backendOptions));
  const daybreakOffered = offersDaybreak(backendOptions, currentModel);
  const daybreakOn = isVirtualChat
    ? newChatDaybreak && daybreakOffered
    : activeSessionRow?.codex_daybreak === true;
  const toggleDaybreak = (enabled: boolean) => {
    if (isVirtualChat) setNewChatDaybreak(enabled);
    else void setSessionDaybreak(activeSession, enabled);
  };

  const pickModel = (backend: string, id: string) => {
    if (isVirtualChat) {
      if (backend !== activeBackend) setNewChatBackend(backend === backendDefault ? null : backend);
      setNewChatModel(backend, id === (modelDefaults[backend] ?? null) ? null : id);
      // A new chat keeps Daybreak only on a model that offers it.
      if (!offersDaybreak(backendOptions, id)) setNewChatDaybreak(false);
    } else if (backend === activeBackend) {
      void setSessionModel(activeSession, id);
    }
    setTimeout(() => textareaRef.current?.focus(), 0);
  };
  // Switching the model recreates the runtime, which would end every live
  // agent of this session (the server refuses it too).
  const liveAgents = useAgentStore(s => (s.sessionId === activeSession ? s.summary.live : 0));

  const [prevQuoteCount, setPrevQuoteCount] = useState(0);

  // ── Prompt rewrite ──
  // Server-side availability (config master switch) + per-user toggle.
  const [rewriteAvailable, setRewriteAvailable] = useState(false);
  const [rewriteEnabled, setRewriteEnabled] = useState(
    () => localStorage.getItem(REWRITE_PREF_KEY) === '1',
  );
  const [rewrite, setRewrite] = useState<RewriteFlowState>({ status: 'idle' });
  const rewriteAbortRef = useRef<AbortController | null>(null);
  const rewriteActive = rewrite.status !== 'idle';

  useEffect(() => {
    api.getPromptRewriteStatus()
      .then(s => setRewriteAvailable(s.enabled))
      .catch(() => setRewriteAvailable(false));
  }, []);

  // Load selectable models once — the picker renders when the active backend
  // offers more than one model (the configured Claude list, the Codex
  // app-server's models, local Ollama models).
  useEffect(() => { loadModels(); }, [loadModels]);

  useEffect(() => {
    localStorage.setItem(REWRITE_PREF_KEY, rewriteEnabled ? '1' : '0');
  }, [rewriteEnabled]);

  const cancelRewrite = useCallback((refocus = true) => {
    rewriteAbortRef.current?.abort();
    rewriteAbortRef.current = null;
    setRewrite({ status: 'idle' });
    if (refocus) setTimeout(() => textareaRef.current?.focus(), 0);
  }, []);

  // Discard any pending rewrite preview when switching sessions.
  useEffect(() => {
    cancelRewrite(false);
  }, [activeSession, cancelRewrite]);

  // Load this chat's saved draft when switching sessions — an empty box for a
  // chat with no draft, the unfinished text for one that has it. Reads via
  // getState so a draft mutation (the keystrokes below) doesn't reload mid-edit.
  // Focus the composer on every switch so you can start typing right away.
  useEffect(() => {
    let text = useChatStore.getState().drafts[activeSession] ?? '';
    // The one-time seed takes the first session that has no draft, and is
    // persisted as that session's draft so a reload keeps it.
    if (!text && activeSession && seedRef.current) {
      text = seedRef.current;
      seedRef.current = null;
      setDraft(activeSession, text);
    }
    setInput(text);
    historyIndexRef.current = -1;
    historyStashRef.current = '';
    if (activeSession) setTimeout(() => textareaRef.current?.focus(), 0);
  }, [activeSession, setDraft]);

  // The latest composer text, for the agent-store subscription below.
  const inputRef = useRef(input);
  useEffect(() => { inputRef.current = input; }, [input]);

  // Whether the composer holds text, for what must not write into text of
  // the person's own (a view of an MCP App).
  useEffect(() => {
    useAgentStore.getState().setComposerHasText(input.trim().length > 0);
  }, [input]);
  useEffect(() => () => useAgentStore.getState().setComposerHasText(false), []);

  // Text other surfaces put into the composer (Follow up…, Relaunch…, a
  // cancelled queued message): applied once per request — it replaces an
  // empty composer and goes on a new paragraph after existing text — then
  // the composer takes focus with the caret at the end.
  //
  // An `offer` is text the person neither wrote nor asked for: it goes in
  // only when the composer is empty, and the focus stays where it is. Put
  // next to their own words, or under their caret, it would go out with
  // their next Enter.
  useEffect(() => {
    let handled = useAgentStore.getState().composerInsert?.nonce ?? null;
    return useAgentStore.subscribe((state) => {
      const insert = state.composerInsert;
      if (!insert || insert.nonce === handled) return;
      handled = insert.nonce;
      if (insert.mode === 'offer') {
        const empty = !inputRef.current.trim();
        if (empty) {
          inputRef.current = insert.text;
          setInput(insert.text);
          historyIndexRef.current = -1;
          scheduleDraft(activeSession, insert.text);
        }
        insert.taken?.(empty);
        return;
      }
      const current = inputRef.current;
      const next = insert.mode === 'replace' || !current.trim()
        ? insert.text
        : `${current.replace(/\s+$/, '')}\n\n${insert.text}`;
      inputRef.current = next;
      setInput(next);
      historyIndexRef.current = -1;
      scheduleDraft(activeSession, next);
      setTimeout(() => {
        const el = textareaRef.current;
        if (!el) return;
        el.focus();
        el.setSelectionRange(el.value.length, el.value.length);
      }, 0);
    });
  }, [activeSession, scheduleDraft]);

  // Keep the textarea height in sync with its content (typing + draft load).
  useLayoutEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 200) + 'px';
  }, [input]);

  // Esc anywhere dismisses the preview (cancels an in-flight rewrite).
  useEffect(() => {
    if (!rewriteActive) return;
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === 'Escape') cancelRewrite();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [rewriteActive, cancelRewrite]);

  // Auto-focus instruction input when a new quote is added
  useEffect(() => {
    if (quotes.length > prevQuoteCount && quotes.length > 0) {
      const last = quotes[quotes.length - 1];
      if (FOCUS_ACTIONS.has(last.action)) {
        setTimeout(() => lastInstructionRef.current?.focus(), 0);
      }
    }
    setPrevQuoteCount(quotes.length);
  }, [quotes.length, prevQuoteCount, quotes]);

  // Auto-focus textarea when active session changes (new chat or session switch)
  useEffect(() => {
    if (activeSession && !disabled && textareaRef.current) {
      textareaRef.current.focus();
    }
  }, [activeSession, disabled]);

  // Cleanup object URLs on unmount
  useEffect(() => {
    return () => {
      attachments.forEach(a => { if (a.preview) URL.revokeObjectURL(a.preview); });
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const addFiles = useCallback(async (files: File[]) => {
    // Uploading materializes the session (ensureRealSession) — which would
    // BIND AND START a filled review-loop config prematurely. Block uploads
    // while the loop form is dirty; start the loop (or clear the form) first.
    const rl = useChatStore.getState().newChatReviewLoop;
    const virtual = useChatStore.getState().virtualSession;
    if (virtual && virtual.id === useChatStore.getState().activeSession
        && rl && (rl.goal.trim() || rl.verifier.trim())) {
      return;
    }
    const newAttachments: AttachmentFile[] = files.map(file => ({
      id: randomUUID(),
      file,
      preview: file.type.startsWith('image/') ? URL.createObjectURL(file) : undefined,
      uploading: true,
    }));

    setAttachments(prev => [...prev, ...newAttachments]);

    // Upload all files
    try {
      // A brand-new chat is only a client-side "virtual" session until its
      // first message. The upload endpoint requires a persisted session, so
      // materialize it first and upload against the real server id — otherwise
      // the temp id 404s ("Session not found").
      const sid = await ensureRealSession();
      const result = await api.uploadFiles(files, sid);
      setAttachments(prev => prev.map(a => {
        const idx = newAttachments.findIndex(n => n.id === a.id);
        if (idx >= 0 && result.files[idx]) {
          const meta = result.files[idx];
          return {
            ...a,
            uploading: false,
            uploadedId: meta.id,
            uploadedMeta: { filename: meta.filename, media_type: meta.media_type, file_type: meta.file_type },
          };
        }
        return a;
      }));
    } catch (err) {
      setAttachments(prev => prev.map(a => {
        if (newAttachments.some(n => n.id === a.id)) {
          return { ...a, uploading: false, error: String(err) };
        }
        return a;
      }));
    }
  }, [ensureRealSession]);

  const removeAttachment = useCallback((id: string) => {
    setAttachments(prev => {
      const removed = prev.find(a => a.id === id);
      if (removed?.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter(a => a.id !== id);
    });
  }, []);

  const composeMessage = (): string => {
    const parts: string[] = [];
    const ACTION_LABELS: Record<QuoteAction, string> = {
      add: 'Add', remove: 'Remove', improve: 'Improve', question: 'Question', note: 'Note',
    };

    for (const q of quotes) {
      const blockquote = q.text.split('\n').map(l => `> ${l}`).join('\n');
      const instr = q.instruction.trim();
      const label = ACTION_LABELS[q.action];
      parts.push(instr ? `${blockquote}\n${label}: ${instr}` : blockquote);
    }

    if (input.trim()) {
      parts.push(input.trim());
    }

    return parts.join('\n\n');
  };

  const allUploaded = attachments.length === 0 || attachments.every(a => !a.uploading);
  const hasContent = input.trim() || quotes.length > 0 || attachments.some(a => a.uploadedId);
  // Not gated on `isStreaming`: while a turn runs, sending queues the message
  // on the server and it runs next.
  const canSend = !disabled && !rewriteActive && !!hasContent && allUploaded;
  // An on-screen keyboard has no Shift+Enter, so on a touch device Enter types
  // a newline and the Send button sends. Cmd/Ctrl+Enter sends everywhere, for
  // a tablet with a hardware keyboard.
  const enterSends = !useIsTouch();

  /** Actually dispatch a message (with current attachments) and reset the composer. */
  const dispatchSend = (message: string) => {
    const fileIds = attachments.filter(a => a.uploadedId).map(a => a.uploadedId!);
    const imageBlocks = attachments
      .filter(a => a.uploadedId && a.uploadedMeta?.file_type === 'image')
      .map(a => ({
        url: `/api/files/uploads/${a.uploadedId}`,
        filename: a.uploadedMeta!.filename,
        media_type: a.uploadedMeta!.media_type,
      }));

    onSend(message, fileIds.length > 0 ? fileIds : undefined, imageBlocks.length > 0 ? imageBlocks : undefined);
    cancelDraftFlush();
    setInput('');
    historyIndexRef.current = -1;
    historyStashRef.current = '';
    setDraft(activeSession, '');
    clearQuotes();
    // Clean up previews
    attachments.forEach(a => { if (a.preview) URL.revokeObjectURL(a.preview); });
    setAttachments([]);
    rewriteAbortRef.current?.abort();
    rewriteAbortRef.current = null;
    setRewrite({ status: 'idle' });
  };

  /** Request a rewrite and open the preview card. Sends nothing by itself. */
  const startRewrite = async (message: string) => {
    rewriteAbortRef.current?.abort();
    const ctrl = new AbortController();
    rewriteAbortRef.current = ctrl;
    setRewrite({ status: 'loading', original: message });
    try {
      const res = await api.rewritePrompt(message, ctrl.signal);
      if (ctrl.signal.aborted) return;
      if (!res.changed) {
        // Model judged the prompt fine as-is — send the original directly.
        dispatchSend(message);
        return;
      }
      setRewrite({
        status: 'ready',
        original: message,
        rewritten: res.rewritten,
        model: res.model,
      });
    } catch (e) {
      if (ctrl.signal.aborted) return;
      setRewrite({
        status: 'error',
        original: message,
        message: e instanceof Error ? e.message : String(e),
      });
    }
  };

  const handleSend = () => {
    const message = composeMessage();
    if (!message && attachments.length === 0) return;

    // First message of a new chat with rewrite on → preview instead of send.
    const shouldRewrite =
      rewriteAvailable && rewriteEnabled && isNewChat && rewrite.status === 'idle' &&
      message.trim().length >= REWRITE_MIN_CHARS && message.length <= REWRITE_MAX_CHARS;
    if (shouldRewrite) {
      void startRewrite(message);
      return;
    }

    dispatchSend(message);
  };

  /** "Stop turn and send": the message queues behind the stopping turn and runs next. */
  const handleStopAndSend = async () => {
    await onStop();
    handleSend();
  };

  // Close the kebab menu on an outside click (mirrors SessionSidebar).
  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
        setRunLaterOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [menuOpen]);

  /** Defer the composed message via a chosen "Run later" option, then clear
   *  the composer exactly as a normal send does. */
  const handleRunLater = (delay: string) => {
    const message = composeMessage();
    if (!message && attachments.length === 0) return;
    const fileIds = attachments.filter(a => a.uploadedId).map(a => a.uploadedId!);
    const imageBlocks = attachments
      .filter(a => a.uploadedId && a.uploadedMeta?.file_type === 'image')
      .map(a => ({
        url: `/api/files/uploads/${a.uploadedId}`,
        filename: a.uploadedMeta!.filename,
        media_type: a.uploadedMeta!.media_type,
      }));
    setMenuOpen(false);
    setRunLaterOpen(false);
    // Fire the deferred create, then clear the composer synchronously — same
    // ordering as dispatchSend so the carried draft ends up empty.
    void runLater(
      message, delay,
      fileIds.length > 0 ? fileIds : undefined,
      imageBlocks.length > 0 ? imageBlocks : undefined,
    ).catch((e) => console.error('Run later failed:', e));
    cancelDraftFlush();
    setInput('');
    historyIndexRef.current = -1;
    historyStashRef.current = '';
    setDraft(activeSession, '');
    clearQuotes();
    attachments.forEach(a => { if (a.preview) URL.revokeObjectURL(a.preview); });
    setAttachments([]);
  };

  // Previously-sent user prompts of this chat, newest first (adjacent dupes dropped).
  const getPromptHistory = (): string[] => {
    const msgs = useChatStore.getState().messages;
    const out: string[] = [];
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i].role !== 'user') continue;
      const text = msgs[i].blocks.find(b => b.type === 'text')?.content || '';
      if (!text.trim()) continue;
      if (out.length > 0 && out[out.length - 1] === text) continue;
      out.push(text);
    }
    return out;
  };

  // Put the caret at the end after a programmatic recall (next tick, once React
  // has committed the new value to the textarea).
  const setCaretToEnd = () => {
    setTimeout(() => {
      const el = textareaRef.current;
      if (el) el.setSelectionRange(el.value.length, el.value.length);
    }, 0);
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    // Never intercept mid-IME-composition (Enter commits the candidate, etc.).
    if (e.nativeEvent.isComposing) return;

    if (e.key === 'Enter' && !e.shiftKey && (enterSends || e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      if (canSend) handleSend();
      return;
    }

    // ↓ in an empty composer (not browsing the prompt history) opens the
    // Agents panel with focus in its list, while the session has agents.
    if (e.key === 'ArrowDown' && !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey
        && historyIndexRef.current === -1 && input.length === 0) {
      const agents = useAgentStore.getState();
      const visible = agents.sessionId === activeSession
        && dockLine(agents.agents, agents.seen, Date.now(), agents.capabilities.quiet_after_minutes).visible;
      if (visible) {
        e.preventDefault();
        useChatStore.getState().openAgentsPanel(true);
        return;
      }
    }

    // Shell-style history recall — only when the arrow wouldn't just move the
    // caret within existing multi-line text: Up on the first line, Down on the
    // last line, no active selection, no modifier held.
    if ((e.key === 'ArrowUp' || e.key === 'ArrowDown') &&
        !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey) {
      const el = textareaRef.current;
      if (!el || el.selectionStart !== el.selectionEnd) return;
      const caret = el.selectionStart;

      if (e.key === 'ArrowUp') {
        if (input.slice(0, caret).includes('\n')) return;   // not on first line
        const history = getPromptHistory();
        if (history.length === 0) return;
        let idx = historyIndexRef.current;
        if (idx === -1) historyStashRef.current = input;     // stash live draft
        if (idx >= history.length - 1) return;               // already oldest
        idx += 1;
        historyIndexRef.current = idx;
        e.preventDefault();
        setInput(history[idx]);
        setCaretToEnd();
      } else {
        if (historyIndexRef.current === -1) return;          // not navigating
        if (input.slice(caret).includes('\n')) return;       // not on last line
        e.preventDefault();
        const history = getPromptHistory();
        const idx = historyIndexRef.current;
        if (idx <= 0) {
          historyIndexRef.current = -1;
          setInput(historyStashRef.current);                 // restore live draft
        } else {
          historyIndexRef.current = idx - 1;
          setInput(history[idx - 1] ?? '');
        }
        setCaretToEnd();
      }
    }
  };

  const handlePaste = (e: ClipboardEvent) => {
    const items = e.clipboardData?.items;
    if (!items) return;

    const files: File[] = [];
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === 'file') {
        const file = item.getAsFile();
        if (file) files.push(file);
      }
    }

    if (files.length > 0) {
      e.preventDefault();
      addFiles(files);
    }
  };

  const handleDragEnter = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragging(true);
    }
  };

  const handleDragLeave = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current--;
    if (dragCountRef.current === 0) {
      setIsDragging(false);
    }
  };

  const handleDragOver = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCountRef.current = 0;
    setIsDragging(false);

    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      addFiles(files);
    }
  };

  // The box reads as one text field: a press on its padding or between the
  // controls puts the caret in the message text. A press on a control or a
  // card does what that element does.
  const focusFromBlank = (e: ReactMouseEvent<HTMLDivElement>) => {
    if (e.target instanceof HTMLElement && e.target.hasAttribute('data-composer-blank')) {
      e.preventDefault();
      textareaRef.current?.focus();
    }
  };

  return (
    <div
      className="bg-bg shrink-0 relative"
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* Drag overlay */}
      {isDragging && (
        <div className="absolute inset-0 z-50 bg-accent/10 border-2 border-dashed border-accent rounded-lg flex items-center justify-center">
          <span className="text-accent font-medium text-sm">Drop files here</span>
        </div>
      )}

      {/* Review-loop config panel — new chats only */}
      {isVirtualChat && newChatReviewLoop && (
        <ReviewLoopPanel disabled={disabled || isStreaming || rewriteActive} />
      )}

      {/* Prompt rewrite preview */}
      {rewrite.status !== 'idle' && (
        <div className="px-4 pt-3 pb-1">
          <div>
            <PromptRewriteCard
              state={
                rewrite.status === 'loading' ? { status: 'loading' }
                : rewrite.status === 'ready' ? { status: 'ready', rewritten: rewrite.rewritten, model: rewrite.model }
                : { status: 'error', message: rewrite.message }
              }
              original={rewrite.original}
              onApprove={(text) => dispatchSend(text)}
              onSendOriginal={() => dispatchSend(rewrite.original)}
              onDiscard={() => cancelRewrite()}
              onRetry={() => void startRewrite(rewrite.original)}
            />
          </div>
        </div>
      )}

      {/* The composer is one rounded box. From top to bottom it holds the
          quote cards, the attachment previews, the message text and one row
          of controls. The text has the full width of the box at all viewport
          widths, and the controls take no width from it.

          The composer does not carry `max-w-[var(--chat-width)]`, which
          everything on the transcript side does. A reading column stays
          narrow for prose. The composer is a control surface: a narrow cap
          leaves the control row less room for the model picker and the send
          controls, so the composer fills the pane. */}
      <div className="px-4 pt-2 pb-3">
        {/* Messages waiting for the running turn, and this tab's unconfirmed sends. */}
        <QueuedMessages />
        {/* `relative` is the anchor for the run-later popup. The popup aligns
            to the right edge of the box and opens above it, so it stays in
            the viewport. */}
        <div
          data-composer-blank=""
          onMouseDown={focusFromBlank}
          className="relative cursor-text rounded-2xl border border-border bg-surface-raised transition-colors has-[textarea:focus]:border-accent/50"
        >
          {quotes.length > 0 && (
            <div data-composer-blank="" className="px-3 pt-3 space-y-2">
              {quotes.map((quote, idx) => (
                <QuoteCard
                  key={quote.id}
                  quote={quote}
                  instructionRef={idx === quotes.length - 1 ? lastInstructionRef : undefined}
                  onRemove={() => removeQuote(quote.id)}
                  onUpdateInstruction={(v) => updateQuoteInstruction(quote.id, v)}
                  onSend={canSend ? handleSend : undefined}
                />
              ))}
            </div>
          )}

          {attachments.length > 0 && (
            <div data-composer-blank="" className="px-3 pt-3 flex gap-2 flex-wrap">
              {attachments.map(a => (
                <AttachmentPreview key={a.id} attachment={a} onRemove={() => removeAttachment(a.id)} />
              ))}
            </div>
          )}

          <div className="relative">
            <textarea
              id="nerve-chat-input"
              ref={textareaRef}
              value={input}
              onChange={(e) => { const v = e.target.value; setInput(v); historyIndexRef.current = -1; scheduleDraft(activeSession, v); }}
              onFocus={() => setTextFocused(true)}
              onBlur={() => { setTextFocused(false); cancelDraftFlush(); setDraft(activeSession, input); }}
              onKeyDown={handleKeyDown}
              onPaste={handlePaste}
              // The label of the Enter key on an on-screen keyboard. It agrees
              // with what Enter does.
              enterKeyHint={enterSends ? 'send' : 'enter'}
              placeholder={
                quotes.length > 0 ? 'Add context (optional)...'
                : attachments.length > 0 ? 'Add a message (optional)...'
                : isStreaming ? 'Queue a message — it runs after this turn...'
                : rewriteAvailable && rewriteEnabled && isNewChat ? 'Send a message — it will be refined before sending...'
                : 'Send a message...'
              }
              rows={1}
              disabled={disabled || rewriteActive}
              // `text-sm` matches the other inputs in the app (`FIELD_SIZES.md`
              // is `text-sm` too). At `text-base` (16px) the text looks too
              // large next to the transcript, and a line holds less of it.
              // The minimum height gives an empty box room to type into; the
              // layout effect above grows it with the text, up to 200px.
              className="block w-full min-h-14 md:min-h-[4.5rem] px-4 pt-3 pb-1 bg-transparent text-sm text-text outline-none resize-none disabled:opacity-50 placeholder:text-text-faint"
            />
            {/* Shows only while the box is empty and has no focus, so it
                cannot cover text. Not on a phone, which has no shortcut keys.
                It lets pointer presses through to the text below it. */}
            {!textFocused && !input && !disabled && (
              <span
                aria-hidden="true"
                className="hidden md:block absolute top-3 right-4 text-xs text-text-muted pointer-events-none select-none"
              >
                {FOCUS_HINT}
              </span>
            )}
          </div>

          {/* One row of controls: the settings for this chat, then the
              actions on the message at the right edge. On a phone a new chat
              has more controls than one line holds. The row then wraps, and
              the actions move to the next line as one group. */}
          <div data-composer-blank="" className="flex flex-wrap items-center gap-1 px-2 pb-2">
            {/* PER-CHAT model menu. A pick here re-points only this chat:
                virtual chats bind it at creation, real sessions PATCH their
                own row, never a global preference. In a new chat a model of
                the other backend also picks that backend (Claude or Codex),
                which binds at session creation and is fixed after that. It
                picks the OBSERVER session's backend; loop legs use the panel
                config. A new chat shows it whenever there is more than one
                backend, so that a backend that is unavailable says why. */}
            {(pickableModels.length > 1 || (isVirtualChat && backendOptions.length > 1)) && (
              <ModelSelector
                models={availableModels}
                backends={backendOptions}
                activeBackend={activeBackend}
                currentModel={currentModel}
                backendFixed={!isVirtualChat}
                onPick={pickModel}
                disabled={disabled || isStreaming || rewriteActive || liveAgents > 0}
                title={liveAgents > 0
                  ? `Switching model restarts the runtime and would stop ${liveAgents} running agent${liveAgents === 1 ? '' : 's'}`
                  : 'Model for this chat (other chats keep theirs)'}
              />
            )}
            {/* PER-CHAT Daybreak switch, for a Codex chat whose account has
                Daybreak on some model. Like the model: a virtual chat binds
                it at creation, a real session PATCHes its own row, and it
                applies from the chat's next message. A chat that has it on
                always shows it, so that it can be turned off. */}
            {activeBackend === 'codex' && (daybreakModels.length > 0 || daybreakOn) && (
              <DaybreakToggle
                enabled={daybreakOn}
                offered={daybreakOffered}
                models={daybreakModels}
                onToggle={toggleDaybreak}
                disabled={disabled || isStreaming || rewriteActive}
              />
            )}
            {/* Prompt rewrite toggle, only on a fresh chat, where it applies */}
            {rewriteAvailable && isNewChat && (
              <button
                onClick={() => setRewriteEnabled(v => !v)}
                disabled={disabled || isStreaming || rewriteActive}
                className={`w-8 h-8 rounded-lg flex items-center justify-center cursor-pointer transition-all shrink-0 disabled:opacity-30 ${
                  rewriteEnabled
                    ? 'text-hue-purple bg-hue-purple/10 hover:bg-hue-purple/15 ring-1 ring-inset ring-hue-purple/25'
                    : 'text-text-muted hover:text-text-secondary hover:bg-surface-hover'
                }`}
                title={rewriteEnabled
                  ? 'Prompt rewrite on — your first message will be refined for approval before sending'
                  : 'Prompt rewrite off — click to refine your first message with AI before sending'}
              >
                <Sparkles size={16} />
              </button>
            )}
            {/* Review-loop toggle, new chats only. Opens the Goal/Verifier
                panel; the loop binds at session creation. */}
            {isVirtualChat && (
              <button
                onClick={() => setNewChatReviewLoop(newChatReviewLoop ? null : { ...EMPTY_REVIEW_LOOP })}
                disabled={disabled || isStreaming || rewriteActive}
                className={`w-8 h-8 rounded-lg flex items-center justify-center cursor-pointer transition-all shrink-0 disabled:opacity-30 ${
                  newChatReviewLoop
                    ? 'text-hue-emerald bg-hue-emerald/10 hover:bg-hue-emerald/15 ring-1 ring-inset ring-hue-emerald/25'
                    : 'text-text-muted hover:text-text-secondary hover:bg-surface-hover'
                }`}
                title={newChatReviewLoop
                  ? 'Review loop on — configure Goal + Verifier criteria; an implementer/verifier agent pair iterates until the criteria pass'
                  : 'Review loop — set a Goal and Verifier criteria; an implementer/verifier agent pair iterates until the criteria pass'}
              >
                <Repeat size={16} />
              </button>
            )}

            {/* Actions on the message: more options, attach, send. */}
            <div data-composer-blank="" className="ml-auto flex shrink-0 items-center gap-1">
              {/* Run-later kebab. Its submenu defers the composed prompt into
                  a new session without spending tokens now. It opens upward,
                  because the composer sits at the bottom of the page. */}
              <div className="shrink-0" ref={menuRef}>
                <IconButton
                  label="More options"
                  size="sm"
                  onClick={() => { setMenuOpen(v => !v); setRunLaterOpen(false); }}
                  disabled={disabled || isStreaming || rewriteActive}
                  // `aria-expanded` only. `aria-haspopup="menu"` would promise the
                  // ARIA menu pattern — `menu`/`menuitem` roles, focus moving into
                  // the popup, arrow-key navigation, Escape to close — but the popup
                  // below is a `div` of ordinary buttons. It is a disclosure, so it
                  // says so rather than announcing a keyboard model that is not
                  // there.
                  aria-expanded={menuOpen}
                  className={ROW_ICON}
                >
                  <MoreHorizontal size={16} />
                </IconButton>
                {menuOpen && (
                  // A fixed width, not a minimum. The popup is anchored to the
                  // composer, so it has ~500px to shrink-to-fit against and would
                  // take all of it for four short labels. `w-max` does not help:
                  // Chrome's max-content for these `w-full` children lands in the
                  // same place.
                  <div className="absolute right-0 bottom-full mb-1 z-50 w-[170px] bg-surface-raised border border-border-subtle rounded-lg shadow-xl py-1">
                    <Button
                      variant="subtle"
                      size="sm"
                      fullWidth
                      onClick={() => setRunLaterOpen(v => !v)}
                      aria-expanded={runLaterOpen}
                      className="justify-between gap-2.5 px-3 py-1.5 rounded-none text-left"
                    >
                      <span className="flex items-center gap-2.5"><Clock size={14} /> Run later</span>
                      <ChevronRight size={14} className={`transition-transform ${runLaterOpen ? 'rotate-90' : ''}`} />
                    </Button>
                    {runLaterOpen && (
                      <div className="border-t border-border mt-1 pt-1">
                        {RUN_LATER_OPTIONS.map(opt => (
                          <Button
                            key={opt.delay}
                            variant="subtle"
                            size="sm"
                            fullWidth
                            onClick={() => handleRunLater(opt.delay)}
                            disabled={!canSend}
                            className="justify-start px-3 py-1.5 pl-8 rounded-none text-left"
                          >
                            {opt.label}
                          </Button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* File attach button */}
              <IconButton
                size="sm"
                onClick={() => fileInputRef.current?.click()}
                disabled={disabled || isStreaming || rewriteActive
                  || (isVirtualChat && !!(newChatReviewLoop && (newChatReviewLoop.goal.trim() || newChatReviewLoop.verifier.trim())))}
                label={isVirtualChat && newChatReviewLoop && (newChatReviewLoop.goal.trim() || newChatReviewLoop.verifier.trim())
                  ? 'Attaching files would start the review loop — start it or clear the form first'
                  : 'Attach files'}
                className={ROW_ICON}
              >
                <Paperclip size={16} />
              </IconButton>

              {isStreaming ? (
                <>
                  <IconButton label="Queue message" variant="primary" size="sm" onClick={handleSend} disabled={!canSend} className="rounded-lg">
                    <Clock size={16} />
                  </IconButton>
                  <StopSplitButton
                    onStopTurn={() => { void onStop(); }}
                    hasContent={canSend}
                    onStopAndSend={() => { void handleStopAndSend(); }}
                  />
                </>
              ) : (
                <IconButton label="Send" variant="primary" size="sm" onClick={handleSend} disabled={!canSend} className="rounded-lg">
                  <ArrowUp size={16} />
                </IconButton>
              )}
            </div>
          </div>
        </div>
      </div>

      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          const files = Array.from(e.target.files || []);
          if (files.length > 0) addFiles(files);
          e.target.value = '';
        }}
      />
    </div>
  );
}


function AttachmentPreview({ attachment, onRemove }: { attachment: AttachmentFile; onRemove: () => void }) {
  const isImage = attachment.file.type.startsWith('image/');

  return (
    <div className="relative group rounded-lg border border-border bg-surface overflow-hidden flex items-center gap-2">
      {isImage && attachment.preview ? (
        <img src={attachment.preview} alt={attachment.file.name} className="w-16 h-16 object-cover" />
      ) : (
        <div className="w-16 h-16 flex items-center justify-center bg-surface-raised">
          <FileText size={20} className="text-text-muted" />
        </div>
      )}
      <div className="pr-7 py-1.5 min-w-0">
        <div className="text-xs text-text-secondary truncate max-w-[120px]">{attachment.file.name}</div>
        <div className="text-xs text-text-muted">
          {attachment.uploading ? (
            <span className="flex items-center gap-1"><Loader2 size={10} className="animate-spin" /> Uploading...</span>
          ) : attachment.error ? (
            <span className="text-error">Failed</span>
          ) : (
            <span className="text-success">Ready</span>
          )}
        </div>
      </div>
      <IconButton
        label={`Remove ${attachment.file.name}`}
        size="xs"
        onClick={onRemove}
        className="absolute top-1 right-1 rounded-full bg-bg/80 opacity-0 group-hover:opacity-100 transition-opacity"
      >
        <X size={12} />
      </IconButton>
    </div>
  );
}


function QuoteCard({ quote, instructionRef, onRemove, onUpdateInstruction, onSend }: {
  quote: QuoteEntry;
  instructionRef?: React.RefObject<HTMLInputElement | null>;
  onRemove: () => void;
  onUpdateInstruction: (v: string) => void;
  onSend?: () => void;
}) {
  const config = ACTION_CONFIG[quote.action];
  const Icon = config.icon;
  const truncated = quote.text.length > 120 ? quote.text.slice(0, 120) + '…' : quote.text;

  return (
    <div
      className="quote-card rounded-lg bg-surface border border-border overflow-hidden"
      style={{ borderLeftColor: config.color, borderLeftWidth: '3px' }}
    >
      <div className="flex items-start gap-2 px-3 py-2">
        {/* Icon + label */}
        <div className="flex items-center gap-1.5 shrink-0 pt-0.5">
          <Icon size={13} style={{ color: config.color }} />
          <span className="text-xs font-medium uppercase tracking-wider" style={{ color: config.color }}>
            {config.label}
          </span>
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          <div className="text-xs text-text-muted leading-relaxed line-clamp-2">{truncated}</div>
          <TextField
            bare
            ref={instructionRef}
            value={quote.instruction}
            onChange={(e) => onUpdateInstruction(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter' && onSend) { e.preventDefault(); onSend(); } }}
            enterKeyHint="send"
            placeholder={config.placeholder}
            className="mt-1.5 py-0.5 text-sm text-text-secondary border-b border-border transition-colors"
          />
        </div>

        {/* Remove */}
        <IconButton label="Remove this quote" size="xs" onClick={onRemove} className="shrink-0 mt-0.5">
          <X size={14} />
        </IconButton>
      </div>
    </div>
  );
}
