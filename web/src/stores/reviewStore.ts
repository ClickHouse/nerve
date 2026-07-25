import { create } from 'zustand';
import { api } from '../api/client';
import type { FileDiff } from '../types/chat';
import type { Review, ReviewChangedFile, ReviewRepo } from '../types/review';

export interface CommentAnchor {
  file_path: string;
  side: 'new' | 'old';
  line_start: number | null;
  line_end: number | null;
  anchor_snippet: string | null;
}

// Session-scoped code-review state: the worktrees attached to the *active*
// session (each is a code_reviews row with target_session_id = session), their
// git diffs, and line-anchored comment threads. Rendered inside the Chat page's
// file-changes panel. Comments route back into this same session.
interface ReviewState {
  sessionId: string | null;
  attached: Review[];               // reviews (attached worktrees) for this session
  repos: ReviewRepo[];              // for the attach picker
  reposLoaded: boolean;
  activeReview: Review | null;      // selected attached worktree (full, with threads)
  changed: ReviewChangedFile[];     // changed files of the active worktree vs base
  path: string | null;
  mode: 'diff' | 'file';
  diff: FileDiff | null;
  fileContent: { content: string | null; binary: boolean; too_large: boolean } | null;
  loadingAttached: boolean;
  loadingChanged: boolean;
  loadingDiff: boolean;
  busy: boolean;
  error: string | null;

  setSession: (sessionId: string) => Promise<void>;
  loadAttached: () => Promise<void>;
  loadRepos: () => Promise<void>;
  attach: (worktree: string, branch: string | null, base: string) => Promise<void>;
  detach: (reviewId: string) => Promise<void>;
  selectReview: (reviewId: string) => Promise<void>;
  closeReview: () => void;
  selectFile: (path: string) => Promise<void>;
  openFullFile: (path: string) => Promise<void>;
  backToFiles: () => void;
  refreshActive: () => Promise<void>;
  addComment: (anchor: CommentAnchor, body: string) => Promise<void>;
  reply: (threadId: string, body: string) => Promise<void>;
  resolve: (threadId: string) => Promise<void>;
  clearError: () => void;
}

function errMsg(e: unknown, fallback: string): string {
  const raw = e instanceof Error ? e.message : String(e);
  const m = raw.match(/^\d+:\s*(.*)$/s);
  const body = m ? m[1] : raw;
  try {
    const parsed = JSON.parse(body);
    if (parsed && typeof parsed.detail === 'string') return parsed.detail;
  } catch { /* not JSON */ }
  return body || fallback;
}

export const useReviewStore = create<ReviewState>((set, get) => ({
  sessionId: null,
  attached: [],
  repos: [],
  reposLoaded: false,
  activeReview: null,
  changed: [],
  path: null,
  mode: 'diff',
  diff: null,
  fileContent: null,
  loadingAttached: false,
  loadingChanged: false,
  loadingDiff: false,
  busy: false,
  error: null,

  setSession: async (sessionId) => {
    if (get().sessionId === sessionId) return;
    set({
      sessionId, attached: [], activeReview: null, changed: [],
      path: null, diff: null, fileContent: null, mode: 'diff', error: null,
    });
    await get().loadAttached();
  },

  loadAttached: async () => {
    const sessionId = get().sessionId;
    if (!sessionId) return;
    set({ loadingAttached: true });
    try {
      const { reviews } = await api.listReviews({ session: sessionId });
      set({ attached: reviews, loadingAttached: false });
    } catch (e) {
      set({ loadingAttached: false, error: errMsg(e, 'Failed to load attached worktrees') });
    }
  },

  loadRepos: async () => {
    if (get().reposLoaded) return;
    try {
      const { repos } = await api.reviewRepos();
      set({ repos, reposLoaded: true });
    } catch (e) {
      set({ error: errMsg(e, 'Failed to load repos') });
    }
  },

  attach: async (worktree, branch, base) => {
    const sessionId = get().sessionId;
    if (!sessionId) return;
    set({ busy: true, error: null });
    try {
      const review = await api.createReview({
        worktree, branch, base_ref: base || 'HEAD',
        target_session_id: sessionId, created_by: 'human',
        title: branch || worktree,
      });
      await get().loadAttached();
      await get().selectReview(review.id);
    } catch (e) {
      set({ error: errMsg(e, 'Failed to attach worktree') });
    } finally {
      set({ busy: false });
    }
  },

  detach: async (reviewId) => {
    set({ busy: true });
    try {
      await api.deleteReview(reviewId);
      if (get().activeReview?.id === reviewId) {
        set({ activeReview: null, changed: [], path: null, diff: null, fileContent: null });
      }
      await get().loadAttached();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to detach') });
    } finally {
      set({ busy: false });
    }
  },

  selectReview: async (reviewId) => {
    set({ activeReview: null, changed: [], path: null, diff: null, fileContent: null, loadingChanged: true });
    try {
      const full = await api.getReview(reviewId);
      set({ activeReview: full });
      try {
        const { files } = await api.reviewChanged(full.worktree, full.base_ref);
        set({ changed: files, loadingChanged: false });
      } catch (e) {
        set({ loadingChanged: false, error: errMsg(e, 'Failed to list changes') });
      }
    } catch (e) {
      set({ loadingChanged: false, error: errMsg(e, 'Failed to open worktree') });
    }
  },

  closeReview: () => set({
    activeReview: null, changed: [], path: null, diff: null, fileContent: null,
  }),

  selectFile: async (path) => {
    const r = get().activeReview;
    if (!r) return;
    set({ path, mode: 'diff', diff: null, fileContent: null, loadingDiff: true });
    try {
      const diff = await api.reviewDiff(r.worktree, path, r.base_ref);
      set({ diff, loadingDiff: false });
    } catch (e) {
      set({ loadingDiff: false, error: errMsg(e, 'Failed to load diff') });
    }
  },

  openFullFile: async (path) => {
    const r = get().activeReview;
    if (!r) return;
    set({ path, mode: 'file', diff: null, fileContent: null, loadingDiff: true });
    try {
      const fileContent = await api.reviewFile(r.worktree, path);
      set({ fileContent, loadingDiff: false });
    } catch (e) {
      set({ loadingDiff: false, error: errMsg(e, 'Failed to load file') });
    }
  },

  backToFiles: () => set({ path: null, diff: null, fileContent: null }),

  refreshActive: async () => {
    const r = get().activeReview;
    if (!r) return;
    try {
      const full = await api.getReview(r.id);
      set({ activeReview: full });
    } catch { /* ignore */ }
    await get().loadAttached();  // refresh open-thread counts on the list
  },

  addComment: async (anchor, body) => {
    const r = get().activeReview;
    if (!r) return;
    set({ busy: true, error: null });
    try {
      await api.addReviewThread(r.id, {
        file_path: anchor.file_path, side: anchor.side,
        line_start: anchor.line_start, line_end: anchor.line_end,
        anchor_snippet: anchor.anchor_snippet, body, author: 'human',
      });
      await get().refreshActive();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to add comment') });
    } finally {
      set({ busy: false });
    }
  },

  reply: async (threadId, body) => {
    const r = get().activeReview;
    if (!r) return;
    set({ busy: true, error: null });
    try {
      await api.addReviewComment(r.id, threadId, { body, author: 'human' });
      await get().refreshActive();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to reply') });
    } finally {
      set({ busy: false });
    }
  },

  resolve: async (threadId) => {
    const r = get().activeReview;
    if (!r) return;
    set({ busy: true });
    try {
      await api.resolveReviewThread(r.id, threadId);
      await get().refreshActive();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to resolve') });
    } finally {
      set({ busy: false });
    }
  },

  clearError: () => set({ error: null }),
}));
